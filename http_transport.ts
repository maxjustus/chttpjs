import { createLz4FrameDecoder } from "./lz4_frame.ts";

/** Content codings ClickHouse flushes per block, so rows arrive as produced. */
export type HttpEncoding = "zstd" | "lz4";

export interface NodeRequestInit {
  method: string;
  headers: Record<string, string>;
  body?: string | Uint8Array | ReadableStream<Uint8Array>;
  signal?: AbortSignal | null;
}

type ByteSource = AsyncIterable<Uint8Array>;

async function* decodeLz4(source: ByteSource): AsyncGenerator<Uint8Array> {
  const decoder = createLz4FrameDecoder();
  for await (const chunk of source) {
    const out = decoder.push(chunk);
    if (out.length > 0) yield out;
  }
}

async function* decodeZstd(source: ByteSource): AsyncGenerator<Uint8Array> {
  const { DecompressStream } = await import("zstd-napi");
  const stream = new DecompressStream();
  const ready: Uint8Array[] = [];
  stream.on("data", (chunk: Buffer) => ready.push(new Uint8Array(chunk)));
  stream.on("error", () => {});

  for await (const chunk of source) {
    await new Promise<void>((resolve, reject) => {
      stream.write(chunk, (err) => (err ? reject(err) : resolve()));
    });
    while (ready.length > 0) yield ready.shift() as Uint8Array;
  }

  // A mid-stream server error leaves the frame unterminated. Every byte already
  // decoded still counts, including the exception trailer, so a failed flush is
  // not fatal - but it must still settle, or the response never ends.
  await new Promise<void>((resolve) => {
    stream.once("error", () => resolve());
    stream.end(() => resolve());
  });
  while (ready.length > 0) yield ready.shift() as Uint8Array;
}

const DECODERS: Record<HttpEncoding, (source: ByteSource) => AsyncGenerator<Uint8Array>> = {
  lz4: decodeLz4,
  zstd: decodeZstd,
};

/**
 * Request over `node:http`, which applies no content decoding of its own.
 *
 * `fetch` decompresses inside its pipeline, so an abrupt server close races the
 * decoder and discards buffered output — including the framed exception trailer
 * that reports why the query failed. Decoding here keeps those bytes.
 */
export async function nodeHttpRequest(
  url: string,
  init: NodeRequestInit,
  encoding: HttpEncoding,
): Promise<Response> {
  const target = new URL(url);
  const isHttps = target.protocol === "https:";
  const { request } = await import(isHttps ? "node:https" : "node:http");

  const incoming = await new Promise<import("node:http").IncomingMessage>((resolve, reject) => {
    const req = request(
      {
        protocol: target.protocol,
        hostname: target.hostname,
        port: target.port || (isHttps ? 443 : 80),
        path: `${target.pathname}${target.search}`,
        method: init.method,
        headers: init.headers,
      },
      resolve,
    );
    req.on("error", reject);
    init.signal?.addEventListener("abort", () =>
      req.destroy(new Error("The operation was aborted")),
    );

    if (init.body instanceof ReadableStream) {
      (async () => {
        for await (const chunk of init.body as ReadableStream<Uint8Array>) req.write(chunk);
        req.end();
      })().catch(reject);
    } else {
      req.end(init.body);
    }
  });

  let truncation: Error | undefined;

  // End the source on a transport failure rather than throwing through it, so
  // the decoder still flushes. Raising here would strand buffered output -
  // exactly the loss that makes `fetch` unusable for this.
  async function* rawBody(): AsyncGenerator<Uint8Array> {
    try {
      for await (const chunk of incoming) yield new Uint8Array(chunk as Buffer);
    } catch (err) {
      truncation = err as Error;
    }
  }

  async function* body(): AsyncGenerator<Uint8Array> {
    // Yield every decoded byte before reporting the failure. The consumer scans
    // for the exception trailer and throws the real server error first.
    yield* DECODERS[encoding](rawBody());
    if (truncation) {
      throw new Error(
        `Server closed the connection mid-response (${truncation.message}) without an ` +
          "exception trailer; the query may have been killed - check the server query log",
      );
    }
  }

  const headers = new Headers();
  for (const [name, value] of Object.entries(incoming.headers)) {
    if (typeof value === "string") headers.set(name, value);
    else if (Array.isArray(value)) for (const v of value) headers.append(name, v);
  }
  // Content-Encoding is dropped: the body this Response carries is already decoded.
  headers.delete("content-encoding");

  return new Response(ReadableStream.from(body()), {
    status: incoming.statusCode ?? 0,
    statusText: incoming.statusMessage ?? "",
    headers,
  });
}
