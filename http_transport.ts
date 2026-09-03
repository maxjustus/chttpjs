import { createLz4FrameDecoder } from "./lz4_frame.ts";

/** Content codings ClickHouse flushes per block, so rows arrive as produced. */
export type HttpEncoding = "zstd" | "lz4";

export interface NodeRequestInit {
  method: string;
  headers: Record<string, string>;
  body?: string | Uint8Array | ReadableStream<Uint8Array>;
  signal?: AbortSignal | null;
}

async function* decodeLz4(source: AsyncIterable<Uint8Array>): AsyncGenerator<Uint8Array> {
  const decoder = createLz4FrameDecoder();
  for await (const chunk of source) {
    const out = decoder.push(chunk);
    if (out.length > 0) yield out;
  }
}

async function* decodeZstd(source: AsyncIterable<Uint8Array>): AsyncGenerator<Uint8Array> {
  const { DecompressStream } = await import("zstd-napi");
  const stream = new DecompressStream();
  const ready: Uint8Array[] = [];
  stream.on("data", (chunk: Buffer) => ready.push(new Uint8Array(chunk)));
  // A failed flush is expected when the server dies mid-frame: the bytes decoded
  // so far carry the exception trailer. Any other error is corruption, recorded
  // here and raised after the drain below so no decoded byte is lost.
  let decodeError: Error | undefined;
  stream.on("error", (err: Error) => (decodeError ??= err));

  for await (const chunk of source) {
    await new Promise<void>((resolve) => stream.write(chunk, () => resolve()));
    while (ready.length > 0) yield ready.shift() as Uint8Array;
  }

  // The end must settle even on an unterminated frame, or the response never
  // ends.
  await new Promise<void>((resolve) => {
    stream.once("error", () => resolve());
    stream.end(() => resolve());
  });
  while (ready.length > 0) yield ready.shift() as Uint8Array;
  if (decodeError) throw decodeError;
}

/**
 * Request over `node:http`, which applies no content decoding of its own.
 *
 * `fetch` decompresses inside its pipeline, so an abrupt server close races the
 * decoder and discards buffered output — including the framed exception trailer
 * that reports why the query failed. Decoding here keeps those bytes.
 */
export async function nodeHttpRequest(url: string, init: NodeRequestInit): Promise<Response> {
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
        // pipeline applies backpressure; req.write ignores it, which would
        // buffer a large body in memory.
        const { pipeline } = await import("node:stream/promises");
        const { Readable } = await import("node:stream");
        await pipeline(
          Readable.fromWeb(init.body as import("node:stream/web").ReadableStream),
          req,
        );
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
      for await (const chunk of incoming) yield chunk as Uint8Array;
    } catch (err) {
      truncation = err as Error;
    }
  }

  // Decode by the coding the server actually used, not the one requested. An
  // early failure (auth, parse) arrives uncompressed, and assuming the request
  // coding would replace ClickHouse's message with a codec error.
  const coding = incoming.headers["content-encoding"];

  function truncationError(): unknown {
    if (init.signal?.aborted) return init.signal.reason;
    return new Error(
      `Server closed the connection mid-response (${truncation!.message}) without an ` +
        "exception trailer; the query may have been killed - check the server query log",
    );
  }

  async function* body(): AsyncGenerator<Uint8Array> {
    // Yield every decoded byte before reporting the failure. The consumer scans
    // for the exception trailer and throws the real server error first.
    try {
      if (coding === "lz4") yield* decodeLz4(rawBody());
      else if (coding === "zstd") yield* decodeZstd(rawBody());
      else yield* rawBody();
    } catch (err) {
      // A decode failure alongside a transport failure is the expected
      // unterminated frame; the transport error says what actually happened.
      if (truncation) throw truncationError();
      throw err;
    }
    if (truncation) throw truncationError();
  }

  const headers = new Headers();
  const raw = incoming.rawHeaders;
  for (let i = 0; i < raw.length; i += 2) headers.append(raw[i]!, raw[i + 1]!);
  // Content-Encoding is dropped only when this Response carries decoded bytes.
  if (coding === "lz4" || coding === "zstd") headers.delete("content-encoding");

  return new Response(ReadableStream.from(body()), {
    status: incoming.statusCode ?? 0,
    statusText: incoming.statusMessage ?? "",
    headers,
  });
}
