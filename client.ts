import {
  init,
  encodeBlock,
  decodeBlock,
  Method,
  type MethodCode,
} from "./compression.ts";

export {
  encodeRowBinary,
  decodeRowBinary,
  streamEncodeRowBinary,
  streamDecodeRowBinary,
  type ColumnDef,
  type DecodeResult,
  type StreamDecodeResult,
  ClickHouseDateTime64,
} from "./formats/rowbinary.ts";

export {
  encodeNative,
  decodeNative,
  streamEncodeNative,
  streamDecodeNative,
  type StreamDecodeNativeResult,
} from "./formats/native/index.ts";

export type Compression = "lz4" | "zstd" | "none";

// AbortSignal.any() added in Node 20+, ES2024
const AbortSignalAny = AbortSignal as typeof AbortSignal & {
  any(signals: AbortSignal[]): AbortSignal;
};

function createSignal(
  signal?: AbortSignal,
  timeout?: number,
): AbortSignal | undefined {
  if (!signal && !timeout) return undefined;
  if (signal && !timeout) return signal;
  if (!signal && timeout) return AbortSignal.timeout(timeout);
  return AbortSignalAny.any([signal!, AbortSignal.timeout(timeout!)]);
}

function compressionToMethod(compression: Compression): MethodCode {
  switch (compression) {
    case "lz4":
      return Method.LZ4;
    case "zstd":
      return Method.ZSTD;
    case "none":
      return Method.None;
  }
}

// Uint8Array helpers
const encoder = new TextEncoder();

function readUInt32LE(arr: Uint8Array, offset: number): number {
  return (
    arr[offset] |
    (arr[offset + 1] << 8) |
    (arr[offset + 2] << 16) |
    ((arr[offset + 3] << 24) >>> 0)
  );
}

interface AuthConfig {
  username?: string;
  password?: string;
}

/**
 * Build a ClickHouse HTTP URL with query parameters.
 * @param params - Query params including ClickHouse settings (max_execution_time, etc.)
 *   See: https://clickhouse.com/docs/en/operations/settings/settings
 */
function buildReqUrl(
  baseUrl: string,
  params: Record<string, string>,
  auth?: AuthConfig,
): URL {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, value);
  });

  if (auth && auth.username) {
    url.searchParams.append("user", auth.username);
    if (auth.password) {
      url.searchParams.append("password", auth.password);
    }
  }

  return url;
}

interface ProgressInfo {
  blocksSent: number;
  bytesCompressed: number;
  bytesUncompressed: number;
  complete?: boolean;
}

interface InsertOptions {
  baseUrl?: string;
  /** Compression method: "lz4" (default), "zstd", or "none" */
  compression?: Compression;
  /** Size in bytes for the compression buffer (default: 1MB) */
  bufferSize?: number;
  /** Byte threshold to trigger compression flush (default: bufferSize - 2048) */
  threshold?: number;
  onProgress?: (progress: ProgressInfo) => void;
  auth?: AuthConfig;
  /** AbortSignal for manual cancellation */
  signal?: AbortSignal;
  /** Request timeout in milliseconds */
  timeout?: number;
}

type InsertData =
  | Uint8Array
  | Uint8Array[]
  | AsyncIterable<Uint8Array>
  | Iterable<Uint8Array>;

async function insert(
  query: string,
  data: InsertData,
  sessionId: string,
  options: InsertOptions = {},
): Promise<string> {
  await init();
  const baseUrl = options.baseUrl || "http://localhost:8123/";
  const {
    compression = "lz4",
    bufferSize = 1024 * 1024,
    threshold = bufferSize - 2048,
    onProgress = null,
  } = options;
  const method = compressionToMethod(compression);

  const params: Record<string, string> = {
    session_id: sessionId,
    query: query,
    decompress: "1",
  };

  // Single Uint8Array - compress and send directly
  if (data instanceof Uint8Array) {
    const compressed = encodeBlock(data, method);
    const url = buildReqUrl(baseUrl, params, options.auth);

    if (onProgress) {
      onProgress({
        blocksSent: 1,
        bytesCompressed: compressed.length,
        bytesUncompressed: data.length,
        complete: true,
      });
    }

    const response = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream", "Connection": "close" },
      body: compressed,
      signal: createSignal(options.signal, options.timeout),
    });

    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Insert failed: ${response.status} - ${body}`);
    }
    return body;
  }

  // Array of Uint8Array - concatenate, compress, send
  if (Array.isArray(data)) {
    const chunks = data as Uint8Array[];
    const totalLen = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const combined = new Uint8Array(totalLen);
    let offset = 0;
    for (const chunk of chunks) {
      combined.set(chunk, offset);
      offset += chunk.length;
    }
    const compressed = encodeBlock(combined, method);
    const url = buildReqUrl(baseUrl, params, options.auth);

    if (onProgress) {
      onProgress({
        blocksSent: 1,
        bytesCompressed: compressed.length,
        bytesUncompressed: totalLen,
        complete: true,
      });
    }

    const response = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/octet-stream", "Connection": "close" },
      body: compressed,
      signal: createSignal(options.signal, options.timeout),
    });

    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Insert failed: ${response.status} - ${body}`);
    }
    return body;
  }

  // Streaming: Iterable<Uint8Array> or AsyncIterable<Uint8Array>
  const url = buildReqUrl(baseUrl, params, options.auth);

  let blocksSent = 0;
  let totalCompressed = 0;
  let totalUncompressed = 0;

  const stream = new ReadableStream({
    async start(controller) {
      try {
        let bufferA = new Uint8Array(bufferSize);
        let bufferB = new Uint8Array(bufferSize);
        let fillBuffer = bufferA;
        let fillLen = 0;
        let flushPromise: Promise<void> | null = null;

        const flush = async (buf: Uint8Array, len: number) => {
          const compressed = encodeBlock(buf.subarray(0, len), method);
          controller.enqueue(compressed);
          blocksSent++;
          totalCompressed += compressed.length;
          totalUncompressed += len;

          if (onProgress) {
            onProgress({
              blocksSent,
              bytesCompressed: compressed.length,
              bytesUncompressed: len,
            });
          }
        };

        for await (const chunk of data as AsyncIterable<Uint8Array>) {
          let chunkOffset = 0;

          while (chunkOffset < chunk.length) {
            const spaceAvailable = fillBuffer.length - fillLen;
            const bytesToCopy = Math.min(
              spaceAvailable,
              chunk.length - chunkOffset,
            );

            fillBuffer.set(
              chunk.subarray(chunkOffset, chunkOffset + bytesToCopy),
              fillLen,
            );
            fillLen += bytesToCopy;
            chunkOffset += bytesToCopy;

            if (fillLen >= threshold) {
              if (flushPromise) await flushPromise;

              const flushBuf = fillBuffer;
              const flushLen = fillLen;
              fillBuffer = fillBuffer === bufferA ? bufferB : bufferA;
              fillLen = 0;

              flushPromise = flush(flushBuf, flushLen);
            }
          }
        }

        if (flushPromise) await flushPromise;

        if (fillLen > 0) {
          await flush(fillBuffer, fillLen);
        }

        if (onProgress) {
          onProgress({
            blocksSent,
            bytesCompressed: totalCompressed,
            bytesUncompressed: totalUncompressed,
            complete: true,
          });
        }

        controller.close();
      } catch (err) {
        controller.error(err);
      }
    },
  });

  const response = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/octet-stream", "Connection": "close" },
    body: stream,
    duplex: "half",
    signal: createSignal(options.signal, options.timeout),
  } as RequestInit);

  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Insert failed: ${response.status} - ${body}`);
  }

  return body;
}

/**
 * Convert objects to JSONEachRow format as Uint8Array chunks.
 * Use with insert() for JSON data.
 */
function streamJsonEachRow(data: Iterable<unknown>): Generator<Uint8Array>;
function streamJsonEachRow(
  data: AsyncIterable<unknown>,
): AsyncGenerator<Uint8Array>;
function streamJsonEachRow(
  data: Iterable<unknown> | AsyncIterable<unknown>,
): Generator<Uint8Array> | AsyncGenerator<Uint8Array> {
  if (Symbol.asyncIterator in data) {
    return (async function*() {
      for await (const row of data) {
        yield encoder.encode(JSON.stringify(row) + "\n");
      }
    })();
  }
  return (function*() {
    for (const row of data as Iterable<unknown>) {
      yield encoder.encode(JSON.stringify(row) + "\n");
    }
  })();
}

/**
 * Convert objects to JSONCompactEachRowWithNames format.
 * First yields column names header, then each row as a JSON array.
 *
 * @param data - Iterable of objects to encode
 * @param columns - Column names (if omitted, extracted from first object's keys)
 *
 * @example
 * const rows = [{ id: 1, name: "foo" }, { id: 2, name: "bar" }];
 * await insert(
 *   "INSERT INTO t FORMAT JSONCompactEachRowWithNames",
 *   streamJsonCompactEachRowWithNames(rows),
 *   sessionId
 * );
 */
function streamJsonCompactEachRowWithNames(
  data: Iterable<Record<string, unknown>>,
  columns?: string[],
): Generator<Uint8Array>;
function streamJsonCompactEachRowWithNames(
  data: AsyncIterable<Record<string, unknown>>,
  columns?: string[],
): AsyncGenerator<Uint8Array>;
function streamJsonCompactEachRowWithNames(
  data:
    | Iterable<Record<string, unknown>>
    | AsyncIterable<Record<string, unknown>>,
  columns?: string[],
): Generator<Uint8Array> | AsyncGenerator<Uint8Array> {
  if (Symbol.asyncIterator in data) {
    return (async function*() {
      let cols = columns;
      for await (const row of data) {
        if (!cols) {
          cols = Object.keys(row);
          yield encoder.encode(JSON.stringify(cols) + "\n");
        } else if (cols === columns) {
          yield encoder.encode(JSON.stringify(cols) + "\n");
        }
        yield encoder.encode(JSON.stringify(cols.map((k) => row[k])) + "\n");
      }
    })();
  }
  return (function*() {
    let cols = columns;
    for (const row of data as Iterable<Record<string, unknown>>) {
      if (!cols) {
        cols = Object.keys(row);
        yield encoder.encode(JSON.stringify(cols) + "\n");
      } else if (cols === columns) {
        yield encoder.encode(JSON.stringify(cols) + "\n");
      }
      yield encoder.encode(JSON.stringify(cols.map((k) => row[k])) + "\n");
    }
  })();
}

/**
 * Parse JSONCompactEachRowWithNames format into objects.
 * First line is column names, subsequent lines are value arrays.
 *
 * @example
 * for await (const row of parseJsonCompactEachRowWithNames(query("SELECT * FROM t FORMAT JSONCompactEachRowWithNames", session, config))) {
 *   console.log(row.id, row.name);
 * }
 */
async function* parseJsonCompactEachRowWithNames<T = Record<string, unknown>>(
  chunks: AsyncIterable<Uint8Array>,
): AsyncGenerator<T> {
  let columns: string[] | null = null;
  for await (const line of streamLines(chunks)) {
    const parsed = JSON.parse(line) as unknown[];
    if (!columns) {
      columns = parsed as string[];
      continue;
    }
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columns.length; i++) {
      obj[columns[i]] = parsed[i];
    }
    yield obj as T;
  }
}

interface QueryOptions {
  baseUrl?: string;
  auth?: AuthConfig;
  /** Compression method for response: "lz4" (default), "zstd", or "none" */
  compression?: Compression;
  /** AbortSignal for manual cancellation */
  signal?: AbortSignal;
  /** Request timeout in milliseconds */
  timeout?: number;
}

async function* query(
  query: string,
  sessionId: string,
  options: QueryOptions = {},
): AsyncGenerator<Uint8Array, void, unknown> {
  await init();
  const baseUrl = options.baseUrl || "http://localhost:8123/";
  const compression = options.compression ?? "lz4";
  const compressed = compression !== "none";
  const params: Record<string, string> = {
    session_id: sessionId,
    default_format: "JSONEachRowWithProgress",
  };

  if (compressed) {
    params.compress = "1";
  }

  const url = buildReqUrl(baseUrl, params, options.auth);

  const response = await fetch(url.toString(), {
    method: "POST",
    body: query,
    headers: { "Connection": "close" },
    signal: createSignal(options.signal, options.timeout),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Query failed: ${response.status} - ${body}`);
  }

  if (!response.body) {
    throw new Error("Response body is null");
  }

  const reader = response.body.getReader();

  if (!compressed) {
    // For non-compressed, stream data directly
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      yield value;
    }
  } else {
    // For compressed, decompress blocks as they arrive
    // Use growing buffer to avoid O(n²) concat allocations
    let buffer = new Uint8Array(64 * 1024);
    let bufferLen = 0;

    while (true) {
      const { done, value } = await reader.read();

      if (value) {
        // Grow buffer if needed
        if (bufferLen + value.length > buffer.length) {
          const newSize = Math.max(buffer.length * 2, bufferLen + value.length);
          const newBuffer = new Uint8Array(newSize);
          newBuffer.set(buffer.subarray(0, bufferLen));
          buffer = newBuffer;
        }
        buffer.set(value, bufferLen);
        bufferLen += value.length;
      }

      // Process complete blocks from buffer
      let consumed = 0;
      while (bufferLen - consumed >= 25) {
        const compressedSize = readUInt32LE(buffer, consumed + 17);
        const blockSize = 16 + compressedSize;

        if (bufferLen - consumed < blockSize) break;

        const block = buffer.subarray(consumed, consumed + blockSize);
        consumed += blockSize;

        try {
          const decompressed = decodeBlock(block);
          yield decompressed;
        } catch (err: unknown) {
          const message = err instanceof Error ? err.message : String(err);
          throw new Error(`Block decompression failed: ${message}`);
        }
      }

      // Shift remaining data to front
      if (consumed > 0) {
        buffer.copyWithin(0, consumed, bufferLen);
        bufferLen -= consumed;
      }

      if (done) break;
    }
  }
}

/**
 * Buffer byte chunks, decode to text, and yield complete lines.
 *
 * @example
 * for await (const line of streamLines(query("SELECT * FROM t FORMAT CSV", session, config))) {
 *   console.log(line);
 * }
 */
async function* streamLines(
  chunks: AsyncIterable<Uint8Array>,
  delimiter: string = "\n",
): AsyncGenerator<string> {
  let buffer = "";
  for await (const text of streamText(chunks)) {
    buffer += text;
    const parts = buffer.split(delimiter);
    buffer = parts.pop() ?? "";
    for (const part of parts) {
      if (part) yield part;
    }
  }
  if (buffer) yield buffer;
}

/**
 * Buffer byte chunks, split by newlines, and parse as JSON.
 * Use with query() for JSONEachRow format.
 *
 * @example
 * for await (const row of streamJsonLines(query("SELECT * FROM t FORMAT JSONEachRow", session, config))) {
 *   console.log(row.id, row.name);
 * }
 */
async function* streamJsonLines<T = unknown>(
  chunks: AsyncIterable<Uint8Array>,
): AsyncGenerator<T> {
  for await (const line of streamLines(chunks)) {
    yield JSON.parse(line) as T;
  }
}

/**
 * Decode bytes to text strings with streaming support.
 *
 * @example
 * for await (const text of streamText(query("SELECT * FROM t FORMAT JSON", session, config))) {
 *   console.log(text);
 * }
 */
async function* streamText(
  chunks: AsyncIterable<Uint8Array>,
): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  for await (const chunk of chunks) {
    yield decoder.decode(chunk, { stream: true });
  }
  // Flush any remaining bytes
  const final = decoder.decode();
  if (final) yield final;
}

/**
 * Collect all chunks into a single Uint8Array.
 *
 * @example
 * const data = await collectBytes(query("SELECT * FROM t FORMAT RowBinaryWithNamesAndTypes", session, config));
 * const result = decodeRowBinary(data);
 */
async function collectBytes(
  chunks: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let totalLen = 0;
  for await (const chunk of chunks) {
    parts.push(chunk);
    totalLen += chunk.length;
  }
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

/**
 * Collect all bytes and decode to a single string.
 *
 * @example
 * const json = await collectText(query("SELECT * FROM t FORMAT JSON", session, config));
 * const data = JSON.parse(json);
 */
async function collectText(chunks: AsyncIterable<Uint8Array>): Promise<string> {
  let result = "";
  for await (const text of streamText(chunks)) {
    result += text;
  }
  return result;
}

export {
  init,
  insert,
  query,
  buildReqUrl,
  streamJsonEachRow,
  streamJsonCompactEachRowWithNames,
  parseJsonCompactEachRowWithNames,
  streamText,
  streamLines,
  streamJsonLines,
  collectBytes,
  collectText,
};
