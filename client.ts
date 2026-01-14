import {
  decodeBlock,
  decodeBlocks,
  encodeBlock,
  init,
  lz4CompressFrame,
  Method,
  type MethodCode,
  zstdCompressRaw,
} from "./compression.ts";
import type { ClickHouseSettings } from "./settings.ts";

export {
  ClickHouseDateTime64,
  type ColumnDef,
  collectRows,
  type DecodeResult,
  encodeNative,
  RecordBatch,
  rows,
  streamDecodeNative,
  streamEncodeNative,
} from "@maxjustus/chttp/native";
import { StreamBuffer } from "./native/io.ts";
import { type CollectableAsyncGenerator, collectable } from "./util.ts";

export type { CollectableAsyncGenerator } from "./util.ts";

export type Compression = "lz4" | "zstd" | false;

/** Query parameters for parameterized queries like SELECT {x:UInt64} */
export type QueryParams = Record<string, string | number | boolean | bigint>;

// AbortSignal.any() added in Node 20+, ES2024
const AbortSignalAny = AbortSignal as typeof AbortSignal & {
  any(signals: AbortSignal[]): AbortSignal;
};

function createSignal(signal?: AbortSignal, timeout?: number): AbortSignal | undefined {
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
    case false:
      return Method.None;
  }
}

function* chunkUint8Array(data: Uint8Array, chunkSize: number): Generator<Uint8Array> {
  let offset = 0;
  while (offset < data.length) {
    const end = Math.min(offset + chunkSize, data.length);
    yield data.subarray(offset, end);
    offset = end;
  }
}

// Uint8Array helpers
const encoder = new TextEncoder();

function concatBytes(arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

function readUInt32LE(arr: Uint8Array, offset: number): number {
  return (
    arr[offset] | (arr[offset + 1] << 8) | (arr[offset + 2] << 16) | ((arr[offset + 3] << 24) >>> 0)
  );
}

function mergeParams(target: Record<string, string>, source?: Record<string, unknown>): void {
  if (!source) return;
  for (const [key, value] of Object.entries(source)) {
    target[key] = String(value);
  }
}

/** Merge query parameters with param_ prefix for ClickHouse parameterized queries */
function mergeQueryParams(target: Record<string, string>, source?: QueryParams): void {
  if (!source) return;
  for (const [key, value] of Object.entries(source)) {
    target[`param_${key}`] = String(value);
  }
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
function buildReqUrl(baseUrl: string, params: Record<string, string>, auth?: AuthConfig): URL {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, value);
  });

  if (auth?.username) {
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

/** Summary statistics from X-ClickHouse-Summary response header */
export interface QuerySummary {
  read_rows: string;
  read_bytes: string;
  written_rows: string;
  written_bytes: string;
  total_rows_to_read: string;
  result_rows: string;
  result_bytes: string;
  elapsed_ns: string;
}

/** Progress info from X-ClickHouse-Progress header */
export interface HttpProgress {
  read_rows: string;
  read_bytes: string;
  total_rows_to_read: string;
  written_rows?: string;
  written_bytes?: string;
  elapsed_ns?: string;
}

/** Packet types yielded by query() - mirrors TCP client pattern */
export type QueryPacket =
  | { type: "Progress"; progress: HttpProgress }
  | { type: "Data"; chunk: Uint8Array }
  | { type: "Summary"; summary: QuerySummary; queryId: string };

/** Result from insert() with metadata */
export interface InsertResult {
  summary: QuerySummary;
  queryId: string;
}

function parseSummary(response: Response): QuerySummary {
  const header = response.headers.get("X-ClickHouse-Summary");
  if (header) {
    try {
      return JSON.parse(header) as QuerySummary;
    } catch {
      // Fall through to default
    }
  }
  return {
    read_rows: "0",
    read_bytes: "0",
    written_rows: "0",
    written_bytes: "0",
    total_rows_to_read: "0",
    result_rows: "0",
    result_bytes: "0",
    elapsed_ns: "0",
  };
}

function parseProgress(header: string): HttpProgress {
  try {
    return JSON.parse(header) as HttpProgress;
  } catch {
    return {
      read_rows: "0",
      read_bytes: "0",
      total_rows_to_read: "0",
    };
  }
}

export interface InsertOptions {
  baseUrl?: string;
  /** Compression method: "lz4" (default), "zstd", or false */
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
  /** ClickHouse settings applied to this insert */
  settings?: ClickHouseSettings;
  /** Query parameters for parameterized queries like SELECT {x:UInt64} */
  params?: QueryParams;
  /** Custom query ID for tracking in system.query_log and KILL QUERY */
  queryId?: string;
}

type InsertData = Uint8Array | Uint8Array[] | AsyncIterable<Uint8Array> | Iterable<Uint8Array>;

async function insert(
  query: string,
  data: InsertData,
  sessionId: string,
  options: InsertOptions = {},
): Promise<InsertResult> {
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
  if (options.queryId) {
    params.query_id = options.queryId;
  }
  mergeParams(params, options.settings);
  mergeQueryParams(params, options.params);

  // Normalize all input types to Iterable<Uint8Array>
  // This ensures consistent chunking behavior (1MB threshold) for all inputs
  let inputData: Iterable<Uint8Array> | AsyncIterable<Uint8Array>;

  if (data instanceof Uint8Array) {
    // Single Uint8Array - chunk at threshold for consistent progress reporting
    inputData = chunkUint8Array(data, threshold);
  } else if (Array.isArray(data)) {
    // Array of Uint8Arrays - yield chunks from each
    const chunks = data as Uint8Array[];
    inputData = (function* () {
      for (const chunk of chunks) {
        yield* chunkUint8Array(chunk, threshold);
      }
    })();
  } else {
    // Already an Iterable or AsyncIterable
    inputData = data;
  }

  // Streaming path: buffer, compress at threshold, report progress
  const url = buildReqUrl(baseUrl, params, options.auth);

  let blocksSent = 0;
  let totalCompressed = 0;
  let totalUncompressed = 0;

  const stream = new ReadableStream({
    async start(controller) {
      try {
        const bufferA = new Uint8Array(bufferSize);
        const bufferB = new Uint8Array(bufferSize);
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

        for await (const chunk of inputData as AsyncIterable<Uint8Array>) {
          let chunkOffset = 0;

          while (chunkOffset < chunk.length) {
            const spaceAvailable = fillBuffer.length - fillLen;
            const bytesToCopy = Math.min(spaceAvailable, chunk.length - chunkOffset);

            fillBuffer.set(chunk.subarray(chunkOffset, chunkOffset + bytesToCopy), fillLen);
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
    headers: {
      "Content-Type": "application/octet-stream",
      Connection: "close",
    },
    body: stream,
    duplex: "half",
    signal: createSignal(options.signal, options.timeout),
  } as RequestInit);

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Insert failed: ${response.status} - ${body}`);
  }

  return {
    summary: parseSummary(response),
    queryId: response.headers.get("X-ClickHouse-Query-Id") || "",
  };
}

/**
 * Convert objects to JSONEachRow format as Uint8Array chunks.
 * Use with insert() for JSON data.
 */
function streamEncodeJsonEachRow(data: Iterable<unknown>): Generator<Uint8Array>;
function streamEncodeJsonEachRow(data: AsyncIterable<unknown>): AsyncGenerator<Uint8Array>;
function streamEncodeJsonEachRow(
  data: Iterable<unknown> | AsyncIterable<unknown>,
): Generator<Uint8Array> | AsyncGenerator<Uint8Array> {
  if (Symbol.asyncIterator in data) {
    return (async function* () {
      for await (const row of data) {
        yield encoder.encode(`${JSON.stringify(row)}\n`);
      }
    })();
  }
  return (function* () {
    for (const row of data as Iterable<unknown>) {
      yield encoder.encode(`${JSON.stringify(row)}\n`);
    }
  })();
}

/** Data for an HTTP external table */
export type HttpExternalTableData = string | Uint8Array | AsyncIterable<Uint8Array>;

/** An external table to send via HTTP multipart/form-data */
export interface HttpExternalTable {
  /** Column structure, e.g. "id UInt32, name String" */
  structure: string;
  /** Data format (default: TabSeparated) */
  format?: string;
  /** The actual data */
  data: HttpExternalTableData;
}

export interface QueryOptions {
  baseUrl?: string;
  auth?: AuthConfig;
  /** Compression method for response: "lz4" (default), "zstd", or false */
  compression?: Compression;
  /**
   * Compress query body using HTTP Content-Encoding.
   * - "zstd": ZSTD compression (recommended, works with native and WASM)
   * - "lz4": LZ4 frame compression (requires lz4-napi, not available in WASM builds)
   * Requires server setting: enable_http_compression=1
   */
  compressQuery?: "zstd" | "lz4";
  /** AbortSignal for manual cancellation */
  signal?: AbortSignal;
  /** Request timeout in milliseconds */
  timeout?: number;
  /** Client version string (e.g. "24.8") or numeric revision */
  clientVersion?: string | number;
  /** ClickHouse settings applied to this query */
  settings?: ClickHouseSettings;
  /** Query parameters for parameterized queries like SELECT {x:UInt64} */
  params?: QueryParams;
  /** External tables to send with the query */
  externalTables?: Record<string, HttpExternalTable>;
  /** Custom query ID for tracking in system.query_log and KILL QUERY */
  queryId?: string;
}

/**
 * Build a multipart/form-data body for external tables.
 * Returns sync Uint8Array for string/Uint8Array data, or ReadableStream for async data.
 */
function buildMultipartBody(tables: Record<string, HttpExternalTable>): {
  body: Uint8Array | ReadableStream<Uint8Array>;
  boundary: string;
} {
  const boundary = `----chttpBoundary${crypto.randomUUID().replace(/-/g, "")}`;

  // Check if any table has async data
  const hasAsync = Object.values(tables).some(
    (t) => typeof t.data === "object" && t.data !== null && Symbol.asyncIterator in t.data,
  );

  if (!hasAsync) {
    // Build complete body synchronously
    const parts: Uint8Array[] = [];
    for (const [name, table] of Object.entries(tables)) {
      const header = `--${boundary}\r\nContent-Disposition: form-data; name="${name}"; filename="data"\r\n\r\n`;
      parts.push(encoder.encode(header));
      if (typeof table.data === "string") {
        parts.push(encoder.encode(table.data));
      } else {
        parts.push(table.data as Uint8Array);
      }
      parts.push(encoder.encode("\r\n"));
    }
    parts.push(encoder.encode(`--${boundary}--\r\n`));
    return { body: concatBytes(parts), boundary };
  }

  // Return streaming ReadableStream for async data
  const entries = Object.entries(tables);
  let entryIndex = 0;
  let currentIterator: AsyncIterator<Uint8Array> | null = null;
  let sentHeader = false;
  let sentFooter = false;

  return {
    body: new ReadableStream<Uint8Array>({
      async pull(controller) {
        // Send headers and data for each table
        while (entryIndex < entries.length) {
          const [name, table] = entries[entryIndex];

          if (!sentHeader) {
            const header = `--${boundary}\r\nContent-Disposition: form-data; name="${name}"; filename="data"\r\n\r\n`;
            controller.enqueue(encoder.encode(header));
            sentHeader = true;

            // For sync data, enqueue it all at once
            if (typeof table.data === "string") {
              controller.enqueue(encoder.encode(table.data));
              controller.enqueue(encoder.encode("\r\n"));
              sentHeader = false;
              entryIndex++;
              continue;
            } else if (table.data instanceof Uint8Array) {
              controller.enqueue(table.data);
              controller.enqueue(encoder.encode("\r\n"));
              sentHeader = false;
              entryIndex++;
              continue;
            } else {
              // Async iterable
              currentIterator = (table.data as AsyncIterable<Uint8Array>)[Symbol.asyncIterator]();
            }
          }

          // Stream async data
          if (currentIterator) {
            const { done, value } = await currentIterator.next();
            if (!done) {
              controller.enqueue(value);
              return;
            }
            // Done with this async iterable
            controller.enqueue(encoder.encode("\r\n"));
            currentIterator = null;
            sentHeader = false;
            entryIndex++;
          }
        }

        // All tables done, send closing boundary
        if (!sentFooter) {
          controller.enqueue(encoder.encode(`--${boundary}--\r\n`));
          sentFooter = true;
        }
        controller.close();
      },
    }),
    boundary,
  };
}

function query(
  sql: string,
  sessionId: string,
  options: QueryOptions & Record<string, any> = {},
): CollectableAsyncGenerator<QueryPacket> {
  return collectable(queryImpl(sql, sessionId, options));
}

async function* queryImpl(
  sql: string,
  sessionId: string,
  options: QueryOptions & Record<string, any> = {},
): AsyncGenerator<QueryPacket> {
  await init();
  const baseUrl = options.baseUrl || "http://localhost:8123/";
  const compression = options.compression ?? "lz4";
  const compressed = compression !== false;
  const params: Record<string, string> = {
    session_id: sessionId,
    default_format: "JSONEachRowWithProgress",
  };

  if (compressed) {
    params.compress = "1";
  }

  if (options.compressQuery) {
    params.enable_http_compression = "1";
  }

  if (options.clientVersion) {
    params.client_protocol_version = String(options.clientVersion);
  }

  if (options.queryId) {
    params.query_id = options.queryId;
  }

  // Include any other settings/params passed in options
  const reserved = [
    "baseUrl",
    "auth",
    "compression",
    "compressQuery",
    "signal",
    "timeout",
    "clientVersion",
    "settings",
    "params",
    "externalTables",
    "queryId",
  ];
  mergeParams(params, options.settings);
  mergeQueryParams(params, options.params);
  for (const [key, value] of Object.entries(options)) {
    if (!reserved.includes(key) && value !== undefined) {
      params[key] = String(value);
    }
  }

  // Handle external tables: query goes in URL, body is multipart
  const hasExternalTables =
    options.externalTables && Object.keys(options.externalTables).length > 0;
  if (hasExternalTables) {
    params.query = sql;
    for (const [name, table] of Object.entries(options.externalTables!)) {
      params[`${name}_structure`] = table.structure;
      if (table.format) {
        params[`${name}_format`] = table.format;
      }
    }
  }

  const url = buildReqUrl(baseUrl, params, options.auth);

  const headers: Record<string, string> = {
    Connection: "close",
    "User-Agent": `chttp/${options.clientVersion || "1.0"}`,
  };

  let response: Response;
  if (hasExternalTables) {
    const { body, boundary } = buildMultipartBody(options.externalTables!);
    headers["Content-Type"] = `multipart/form-data; boundary=${boundary}`;

    // Need duplex: "half" for streaming body
    const fetchOptions: RequestInit & { duplex?: string } = {
      method: "POST",
      body,
      headers,
      signal: createSignal(options.signal, options.timeout),
    };
    if (body instanceof ReadableStream) {
      fetchOptions.duplex = "half";
    }
    response = await fetch(url.toString(), fetchOptions);
  } else {
    let body: string | Uint8Array = sql;
    if (options.compressQuery) {
      const queryBytes = encoder.encode(sql);
      body =
        options.compressQuery === "lz4"
          ? lz4CompressFrame(queryBytes)
          : zstdCompressRaw(queryBytes);
      headers["Content-Encoding"] = options.compressQuery;
    }
    response = await fetch(url.toString(), {
      method: "POST",
      body,
      headers,
      signal: createSignal(options.signal, options.timeout),
    });
  }

  if (!response.ok) {
    // Error responses may be compressed if we requested compression
    let body: string;
    if (compressed && response.body) {
      const raw = new Uint8Array(await response.arrayBuffer());
      try {
        body = new TextDecoder().decode(decodeBlocks(raw));
      } catch {
        // Decompression failed - response is likely plain text
        body = new TextDecoder().decode(raw);
      }
    } else {
      body = await response.text();
    }
    throw new Error(`Query failed: ${response.status} - ${body}`);
  }

  if (!response.body) {
    throw new Error("Response body is null");
  }

  const summary = parseSummary(response);
  const queryId = response.headers.get("X-ClickHouse-Query-Id") || "";
  const reader = response.body.getReader();

  async function* createStream(): AsyncGenerator<Uint8Array, void, unknown> {
    if (!compressed) {
      // For non-compressed, stream data directly
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        yield value;
      }
    } else {
      const streamBuffer = new StreamBuffer(64 * 1024);

      while (true) {
        const { done, value } = await reader.read();

        if (value) streamBuffer.append(value);

        // process complete blocks
        while (streamBuffer.available >= 25) {
          const bufferView = streamBuffer.view;

          const compressedSize = readUInt32LE(bufferView, 17);
          const blockSize = 16 + compressedSize;

          if (streamBuffer.available < blockSize) break;

          const block = bufferView.subarray(0, blockSize);

          try {
            const decompressed = decodeBlock(block);
            yield decompressed;
          } catch (err: unknown) {
            const message = err instanceof Error ? err.message : String(err);
            throw new Error(`Block decompression failed: ${message}`);
          }

          streamBuffer.consume(blockSize);
        }

        if (done) {
          if (streamBuffer.available > 0) {
            throw new Error("Incomplete block");
          }

          break;
        }
      }
    }
  }

  // Yield Progress packets from X-ClickHouse-Progress headers (if present)
  const progressHeader = response.headers.get("X-ClickHouse-Progress");
  if (progressHeader) {
    yield { type: "Progress", progress: parseProgress(progressHeader) };
  }

  // Yield Data packets from body stream
  for await (const chunk of createStream()) {
    yield { type: "Data", chunk };
  }

  // Yield Summary packet at end
  yield { type: "Summary", summary, queryId };
}

/** Input type for stream helpers - accepts query() result or any async iterable of packets */
type QueryInput = AsyncIterable<QueryPacket>;

/** Extract Data chunks from packet stream */
async function* dataChunks(input: QueryInput): AsyncGenerator<Uint8Array> {
  for await (const packet of input) {
    if (packet.type === "Data") {
      yield packet.chunk;
    }
  }
}

/**
 * Buffer byte chunks, decode to text, and yield complete lines.
 *
 * @example
 * for await (const line of streamLines(query("SELECT ...", session, config))) {
 *   console.log(line);
 * }
 */
async function* streamLines(input: QueryInput, delimiter: string = "\n"): AsyncGenerator<string> {
  let buffer = "";
  for await (const text of decodeText(dataChunks(input))) {
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
 * for await (const row of streamDecodeJsonEachRow(query("SELECT ...", session, config))) {
 *   console.log(row.id, row.name);
 * }
 */
async function* streamDecodeJsonEachRow<T = unknown>(input: QueryInput): AsyncGenerator<T> {
  for await (const line of streamLines(input)) {
    yield JSON.parse(line) as T;
  }
}

/** Internal text decoder for raw streams */
async function* decodeText(chunks: AsyncIterable<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  for await (const chunk of chunks) {
    yield decoder.decode(chunk, { stream: true });
  }
  const final = decoder.decode();
  if (final) yield final;
}

/**
 * Decode bytes to text strings with streaming support.
 *
 * @example
 * for await (const text of streamText(query("SELECT ...", session, config))) {
 *   console.log(text);
 * }
 */
async function* streamText(input: QueryInput): AsyncGenerator<string> {
  yield* decodeText(dataChunks(input));
}

/**
 * Collect all chunks into a single Uint8Array.
 *
 * @example
 * const data = await collectBytes(query("SELECT ...", session, config));
 * const result = await decodeNative(data);
 */
async function collectBytes(input: QueryInput): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let totalLen = 0;
  for await (const chunk of dataChunks(input)) {
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
 * const json = await collectText(query("SELECT ...", session, config));
 * const data = JSON.parse(json);
 */
async function collectText(input: QueryInput): Promise<string> {
  let result = "";
  for await (const text of decodeText(dataChunks(input))) {
    result += text;
  }
  return result;
}

/**
 * collect all JSON lines into an array of objects.
 *
 * @example
 * const rows = await collectJsonEachRow<{ id: number }>(query("SELECT ...", session, config));
 */
async function collectJsonEachRow<T = unknown>(input: QueryInput): Promise<T[]> {
  const result: T[] = [];
  for await (const row of streamDecodeJsonEachRow<T>(input)) {
    result.push(row);
  }
  return result;
}

export {
  init,
  insert,
  query,
  buildReqUrl,
  streamEncodeJsonEachRow,
  streamText,
  streamLines,
  streamDecodeJsonEachRow,
  collectBytes,
  collectText,
  collectJsonEachRow,
  dataChunks,
};
