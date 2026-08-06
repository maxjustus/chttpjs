import {
  type Compression,
  compressionLevel,
  concat,
  decodeBlock,
  decodeBlocks,
  encodeBlock,
  init,
  lz4CompressFrame,
  readUInt32LE,
  zstdCompressRaw,
} from "./compression.ts";
import { ClickHouseException } from "./errors.ts";
import type { ClickHouseSettings } from "./settings.generated.ts";

export {
  ClickHouseDateTime64,
  type ColumnDef,
  collectRows,
  type ExternalTableData,
  encodeNative,
  RecordBatch,
  rows,
  streamDecodeNative,
  streamEncodeNative,
} from "./native/index.ts";

import { type HttpEncoding, nodeHttpRequest } from "./http_transport.ts";
import { mapAsync, prepend, readChunks, toAsyncIterable } from "./iter.ts";
import { type ExternalTableData, encodeNative, RecordBatch } from "./native/index.ts";
import { BlockBuffer } from "./native/io.ts";
import { SQL_NULL, serializeParams } from "./params.ts";
import { type CollectableAsyncGenerator, collectable } from "./util.ts";

export type { QueryParams, QueryParamValue } from "./types.ts";
export type { CollectableAsyncGenerator } from "./util.ts";

import type { QueryParams } from "./types.ts";

export type { Compression };

function createSignal(signal?: AbortSignal, timeout?: number): AbortSignal | undefined {
  if (!signal && !timeout) return undefined;
  if (signal?.aborted) return signal;
  if (signal && !timeout) return signal;
  if (!signal && timeout) return AbortSignal.timeout(timeout);
  return AbortSignal.any([signal!, AbortSignal.timeout(timeout!)]);
}

const encoder = new TextEncoder();

/** Convert RecordBatch schema to ClickHouse structure string. */
function schemaToStructure(batch: RecordBatch): string {
  return batch.schema.map((c) => `${c.name} ${c.type}`).join(", ");
}

/** Check if value is an HttpExternalTable (has structure and data fields). */
function isHttpExternalTable(v: unknown): v is HttpExternalTable {
  return v !== null && typeof v === "object" && "structure" in v && "data" in v;
}

/**
 * Normalize HttpExternalTableInput to HttpExternalTable.
 * For RecordBatch: encodes to Native format, extracts schema.
 * For iterables: collects and encodes all batches.
 * For async iterables: buffers first batch for schema, returns streaming encoder.
 */
async function normalizeExternalTable(input: HttpExternalTableInput): Promise<HttpExternalTable> {
  // Already an HttpExternalTable
  if (isHttpExternalTable(input)) {
    return input;
  }

  // Single RecordBatch
  if (RecordBatch.isRecordBatch(input)) {
    return {
      structure: schemaToStructure(input),
      format: "Native",
      data: encodeNative(input),
    };
  }

  // AsyncIterable<RecordBatch>
  if (Symbol.asyncIterator in input) {
    const iter = (input as AsyncIterable<RecordBatch>)[Symbol.asyncIterator]();
    const first = await iter.next();
    if (first.done) {
      throw new Error("Empty async iterable for external table");
    }
    const firstBatch = first.value;
    const structure = schemaToStructure(firstBatch);
    return {
      structure,
      format: "Native",
      data: mapAsync(prepend(firstBatch, iter), encodeNative),
    };
  }

  // Sync Iterable<RecordBatch>
  const batches = [...(input as Iterable<RecordBatch>)];
  if (batches.length === 0) {
    throw new Error("Empty iterable for external table");
  }
  const structure = schemaToStructure(batches[0]!);
  const encoded = batches.map((b) => encodeNative(b));
  return { structure, format: "Native", data: concat(encoded) };
}

/** Normalize all external tables in a record. */
async function normalizeExternalTables(
  tables: Record<string, HttpExternalTableInput>,
): Promise<Record<string, HttpExternalTable>> {
  const entries = await Promise.all(
    Object.entries(tables).map(
      async ([name, input]) => [name, await normalizeExternalTable(input)] as const,
    ),
  );
  return Object.fromEntries(entries);
}

function mergeParams(target: Record<string, string>, source?: Record<string, unknown>): void {
  if (!source) return;
  for (const [key, value] of Object.entries(source)) {
    target[key] = String(value);
  }
}

/** Merge query parameters with param_ prefix for ClickHouse parameterized queries */
function mergeQueryParams(
  target: Record<string, string>,
  query: string,
  source?: QueryParams,
): void {
  const serialized = serializeParams(query, source ?? {});
  for (const [key, value] of Object.entries(serialized)) {
    // For HTTP params, SQL_NULL symbol becomes \N escape sequence
    target[`param_${key}`] = value === SQL_NULL ? "\\N" : value;
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
function buildReqUrl(base: string, params: Record<string, string>): URL {
  const url = new URL(base);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, value);
  });
  return url;
}

/** Credentials go in headers, not the URL, to keep them out of logs and caches. */
function authHeaders(auth?: AuthConfig): Record<string, string> {
  if (!auth?.username) return {};
  const headers: Record<string, string> = { "X-ClickHouse-User": auth.username };
  if (auth.password) headers["X-ClickHouse-Key"] = auth.password;
  return headers;
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

const EXCEPTION_MARKER_BYTES = new TextEncoder().encode("__exception__");
const EXCEPTION_CODE_BYTES = new TextEncoder().encode("Code:");
// Must exceed the longest trailer preamble a chunk boundary can split:
// marker + newline + exception tag + newline + "Code: <digits>." (~60 bytes
// with the 16-char tags current servers send).
const EXCEPTION_SCAN_TAIL_BYTES = 128;

/** Parse ClickHouse error text: "Code: N. DB::Exception: message (ERROR_TAG)" */
function parseErrorText(text: string): { code: number; name: string; message: string } {
  const match = text.match(/Code:\s*(\d+)\.\s*(\S+):\s+(.*)/s);
  if (match) {
    return { code: parseInt(match[1]!, 10), name: match[2]!.trim(), message: match[3]!.trim() };
  }
  return { code: 0, name: "Unknown", message: text.trim() };
}

function exceptionFromText(text: string, codeOverride?: number): ClickHouseException {
  const parsed = parseErrorText(text);
  return new ClickHouseException(
    codeOverride ?? parsed.code,
    parsed.name,
    parsed.message,
    "",
    false,
  );
}

/** Build ClickHouseException from HTTP error response */
function parseHttpError(response: Response, body: string): ClickHouseException {
  const headerCode = response.headers.get("X-ClickHouse-Exception-Code");
  return exceptionFromText(body, headerCode ? parseInt(headerCode, 10) : undefined);
}

function extractResponseFormat(sql: string, options: QueryOptions): string {
  const matches = [...sql.matchAll(/\bFORMAT\s+([A-Za-z][A-Za-z0-9_]*)\b/gi)];
  const rawFormat = options.default_format;
  const settingFormat = options.settings?.default_format;
  return (
    matches.at(-1)?.[1] ??
    (typeof rawFormat === "string" ? rawFormat : undefined) ??
    (typeof settingFormat === "string" ? settingFormat : undefined) ??
    "JSONEachRowWithProgress"
  );
}

function isTextFormat(format: string): boolean {
  const normalized = format.toLowerCase();
  return (
    normalized.includes("json") ||
    normalized.includes("csv") ||
    normalized.includes("tsv") ||
    normalized.includes("tabseparated") ||
    normalized.includes("pretty") ||
    normalized.includes("vertical") ||
    normalized.includes("xml") ||
    normalized.includes("markdown") ||
    normalized.includes("template") ||
    normalized.includes("customseparated") ||
    normalized === "values"
  );
}

/** Index just past an LF or CRLF at `p`, or -1 if `p` is not at a newline. */
function consumeNewline(buf: Uint8Array, p: number): number {
  if (p < buf.length && buf[p] === 10) return p + 1;
  if (p + 1 < buf.length && buf[p] === 13 && buf[p + 1] === 10) return p + 2;
  return -1;
}

function matchesAt(buf: Uint8Array, offset: number, expected: Uint8Array): boolean {
  if (offset + expected.length > buf.length) return false;
  for (let i = 0; i < expected.length; i++) {
    if (buf[offset + i] !== expected[i]) return false;
  }
  return true;
}

/**
 * Validate a framed exception at `offset`: `__exception__` + newline, an
 * optional `<tag>` + newline (servers that send X-ClickHouse-Exception-Tag
 * frame the trailer with that random tag), then the full `Code: <digits>.`
 * preamble ClickHouse always emits ("Code: 395. DB::Exception: ...").
 *
 * Requiring the tag/preamble is what lets binary-format detection work: a
 * Native/RowBinary stream's exception trailer is preceded by arbitrary block
 * bytes (e.g. \0), not a newline, so the line-start guard text relies on cannot
 * apply — the strict preamble is the only thing distinguishing a real trailer
 * from marker bytes that happen to appear in a column value.
 */
function isFramedExceptionAt(buf: Uint8Array, offset: number, tagBytes?: Uint8Array): boolean {
  if (!matchesAt(buf, offset, EXCEPTION_MARKER_BYTES)) return false;

  let p = consumeNewline(buf, offset + EXCEPTION_MARKER_BYTES.length);
  if (p < 0) return false;

  // Tag line between marker and Code: when the server announced one. Tagless
  // is still accepted: the tag arrived via header, but pre-26.x trailers omit it.
  if (tagBytes && matchesAt(buf, p, tagBytes)) {
    const afterTag = consumeNewline(buf, p + tagBytes.length);
    if (afterTag < 0) return false;
    p = afterTag;
  }

  if (!matchesAt(buf, p, EXCEPTION_CODE_BYTES)) return false;
  p += EXCEPTION_CODE_BYTES.length;

  while (p < buf.length && buf[p] === 32) p++; // optional spaces after "Code:"
  const digitStart = p;
  while (p < buf.length && buf[p]! >= 48 && buf[p]! <= 57) p++; // one or more digits
  if (p === digitStart) return false;
  return p < buf.length && buf[p] === 46; // trailing '.'
}

/**
 * Find a framed __exception__ block, return its offset or -1.
 *
 * `requireLineStart` (text formats) demands the marker begin a line so a marker
 * sitting inside a row value isn't mistaken for the real trailer. Binary formats
 * pass false: the marker follows arbitrary block bytes, so isFramedExceptionAt's
 * strict preamble is the only guard.
 */
function findExceptionMarker(buf: Uint8Array, requireLineStart: boolean, tag?: string): number {
  const first = EXCEPTION_MARKER_BYTES[0]; // '_'
  const len = EXCEPTION_MARKER_BYTES.length;
  const tagBytes = tag ? encoder.encode(tag) : undefined;
  for (let i = 0; i <= buf.length - len; i++) {
    if (requireLineStart && i > 0 && buf[i - 1] !== 10) continue;
    if (buf[i] !== first) continue;
    if (!isFramedExceptionAt(buf, i, tagBytes)) continue;
    return i;
  }
  return -1;
}

/** Parse a mid-stream __exception__ block from bytes into a ClickHouseException */
function parseStreamException(buf: Uint8Array, tag?: string): ClickHouseException {
  const text = new TextDecoder().decode(buf);
  // Strip "__exception__\r\n" prefix
  let body = text.replace(/^__exception__\r?\n/, "");
  if (tag) {
    // Tagged trailers wrap the text: <tag>\n<text><len> <tag>\n__exception__\n
    if (body.startsWith(tag)) body = body.slice(tag.length).replace(/^\r?\n/, "");
    body = body.replace(new RegExp(`\\n?\\d+ ${tag}\\r?\\n__exception__\\r?\\n?$`), "");
  }
  return exceptionFromText(body);
}

function splitStreamException(
  buf: Uint8Array,
  requireLineStart: boolean,
  tag?: string,
): { prefix: Uint8Array; error: ClickHouseException } | null {
  const pos = findExceptionMarker(buf, requireLineStart, tag);
  if (pos < 0) return null;
  return {
    prefix: buf.subarray(0, pos),
    error: parseStreamException(buf.subarray(pos), tag),
  };
}

export interface InsertOptions {
  url?: string;
  /**
   * Compression method: "lz4" (default), "zstd", or false.
   * Use `{ method: "zstd", level }` to set an explicit ZSTD level (1-22, default: 3).
   */
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
  /** Session ID for server-side state (temp tables, SET variables). Omit for stateless requests. */
  sessionId?: string;
}

type InsertData = Uint8Array | Uint8Array[] | AsyncIterable<Uint8Array> | Iterable<Uint8Array>;

async function insert(
  query: string,
  data: InsertData,
  options: InsertOptions = {},
): Promise<InsertResult> {
  await init();
  const baseUrl = options.url || "http://localhost:8123/";
  const { compression = "lz4", bufferSize = 1024 * 1024, onProgress = null } = options;
  if (!Number.isInteger(bufferSize) || bufferSize <= 0) {
    // A zero-capacity buffer can never absorb input, so the flush loop would
    // spin forever emitting empty blocks.
    throw new Error(`insert: bufferSize must be a positive integer, got ${bufferSize}`);
  }
  // A threshold above bufferSize can never trigger a flush, so clamp it.
  const threshold = Math.min(options.threshold ?? bufferSize - 2048, bufferSize);

  const params: Record<string, string> = {
    query: query,
    decompress: "1",
  };
  if (options.sessionId) {
    params.session_id = options.sessionId;
  }
  if (options.queryId) {
    params.query_id = options.queryId;
  }
  mergeParams(params, options.settings);
  mergeQueryParams(params, query, options.params);

  const inputData: Iterable<Uint8Array> | AsyncIterable<Uint8Array> =
    data instanceof Uint8Array ? [data] : data;

  // Streaming path: buffer, compress at threshold, report progress
  const url = buildReqUrl(baseUrl, params);

  let blocksSent = 0;
  let totalCompressed = 0;
  let totalUncompressed = 0;

  // Pull-based stream: blocks are compressed on demand as fetch consumes the
  // body, so a fast producer can't buffer the whole payload ahead of the network.
  async function* compressedBlocks(): AsyncGenerator<Uint8Array> {
    const buffer = new Uint8Array(bufferSize);
    let fillLen = 0;

    const flush = (): Uint8Array => {
      const compressed = encodeBlock(buffer.subarray(0, fillLen), compression);
      blocksSent++;
      totalCompressed += compressed.length;
      totalUncompressed += fillLen;
      onProgress?.({
        blocksSent,
        bytesCompressed: compressed.length,
        bytesUncompressed: fillLen,
      });
      fillLen = 0;
      return compressed;
    };

    for await (const chunk of inputData as AsyncIterable<Uint8Array>) {
      let chunkOffset = 0;
      while (chunkOffset < chunk.length) {
        const bytesToCopy = Math.min(buffer.length - fillLen, chunk.length - chunkOffset);
        buffer.set(chunk.subarray(chunkOffset, chunkOffset + bytesToCopy), fillLen);
        fillLen += bytesToCopy;
        chunkOffset += bytesToCopy;

        if (fillLen >= threshold) yield flush();
      }
    }

    if (fillLen > 0) yield flush();

    onProgress?.({
      blocksSent,
      bytesCompressed: totalCompressed,
      bytesUncompressed: totalUncompressed,
      complete: true,
    });
  }

  const stream = ReadableStream.from(compressedBlocks());

  const response = await fetch(url.toString(), {
    method: "POST",
    headers: {
      "Content-Type": "application/octet-stream",
      ...authHeaders(options.auth),
    },
    body: stream,
    duplex: "half",
    signal: createSignal(options.signal, options.timeout),
  } as RequestInit);

  if (!response.ok) {
    const body = await response.text();
    throw parseHttpError(response, body);
  }

  // ClickHouse can fail after committing 200 headers and deliver the
  // exception in the body - drain it and surface any error it carries.
  const body = new Uint8Array(await response.arrayBuffer());
  if (body.length > 0) {
    const tag = response.headers.get("X-ClickHouse-Exception-Tag") ?? undefined;
    const match = splitStreamException(body, true, tag);
    if (match) throw match.error;
    const text = new TextDecoder().decode(body);
    if (parseErrorText(text).code !== 0) throw exceptionFromText(text);
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

/**
 * Input for HTTP external tables.
 * Accepts RecordBatch (schema auto-extracted), iterables of RecordBatch, or explicit HttpExternalTable.
 */
export type HttpExternalTableInput = ExternalTableData | HttpExternalTable;

const HTTP_QUERY_OPTION_RESERVED = new Set([
  "url",
  "auth",
  "compression",
  "compressQuery",
  "httpCompression",
  "signal",
  "timeout",
  "clientVersion",
  "settings",
  "params",
  "externalTables",
  "queryId",
  "sessionId",
]);

function mergeRawHttpQueryOptions(target: Record<string, string>, options: QueryOptions): void {
  for (const [key, value] of Object.entries(options)) {
    if (HTTP_QUERY_OPTION_RESERVED.has(key) || value === undefined) continue;
    target[key] = String(value);
  }
}

/**
 * HTTP query transport options.
 *
 * Unknown root-level keys are forwarded as raw ClickHouse URL params for compatibility
 * and for unmodeled server options. Prefer `settings` for standard ClickHouse settings
 * and `params` for typed `{name: Type}` query parameters.
 *
 * Reserved transport keys are not forwarded: `url`, `auth`, `compression`,
 * `compressQuery`, `signal`, `timeout`, `clientVersion`, `settings`, `params`,
 * `externalTables`, `queryId`, and `sessionId`.
 */
export interface QueryOptions {
  [key: string]: unknown;
  url?: string;
  auth?: AuthConfig;
  /**
   * Compression method for response: "lz4" (default), "zstd", or false.
   * Use `{ method: "zstd", level }` to set an explicit ZSTD level (1-22, default: 3).
   */
  compression?: Compression;
  /**
   * Compress query body using HTTP Content-Encoding.
   * - "zstd": ZSTD compression (recommended, works with native and WASM)
   * - "lz4": LZ4 frame compression (requires lz4-napi, not available in WASM builds)
   * - `{ method: "zstd", level }`: ZSTD with an explicit level (1-22, default: 3)
   * Requires server setting: enable_http_compression=1
   */
  compressQuery?: "lz4" | "zstd" | { method: "zstd"; level?: number };
  /**
   * Compress the response with an HTTP content coding instead of ClickHouse
   * block compression, which the server flushes per block. Rows and progress
   * then arrive as the server produces them, rather than at the end.
   *
   * Overrides `compression`. Requires `node:http`, so it is unavailable in
   * browsers, and "zstd" needs the optional `zstd-napi` dependency.
   */
  httpCompression?: HttpEncoding;
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
  /** External tables to send with the query (RecordBatch, iterables, or HttpExternalTable) */
  externalTables?: Record<string, HttpExternalTableInput>;
  /** Custom query ID for tracking in system.query_log and KILL QUERY */
  queryId?: string;
  /** Session ID for server-side state (temp tables, SET variables). Omit for stateless requests. */
  sessionId?: string;
}

/**
 * Build a multipart/form-data body for external tables.
 * Returns sync Uint8Array for string/Uint8Array data, or ReadableStream for async data.
 */
function buildMultipartBody(tables: Record<string, HttpExternalTable>): {
  body: Uint8Array | ReadableStream<Uint8Array>;
  boundary: string;
} {
  const boundary = `----chwireBoundary${crypto.randomUUID().replace(/-/g, "")}`;

  // Check if any table has async data
  const hasAsync = Object.values(tables).some(
    (t) => typeof t.data === "object" && t.data !== null && Symbol.asyncIterator in t.data,
  );

  const partHeader = (name: string) =>
    `--${boundary}\r\nContent-Disposition: form-data; name="${name}"; filename="data"\r\n\r\n`;

  if (!hasAsync) {
    // Build complete body synchronously
    const parts: Uint8Array[] = [];
    for (const [name, table] of Object.entries(tables)) {
      parts.push(encoder.encode(partHeader(name)));
      if (typeof table.data === "string") {
        parts.push(encoder.encode(table.data));
      } else {
        parts.push(table.data as Uint8Array);
      }
      parts.push(encoder.encode("\r\n"));
    }
    parts.push(encoder.encode(`--${boundary}--\r\n`));
    return { body: concat(parts), boundary };
  }

  async function* parts(): AsyncGenerator<Uint8Array> {
    for (const [name, table] of Object.entries(tables)) {
      yield encoder.encode(partHeader(name));
      if (typeof table.data === "string") {
        yield encoder.encode(table.data);
      } else {
        yield* toAsyncIterable(table.data as Uint8Array | AsyncIterable<Uint8Array>);
      }
      yield encoder.encode("\r\n");
    }
    yield encoder.encode(`--${boundary}--\r\n`);
  }

  return { body: ReadableStream.from(parts()), boundary };
}

function query(sql: string, options: QueryOptions = {}): CollectableAsyncGenerator<QueryPacket> {
  return collectable(queryImpl(sql, options));
}

async function* queryImpl(sql: string, options: QueryOptions = {}): AsyncGenerator<QueryPacket> {
  await init();
  const baseUrl = options.url || "http://localhost:8123/";
  const compression = options.compression ?? "lz4";
  // Block compression and HTTP content coding are alternatives, not layers.
  // Only the latter makes the server flush each block as it is produced.
  const httpCompression = options.httpCompression;
  const compressed = compression !== false && !httpCompression;
  const params: Record<string, string> = {
    default_format: "JSONEachRowWithProgress",
  };

  if (options.sessionId) {
    params.session_id = options.sessionId;
  }

  if (compressed) {
    params.compress = "1";
  }

  if (options.compressQuery || httpCompression) {
    params.enable_http_compression = "1";
  }

  if (options.clientVersion) {
    params.client_protocol_version = String(options.clientVersion);
  }

  if (options.queryId) {
    params.query_id = options.queryId;
  }

  mergeParams(params, options.settings);
  mergeQueryParams(params, sql, options.params);
  mergeRawHttpQueryOptions(params, options);

  // Handle external tables: normalize inputs, query goes in URL, body is multipart
  const hasExternalTables =
    options.externalTables && Object.keys(options.externalTables).length > 0;
  let normalizedTables: Record<string, HttpExternalTable> | undefined;
  if (hasExternalTables) {
    normalizedTables = await normalizeExternalTables(options.externalTables!);
    params.query = sql;
    for (const [name, table] of Object.entries(normalizedTables)) {
      params[`${name}_structure`] = table.structure;
      if (table.format) {
        params[`${name}_format`] = table.format;
      }
    }
  }

  const url = buildReqUrl(baseUrl, params);

  const headers: Record<string, string> = {
    ...authHeaders(options.auth),
    "User-Agent": `chwire/${options.clientVersion || "1.0"}`,
    // The client does its own block compression (compress=1), so HTTP content
    // coding on top only adds CPU - and it breaks against 26.x: with
    // compress=1 plus a non-identity Accept-Encoding the server sends empty
    // error bodies, and gzip framing perturbs mid-stream exception delivery.
    "Accept-Encoding": httpCompression ?? "identity",
  };

  const requestSignal = createSignal(options.signal, options.timeout) ?? null;

  function send(body: string | Uint8Array | ReadableStream<Uint8Array>): Promise<Response> {
    if (httpCompression) {
      return nodeHttpRequest(
        url.toString(),
        { method: "POST", headers, body, signal: requestSignal },
        httpCompression,
      );
    }
    // Need duplex: "half" for streaming body
    const fetchOptions: RequestInit & { duplex?: string } = {
      method: "POST",
      body,
      headers,
      signal: requestSignal,
    };
    if (body instanceof ReadableStream) {
      fetchOptions.duplex = "half";
    }
    return fetch(url.toString(), fetchOptions);
  }

  let response: Response;
  if (hasExternalTables) {
    const { body, boundary } = buildMultipartBody(normalizedTables!);
    headers["Content-Type"] = `multipart/form-data; boundary=${boundary}`;
    response = await send(body);
  } else {
    let body: string | Uint8Array = sql;
    if (options.compressQuery) {
      const queryBytes = encoder.encode(sql);
      const method =
        typeof options.compressQuery === "object"
          ? options.compressQuery.method
          : options.compressQuery;
      body =
        method === "lz4"
          ? lz4CompressFrame(queryBytes)
          : zstdCompressRaw(queryBytes, compressionLevel(options.compressQuery));
      headers["Content-Encoding"] = method;
    }
    response = await send(body);
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
    throw parseHttpError(response, body);
  }

  if (!response.body) {
    throw new Error("Response body is null");
  }

  const summary = parseSummary(response);
  const queryId = response.headers.get("X-ClickHouse-Query-Id") || "";
  const reader = response.body.getReader();
  // ClickHouse appends a framed __exception__ trailer when a query errors after
  // the response headers are committed — in every format, text and binary alike.
  // Always scan for it; the format only decides the guard: text rows let us
  // require the marker to start a line, binary cannot (see findExceptionMarker).
  const requireLineStart = isTextFormat(extractResponseFormat(sql, options));
  // 26.x servers announce a random tag and frame the trailer with it.
  const exceptionTag = response.headers.get("X-ClickHouse-Exception-Tag") ?? undefined;

  async function* createStream(): AsyncGenerator<Uint8Array, void, unknown> {
    try {
      if (!compressed) {
        // Keep a small tail so a framed exception preamble split across reads
        // is validated before we emit those bytes to the caller.
        let pending: Uint8Array = new Uint8Array(0);

        for await (const value of readChunks(reader)) {
          const combined = pending.length === 0 ? value : concat([pending, value]);
          const match = splitStreamException(combined, requireLineStart, exceptionTag);

          if (match) {
            pending = new Uint8Array(0);
            if (match.prefix.length > 0) yield match.prefix;
            throw match.error;
          }

          const keep = Math.min(combined.length, EXCEPTION_SCAN_TAIL_BYTES);
          const emitLen = combined.length - keep;
          if (emitLen > 0) yield combined.subarray(0, emitLen);
          pending = combined.subarray(emitLen);
        }
        // Stream ended: scan the held-back tail one final time. A marker at the
        // tail's offset 0 can match here even when it didn't mid-stream, since
        // findExceptionMarker drops the preceding-newline requirement at offset 0.
        if (pending.length > 0) {
          const match = splitStreamException(pending, requireLineStart, exceptionTag);
          if (match) {
            if (match.prefix.length > 0) yield match.prefix;
            throw match.error;
          }
          yield pending;
        }
      } else {
        const streamBuffer = new BlockBuffer(64 * 1024);
        const exceptionTagBytes = exceptionTag ? encoder.encode(exceptionTag) : undefined;

        for await (const value of readChunks(reader)) {
          streamBuffer.append(value);

          // process complete blocks
          while (streamBuffer.available >= 25) {
            const bufferView = streamBuffer.view;

            // An uncompressed __exception__ trailer sits at the buffer start once
            // every complete block ahead of it has been consumed. Offset 0 only,
            // so the line-start guard is moot — check it directly and cheaply.
            if (isFramedExceptionAt(bufferView, 0, exceptionTagBytes)) {
              throw parseStreamException(bufferView, exceptionTag);
            }

            const compressedSize = readUInt32LE(bufferView, 17);
            const blockSize = 16 + compressedSize;

            if (streamBuffer.available < blockSize) break;

            const block = bufferView.subarray(0, blockSize);

            try {
              const decompressed = decodeBlock(block);
              const decompressedMatch = splitStreamException(
                decompressed,
                requireLineStart,
                exceptionTag,
              );
              // In-place consume is safe here: nothing aliasing streamBuffer
              // escapes this loop — decodeBlock always returns an independent
              // buffer (including the None method, which copies).
              streamBuffer.consume(blockSize);

              if (decompressedMatch) {
                if (decompressedMatch.prefix.length > 0) {
                  yield decompressedMatch.prefix;
                }
                throw decompressedMatch.error;
              }

              yield decompressed;
            } catch (err: unknown) {
              if (err instanceof ClickHouseException) throw err;
              // A failed decode at the buffer start is usually the exception
              // trailer being mistaken for a block — surface it as the real error.
              if (isFramedExceptionAt(streamBuffer.view, 0, exceptionTagBytes)) {
                throw parseStreamException(streamBuffer.view, exceptionTag);
              }
              const message = err instanceof Error ? err.message : String(err);
              throw new Error(`Block decompression failed: ${message}`);
            }
          }
        }

        // after stream ends: any remaining bytes mean an incomplete compressed
        // block, or the exception trailer left after the last complete block.
        if (streamBuffer.available > 0) {
          if (isFramedExceptionAt(streamBuffer.view, 0, exceptionTagBytes)) {
            throw parseStreamException(streamBuffer.view, exceptionTag);
          }
          throw new Error(
            `Incomplete block: stream ended with ${streamBuffer.available} unparsed bytes and no exception trailer; ` +
              `the server may have closed the connection mid-stream (e.g. send_timeout) — check the server query log`,
          );
        }
      }
    } finally {
      // Cancel releases the lock on response.body so the HTTP connection
      // can be returned to the pool. Safe to call on an already-done stream.
      await reader.cancel().catch(() => {});
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
  return concat(await Array.fromAsync(dataChunks(input)));
}

/**
 * Collect all bytes and decode to a single string.
 *
 * @example
 * const json = await collectText(query("SELECT ...", session, config));
 * const data = JSON.parse(json);
 */
async function collectText(input: QueryInput): Promise<string> {
  return (await Array.fromAsync(decodeText(dataChunks(input)))).join("");
}

/**
 * collect all JSON lines into an array of objects.
 *
 * @example
 * const rows = await collectJsonEachRow<{ id: number }>(query("SELECT ...", session, config));
 */
async function collectJsonEachRow<T = unknown>(input: QueryInput): Promise<T[]> {
  return Array.fromAsync(streamDecodeJsonEachRow<T>(input));
}

export { ClickHouseException } from "./errors.ts";
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
  // Exported for testing
  createSignal as _createSignal,
  findExceptionMarker as _findExceptionMarker,
  parseErrorText as _parseErrorText,
  parseStreamException as _parseStreamException,
};
