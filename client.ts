import {
  init,
  encodeBlock,
  decodeBlock,
  Method,
  type MethodCode,
} from "./compression.ts";

export type Compression = "lz4" | "zstd" | "none";

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
    case "lz4": return Method.LZ4;
    case "zstd": return Method.ZSTD;
    case "none": return Method.None;
  }
}

// Uint8Array helpers
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function readUInt32LE(arr: Uint8Array, offset: number): number {
  return arr[offset] | (arr[offset + 1] << 8) | (arr[offset + 2] << 16) | (arr[offset + 3] << 24) >>> 0;
}

/**
 * Convert array of row objects to JSONColumns format.
 * Supports heterogeneous rows - missing fields are filled with null.
 */
function rowsToJSONColumns(rows: Record<string, unknown>[]): string {
  const columns: Record<string, unknown[]> = {};
  let rowIndex = 0;

  for (const row of rows) {
    // Add new columns with null backfill for previous rows
    for (const key of Object.keys(row)) {
      if (!(key in columns)) {
        columns[key] = new Array(rowIndex).fill(null);
      }
    }
    // Append values (null for missing keys in this row)
    for (const key of Object.keys(columns)) {
      columns[key].push(key in row ? row[key] : null);
    }
    rowIndex++;
  }

  return JSON.stringify(columns);
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
  rowsProcessed: number;
  complete?: boolean;
}

export type InsertFormat = "JSONEachRow" | "JSONColumns";

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
  /** Data format: "JSONEachRow" (default) or "JSONColumns" */
  format?: InsertFormat;
}

async function insert(
  query: string,
  data: any[] | AsyncIterable<any> | Iterable<any>,
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
    format = "JSONEachRow",
  } = options;
  const method = compressionToMethod(compression);

  const isGenerator =
    data &&
    (typeof (data as any)[Symbol.asyncIterator] === "function" ||
      typeof (data as any)[Symbol.iterator] === "function");

  const params: Record<string, string> = {
    session_id: sessionId,
    query: query,
    decompress: "1",
  };

  // Helper to serialize rows based on format
  function serializeRows(rows: Record<string, unknown>[]): string {
    if (format === "JSONColumns") {
      return rowsToJSONColumns(rows);
    }
    return rows.map((d) => JSON.stringify(d)).join("\n") + "\n";
  }

  if (!isGenerator) {
    // Array implementation
    const dataStr = serializeRows(data as Record<string, unknown>[]);
    const dataBytes = encoder.encode(dataStr);
    const compressed = encodeBlock(dataBytes, method);

    const url = buildReqUrl(baseUrl, params, options.auth);

    const response = await fetch(url.toString(), {
      method: "POST",
      headers: {
        "Content-Type": "application/octet-stream",
      },
      body: compressed,
      signal: createSignal(options.signal, options.timeout),
    });

    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Insert failed: ${response.status} - ${body}`);
    }
    return body;
  }

  // Streaming implementation for generators
  const url = buildReqUrl(baseUrl, params, options.auth);

  let totalRows = 0;
  let blocksSent = 0;
  let totalCompressed = 0;
  let totalUncompressed = 0;

  // Stream compressed blocks directly to fetch
  const stream = new ReadableStream({
    async start(controller) {
      try {
        // For JSONColumns, accumulate rows; for JSONEachRow, accumulate bytes
        let rowBuffer: Record<string, unknown>[] = [];
        let byteBuffer = new Uint8Array(bufferSize);
        let byteBufferLen = 0;

        function flushBuffer() {
          let dataStr: string;
          let uncompressedSize: number;

          if (format === "JSONColumns") {
            dataStr = rowsToJSONColumns(rowBuffer);
            const dataBytes = encoder.encode(dataStr);
            uncompressedSize = dataBytes.length;
            const compressed = encodeBlock(dataBytes, method);
            controller.enqueue(compressed);
            totalCompressed += compressed.length;
            rowBuffer = [];
          } else {
            const compressed = encodeBlock(byteBuffer.subarray(0, byteBufferLen), method);
            controller.enqueue(compressed);
            uncompressedSize = byteBufferLen;
            totalCompressed += compressed.length;
            byteBufferLen = 0;
          }

          blocksSent++;
          totalUncompressed += uncompressedSize;

          if (onProgress) {
            onProgress({
              blocksSent,
              bytesCompressed: totalCompressed,
              bytesUncompressed: uncompressedSize,
              rowsProcessed: totalRows,
            });
          }
        }

        for await (const rows of data) {
          const rowsArray = Array.isArray(rows) ? rows : [rows];

          for (const row of rowsArray) {
            totalRows++;

            if (format === "JSONColumns") {
              rowBuffer.push(row);
              // Estimate size: ~100 bytes per row as heuristic
              if (rowBuffer.length * 100 >= threshold) {
                flushBuffer();
              }
            } else {
              const line = JSON.stringify(row) + "\n";

              // Ensure capacity (line.length * 3 covers worst-case UTF-8 expansion)
              if (byteBufferLen + line.length * 3 > byteBuffer.length) {
                const newSize = Math.max(byteBuffer.length * 2, byteBufferLen + line.length * 3);
                const newBuffer = new Uint8Array(newSize);
                newBuffer.set(byteBuffer.subarray(0, byteBufferLen));
                byteBuffer = newBuffer;
              }

              const { written } = encoder.encodeInto(line, byteBuffer.subarray(byteBufferLen));
              byteBufferLen += written;

              if (byteBufferLen >= threshold) {
                flushBuffer();
              }
            }
          }
        }

        // Send remaining data
        const hasRemaining = format === "JSONColumns" ? rowBuffer.length > 0 : byteBufferLen > 0;
        if (hasRemaining) {
          let uncompressedSize: number;
          if (format === "JSONColumns") {
            const dataStr = rowsToJSONColumns(rowBuffer);
            const dataBytes = encoder.encode(dataStr);
            uncompressedSize = dataBytes.length;
            const compressed = encodeBlock(dataBytes, method);
            controller.enqueue(compressed);
            totalCompressed += compressed.length;
          } else {
            const compressed = encodeBlock(byteBuffer.subarray(0, byteBufferLen), method);
            controller.enqueue(compressed);
            uncompressedSize = byteBufferLen;
            totalCompressed += compressed.length;
          }

          blocksSent++;
          totalUncompressed += uncompressedSize;

          if (onProgress) {
            onProgress({
              blocksSent,
              bytesCompressed: totalCompressed,
              bytesUncompressed: uncompressedSize,
              rowsProcessed: totalRows,
              complete: true,
            });
          }
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
    },
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
): AsyncGenerator<string, void, unknown> {
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
      yield decoder.decode(value, { stream: true });
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
          yield decoder.decode(decompressed);
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

export { init, insert, query, buildReqUrl };
