import {
  init,
  encodeBlock,
  decodeBlock,
  Method,
  type MethodCode,
} from "./compression.ts";

export type Compression = "lz4" | "zstd" | "none";

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

function concat(arrays: Uint8Array[]): Uint8Array {
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
  return arr[offset] | (arr[offset + 1] << 8) | (arr[offset + 2] << 16) | (arr[offset + 3] << 24) >>> 0;
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

interface InsertOptions {
  baseUrl?: string;
  /** Compression method: "lz4" (default), "zstd", or "none" */
  compression?: Compression;
  /** Size in bytes for the compression buffer (default: 256KB) */
  bufferSize?: number;
  /** Byte threshold to trigger compression flush (default: bufferSize - 2048) */
  threshold?: number;
  onProgress?: (progress: ProgressInfo) => void;
  auth?: AuthConfig;
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
    bufferSize = 256 * 1024,
    threshold = bufferSize - 2048,
    onProgress = null,
  } = options;
  const method = compressionToMethod(compression);

  const isGenerator =
    data &&
    (typeof (data as any)[Symbol.asyncIterator] === "function" ||
      typeof (data as any)[Symbol.iterator] === "function");

  if (!isGenerator) {
    // Array implementation
    const dataStr = (data as any[]).map((d: any) => JSON.stringify(d)).join("\n") + "\n";
    const dataBytes = encoder.encode(dataStr);
    const compressed = encodeBlock(dataBytes, method);

    const url = buildReqUrl(
      baseUrl,
      {
        session_id: sessionId,
        query: query,
        decompress: "1",
      },
      options.auth,
    );

    const response = await fetch(url.toString(), {
      method: "POST",
      headers: {
        "Content-Type": "application/octet-stream",
      },
      body: compressed,
    });

    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Insert failed: ${response.status} - ${body}`);
    }
    return body;
  }

  // Streaming implementation for generators
  const url = buildReqUrl(
    baseUrl,
    {
      session_id: sessionId,
      query: query,
      decompress: "1",
    },
    options.auth,
  );

  let buffer: string[] = [];
  let bufferBytes = 0;
  let totalRows = 0;
  let blocksSent = 0;
  let totalCompressed = 0;
  let totalUncompressed = 0;

  // Collect all compressed blocks
  const blocks: Uint8Array[] = [];

  for await (const rows of data) {
    const rowsArray = Array.isArray(rows) ? rows : [rows];

    for (const row of rowsArray) {
      const line = JSON.stringify(row) + "\n";
      buffer.push(line);
      bufferBytes += encoder.encode(line).length;
      totalRows++;

      if (bufferBytes >= threshold) {
        const dataBytes = encoder.encode(buffer.join(""));
        const compressed = encodeBlock(dataBytes, method);

        blocks.push(compressed);
        blocksSent++;
        totalCompressed += compressed.length;
        totalUncompressed += bufferBytes;

        if (onProgress) {
          onProgress({
            blocksSent,
            bytesCompressed: compressed.length,
            bytesUncompressed: bufferBytes,
            rowsProcessed: totalRows,
          });
        }

        buffer = [];
        bufferBytes = 0;
      }
    }
  }

  // Send remaining data
  if (buffer.length > 0) {
    const dataBytes = encoder.encode(buffer.join(""));
    const compressed = encodeBlock(dataBytes, method);
    blocks.push(compressed);
    blocksSent++;
    totalCompressed += compressed.length;
    totalUncompressed += bufferBytes;

    if (onProgress) {
      onProgress({
        blocksSent,
        bytesCompressed: compressed.length,
        bytesUncompressed: bufferBytes,
        rowsProcessed: totalRows,
        complete: true,
      });
    }
  }

  // Combine all blocks and send
  const allData = concat(blocks);

  const response = await fetch(url.toString(), {
    method: "POST",
    headers: {
      "Content-Type": "application/octet-stream",
    },
    body: allData,
  });

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
    let buffer = new Uint8Array(0);

    while (true) {
      const { done, value } = await reader.read();

      if (value) {
        buffer = concat([buffer, value]);
      }

      // Process complete blocks from buffer
      while (buffer.length >= 25) {
        if (buffer.length < 17) break;

        const compressedSize = readUInt32LE(buffer, 17);
        const blockSize = 16 + compressedSize;

        if (buffer.length < blockSize) break;

        const block = buffer.subarray(0, blockSize);
        buffer = buffer.subarray(blockSize);

        try {
          const decompressed = decodeBlock(block);
          yield decoder.decode(decompressed);
        } catch (err: unknown) {
          const message = err instanceof Error ? err.message : String(err);
          throw new Error(`Block decompression failed: ${message}`);
        }
      }

      if (done) break;
    }

    // Process any remaining complete blocks
    while (buffer.length >= 25) {
      if (buffer.length < 17) break;
      const compressedSize = readUInt32LE(buffer, 17);
      const blockSize = 16 + compressedSize;
      if (buffer.length < blockSize) break;

      const block = buffer.subarray(0, blockSize);
      buffer = buffer.subarray(blockSize);

      const decompressed = decodeBlock(block);
      yield decoder.decode(decompressed);
    }
  }
}

export { init, insert, query, buildReqUrl };
