import {
  encodeBlock,
  decodeBlock,
  Method,
  type MethodCode,
} from "./compression.ts";
import http from "node:http";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

interface AuthConfig {
  username?: string;
  password?: string;
}

function buildReqUrl(baseUrl: string, params: Record<string, string>, auth?: AuthConfig): URL {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, value); // Don't double-encode
  });

  // Add basic auth if provided
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
  bufferSize?: number;
  threshold?: number;
  onProgress?: (progress: ProgressInfo) => void;
  auth?: AuthConfig;
}

async function insertCompressed(
  query: string,
  data: any[] | AsyncIterable<any> | Iterable<any>,
  sessionId: string,
  method: MethodCode = Method.LZ4,
  options: InsertOptions = {},
): Promise<string> {
  const baseUrl = options.baseUrl || "http://localhost:8123/";
  const {
    bufferSize = 256 * 1024, // 256KB like clickhouse-rs
    threshold = bufferSize - 2048, // Leave room for last row
    onProgress = null,
  } = options;

  // Check if data is a generator/async iterable
  const isGenerator =
    data &&
    (typeof (data as any)[Symbol.asyncIterator] === "function" ||
      typeof (data as any)[Symbol.iterator] === "function");

  if (!isGenerator) {
    // Original array implementation
    const dataStr = (data as any[]).map((d: any) => JSON.stringify(d)).join("\n") + "\n";
    const dataBytes = Buffer.from(dataStr, "utf8");

    // Compress using ClickHouse format
    const compressed = encodeBlock(dataBytes, method);

    const methodName =
      method === Method.LZ4 ? "LZ4" : method === Method.ZSTD ? "ZSTD" : "None";
    console.log(`Compression: ${methodName}`);
    console.log("Original size:", dataBytes.length);
    console.log("Compressed size:", compressed.length);
    console.log(
      "Compression ratio:",
      (dataBytes.length / compressed.length).toFixed(2) + "x",
    );

    const url = buildReqUrl(
      baseUrl,
      {
        session_id: sessionId,
        query: query,
        decompress: "1",
        http_native_compression_disable_checksumming_on_decompress: "1",
      },
      options.auth,
    );

    return new Promise((resolve, reject) => {
      const req = http.request(
        url,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/octet-stream",
            "Content-Length": compressed.length,
          },
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => (body += chunk));
          res.on("end", () => {
            if (res.statusCode !== 200) {
              reject(new Error(`Insert failed: ${res.statusCode} - ${body}`));
            } else {
              resolve(body);
            }
          });
        },
      );

      req.on("error", reject);
      req.write(compressed);
      req.end();
    });
  }

  // Streaming implementation for generators
  const url = buildReqUrl(
    baseUrl,
    {
      session_id: sessionId,
      query: query,
      decompress: "1",
      http_native_compression_disable_checksumming_on_decompress: "1",
    },
    options.auth,
  );

  return new Promise((resolve, reject) => {
    const req = http.request(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/octet-stream",
        "Transfer-Encoding": "chunked",
      },
    });

    let buffer: string[] = [];
    let bufferBytes = 0;
    let totalRows = 0;
    let blocksSent = 0;
    let totalCompressed = 0;
    let totalUncompressed = 0;

    req.on("response", (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        if (res.statusCode !== 200) {
          reject(new Error(`Insert failed: ${res.statusCode} - ${body}`));
        } else {
          console.log(`Streamed ${blocksSent} blocks, ${totalRows} rows`);
          console.log(
            `Total compression ratio: ${(totalUncompressed / totalCompressed).toFixed(2)}x`,
          );
          resolve(body);
        }
      });
    });

    req.on("error", reject);

    async function processStream() {
      try {
        for await (const rows of data) {
          // Handle both single row and array of rows
          const rowsArray = Array.isArray(rows) ? rows : [rows];

          for (const row of rowsArray) {
            const line = JSON.stringify(row) + "\n";
            buffer.push(line);
            bufferBytes += Buffer.byteLength(line);
            totalRows++;

            if (bufferBytes >= threshold) {
              // Compress and send this chunk
              const dataBytes = Buffer.from(buffer.join(""), "utf8");
              const compressed = encodeBlock(dataBytes, method);

              req.write(compressed);
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

              // Reset buffer
              buffer = [];
              bufferBytes = 0;
            }
          }
        }

        // Send remaining data
        if (buffer.length > 0) {
          const dataBytes = Buffer.from(buffer.join(""), "utf8");
          const compressed = encodeBlock(dataBytes, method);
          req.write(compressed);
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

        req.end();
      } catch (error: unknown) {
        req.destroy();
        reject(error);
      }
    }

    processStream();
  });
}

interface QueryOptions {
  baseUrl?: string;
  auth?: AuthConfig;
}

async function* execQuery(
  query: string,
  sessionId: string,
  compressed: boolean = false,
  options: QueryOptions = {},
): AsyncGenerator<string, void, unknown> {
  const baseUrl = options.baseUrl || "http://localhost:8123/";
  const params: Record<string, string> = {
    session_id: sessionId,
    default_format: "JSONEachRowWithProgress",
  };

  if (compressed) {
    params.compress = "1"; // Request compressed response
  }

  const url = buildReqUrl(baseUrl, params, options.auth);

  const stream = await new Promise<http.IncomingMessage>((resolve, reject) => {
    const req = http.request(
      url,
      {
        method: "POST",
      },
      (res) => {
        if (res.statusCode !== 200) {
          // Error handling - need to buffer error response
          const chunks: Buffer[] = [];
          res.on("data", (chunk) => chunks.push(chunk));
          res.on("end", () => {
            const body = Buffer.concat(chunks);
            reject(
              new Error(`Query failed: ${res.statusCode} - ${body.toString()}`),
            );
          });
          return;
        }

        resolve(res);
      },
    );

    req.on("error", reject);
    req.write(query);
    req.end();
  });

  if (!compressed) {
    // For non-compressed, stream data directly
    for await (const chunk of stream) {
      yield chunk.toString();
    }
  } else {
    // For compressed, decompress blocks as they arrive
    let buffer = Buffer.alloc(0);

    for await (const chunk of stream) {
      buffer = Buffer.concat([buffer, chunk]);

      // Process complete blocks from buffer
      while (buffer.length >= 25) {
        // Check if we have enough data for a complete block
        if (buffer.length < 17) break;

        // Read compressed size to know block size
        const compressedSize = buffer.readUInt32LE(17);
        const blockSize = 16 + compressedSize;

        if (buffer.length < blockSize) break;

        // Extract and decompress block
        const block = buffer.slice(0, blockSize);
        buffer = buffer.slice(blockSize);

        try {
          const decompressed = decodeBlock(block, true);
          yield decompressed.toString();
        } catch (err: unknown) {
          const message = err instanceof Error ? err.message : String(err);
          throw new Error(`Block decompression failed: ${message}`);
        }
      }
    }

    // Process any remaining complete blocks
    while (buffer.length >= 25) {
      if (buffer.length < 17) break;
      const compressedSize = buffer.readUInt32LE(17);
      const blockSize = 16 + compressedSize;
      if (buffer.length < blockSize) break;

      const block = buffer.slice(0, blockSize);
      buffer = buffer.slice(blockSize);

      const decompressed = decodeBlock(block, true);
      yield decompressed.toString();
    }
  }
}

async function main() {
  const sessionId = "12345";

  try {
    // Create table (drop first to ensure clean state)
    console.log("Creating table...");
    for await (const chunk of execQuery(
      "DROP TABLE IF EXISTS test",
      sessionId,
    )) {
      // Just consume the stream
    }
    for await (const chunk of execQuery(
      "CREATE TABLE test (hello String) ENGINE = Memory",
      sessionId,
    )) {
      // Just consume the stream
    }

    // Test data
    let data: Array<{ hello: string }> = [];
    for (let i = 0; i < 10000; i++) {
      data.push({ hello: "world" + i });
    }

    // Test LZ4 compression
    console.log("\n=== Testing LZ4 Compression ===");
    const insertQuery = "INSERT INTO test (hello) FORMAT JSONEachRow";
    await insertCompressed(
      insertQuery,
      data.slice(0, 5000),
      sessionId,
      Method.LZ4,
    );
    console.log("LZ4 insert successful!");

    // Test ZSTD compression
    console.log("\n=== Testing ZSTD Compression ===");
    await insertCompressed(
      insertQuery,
      data.slice(5000),
      sessionId,
      Method.ZSTD,
    );
    console.log("ZSTD insert successful!");

    // Query to verify
    console.log("\nQuerying to verify inserts...");
    let countResult = "";
    for await (const chunk of execQuery(
      "SELECT count(*) as cnt FROM test FORMAT JSON",
      sessionId,
    )) {
      countResult += chunk;
    }
    const countData = JSON.parse(countResult);
    console.log("Total rows inserted:", countData.data[0].cnt);

    // Test compressed response
    console.log("\nQuerying with compressed response...");
    let compressedResult = "";
    for await (const chunk of execQuery(
      "SELECT count(*) as cnt FROM test FORMAT JSON",
      sessionId,
      true,
    )) {
      compressedResult += chunk;
    }
    const compressedData = JSON.parse(compressedResult);
    console.log(
      "Total rows (from compressed response):",
      compressedData.data[0].cnt,
    );

    let selectResult = "";
    for await (const chunk of execQuery(
      "SELECT * FROM test FORMAT JSON",
      sessionId,
      true,
    )) {
      selectResult += chunk;
    }
    const selectData = JSON.parse(selectResult);
    const matches = selectData.data.every(
      (row: any, index: number) => row.hello === "world" + index,
    );
    console.log("rows match expected:", matches);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Error:", message);
  }
}

// Export functions for use in tests
export { insertCompressed, execQuery, buildReqUrl };

// Only run main if this is the main module
const isDirectExecution =
  process.argv[1] !== undefined &&
  resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isDirectExecution) {
  void main();
}
