import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";

import { startClickHouse, stopClickHouse } from "./setup.ts";
import {
  init,
  insert,
  query,
  streamEncodeJsonEachRow,
  streamText,
  collectText,
} from "../client.ts";
import { consume, generateSessionId } from "./test_utils.ts";

const encoder = new TextEncoder();

describe("ClickHouse Integration Tests", { timeout: 60000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = generateSessionId("integration");

  before(async () => {
    await init();
    clickhouse = await startClickHouse();
    baseUrl = clickhouse.url + "/";
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  describe("Basic operations", () => {
    it("should create and query a table", async () => {
      // Create table
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_basic (id UInt32, name String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      // Insert data using streamEncodeJsonEachRow helper
      const data = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ];

      await insert(
        "INSERT INTO test_basic FORMAT JSONEachRow",
        streamEncodeJsonEachRow(data),
        sessionId,
        { baseUrl, auth },
      );

      // Query data
      const result = await collectText(
        query("SELECT * FROM test_basic ORDER BY id FORMAT JSON", sessionId, {
          baseUrl,
          auth,
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].name, "Alice");
      assert.strictEqual(parsed.data[2].name, "Charlie");

      // Clean up
      await consume(query("DROP TABLE test_basic", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });
  });

  describe("Compression methods", () => {
    for (const compression of ["none", "lz4", "zstd"] as const) {
      it(`should insert with ${compression} compression`, async () => {
        await consume(query(
          "CREATE TABLE IF NOT EXISTS test_compression (value String) ENGINE = Memory",
          sessionId,
          { baseUrl, auth, compression },
        ));

        const rows = Array.from({ length: 1000 }, (_, i) => ({
          value: `test_${i}`,
        }));
        const data = encoder.encode(
          rows.map((r) => JSON.stringify(r)).join("\n") + "\n",
        );

        await insert(`INSERT INTO test_compression FORMAT JSONEachRow`, data, sessionId, {
          baseUrl,
          auth,
          compression,
        });

        const result = await collectText(
          query("SELECT count(*) as cnt FROM test_compression FORMAT JSON", sessionId, {
            baseUrl,
            auth,
          }),
        );

        const parsed = JSON.parse(result);
        assert.strictEqual(Number(parsed.data[0].cnt), 1000);

        await consume(query("DROP TABLE test_compression", sessionId, {
          baseUrl,
          auth,
          compression,
        }));
      });
    }
  });

  describe("Streaming inserts with generators", () => {
    it("should handle generator that yields batches", async () => {
      // Create table
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_generator (id UInt32, value String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      // Generator that yields byte batches
      async function* generateBatches() {
        for (let batch = 0; batch < 10; batch++) {
          const batchData: { id: number, value: string }[] = [];
          for (let i = 0; i < 100; i++) {
            batchData.push({
              id: batch * 100 + i,
              value: `batch_${batch}_item_${i}`,
            });
          }

          yield encoder.encode(
            batchData.map((r) => JSON.stringify(r)).join("\n") + "\n",
          );
        }
      }

      let progressUpdates = 0;
      await insert(
        "INSERT INTO test_generator FORMAT JSONEachRow",
        generateBatches(),
        sessionId,
        {
          baseUrl,
          auth,
          compression: "lz4",
          onProgress: (progress) => {
            progressUpdates++;
            assert.ok(progress.bytesUncompressed > 0);
          },
        },
      );

      assert.ok(progressUpdates > 0, "Should have progress updates");

      // Verify count
      const result = await collectText(
        query(
          "SELECT count(*) as cnt FROM test_generator FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].cnt), 1000);

      // Clean up
      await consume(query("DROP TABLE test_generator", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });

    it("should handle generator that yields single rows", async () => {
      // Create table
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_single (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      // Use streamEncodeJsonEachRow with async generator
      async function* generateSingle() {
        for (let i = 0; i < 500; i++) {
          yield { id: i };
        }
      }

      await insert(
        "INSERT INTO test_single FORMAT JSONEachRow",
        streamEncodeJsonEachRow(generateSingle()),
        sessionId,
        { baseUrl, auth, compression: "zstd" },
      );

      // Verify
      const result = await collectText(
        query(
          "SELECT count(*) as cnt FROM test_single FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].cnt), 500);

      // Clean up
      await consume(query("DROP TABLE test_single", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });
  });

  describe("Streaming queries with compression", () => {
    it("should stream compressed query results", async () => {
      // Setup: Create table with data
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_stream (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      // Insert test data
      const rows = Array.from({ length: 10000 }, (_, i) => ({ id: i }));
      await insert(
        "INSERT INTO test_stream FORMAT JSONEachRow",
        streamEncodeJsonEachRow(rows),
        sessionId,
        { baseUrl, auth },
      );

      // Query with compression
      let chunks = 0;
      let totalRows = 0;

      for await (const chunk of streamText(
        query("SELECT * FROM test_stream FORMAT JSONEachRow", sessionId, {
          baseUrl,
          auth,
        }),
      )) {
        chunks++;
        // Count newlines to estimate rows
        totalRows += (chunk.match(/\n/g) || []).length;
      }

      assert.ok(chunks > 0, "Should receive chunks");
      assert.strictEqual(totalRows, 10000);

      // Clean up
      await consume(query("DROP TABLE test_stream", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });

    it("should handle large compressed responses", async () => {
      // Query system.numbers with compression
      let chunks = 0;
      let totalRows = 0;

      for await (const chunk of streamText(
        query(
          "SELECT number FROM system.numbers LIMIT 100000 FORMAT CSV",
          sessionId,
          { baseUrl, auth },
        ),
      )) {
        chunks++;
        // Count actual data rows (CSV format, one number per line)
        const lines = chunk.split("\n").filter((line) => line.trim() !== "");
        totalRows += lines.length;
      }

      assert.ok(chunks > 0, "Should receive chunks");
      assert.strictEqual(totalRows, 100000);
    });
  });

  describe("Error handling", () => {
    it("should handle invalid queries", async () => {
      try {
        await consume(query(
          "SELECT * FROM non_existent_table",
          sessionId,
          { baseUrl, auth, compression: "none" },
        ));
        assert.fail("Should have thrown an error");
      } catch (err) {
        const error = err as Error;
        assert.ok(
          error.message.includes("UNKNOWN_TABLE") ||
          error.message.includes("doesn't exist"),
        );
      }
    });

    it("should handle insert errors", async () => {
      // Create table with specific schema
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_error (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      // Try to insert wrong data type
      const invalidData = encoder.encode(
        JSON.stringify({ id: "not_a_number" }) + "\n",
      );

      try {
        await insert(
          "INSERT INTO test_error FORMAT JSONEachRow",
          invalidData,
          sessionId,
          { baseUrl, auth },
        );
        assert.fail("Should have thrown an error");
      } catch (err) {
        const error = err as Error;
        assert.ok(
          error.message.includes("TYPE_MISMATCH") ||
          error.message.includes("Cannot parse"),
        );
      }

      // Clean up
      await consume(query("DROP TABLE test_error", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });
  });

  describe("Streaming error scenarios", () => {
    it("should handle generator that throws mid-stream", async () => {
      // Create table
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_stream_error (id UInt32, value String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      // Generator that throws after some items
      async function* errorGenerator() {
        for (let i = 0; i < 100; i++) {
          yield encoder.encode(
            JSON.stringify({ id: i, value: `value_${i}` }) + "\n",
          );
        }
        throw new Error("Generator error mid-stream");
      }

      try {
        await insert(
          "INSERT INTO test_stream_error FORMAT JSONEachRow",
          errorGenerator(),
          sessionId,
          { baseUrl, auth, bufferSize: 128 },
        );
        assert.fail("Should have thrown an error");
      } catch (err) {
        const error = err as Error;
        // Error should be thrown (exact message may vary based on stream error handling)
        assert.ok(error instanceof Error, "Should throw an error");
      }

      // Clean up
      await consume(query(
        "DROP TABLE test_stream_error",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));
    });

    it("should handle AbortSignal cancellation", async () => {
      // Create table
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_abort (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      const controller = new AbortController();

      // Generator that yields many items
      async function* slowGenerator() {
        for (let i = 0; i < 100000; i++) {
          yield encoder.encode(JSON.stringify({ id: i }) + "\n");
          // Abort after first few items
          if (i === 10) {
            controller.abort();
          }
        }
      }

      try {
        await insert(
          "INSERT INTO test_abort FORMAT JSONEachRow",
          slowGenerator(),
          sessionId,
          { baseUrl, auth, signal: controller.signal },
        );
        assert.fail("Should have aborted");
      } catch (err) {
        const error = err as Error;
        assert.ok(
          error.name === "AbortError" ||
          error.message.includes("abort") ||
          error.message.includes("cancelled"),
        );
      }

      // Clean up
      await consume(query("DROP TABLE test_abort", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });

    it("should fire progress callbacks during compression", async () => {
      // Create table
      await consume(query(
        "CREATE TABLE IF NOT EXISTS test_progress (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      ));

      const progressEvents: any[] = [];
      let insertComplete = false;

      async function* dataGenerator() {
        for (let i = 0; i < 1000; i++) {
          yield encoder.encode(JSON.stringify({ id: i }) + "\n");
        }
      }

      const insertPromise = insert(
        "INSERT INTO test_progress FORMAT JSONEachRow",
        dataGenerator(),
        sessionId,
        {
          baseUrl,
          auth,
          bufferSize: 1024,
          threshold: 512,
          onProgress: (progress) => {
            progressEvents.push({ ...progress, insertComplete });
          },
        },
      );

      await insertPromise;
      insertComplete = true;

      // Verify progress callbacks were fired
      assert.ok(progressEvents.length > 0, "Should have progress events");

      // Verify all progress events fired before insert completed
      const allBeforeComplete = progressEvents.every((e) => !e.insertComplete);
      assert.ok(
        allBeforeComplete,
        "Progress events should fire during compression, not after",
      );

      // Clean up
      await consume(query("DROP TABLE test_progress", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      }));
    });
  });

  describe("Multi-block responses", () => {
    it("should handle multiple compressed blocks in response", async () => {
      // This test verifies our multi-block decompression
      // by forcing ClickHouse to send multiple blocks

      // Note: We can't easily control max_block_size in the response
      // without modifying query params, but we can verify the mechanism
      // works with large result sets

      let blocksDetected = 0;
      let lastChunkSize = 0;

      for await (const chunk of query(
        "SELECT * FROM system.numbers LIMIT 1000000",
        sessionId,
        { baseUrl, auth },
      )) {
        // Each chunk is a decompressed block
        if (chunk.length !== lastChunkSize) {
          blocksDetected++;
          lastChunkSize = chunk.length;
        }
      }

      console.log(`    Detected ${blocksDetected} different block sizes`);
      assert.ok(blocksDetected >= 1, "Should process at least one block");
    });
  });

  describe("Query parameters", () => {
    it("should use query parameters with UInt64", async () => {
      const result = await collectText(
        query("SELECT {value:UInt64} as v FORMAT JSON", sessionId, {
          baseUrl,
          auth,
          params: { value: 42 },
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].v), 42);
    });

    it("should use query parameters with String", async () => {
      const result = await collectText(
        query("SELECT {name:String} as s FORMAT JSON", sessionId, {
          baseUrl,
          auth,
          params: { name: "hello world" },
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data[0].s, "hello world");
    });

    it("should use query parameters with multiple values", async () => {
      const result = await collectText(
        query(
          "SELECT {a:UInt32} + {b:UInt32} as sum, {msg:String} as msg FORMAT JSON",
          sessionId,
          {
            baseUrl,
            auth,
            params: { a: 10, b: 32, msg: "test" },
          },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].sum), 42);
      assert.strictEqual(parsed.data[0].msg, "test");
    });

    it("should use query parameters with BigInt", async () => {
      // Use JSONStringsEachRow to preserve precision for large integers
      const result = await collectText(
        query("SELECT {big:UInt64} as v FORMAT JSONStringsEachRow", sessionId, {
          baseUrl,
          auth,
          params: { big: 9007199254740993n },
        }),
      );

      const parsed = JSON.parse(result.trim());
      assert.strictEqual(parsed.v, "9007199254740993");
    });
  });
});
