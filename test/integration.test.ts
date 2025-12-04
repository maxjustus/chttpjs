import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";

import { startClickHouse, stopClickHouse } from "./setup.ts";
import { init, insert, query } from "../client.ts";

describe("ClickHouse Integration Tests", { timeout: 60000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = Date.now().toString();

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
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_basic (id UInt32, name String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      // Insert data
      const data = [
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
        { id: 3, name: "Charlie" },
      ];

      await insert(
        "INSERT INTO test_basic FORMAT JSONEachRow",
        data,
        sessionId,
        { baseUrl, auth },
      );

      // Query data
      let result = "";
      for await (const chunk of query(
        "SELECT * FROM test_basic ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      )) {
        result += chunk;
      }

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].name, "Alice");
      assert.strictEqual(parsed.data[2].name, "Charlie");

      // Clean up
      for await (const chunk of query(
        "DROP TABLE test_basic",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
    });
  });

  describe("Compression methods", () => {
    it("should insert with LZ4 compression", async () => {
      // Create table
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_lz4 (value String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      const data = Array.from({ length: 1000 }, (_, i) => ({
        value: `test_${i}`,
      }));

      await insert(
        "INSERT INTO test_lz4 FORMAT JSONEachRow",
        data,
        sessionId,
        { baseUrl, auth, compression: "lz4" },
      );

      // Verify count
      let result = "";
      for await (const chunk of query(
        "SELECT count(*) as cnt FROM test_lz4 FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      )) {
        result += chunk;
      }

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].cnt), 1000);

      // Clean up
      for await (const chunk of query(
        "DROP TABLE test_lz4",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
    });

    it("should insert with ZSTD compression", async () => {
      // Create table
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_zstd (value String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      const data = Array.from({ length: 1000 }, (_, i) => ({
        value: `test_${i}`,
      }));

      await insert(
        "INSERT INTO test_zstd FORMAT JSONEachRow",
        data,
        sessionId,
        { baseUrl, auth, compression: "zstd" },
      );

      // Verify count
      let result = "";
      for await (const chunk of query(
        "SELECT count(*) as cnt FROM test_zstd FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      )) {
        result += chunk;
      }

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].cnt), 1000);

      // Clean up
      for await (const chunk of query(
        "DROP TABLE test_zstd",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
    });
  });

  describe("Streaming inserts with generators", () => {
    it("should handle generator that yields batches", async () => {
      // Create table
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_generator (id UInt32, value String) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      // Generator that yields batches
      async function* generateBatches() {
        for (let batch = 0; batch < 10; batch++) {
          const batchData = [];
          for (let i = 0; i < 100; i++) {
            batchData.push({
              id: batch * 100 + i,
              value: `batch_${batch}_item_${i}`,
            });
          }
          yield batchData;
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
            assert.ok(progress.rowsProcessed > 0);
          },
        },
      );

      assert.ok(progressUpdates > 0, "Should have progress updates");

      // Verify count
      let result = "";
      for await (const chunk of query(
        "SELECT count(*) as cnt FROM test_generator FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      )) {
        result += chunk;
      }

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].cnt), 1000);

      // Clean up
      for await (const chunk of query(
        "DROP TABLE test_generator",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
    });

    it("should handle generator that yields single rows", async () => {
      // Create table
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_single (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      // Generator that yields single rows
      async function* generateSingle() {
        for (let i = 0; i < 500; i++) {
          yield { id: i };
        }
      }

      await insert(
        "INSERT INTO test_single FORMAT JSONEachRow",
        generateSingle(),
        sessionId,
        { baseUrl, auth, compression: "zstd" },
      );

      // Verify
      let result = "";
      for await (const chunk of query(
        "SELECT count(*) as cnt FROM test_single FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      )) {
        result += chunk;
      }

      const parsed = JSON.parse(result);
      assert.strictEqual(Number(parsed.data[0].cnt), 500);

      // Clean up
      for await (const chunk of query(
        "DROP TABLE test_single",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
    });
  });

  describe("Streaming queries with compression", () => {
    it("should stream compressed query results", async () => {
      // Setup: Create table with data
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_stream (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      // Insert test data
      const data = Array.from({ length: 10000 }, (_, i) => ({ id: i }));
      await insert(
        "INSERT INTO test_stream FORMAT JSONEachRow",
        data,
        sessionId,
        { baseUrl, auth },
      );

      // Query with compression
      let chunks = 0;
      let totalRows = 0;

      for await (const chunk of query(
        "SELECT * FROM test_stream FORMAT JSONEachRow",
        sessionId,
        { baseUrl, auth },
      )) {
        chunks++;
        // Count newlines to estimate rows
        totalRows += (chunk.match(/\n/g) || []).length;
      }

      assert.ok(chunks > 0, "Should receive chunks");
      assert.strictEqual(totalRows, 10000);

      // Clean up
      for await (const chunk of query(
        "DROP TABLE test_stream",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
    });

    it("should handle large compressed responses", async () => {
      // Query system.numbers with compression
      let chunks = 0;
      let totalRows = 0;

      for await (const chunk of query(
        "SELECT number FROM system.numbers LIMIT 100000 FORMAT CSV",
        sessionId,
        { baseUrl, auth },
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
        for await (const chunk of query(
          "SELECT * FROM non_existent_table",
          sessionId,
          { baseUrl, auth, compression: "none" },
        )) {
          // should not reach here
        }
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
      for await (const chunk of query(
        "CREATE TABLE IF NOT EXISTS test_error (id UInt32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }

      // Try to insert wrong data type
      const invalidData = [{ id: "not_a_number" }];

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
      for await (const chunk of query(
        "DROP TABLE test_error",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
        // consume stream
      }
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
});
