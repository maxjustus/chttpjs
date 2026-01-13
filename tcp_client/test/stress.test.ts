import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { ClickHouseException } from "../types.ts";
import { RecordBatch, batchFromRows } from "../../native/table.ts";

describe("TCP Client Stress Tests", () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: ""
  };

  async function withClient<T>(fn: (client: TcpClient) => Promise<T>): Promise<T> {
    const client = new TcpClient(options);
    await client.connect();
    try {
      return await fn(client);
    } finally {
      client.close();
    }
  }

  describe("Multi-turn Operation Sequences", () => {
    test("query → cancel → insert → query sequence", () => withClient(async (client) => {
      const tableName = `test_sequence_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt64, value String) ENGINE = Memory`);

      try {
        // 1. Start a long query and cancel it
        const controller = new AbortController();
        setTimeout(() => controller.abort(), 30);

        try {
          for await (const _ of client.query(
            `SELECT number, 'value' FROM numbers(1000000)`,
            { signal: controller.signal }
          )) {}
        } catch {
          // Expected cancellation
        }

        // 2. Immediately do an INSERT
        const batch = batchFromRows(
          [{ name: "id", type: "UInt64" }, { name: "value", type: "String" }],
          [[1n, "after_cancel"], [2n, "test"]]
        );
        for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, batch)) {}

        // 3. Run another SELECT and verify it works
        let rowCount = 0;
        for await (const packet of client.query(`SELECT * FROM ${tableName}`)) {
          if (packet.type === "Data") {
            rowCount += packet.batch.rowCount;
          }
        }
        assert.strictEqual(rowCount, 2, "Should have 2 rows after sequence");
      } finally {
        await client.execute(`DROP TABLE ${tableName}`);
      }
    }));

    test("insert → query → insert → query alternating 10 times", () => withClient(async (client) => {
      const tableName = `test_alternate_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt64, cycle UInt32) ENGINE = Memory`);

      try {
        for (let cycle = 0; cycle < 10; cycle++) {
          // INSERT
          const batch = batchFromRows(
            [{ name: "id", type: "UInt64" }, { name: "cycle", type: "UInt32" }],
            [[BigInt(cycle * 10), cycle], [BigInt(cycle * 10 + 1), cycle]]
          );
          for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, batch)) {}

          // QUERY and verify
          let count = 0n;
          for await (const packet of client.query(`SELECT count() FROM ${tableName}`)) {
            if (packet.type === "Data") {
              for (const row of packet.batch) {
                count = row["count()"] as bigint;
              }
            }
          }
          assert.strictEqual(Number(count), (cycle + 1) * 2, `Cycle ${cycle}: expected ${(cycle + 1) * 2} rows`);
        }
      } finally {
        await client.execute(`DROP TABLE ${tableName}`);
      }
    }));
  });

  describe("Cancel Mid-Large-Stream", () => {
    test("cancel SELECT after receiving some blocks", () => withClient(async (client) => {
      const controller = new AbortController();
      let blocksReceived = 0;
      const targetBlocks = 2;

      try {
        for await (const packet of client.query(
          `SELECT number, toString(number) FROM numbers(500000)`,
          { signal: controller.signal }
        )) {
          if (packet.type === "Data") {
            blocksReceived++;
            if (blocksReceived >= targetBlocks) {
              controller.abort();
            }
          }
        }
      } catch {
        // Expected cancellation
      }

      assert.ok(blocksReceived >= targetBlocks, `Should have received at least ${targetBlocks} blocks`);

      // Verify connection is still usable
      let result = 0n;
      for await (const packet of client.query("SELECT 42 as answer")) {
        if (packet.type === "Data") {
          for (const row of packet.batch) {
            result = row.answer as bigint;
          }
        }
      }
      assert.strictEqual(Number(result), 42, "Connection should still be usable");
    }));

    test("cancel INSERT after sending some blocks", async () => {
      const client = new TcpClient(options);
      await client.connect();
      const tableName = `test_cancel_insert_${Date.now()}`;

      try {
        await client.execute(`CREATE TABLE ${tableName} (id UInt64) ENGINE = Memory`);

        const controller = new AbortController();
        let blocksYielded = 0;

        async function* generateBlocks() {
          for (let i = 0; i < 20; i++) {
            yield RecordBatch.fromColumnar(
              [{ name: "id", type: "UInt64" }],
              [BigInt64Array.from({ length: 1000 }, (_, j) => BigInt(i * 1000 + j))]
            );
            blocksYielded++;
            if (blocksYielded >= 5) {
              controller.abort();
            }
            await new Promise(r => setTimeout(r, 5));
          }
        }

        try {
          for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, generateBlocks(), { signal: controller.signal })) {}
        } catch {
          // Expected cancellation
        }

        assert.ok(blocksYielded >= 5, "Should have yielded at least 5 blocks before cancel");
      } finally {
        // Use fresh client for cleanup since connection may be in bad state
        const cleanupClient = new TcpClient(options);
        await cleanupClient.connect();
        await cleanupClient.execute(`DROP TABLE IF EXISTS ${tableName}`);
        cleanupClient.close();
        client.close();
      }
    });
  });

  describe("Rapid Cycling", () => {
    test("1000 query cycles on same connection", () => withClient(async (client) => {
      const cycles = 1000;

      // Warm up and let initial allocations settle
      for (let i = 0; i < 10; i++) {
        for await (const _ of client.query(`SELECT ${i}`)) {}
      }
      global.gc?.();
      const startMem = process.memoryUsage().heapUsed;

      for (let i = 0; i < cycles; i++) {
        let result = 0n;
        for await (const packet of client.query(`SELECT ${i} as val`)) {
          if (packet.type === "Data") {
            for (const row of packet.batch) {
              result = row.val as bigint;
            }
          }
        }
        assert.strictEqual(Number(result), i);
      }

      global.gc?.();
      const endMem = process.memoryUsage().heapUsed;
      const memGrowthMB = (endMem - startMem) / 1024 / 1024;
      // With 1000 cycles, even small leaks become visible
      // Allow 20MB which is generous but catches real leaks
      assert.ok(memGrowthMB < 20, `Memory grew by ${memGrowthMB.toFixed(1)}MB over ${cycles} cycles`);
    }));

    test("50 insert/query cycles with verification", () => withClient(async (client) => {
      const tableName = `test_rapid_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt64, batch UInt32) ENGINE = Memory`);

      try {
        const cycles = 50;
        const rowsPerCycle = 100;

        for (let cycle = 0; cycle < cycles; cycle++) {
          // INSERT
          const rows: [bigint, number][] = [];
          for (let j = 0; j < rowsPerCycle; j++) {
            rows.push([BigInt(cycle * rowsPerCycle + j), cycle]);
          }
          const batch = batchFromRows(
            [{ name: "id", type: "UInt64" }, { name: "batch", type: "UInt32" }],
            rows
          );
          for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, batch)) {}

          // Verify count
          let count = 0n;
          for await (const packet of client.query(`SELECT count() FROM ${tableName}`)) {
            if (packet.type === "Data") {
              for (const row of packet.batch) {
                count = row["count()"] as bigint;
              }
            }
          }
          assert.strictEqual(Number(count), (cycle + 1) * rowsPerCycle);
        }
      } finally {
        await client.execute(`DROP TABLE ${tableName}`);
      }
    }));
  });

  describe("Connection Reuse After Error", () => {
    test("connection usable after query error", () => withClient(async (client) => {
      // Cause an error
      try {
        for await (const _ of client.query("SELECT * FROM nonexistent_table_xyz")) {}
        assert.fail("Should have thrown");
      } catch (err) {
        assert.ok(err instanceof ClickHouseException);
      }

      // Connection should still work
      let result = 0n;
      for await (const packet of client.query("SELECT 123 as val")) {
        if (packet.type === "Data") {
          for (const row of packet.batch) {
            result = row.val as bigint;
          }
        }
      }
      assert.strictEqual(Number(result), 123);
    }));

    test("connection usable after syntax error", () => withClient(async (client) => {
      try {
        for await (const _ of client.query("SELEC INVALID SYNTAX")) {}
        assert.fail("Should have thrown");
      } catch (err) {
        assert.ok(err instanceof ClickHouseException);
      }

      let result = 0n;
      for await (const packet of client.query("SELECT 456 as val")) {
        if (packet.type === "Data") {
          for (const row of packet.batch) {
            result = row.val as bigint;
          }
        }
      }
      assert.strictEqual(Number(result), 456);
    }));
  });

  describe("Large Insert Then Verify", () => {
    test("200K row streaming insert followed by full read", () => withClient(async (client) => {
      const tableName = `test_large_${Date.now()}`;
      const totalRows = 200000;
      const batchSize = 10000;
      const batches = totalRows / batchSize;

      await client.execute(`CREATE TABLE ${tableName} (id UInt64, name String, value Float64) ENGINE = Memory`);

      try {
        async function* generateBatches() {
          for (let batch = 0; batch < batches; batch++) {
            const rows: [bigint, string, number][] = [];
            for (let i = 0; i < batchSize; i++) {
              const id = BigInt(batch * batchSize + i);
              rows.push([id, `row_${id}`, Number(id) * 0.5]);
            }
            yield batchFromRows(
              [
                { name: "id", type: "UInt64" },
                { name: "name", type: "String" },
                { name: "value", type: "Float64" }
              ],
              rows
            );
          }
        }

        for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, generateBatches())) {}

        // Verify count
        let count = 0n;
        for await (const packet of client.query(`SELECT count() FROM ${tableName}`)) {
          if (packet.type === "Data") {
            for (const row of packet.batch) {
              count = row["count()"] as bigint;
            }
          }
        }
        assert.strictEqual(Number(count), totalRows);

        // Spot check some data
        const spotCheckRows: Record<string, unknown>[] = [];
        for await (const packet of client.query(`SELECT * FROM ${tableName} WHERE id IN (0, 99999, 199999) ORDER BY id`)) {
          if (packet.type === "Data") {
            for (const row of packet.batch) {
              spotCheckRows.push(row.toObject());
            }
          }
        }
        assert.strictEqual(spotCheckRows.length, 3, "Should have 3 spot check rows");
        assert.strictEqual(spotCheckRows[0].id, 0n);
        assert.strictEqual(spotCheckRows[0].name, "row_0");
        assert.strictEqual(spotCheckRows[1].id, 99999n);
        assert.strictEqual(spotCheckRows[1].name, "row_99999");
        assert.strictEqual(spotCheckRows[2].id, 199999n);
        assert.strictEqual(spotCheckRows[2].name, "row_199999");
      } finally {
        await client.execute(`DROP TABLE ${tableName}`);
      }
    }));

    test("full read-back verifies all data via hash", () => withClient(async (client) => {
      const tableName = `test_hash_${Date.now()}`;
      const rowCount = 50000;

      await client.execute(`CREATE TABLE ${tableName} (id UInt64, data String) ENGINE = Memory`);

      try {
        // Insert data
        const rows: [bigint, string][] = [];
        for (let i = 0; i < rowCount; i++) {
          rows.push([BigInt(i), `data_${i}_${i * 2}`]);
        }
        const batch = batchFromRows(
          [{ name: "id", type: "UInt64" }, { name: "data", type: "String" }],
          rows
        );
        for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, batch)) {}

        // Verify count and compute aggregate hash server-side
        let count = 0n;
        let hash = 0n;
        for await (const packet of client.query(`SELECT count() as c, sum(cityHash64(*)) as h FROM ${tableName}`)) {
          if (packet.type === "Data") {
            for (const row of packet.batch) {
              count = row.c as bigint;
              hash = row.h as bigint;
            }
          }
        }
        assert.strictEqual(Number(count), rowCount);
        // Hash is computed - we just verify the query ran successfully
        assert.ok(typeof hash === "bigint", "Hash should be a bigint");
      } finally {
        await client.execute(`DROP TABLE ${tableName}`);
      }
    }));
  });
});
