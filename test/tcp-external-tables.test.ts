/**
 * Integration tests for external tables support in the TCP client.
 */

import assert from "node:assert";
import { after, before, describe, it } from "node:test";
import { batchFromCols, getCodec, RecordBatch } from "../native/index.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { collectQueryResults, connectTcpClient, type TcpConfig } from "./test_utils.ts";

describe("TCP external tables", { timeout: 120000 }, () => {
  let chConfig: TcpConfig;

  before(async () => {
    const ch = await startClickHouse();
    chConfig = { host: ch.host, tcpPort: ch.tcpPort, username: ch.username, password: ch.password };
    await new Promise((resolve) => setTimeout(resolve, 500));
  });

  after(async () => {
    await stopClickHouse();
  });

  it("queries a single external table (object form)", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const batch = batchFromCols({
        id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
        name: getCodec("String").fromValues(["Alice", "Bob", "Charlie"]),
      });

      const rows = await collectQueryResults(client, "SELECT * FROM mydata ORDER BY id", {
        externalTables: { mydata: batch },
      });

      assert.strictEqual(rows.length, 3);
      assert.strictEqual(rows[0][0], 1);
      assert.strictEqual(rows[0][1], "Alice");
      assert.strictEqual(rows[2][0], 3);
      assert.strictEqual(rows[2][1], "Charlie");
    } finally {
      await client.close();
    }
  });

  it("aggregates external table data", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const batch = batchFromCols({
        value: getCodec("Int64").fromValues(new BigInt64Array([100n, 200n, 300n])),
      });

      const rows = await collectQueryResults(client, "SELECT sum(value) as total FROM ext_table", {
        externalTables: { ext_table: batch },
      });

      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 600n);
    } finally {
      await client.close();
    }
  });

  it("queries with multiple external tables", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const users = batchFromCols({
        user_id: getCodec("UInt32").fromValues(new Uint32Array([1, 2])),
        user_name: getCodec("String").fromValues(["Alice", "Bob"]),
      });

      const orders = batchFromCols({
        order_id: getCodec("UInt32").fromValues(new Uint32Array([101, 102, 103])),
        user_id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 1])),
        amount: getCodec("Float64").fromValues(new Float64Array([10.5, 20.0, 15.5])),
      });

      const rows = await collectQueryResults(
        client,
        `SELECT u.user_name, sum(o.amount) as total
         FROM users u
         JOIN orders o ON u.user_id = o.user_id
         GROUP BY u.user_name
         ORDER BY u.user_name`,
        { externalTables: { users, orders } },
      );

      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0][0], "Alice");
      assert.strictEqual(rows[0][1], 26.0); // 10.5 + 15.5
      assert.strictEqual(rows[1][0], "Bob");
      assert.strictEqual(rows[1][1], 20.0);
    } finally {
      await client.close();
    }
  });

  it("handles multiple batches for single external table (sync iterable)", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const batch1 = batchFromCols({
        n: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
      });
      const batch2 = batchFromCols({ n: getCodec("UInt32").fromValues(new Uint32Array([4, 5])) });
      const batch3 = batchFromCols({
        n: getCodec("UInt32").fromValues(new Uint32Array([6, 7, 8, 9, 10])),
      });

      const rows = await collectQueryResults(
        client,
        "SELECT sum(n) as total, count() as cnt FROM numbers_table",
        { externalTables: { numbers_table: [batch1, batch2, batch3] } },
      );

      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 55n); // 1+2+3+4+5+6+7+8+9+10 = 55
      assert.strictEqual(rows[0][1], 10n);
    } finally {
      await client.close();
    }
  });

  it("handles async iterable for external table", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      async function* generateBatches(): AsyncIterable<RecordBatch> {
        yield batchFromCols({ x: getCodec("UInt32").fromValues(new Uint32Array([1, 2])) });
        await new Promise((resolve) => setTimeout(resolve, 10));
        yield batchFromCols({ x: getCodec("UInt32").fromValues(new Uint32Array([3, 4])) });
        await new Promise((resolve) => setTimeout(resolve, 10));
        yield batchFromCols({ x: getCodec("UInt32").fromValues(new Uint32Array([5])) });
      }

      const rows = await collectQueryResults(client, "SELECT sum(x) as total FROM async_data", {
        externalTables: { async_data: generateBatches() },
      });

      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 15n); // 1+2+3+4+5 = 15
    } finally {
      await client.close();
    }
  });

  it("works with compression enabled", async () => {
    const client = await connectTcpClient(chConfig, { compression: "lz4" });
    try {
      // Create a batch with enough data to make compression meaningful
      const ids = new Uint32Array(100);
      const data: string[] = [];
      for (let i = 0; i < 100; i++) {
        ids[i] = i;
        data.push(`value_${i}_${"x".repeat(50)}`);
      }

      const batch = batchFromCols({
        id: getCodec("UInt32").fromValues(ids),
        data: getCodec("String").fromValues(data),
      });

      const rows = await collectQueryResults(
        client,
        "SELECT count() as cnt, max(id) as max_id FROM compressed_table",
        { externalTables: { compressed_table: batch } },
      );

      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 100n);
      assert.strictEqual(rows[0][1], 99);
    } finally {
      await client.close();
    }
  });

  it("handles empty external table", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const emptyBatch = batchFromCols({
        id: getCodec("UInt32").fromValues(new Uint32Array(0)),
      });

      const rows = await collectQueryResults(client, "SELECT count() as cnt FROM empty_table", {
        externalTables: { empty_table: emptyBatch },
      });

      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 0n);
    } finally {
      await client.close();
    }
  });

  it("filters external table data in query", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const batch = batchFromCols({
        id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3, 4, 5])),
        active: getCodec("UInt8").fromValues(new Uint8Array([1, 0, 1, 0, 1])),
      });

      const rows = await collectQueryResults(
        client,
        "SELECT id FROM filter_test WHERE active = 1 ORDER BY id",
        { externalTables: { filter_test: batch } },
      );

      assert.strictEqual(rows.length, 3);
      assert.strictEqual(rows[0][0], 1);
      assert.strictEqual(rows[1][0], 3);
      assert.strictEqual(rows[2][0], 5);
    } finally {
      await client.close();
    }
  });

  it("uses external table in subquery", async () => {
    const client = await connectTcpClient(chConfig);
    try {
      const batch = batchFromCols({
        val: getCodec("UInt32").fromValues(new Uint32Array([10, 20, 30])),
      });

      const rows = await collectQueryResults(
        client,
        "SELECT (SELECT max(val) FROM vals) as max_val",
        { externalTables: { vals: batch } },
      );

      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0][0], 30);
    } finally {
      await client.close();
    }
  });
});
