/**
 * Integration tests for HTTP external tables support.
 */

import assert from "node:assert";
import { after, before, describe, it } from "node:test";
import { collectText, init, query } from "../client.ts";
import { batchFromCols, getCodec } from "../native/index.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { generateSessionId } from "./test_utils.ts";

describe("HTTP external tables", { timeout: 120000 }, () => {
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = generateSessionId("http-ext");

  before(async () => {
    await init();
    const ch = await startClickHouse();
    baseUrl = `${ch.url}/`;
    auth = { username: ch.username, password: ch.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  it("queries a single external table with TSV data", async () => {
    const result = await collectText(
      query("SELECT * FROM mydata ORDER BY id FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          mydata: {
            structure: "id UInt32, name String",
            data: "1\tAlice\n2\tBob\n3\tCharlie\n",
          },
        },
      }),
    );

    const rows = result
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.strictEqual(rows.length, 3);
    assert.strictEqual(rows[0].id, 1);
    assert.strictEqual(rows[0].name, "Alice");
    assert.strictEqual(rows[2].id, 3);
    assert.strictEqual(rows[2].name, "Charlie");
  });

  it("queries with JSONEachRow format", async () => {
    const result = await collectText(
      query("SELECT sum(value) as total FROM numbers FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          numbers: {
            structure: "value Int64",
            format: "JSONEachRow",
            data: '{"value":100}\n{"value":200}\n{"value":300}\n',
          },
        },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.total, 600);
  });

  it("queries with multiple external tables", async () => {
    const result = await collectText(
      query(
        `SELECT u.name, sum(o.amount) as total
       FROM users u
       JOIN orders o ON u.id = o.user_id
       GROUP BY u.name
       ORDER BY u.name
       FORMAT JSONEachRow`,
        sessionId,
        {
          baseUrl,
          auth,
          externalTables: {
            users: {
              structure: "id UInt32, name String",
              data: "1\tAlice\n2\tBob\n",
            },
            orders: {
              structure: "order_id UInt32, user_id UInt32, amount Float64",
              data: "101\t1\t10.5\n102\t2\t20.0\n103\t1\t15.5\n",
            },
          },
        },
      ),
    );

    const rows = result
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[0].name, "Alice");
    assert.strictEqual(rows[0].total, 26.0);
    assert.strictEqual(rows[1].name, "Bob");
    assert.strictEqual(rows[1].total, 20.0);
  });

  it("queries with Uint8Array data", async () => {
    const encoder = new TextEncoder();
    const data = encoder.encode("1\ttest1\n2\ttest2\n3\ttest3\n");

    const result = await collectText(
      query("SELECT count() as cnt FROM binary_data FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          binary_data: {
            structure: "id UInt32, value String",
            data: data,
          },
        },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.cnt, 3);
  });

  it("queries with async iterable data", async () => {
    const encoder = new TextEncoder();

    async function* generateData(): AsyncIterable<Uint8Array> {
      yield encoder.encode("1\tpart1\n");
      await new Promise((resolve) => setTimeout(resolve, 10));
      yield encoder.encode("2\tpart2\n");
      await new Promise((resolve) => setTimeout(resolve, 10));
      yield encoder.encode("3\tpart3\n");
    }

    const result = await collectText(
      query("SELECT sum(id) as total FROM async_data FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          async_data: {
            structure: "id UInt32, value String",
            data: generateData(),
          },
        },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.total, 6);
  });

  it("filters external table data in query", async () => {
    const result = await collectText(
      query(
        "SELECT id FROM filter_test WHERE active = 1 ORDER BY id FORMAT JSONEachRow",
        sessionId,
        {
          baseUrl,
          auth,
          externalTables: {
            filter_test: {
              structure: "id UInt32, active UInt8",
              data: "1\t1\n2\t0\n3\t1\n4\t0\n5\t1\n",
            },
          },
        },
      ),
    );

    const rows = result
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.strictEqual(rows.length, 3);
    assert.strictEqual(rows[0].id, 1);
    assert.strictEqual(rows[1].id, 3);
    assert.strictEqual(rows[2].id, 5);
  });

  it("handles empty external table", async () => {
    const result = await collectText(
      query("SELECT count() as cnt FROM empty_table FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          empty_table: {
            structure: "id UInt32",
            data: "",
          },
        },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.cnt, 0);
  });

  it("uses external table in subquery", async () => {
    const result = await collectText(
      query("SELECT (SELECT max(val) FROM vals) as max_val FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          vals: {
            structure: "val UInt32",
            data: "10\n20\n30\n",
          },
        },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.max_val, 30);
  });

  it("queries with CSV format", async () => {
    const result = await collectText(
      query("SELECT * FROM csv_data ORDER BY id FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: {
          csv_data: {
            structure: "id UInt32, name String",
            format: "CSV",
            data: '1,"Alice"\n2,"Bob"\n3,"Charlie"\n',
          },
        },
      }),
    );

    const rows = result
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.strictEqual(rows.length, 3);
    assert.strictEqual(rows[0].name, "Alice");
    assert.strictEqual(rows[2].name, "Charlie");
  });

  // Unified API tests - pass RecordBatch directly (same as TCP)

  it("queries with RecordBatch directly (unified API)", async () => {
    const batch = batchFromCols({
      id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
      name: getCodec("String").fromValues(["Alice", "Bob", "Charlie"]),
    });

    const result = await collectText(
      query("SELECT * FROM mydata ORDER BY id FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: { mydata: batch },
      }),
    );

    const rows = result
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    assert.strictEqual(rows.length, 3);
    assert.strictEqual(rows[0].id, 1);
    assert.strictEqual(rows[0].name, "Alice");
    assert.strictEqual(rows[2].id, 3);
    assert.strictEqual(rows[2].name, "Charlie");
  });

  it("queries with sync iterable of RecordBatch (unified API)", async () => {
    const batches = [
      batchFromCols({ val: getCodec("UInt32").fromValues(new Uint32Array([1, 2])) }),
      batchFromCols({ val: getCodec("UInt32").fromValues(new Uint32Array([3, 4])) }),
    ];

    const result = await collectText(
      query("SELECT sum(val) as total FROM vals FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: { vals: batches },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.total, 10); // 1+2+3+4
  });

  it("queries with async iterable of RecordBatch (unified API)", async () => {
    async function* generateBatches() {
      yield batchFromCols({ n: getCodec("UInt32").fromValues(new Uint32Array([10])) });
      yield batchFromCols({ n: getCodec("UInt32").fromValues(new Uint32Array([20])) });
      yield batchFromCols({ n: getCodec("UInt32").fromValues(new Uint32Array([30])) });
    }

    const result = await collectText(
      query("SELECT sum(n) as total FROM nums FORMAT JSONEachRow", sessionId, {
        baseUrl,
        auth,
        externalTables: { nums: generateBatches() },
      }),
    );

    const row = JSON.parse(result.trim());
    assert.strictEqual(row.total, 60); // 10+20+30
  });
});
