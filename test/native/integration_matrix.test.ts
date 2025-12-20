/**
 * Deterministic integration matrix against real ClickHouse.
 * Focused on server-sensitive types rather than exhaustive fuzz coverage.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { init, insert, query, collectBytes } from "../../client.ts";
import { type ColumnDef } from "../../native/index.ts";
import { startClickHouse, stopClickHouse } from "../setup.ts";
import { consume, encodeNativeRows, decodeBatch, toArrayRows } from "../test_utils.ts";

describe("Native integration type matrix", { timeout: 120000 }, () => {
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = "native_matrix_" + Date.now();

  before(async () => {
    await init();
    const ch = await startClickHouse();
    baseUrl = ch.url + "/";
    auth = { username: ch.username, password: ch.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  it("round-trips a deterministic matrix of server-sensitive types", async () => {
    const table = "test_native_matrix";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id UInt32,
        e Enum16('a' = 1, 'b' = 2),
        fs FixedString(4),
        dec Decimal128(6),
        ip IPv6,
        ts DateTime64(3),
        lc LowCardinality(Nullable(String)),
        arr Array(Int64),
        m Map(String, UInt64),
        v Variant(String, UInt64),
        p Point
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    try {
      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "e", type: "Enum16('a' = 1, 'b' = 2)" },
        { name: "fs", type: "FixedString(4)" },
        { name: "dec", type: "Decimal128(6)" },
        { name: "ip", type: "IPv6" },
        { name: "ts", type: "DateTime64(3)" },
        { name: "lc", type: "LowCardinality(Nullable(String))" },
        { name: "arr", type: "Array(Int64)" },
        { name: "m", type: "Map(String, UInt64)" },
        { name: "v", type: "Variant(String, UInt64)" },
        { name: "p", type: "Point" },
      ];

      const rows = [
        [
          1,
          1,
          "ab12",
          "12345.678901",
          "2001:db8::1",
          new Date("2024-01-15T10:30:00.123Z"),
          "active",
          [1n, -2n],
          { a: 1n, b: 2n },
          [0, "hello"],
          [1.5, 2.5],
        ],
        [
          2,
          2,
          "wxyz",
          "-99999.000001",
          "::1",
          new Date("2024-01-15T10:30:00.456Z"),
          null,
          [],
          {},
          [1, 42n],
          [0.0, 0.0],
        ],
      ];

      const encoded = encodeNativeRows(columns, rows);
      await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

      const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
      const decoded = await decodeBatch(data);
      const decodedRows = toArrayRows(decoded);
      const decoder = new TextDecoder();

      assert.strictEqual(decoded.rowCount, 2);
      assert.strictEqual(decodedRows[0][0], 1);
      assert.strictEqual(decodedRows[1][0], 2);
      assert.strictEqual(decodedRows[0][1], 1);
      assert.strictEqual(decodedRows[1][1], 2);
      assert.strictEqual(decoder.decode(decodedRows[0][2] as Uint8Array), "ab12");
      assert.strictEqual(decoder.decode(decodedRows[1][2] as Uint8Array), "wxyz");
      assert.strictEqual(decodedRows[0][3], "12345.678901");
      assert.strictEqual(decodedRows[1][3], "-99999.000001");
      assert.ok(typeof decodedRows[0][4] === "string" && (decodedRows[0][4] as string).length > 0);
      assert.ok(typeof decodedRows[1][4] === "string" && (decodedRows[1][4] as string).length > 0);

      const ts0 = decodedRows[0][5] as { toDate(): Date };
      const ts1 = decodedRows[1][5] as { toDate(): Date };
      assert.strictEqual(ts0.toDate().getTime(), new Date("2024-01-15T10:30:00.123Z").getTime());
      assert.strictEqual(ts1.toDate().getTime(), new Date("2024-01-15T10:30:00.456Z").getTime());
      assert.strictEqual(decodedRows[0][6], "active");
      assert.strictEqual(decodedRows[1][6], null);
      assert.deepStrictEqual(decodedRows[0][7], [1n, -2n]);
      assert.deepStrictEqual(decodedRows[1][7], []);

      const map0 = decodedRows[0][8] as Map<string, bigint>;
      const map1 = decodedRows[1][8] as Map<string, bigint>;
      assert.deepStrictEqual(Array.from(map0), [["a", 1n], ["b", 2n]]);
      assert.deepStrictEqual(Array.from(map1), []);

      assert.deepStrictEqual(decodedRows[0][9], [0, "hello"]);
      assert.deepStrictEqual(decodedRows[1][9], [1, 42n]);
      assert.deepStrictEqual(decodedRows[0][10], [1.5, 2.5]);
      assert.deepStrictEqual(decodedRows[1][10], [0.0, 0.0]);
    } finally {
      await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
    }
  });
});
