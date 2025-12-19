/**
 * Integration tests: Native format against real ClickHouse
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { init, insert, query, collectBytes } from "../client.ts";
import { decodeNative, toArrayRows, type ColumnDef } from "../formats/native/index.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { consume, encodeNativeRows } from "./test_utils.ts";

describe("Native format integration", { timeout: 120000 }, () => {
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = "native_int_" + Date.now();

  before(async () => {
    await init();
    const ch = await startClickHouse();
    baseUrl = ch.url + "/";
    auth = { username: ch.username, password: ch.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  it("round-trips scalar types", async () => {
    const table = "test_native_scalars";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        i32 Int32,
        i64 Int64,
        f64 Float64,
        str String,
        b UInt8
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "i32", type: "Int32" },
      { name: "i64", type: "Int64" },
      { name: "f64", type: "Float64" },
      { name: "str", type: "String" },
      { name: "b", type: "UInt8" },
    ];
    const rows = [
      [1, 100n, 1.5, "hello", 1],
      [-1, -100n, -1.5, "world", 0],
    ];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY i32 FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 2);
    assert.strictEqual(decodedRows[0][0], -1);
    assert.strictEqual(decodedRows[0][1], -100n);
    assert.strictEqual(decodedRows[1][0], 1);
    assert.strictEqual(decodedRows[1][3], "hello");

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Nullable", async () => {
    const table = "test_native_nullable";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        val Nullable(Int32)
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "val", type: "Nullable(Int32)" },
    ];
    const rows = [[1, 100], [2, null], [3, 300]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.strictEqual(decodedRows[0][1], 100);
    assert.strictEqual(decodedRows[1][1], null);
    assert.strictEqual(decodedRows[2][1], 300);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Array", async () => {
    const table = "test_native_array";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        arr Array(Int32)
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "arr", type: "Array(Int32)" },
    ];
    const rows = [[1, [1, 2, 3]], [2, []], [3, [42]]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.deepStrictEqual([...decodedRows[0][1] as Int32Array], [1, 2, 3]);
    assert.deepStrictEqual([...decodedRows[1][1] as Int32Array], []);
    assert.deepStrictEqual([...decodedRows[2][1] as Int32Array], [42]);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Map", async () => {
    const table = "test_native_map";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        m Map(String, Int32)
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "m", type: "Map(String, Int32)" },
    ];
    const rows = [[1, { a: 1, b: 2 }], [2, {}]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 2);
    const map = decodedRows[0][1] as Map<string, number>;
    assert.strictEqual(map.get("a"), 1);
    assert.strictEqual(map.get("b"), 2);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Tuple", async () => {
    const table = "test_native_tuple";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        t Tuple(Int32, String)
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "t", type: "Tuple(Int32, String)" },
    ];
    const rows = [[1, [100, "a"]], [2, [200, "b"]]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 2);
    assert.deepStrictEqual(decodedRows[0][1], [100, "a"]);
    assert.deepStrictEqual(decodedRows[1][1], [200, "b"]);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips DateTime64", async () => {
    const table = "test_native_dt64";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        ts DateTime64(3)
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "ts", type: "DateTime64(3)" },
    ];
    const date = new Date("2024-01-15T10:30:00.123Z");
    const rows = [[1, date]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 1);
    // DateTime64 returns ClickHouseDateTime64 wrapper
    const ts = decodedRows[0][1] as { toDate(): Date };
    assert.strictEqual(ts.toDate().getTime(), date.getTime());

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips UUID", async () => {
    const table = "test_native_uuid";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id UUID
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [{ name: "id", type: "UUID" }];
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    const rows = [[uuid]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 1);
    assert.strictEqual(decodedRows[0][0], uuid);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips LowCardinality", async () => {
    const table = "test_native_lowcard";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        status LowCardinality(String),
        category LowCardinality(Nullable(String))
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "status", type: "LowCardinality(String)" },
      { name: "category", type: "LowCardinality(Nullable(String))" },
    ];
    const rows = [
      [1, "active", "electronics"],
      [2, "inactive", null],
      [3, "active", "books"],
      [4, "pending", "electronics"],
      [5, "active", null],
    ];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 5);
    assert.strictEqual(decodedRows[0][1], "active");
    assert.strictEqual(decodedRows[0][2], "electronics");
    assert.strictEqual(decodedRows[1][1], "inactive");
    assert.strictEqual(decodedRows[1][2], null);
    assert.strictEqual(decodedRows[4][2], null);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("handles large dataset", async () => {
    const table = "test_native_large";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        name String,
        value Float64
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "name", type: "String" },
      { name: "value", type: "Float64" },
    ];

    const rowCount = 50000;
    const rows: unknown[][] = [];
    for (let i = 0; i < rowCount; i++) {
      rows.push([i, `name_${i}`, i * 1.5]);
    }

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, rowCount);
    assert.strictEqual(decodedRows[0][0], 0);
    assert.strictEqual(decodedRows[rowCount - 1][0], rowCount - 1);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Point", async () => {
    const table = "test_native_point";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        location Point
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "location", type: "Point" },
    ];
    const rows = [[1, [1.5, 2.5]], [2, [-10.0, 20.0]], [3, [0.0, 0.0]]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.deepStrictEqual(decodedRows[0][1], [1.5, 2.5]);
    assert.deepStrictEqual(decodedRows[1][1], [-10.0, 20.0]);
    assert.deepStrictEqual(decodedRows[2][1], [0.0, 0.0]);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Ring", async () => {
    const table = "test_native_ring";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        boundary Ring
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "boundary", type: "Ring" },
    ];
    const square = [[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]];
    const triangle = [[0, 0], [2, 0], [1, 1], [0, 0]];
    const rows = [[1, square], [2, triangle]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 2);
    assert.strictEqual((decodedRows[0][1] as unknown[]).length, 5);
    assert.strictEqual((decodedRows[1][1] as unknown[]).length, 4);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Polygon", async () => {
    const table = "test_native_polygon";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        area Polygon
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "area", type: "Polygon" },
    ];
    const outerRing = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]];
    const hole = [[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]];
    const rows = [[1, [outerRing, hole]], [2, [outerRing]]];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 2);
    assert.strictEqual((decodedRows[0][1] as unknown[][]).length, 2); // outer + hole
    assert.strictEqual((decodedRows[1][1] as unknown[][]).length, 1); // outer only

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Variant", async () => {
    const table = "test_native_variant";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        v Variant(String, UInt64)
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "v", type: "Variant(String, UInt64)" },
    ];
    const rows = [
      [1, [0, "hello"]],   // String (disc 0)
      [2, [1, 42n]],       // UInt64 (disc 1)
      [3, null],           // null
      [4, [0, "world"]],   // String
    ];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 4);
    assert.deepStrictEqual(decodedRows[0][1], [0, "hello"]);
    assert.deepStrictEqual(decodedRows[1][1], [1, 42n]);
    assert.strictEqual(decodedRows[2][1], null);
    assert.deepStrictEqual(decodedRows[3][1], [0, "world"]);

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips Dynamic", async () => {
    const table = "test_native_dynamic";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        d Dynamic
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "d", type: "Dynamic" },
    ];
    const rows = [
      [1, "hello"],
      [2, 42],
      [3, null],
      [4, "world"],
    ];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    // Use V3 format setting for Dynamic type (requires ClickHouse 25.6+)
    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=1`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 4);
    assert.strictEqual(decodedRows[0][1], "hello");
    assert.strictEqual(decodedRows[1][1], 42n);  // Int64 decoded as bigint
    assert.strictEqual(decodedRows[2][1], null);
    assert.strictEqual(decodedRows[3][1], "world");

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("verifies Table ergonomics and virtual JSON paths", async () => {
    const table = "test_native_ergonomics";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        meta JSON
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "meta", type: "JSON" },
    ];
    const rows = [
      [1, { user: "alice", scores: [10, 20] }],
      [2, { user: "bob", scores: [30] }],
    ];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=1`, sessionId, { baseUrl, auth }));
    const tableResult = await decodeNative(data);

    // Verify row proxy access
    const row0 = tableResult.get(0);
    assert.strictEqual(row0.id, 1);
    assert.deepStrictEqual((row0.meta as any).scores, [10n, 20n]);

    // Verify virtual JSON path extraction
    const metaCol = tableResult.columnData[1] as any; // JsonColumn
    if (typeof metaCol.getPath === "function") {
      const userCol = metaCol.getPath("user");
      assert.ok(userCol);
      assert.strictEqual(userCol.get(0), "alice");
      assert.strictEqual(userCol.get(1), "bob");
    }

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });

  it("round-trips JSON", async () => {
    const table = "test_native_json";
    await consume(query(`DROP TABLE IF EXISTS ${table}`, sessionId, { baseUrl, auth }));
    await consume(query(`
      CREATE TABLE ${table} (
        id Int32,
        data JSON
      ) ENGINE = Memory
    `, sessionId, { baseUrl, auth }));

    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "data", type: "JSON" },
    ];
    const rows = [
      [1, { name: "alice", age: 30 }],
      [2, { name: "bob", age: 25 }],
      [3, { name: "charlie" }],  // missing age
    ];

    const encoded = encodeNativeRows(columns, rows);
    await insert(`INSERT INTO ${table} FORMAT Native`, encoded, sessionId, { baseUrl, auth });

    // Use V3 format setting for JSON type (requires ClickHouse 25.6+)
    const data = await collectBytes(query(`SELECT * FROM ${table} ORDER BY id FORMAT Native SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=1`, sessionId, { baseUrl, auth }));
    const decoded = await decodeNative(data);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    const obj0 = decodedRows[0][1] as Record<string, unknown>;
    const obj1 = decodedRows[1][1] as Record<string, unknown>;
    const obj2 = decodedRows[2][1] as Record<string, unknown>;

    assert.strictEqual(obj0.name, "alice");
    assert.strictEqual(obj0.age, 30n);
    assert.strictEqual(obj1.name, "bob");
    assert.strictEqual(obj1.age, 25n);
    assert.strictEqual(obj2.name, "charlie");

    await consume(query(`DROP TABLE ${table}`, sessionId, { baseUrl, auth }));
  });
});
