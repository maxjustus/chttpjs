import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";

import { startClickHouse, stopClickHouse } from "./setup.ts";
import { init, insert, query, collectBytes, collectText } from "../client.ts";
import { encodeRowBinaryWithNames, decodeRowBinaryWithNamesAndTypes, type ColumnDef } from "../rowbinary.ts";

describe("RowBinary Integration Tests", { timeout: 60000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = "rowbinary_" + Date.now().toString();

  before(async () => {
    await init();
    clickhouse = await startClickHouse();
    baseUrl = clickhouse.url + "/";
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  describe("Basic types", () => {
    it("should insert with RowBinaryWithNames format", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_basic (id UInt32, name String, value Float64) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
        { name: "value", type: "Float64" },
      ];
      const rows = [
        [1, "alice", 1.5],
        [2, "bob", 2.5],
        [3, "charlie", 3.5],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_basic FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth, compression: "lz4" },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_basic ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].name, "alice");
      assert.strictEqual(parsed.data[2].value, 3.5);

      for await (const _ of query(
        "DROP TABLE test_rb_basic",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Nullable columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_nullable (id UInt32, value Nullable(Int32)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "value", type: "Nullable(Int32)" },
      ];
      const rows = [
        [1, 100],
        [2, null],
        [3, 300],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_nullable FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_nullable ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].value, 100);
      assert.strictEqual(parsed.data[1].value, null);
      assert.strictEqual(parsed.data[2].value, 300);

      for await (const _ of query(
        "DROP TABLE test_rb_nullable",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Array columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_array (id UInt32, tags Array(String), values Array(Int32)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "tags", type: "Array(String)" },
        { name: "values", type: "Array(Int32)" },
      ];
      const rows = [
        [1, ["foo", "bar"], [10, 20, 30]],
        [2, ["baz"], new Int32Array([100, 200])],
        [3, [], []],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_array FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_array ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.deepStrictEqual(parsed.data[0].tags, ["foo", "bar"]);
      assert.deepStrictEqual(parsed.data[0].values, [10, 20, 30]);

      for await (const _ of query(
        "DROP TABLE test_rb_array",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Tuple columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_tuple (id UInt32, data Tuple(String, Int32, Float64)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "Tuple(String, Int32, Float64)" },
      ];
      const rows = [
        [1, ["alice", 100, 1.5]],
        [2, ["bob", 200, 2.5]],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_tuple FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_tuple ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.deepStrictEqual(parsed.data[0].data, ["alice", 100, 1.5]);

      for await (const _ of query(
        "DROP TABLE test_rb_tuple",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Map columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_map (id UInt32, attrs Map(String, Int32)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "attrs", type: "Map(String, Int32)" },
      ];
      const rows = [
        [1, { foo: 100, bar: 200 }],
        [2, new Map([["baz", 300]])],
        [3, {}],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_map FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_map ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.deepStrictEqual(parsed.data[0].attrs, { foo: 100, bar: 200 });

      for await (const _ of query(
        "DROP TABLE test_rb_map",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Date/Time types", () => {
    it("should insert RowBinary with Date32", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_date32 (id UInt32, d Date32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "d", type: "Date32" },
      ];
      const rows = [
        [1, new Date("2024-01-15")],
        [2, new Date("1950-06-20")], // Pre-1970 date
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_date32 FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_date32 ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].d, "2024-01-15");
      assert.strictEqual(parsed.data[1].d, "1950-06-20");

      for await (const _ of query(
        "DROP TABLE test_rb_date32",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with DateTime64", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_dt64 (id UInt32, dt DateTime64(3)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "dt", type: "DateTime64(3)" },
      ];
      const rows = [
        [1, new Date("2024-01-15T12:30:45.123Z")],
        [2, new Date("2024-06-20T00:00:00.000Z")],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_dt64 FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_dt64 ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.ok(parsed.data[0].dt.includes("2024-01-15"));
      assert.ok(parsed.data[0].dt.includes("12:30:45"));

      for await (const _ of query(
        "DROP TABLE test_rb_dt64",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("String types", () => {
    it("should insert RowBinary with FixedString", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_fixedstring (id UInt32, code FixedString(4)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "code", type: "FixedString(4)" },
      ];
      const rows = [
        [1, "ABCD"],
        [2, "XY"],  // Will be padded
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_fixedstring FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT id, code FROM test_rb_fixedstring ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].code, "ABCD");
      assert.ok(parsed.data[1].code.startsWith("XY"));

      for await (const _ of query(
        "DROP TABLE test_rb_fixedstring",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Network/ID types", () => {
    it("should insert RowBinary with UUID", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_uuid (id UInt32, uuid UUID) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "uuid", type: "UUID" },
      ];
      const rows = [
        [1, "550e8400-e29b-41d4-a716-446655440000"],
        [2, "00000000-0000-0000-0000-000000000000"],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_uuid FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_uuid ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].uuid, "550e8400-e29b-41d4-a716-446655440000");
      assert.strictEqual(parsed.data[1].uuid, "00000000-0000-0000-0000-000000000000");

      for await (const _ of query(
        "DROP TABLE test_rb_uuid",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with IPv4 and IPv6", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_ip (id UInt32, ip4 IPv4, ip6 IPv6) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "ip4", type: "IPv4" },
        { name: "ip6", type: "IPv6" },
      ];
      const rows = [
        [1, "192.168.1.1", "2001:db8:85a3:0:0:8a2e:370:7334"],
        [2, "10.0.0.1", "::1"],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_ip FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_ip ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].ip4, "192.168.1.1");
      assert.strictEqual(parsed.data[1].ip4, "10.0.0.1");
      assert.ok(parsed.data[0].ip6.includes("2001"));

      for await (const _ of query(
        "DROP TABLE test_rb_ip",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Big integer types", () => {
    it("should insert RowBinary with Int128/UInt128", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_int128 (id UInt32, signed Int128, unsigned UInt128) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "signed", type: "Int128" },
        { name: "unsigned", type: "UInt128" },
      ];
      const rows = [
        [1, 12345678901234567890n, 98765432109876543210n],
        [2, -12345678901234567890n, 0n],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_int128 FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_int128 ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].signed, "12345678901234567890");
      assert.strictEqual(parsed.data[0].unsigned, "98765432109876543210");
      assert.strictEqual(parsed.data[1].signed, "-12345678901234567890");

      for await (const _ of query(
        "DROP TABLE test_rb_int128",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Int256/UInt256", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_int256 (id UInt32, signed Int256, unsigned UInt256) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "signed", type: "Int256" },
        { name: "unsigned", type: "UInt256" },
      ];
      const bigVal = 12345678901234567890123456789012345678901234567890n;
      const rows = [
        [1, bigVal, bigVal],
        [2, -bigVal, 0n],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_int256 FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_int256 ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].signed, bigVal.toString());
      assert.strictEqual(parsed.data[0].unsigned, bigVal.toString());
      assert.strictEqual(parsed.data[1].signed, (-bigVal).toString());

      for await (const _ of query(
        "DROP TABLE test_rb_int256",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Decimal types", () => {
    it("should insert RowBinary with Decimal32/Decimal64", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decimal (id UInt32, d32 Decimal32(2), d64 Decimal64(4)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "d32", type: "Decimal32(9, 2)" },
        { name: "d64", type: "Decimal64(18, 4)" },
      ];
      const rows = [
        [1, 123.45, 12345.6789],
        [2, -99.99, -0.0001],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_decimal FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_decimal ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(Number(parsed.data[0].d32), 123.45);
      assert.strictEqual(Number(parsed.data[0].d64), 12345.6789);
      assert.strictEqual(Number(parsed.data[1].d32), -99.99);

      for await (const _ of query(
        "DROP TABLE test_rb_decimal",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Decimal128/Decimal256", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decimal_big (id UInt32, d128 Decimal128(10), d256 Decimal256(20)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      // Decimal128(10) and Decimal256(20) - scale is in the type parameter
      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "d128", type: "Decimal128(10)" },
        { name: "d256", type: "Decimal256(20)" },
      ];
      const rows = [
        [1, "123456789.1234567890", "1234567890.12345678901234567890"],
        [2, "-999999999.9999999999", "-1.00000000000000000001"],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_decimal_big FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_decimal_big ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      // ClickHouse returns these as strings for big decimals
      const d128 = String(parsed.data[0].d128);
      const d256 = String(parsed.data[0].d256);
      assert.ok(d128.includes("123456789"));
      assert.ok(d256.includes("1234567890"));

      for await (const _ of query(
        "DROP TABLE test_rb_decimal_big",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Enum types", () => {
    it("should insert RowBinary with Enum8", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_enum8 (id UInt32, status Enum8('pending' = 0, 'active' = 1, 'done' = 2)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "status", type: "Enum8('pending' = 0, 'active' = 1, 'done' = 2)" },
      ];
      const rows = [
        [1, 0],
        [2, 1],
        [3, 2],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_enum8 FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_enum8 ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].status, "pending");
      assert.strictEqual(parsed.data[1].status, "active");
      assert.strictEqual(parsed.data[2].status, "done");

      for await (const _ of query(
        "DROP TABLE test_rb_enum8",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should insert RowBinary with Enum16", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_enum16 (id UInt32, priority Enum16('low' = 1, 'medium' = 100, 'high' = 1000)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "priority", type: "Enum16('low' = 1, 'medium' = 100, 'high' = 1000)" },
      ];
      const rows = [
        [1, 1],
        [2, 100],
        [3, 1000],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_enum16 FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_enum16 ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].priority, "low");
      assert.strictEqual(parsed.data[1].priority, "medium");
      assert.strictEqual(parsed.data[2].priority, "high");

      for await (const _ of query(
        "DROP TABLE test_rb_enum16",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Complex/Nested types", () => {
    it("should insert RowBinary with nested Tuple and Array", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_nested (id UInt32, data Tuple(String, Array(Int32), Tuple(Float64, String))) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "Tuple(String, Array(Int32), Tuple(Float64, String))" },
      ];
      const rows = [
        [1, ["outer", [1, 2, 3], [3.14, "inner"]]],
        [2, ["test", [], [2.71, "nested"]]],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_nested FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT * FROM test_rb_nested ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.deepStrictEqual(parsed.data[0].data, ["outer", [1, 2, 3], [3.14, "inner"]]);
      assert.deepStrictEqual(parsed.data[1].data, ["test", [], [2.71, "nested"]]);

      for await (const _ of query(
        "DROP TABLE test_rb_nested",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Variant type", () => {
    it("should insert RowBinary with Variant", async () => {
      // Variant requires ClickHouse 24.1+ with allow_experimental_variant_type
      try {
        for await (const _ of query(
          "SET allow_experimental_variant_type = 1",
          sessionId,
          { baseUrl, auth, compression: "none" },
        )) {}
      } catch {
        // Setting may not exist in older versions
      }

      try {
        for await (const _ of query(
          "CREATE TABLE IF NOT EXISTS test_rb_variant (id UInt32, v Variant(String, Int32, Float64)) ENGINE = Memory",
          sessionId,
          { baseUrl, auth, compression: "none" },
        )) {}
      } catch (err) {
        // Skip test if Variant not supported
        console.log("    Skipping Variant test - not supported in this ClickHouse version");
        return;
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "v", type: "Variant(String, Int32, Float64)" },
      ];
      const rows = [
        [1, { type: 0, value: "hello" }],
        [2, { type: 1, value: 42 }],
        [3, { type: 2, value: 3.14 }],
        [4, null],
      ];
      const data = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_variant FORMAT RowBinaryWithNames",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(query(
        "SELECT id, v, variantType(v) as vtype FROM test_rb_variant ORDER BY id FORMAT JSON",
        sessionId,
        { baseUrl, auth },
      ));

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 4);
      assert.strictEqual(parsed.data[0].v, "hello");
      assert.strictEqual(parsed.data[0].vtype, "String");
      assert.strictEqual(parsed.data[1].v, "42");
      assert.strictEqual(parsed.data[1].vtype, "Int32");
      assert.strictEqual(parsed.data[3].v, null);
      assert.strictEqual(parsed.data[3].vtype, "None");

      for await (const _ of query(
        "DROP TABLE test_rb_variant",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });

  describe("Decoding", () => {
    it("should query and decode with RowBinaryWithNamesAndTypes format", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decode (id UInt32, name String, value Float64, flag Bool) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
        { name: "value", type: "Float64" },
        { name: "flag", type: "Bool" },
      ];
      const rows = [
        [1, "alice", 1.5, true],
        [2, "bob", 2.5, false],
      ];
      const encoded = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_decode FORMAT RowBinaryWithNames",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const data = await collectBytes(query(
        "SELECT * FROM test_rb_decode ORDER BY id FORMAT RowBinaryWithNamesAndTypes",
        sessionId,
        { baseUrl, auth },
      ));

      const decoded = decodeRowBinaryWithNamesAndTypes(data);

      assert.strictEqual(decoded.columns.length, 4);
      assert.strictEqual(decoded.columns[0].name, "id");
      assert.strictEqual(decoded.columns[0].type, "UInt32");
      assert.strictEqual(decoded.rows.length, 2);
      assert.strictEqual(decoded.rows[0][0], 1);
      assert.strictEqual(decoded.rows[0][1], "alice");
      assert.strictEqual(decoded.rows[0][3], true);

      for await (const _ of query(
        "DROP TABLE test_rb_decode",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });

    it("should decode complex types from ClickHouse", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decode_complex (id UInt32, tags Array(String), attrs Map(String, Int32), data Tuple(String, Float64)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "tags", type: "Array(String)" },
        { name: "attrs", type: "Map(String, Int32)" },
        { name: "data", type: "Tuple(String, Float64)" },
      ];
      const rows = [
        [1, ["foo", "bar"], { a: 10, b: 20 }, ["hello", 3.14]],
        [2, [], {}, ["world", 2.71]],
      ];
      const encoded = encodeRowBinaryWithNames(columns, rows);

      await insert(
        "INSERT INTO test_rb_decode_complex FORMAT RowBinaryWithNames",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const data = await collectBytes(query(
        "SELECT * FROM test_rb_decode_complex ORDER BY id FORMAT RowBinaryWithNamesAndTypes",
        sessionId,
        { baseUrl, auth },
      ));

      const decoded = decodeRowBinaryWithNamesAndTypes(data);

      assert.strictEqual(decoded.rows.length, 2);
      assert.deepStrictEqual(decoded.rows[0][1], ["foo", "bar"]);
      assert.deepStrictEqual(decoded.rows[0][2], { a: 10, b: 20 });
      const tuple0 = decoded.rows[0][3] as unknown[];
      assert.strictEqual(tuple0[0], "hello");

      for await (const _ of query(
        "DROP TABLE test_rb_decode_complex",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {}
    });
  });
});
