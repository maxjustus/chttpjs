import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";

import { startClickHouse, stopClickHouse } from "./setup.ts";
import { init, insert, query, collectBytes, collectText } from "../client.ts";
import {
  encodeRowBinary,
  decodeRowBinary,
  type ColumnDef,
} from "../rowbinary.ts";

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
    it("should insert with RowBinaryWithNamesAndTypes format", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_basic (id UInt32, name String, value Float64) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

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
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_basic FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth, compression: "lz4" },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_basic ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].name, "alice");
      assert.strictEqual(parsed.data[2].value, 3.5);

      for await (const _ of query("DROP TABLE test_rb_basic", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Nullable columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_nullable (id UInt32, value Nullable(Int32)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "value", type: "Nullable(Int32)" },
      ];
      const rows = [
        [1, 100],
        [2, null],
        [3, 300],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_nullable FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_nullable ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].value, 100);
      assert.strictEqual(parsed.data[1].value, null);
      assert.strictEqual(parsed.data[2].value, 300);

      for await (const _ of query("DROP TABLE test_rb_nullable", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Array columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_array (id UInt32, tags Array(String), values Array(Int32)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

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
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_array FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_array ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.deepStrictEqual(parsed.data[0].tags, ["foo", "bar"]);
      assert.deepStrictEqual(parsed.data[0].values, [10, 20, 30]);

      for await (const _ of query("DROP TABLE test_rb_array", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Tuple columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_tuple (id UInt32, data Tuple(String, Int32, Float64)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "Tuple(String, Int32, Float64)" },
      ];
      const rows = [
        [1, ["alice", 100, 1.5]],
        [2, ["bob", 200, 2.5]],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_tuple FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_tuple ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.deepStrictEqual(parsed.data[0].data, ["alice", 100, 1.5]);

      for await (const _ of query("DROP TABLE test_rb_tuple", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Map columns", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_map (id UInt32, attrs Map(String, Int32)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "attrs", type: "Map(String, Int32)" },
      ];
      const rows = [
        [1, { foo: 100, bar: 200 }],
        [2, new Map([["baz", 300]])],
        [3, {}],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_map FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query("SELECT * FROM test_rb_map ORDER BY id FORMAT JSON", sessionId, {
          baseUrl,
          auth,
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.deepStrictEqual(parsed.data[0].attrs, { foo: 100, bar: 200 });

      for await (const _ of query("DROP TABLE test_rb_map", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Date/Time types", () => {
    it("should insert RowBinary with Date32", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_date32 (id UInt32, d Date32) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "d", type: "Date32" },
      ];
      const rows = [
        [1, new Date("2024-01-15")],
        [2, new Date("1950-06-20")], // Pre-1970 date
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_date32 FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_date32 ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].d, "2024-01-15");
      assert.strictEqual(parsed.data[1].d, "1950-06-20");

      for await (const _ of query("DROP TABLE test_rb_date32", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with DateTime64", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_dt64 (id UInt32, dt DateTime64(3)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "dt", type: "DateTime64(3)" },
      ];
      const rows = [
        [1, new Date("2024-01-15T12:30:45.123Z")],
        [2, new Date("2024-06-20T00:00:00.000Z")],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_dt64 FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query("SELECT * FROM test_rb_dt64 ORDER BY id FORMAT JSON", sessionId, {
          baseUrl,
          auth,
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.ok(parsed.data[0].dt.includes("2024-01-15"));
      assert.ok(parsed.data[0].dt.includes("12:30:45"));

      for await (const _ of query("DROP TABLE test_rb_dt64", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("String types", () => {
    it("should insert RowBinary with FixedString", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_fixedstring (id UInt32, code FixedString(4)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "code", type: "FixedString(4)" },
      ];
      const rows = [
        [1, "ABCD"],
        [2, "XY"], // Will be padded
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_fixedstring FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT id, code FROM test_rb_fixedstring ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].code, "ABCD");
      assert.ok(parsed.data[1].code.startsWith("XY"));

      for await (const _ of query("DROP TABLE test_rb_fixedstring", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Network/ID types", () => {
    it("should insert RowBinary with UUID", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_uuid (id UInt32, uuid UUID) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "uuid", type: "UUID" },
      ];
      const rows = [
        [1, "550e8400-e29b-41d4-a716-446655440000"],
        [2, "00000000-0000-0000-0000-000000000000"],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_uuid FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query("SELECT * FROM test_rb_uuid ORDER BY id FORMAT JSON", sessionId, {
          baseUrl,
          auth,
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(
        parsed.data[0].uuid,
        "550e8400-e29b-41d4-a716-446655440000",
      );
      assert.strictEqual(
        parsed.data[1].uuid,
        "00000000-0000-0000-0000-000000000000",
      );

      for await (const _ of query("DROP TABLE test_rb_uuid", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with IPv4 and IPv6", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_ip (id UInt32, ip4 IPv4, ip6 IPv6) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "ip4", type: "IPv4" },
        { name: "ip6", type: "IPv6" },
      ];
      const rows = [
        [1, "192.168.1.1", "2001:db8:85a3:0:0:8a2e:370:7334"],
        [2, "10.0.0.1", "::1"],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_ip FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query("SELECT * FROM test_rb_ip ORDER BY id FORMAT JSON", sessionId, {
          baseUrl,
          auth,
        }),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].ip4, "192.168.1.1");
      assert.strictEqual(parsed.data[1].ip4, "10.0.0.1");
      assert.ok(parsed.data[0].ip6.includes("2001"));

      for await (const _ of query("DROP TABLE test_rb_ip", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Big integer types", () => {
    it("should insert RowBinary with Int128/UInt128", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_int128 (id UInt32, signed Int128, unsigned UInt128) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "signed", type: "Int128" },
        { name: "unsigned", type: "UInt128" },
      ];
      const rows = [
        [1, 12345678901234567890n, 98765432109876543210n],
        [2, -12345678901234567890n, 0n],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_int128 FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_int128 ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].signed, "12345678901234567890");
      assert.strictEqual(parsed.data[0].unsigned, "98765432109876543210");
      assert.strictEqual(parsed.data[1].signed, "-12345678901234567890");

      for await (const _ of query("DROP TABLE test_rb_int128", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Int256/UInt256", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_int256 (id UInt32, signed Int256, unsigned UInt256) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

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
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_int256 FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_int256 ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0].signed, bigVal.toString());
      assert.strictEqual(parsed.data[0].unsigned, bigVal.toString());
      assert.strictEqual(parsed.data[1].signed, (-bigVal).toString());

      for await (const _ of query("DROP TABLE test_rb_int256", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Decimal types", () => {
    it("should insert RowBinary with Decimal32/Decimal64", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decimal (id UInt32, d32 Decimal32(2), d64 Decimal64(4)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      // Must use Decimal(precision, scale) format to match ClickHouse
      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "d32", type: "Decimal(9, 2)" },
        { name: "d64", type: "Decimal(18, 4)" },
      ];
      const rows = [
        [1, 123.45, 12345.6789],
        [2, -99.99, -0.0001],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_decimal FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_decimal ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(Number(parsed.data[0].d32), 123.45);
      assert.strictEqual(Number(parsed.data[0].d64), 12345.6789);
      assert.strictEqual(Number(parsed.data[1].d32), -99.99);

      for await (const _ of query("DROP TABLE test_rb_decimal", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Decimal128/Decimal256", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decimal_big (id UInt32, d128 Decimal128(10), d256 Decimal256(20)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      // Decimal128/256 must use Decimal(precision, scale) format to match ClickHouse
      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "d128", type: "Decimal(38, 10)" },
        { name: "d256", type: "Decimal(76, 20)" },
      ];
      const rows = [
        [1, "123456789.1234567890", "1234567890.12345678901234567890"],
        [2, "-999999999.9999999999", "-1.00000000000000000001"],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_decimal_big FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_decimal_big ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      // ClickHouse returns these as strings for big decimals
      const d128 = String(parsed.data[0].d128);
      const d256 = String(parsed.data[0].d256);
      assert.ok(d128.includes("123456789"));
      assert.ok(d256.includes("1234567890"));

      for await (const _ of query("DROP TABLE test_rb_decimal_big", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Enum types", () => {
    it("should insert RowBinary with Enum8", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_enum8 (id UInt32, status Enum8('pending' = 0, 'active' = 1, 'done' = 2)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        {
          name: "status",
          type: "Enum8('pending' = 0, 'active' = 1, 'done' = 2)",
        },
      ];
      const rows = [
        [1, 0],
        [2, 1],
        [3, 2],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_enum8 FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_enum8 ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].status, "pending");
      assert.strictEqual(parsed.data[1].status, "active");
      assert.strictEqual(parsed.data[2].status, "done");

      for await (const _ of query("DROP TABLE test_rb_enum8", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert RowBinary with Enum16", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_enum16 (id UInt32, priority Enum16('low' = 1, 'medium' = 100, 'high' = 1000)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        {
          name: "priority",
          type: "Enum16('low' = 1, 'medium' = 100, 'high' = 1000)",
        },
      ];
      const rows = [
        [1, 1],
        [2, 100],
        [3, 1000],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_enum16 FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_enum16 ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].priority, "low");
      assert.strictEqual(parsed.data[1].priority, "medium");
      assert.strictEqual(parsed.data[2].priority, "high");

      for await (const _ of query("DROP TABLE test_rb_enum16", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Complex/Nested types", () => {
    it("should insert RowBinary with nested Tuple and Array", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_nested (id UInt32, data Tuple(String, Array(Int32), Tuple(Float64, String))) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        {
          name: "data",
          type: "Tuple(String, Array(Int32), Tuple(Float64, String))",
        },
      ];
      const rows = [
        [1, ["outer", [1, 2, 3], [3.14, "inner"]]],
        [2, ["test", [], [2.71, "nested"]]],
      ];
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_nested FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT * FROM test_rb_nested ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.deepStrictEqual(parsed.data[0].data, [
        "outer",
        [1, 2, 3],
        [3.14, "inner"],
      ]);
      assert.deepStrictEqual(parsed.data[1].data, [
        "test",
        [],
        [2.71, "nested"],
      ]);

      for await (const _ of query("DROP TABLE test_rb_nested", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
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
        )) {
        }
      } catch {
        // Setting may not exist in older versions
      }

      try {
        for await (const _ of query(
          "CREATE TABLE IF NOT EXISTS test_rb_variant (id UInt32, v Variant(String, Int32, Float64)) ENGINE = Memory",
          sessionId,
          { baseUrl, auth, compression: "none" },
        )) {
        }
      } catch (err) {
        // Skip test if Variant not supported
        console.log(
          "    Skipping Variant test - not supported in this ClickHouse version",
        );
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
      const data = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_variant FORMAT RowBinaryWithNamesAndTypes",
        data,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT id, v, variantType(v) as vtype FROM test_rb_variant ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 4);
      assert.strictEqual(parsed.data[0].v, "hello");
      assert.strictEqual(parsed.data[0].vtype, "String");
      assert.strictEqual(parsed.data[1].v, "42");
      assert.strictEqual(parsed.data[1].vtype, "Int32");
      assert.strictEqual(parsed.data[3].v, null);
      assert.strictEqual(parsed.data[3].vtype, "None");

      for await (const _ of query("DROP TABLE test_rb_variant", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Decoding", () => {
    it("should query and decode with RowBinaryWithNamesAndTypes format", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decode (id UInt32, name String, value Float64, flag Bool) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

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
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_decode FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const data = await collectBytes(
        query(
          "SELECT * FROM test_rb_decode ORDER BY id FORMAT RowBinaryWithNamesAndTypes",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const decoded = decodeRowBinary(data);

      assert.strictEqual(decoded.columns.length, 4);
      assert.strictEqual(decoded.columns[0].name, "id");
      assert.strictEqual(decoded.columns[0].type, "UInt32");
      assert.strictEqual(decoded.rows.length, 2);
      assert.strictEqual(decoded.rows[0][0], 1);
      assert.strictEqual(decoded.rows[0][1], "alice");
      assert.strictEqual(decoded.rows[0][3], true);

      for await (const _ of query("DROP TABLE test_rb_decode", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should decode complex types from ClickHouse", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_decode_complex (id UInt32, tags Array(String), attrs Map(String, Int32), data Tuple(String, Float64)) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

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
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_decode_complex FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const data = await collectBytes(
        query(
          "SELECT * FROM test_rb_decode_complex ORDER BY id FORMAT RowBinaryWithNamesAndTypes",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const decoded = decodeRowBinary(data);

      assert.strictEqual(decoded.rows.length, 2);
      assert.deepStrictEqual(decoded.rows[0][1], ["foo", "bar"]);
      // Maps are now returned as Map objects
      const attrs = decoded.rows[0][2] as Map<string, number>;
      assert.ok(attrs instanceof Map);
      assert.strictEqual(attrs.get("a"), 10);
      assert.strictEqual(attrs.get("b"), 20);
      const tuple0 = decoded.rows[0][3] as unknown[];
      assert.strictEqual(tuple0[0], "hello");

      for await (const _ of query(
        "DROP TABLE test_rb_decode_complex",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }
    });
  });

  describe("JSON type", () => {
    it("should insert and query JSON column with various value types", async () => {
      // ClickHouse 24.1+ supports JSON type
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_json (id UInt32, data JSON) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "JSON" },
      ];
      const rows = [
        [1, { str: "hello", num: 42, flag: true, arr: [1, 2, 3] }],
        [2, { str: "world", num: 100, flag: false, arr: [4, 5] }],
        [3, { str: "test", num: 0, flag: true, arr: [] }],
      ];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_json FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      // Query back using JSON format to verify data
      const result = await collectText(
        query(
          "SELECT id, data.str, data.num, data.flag FROM test_rb_json ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0]["data.str"], "hello");
      assert.strictEqual(Number(parsed.data[0]["data.num"]), 42);
      assert.strictEqual(parsed.data[1]["data.str"], "world");

      for await (const _ of query("DROP TABLE test_rb_json", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert JSON with null values", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_json_null (id UInt32, data JSON) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "JSON" },
      ];
      const rows = [
        [1, { name: "alice", value: null }],
        [2, { name: "bob", value: 42 }],
      ];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_json_null FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT id, data.name, data.value FROM test_rb_json_null ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 2);
      assert.strictEqual(parsed.data[0]["data.name"], "alice");
      // null values in JSON become NULL in ClickHouse
      assert.ok(
        parsed.data[0]["data.value"] === null ||
          parsed.data[0]["data.value"] === undefined ||
          parsed.data[0]["data.value"] === 0,
      );
      assert.strictEqual(parsed.data[1]["data.name"], "bob");

      for await (const _ of query("DROP TABLE test_rb_json_null", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert JSON with Date values", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_json_date (id UInt32, data JSON) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "JSON" },
      ];
      const testDate = new Date("2024-06-15T10:30:00.123Z");
      const rows = [[1, { event: "login", timestamp: testDate }]];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_json_date FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT id, data.event, data.timestamp FROM test_rb_json_date FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 1);
      assert.strictEqual(parsed.data[0]["data.event"], "login");
      // DateTime64(3) is returned as a string - parse it
      const timestampValue = parsed.data[0]["data.timestamp"];
      // ClickHouse may return it as a numeric timestamp or formatted string
      const returnedMs =
        typeof timestampValue === "string"
          ? new Date(timestampValue).getTime()
          : Number(timestampValue) * 1000; // If numeric, it's Unix seconds
      // Just verify the date was stored and retrieved (ClickHouse may adjust timezone)
      assert.ok(
        !isNaN(returnedMs),
        `timestamp should be parseable: ${timestampValue}`,
      );

      for await (const _ of query("DROP TABLE test_rb_json_date", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should round-trip JSON via RowBinaryWithNamesAndTypes", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_json_rt (id UInt32, data JSON) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "JSON" },
      ];
      const rows = [[1, { name: "test", count: 5, active: true }]];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_json_rt FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      // Query back in RowBinaryWithNamesAndTypes
      const data = await collectBytes(
        query(
          "SELECT * FROM test_rb_json_rt FORMAT RowBinaryWithNamesAndTypes",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const decoded = decodeRowBinary(data);
      assert.strictEqual(decoded.rows.length, 1);
      assert.strictEqual(decoded.rows[0][0], 1);
      // The JSON column should be decoded as an object
      const jsonData = decoded.rows[0][1] as Record<string, unknown>;
      assert.strictEqual(jsonData.name, "test");

      for await (const _ of query("DROP TABLE test_rb_json_rt", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });

  describe("Dynamic type", () => {
    it("should insert and query Dynamic column with inferred types", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_dynamic (id UInt32, data Dynamic) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "Dynamic" },
      ];
      // Use inferred types - plain JS values
      const rows = [
        [1, 42], // Int64
        [2, "hello"], // String
        [3, true], // Bool
        [4, 3.14], // Float64
        [5, null], // Nothing
      ];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_dynamic FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT id, data, dynamicType(data) as dtype FROM test_rb_dynamic ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 5);
      assert.strictEqual(Number(parsed.data[0].data), 42);
      assert.strictEqual(parsed.data[0].dtype, "Int64");
      assert.strictEqual(parsed.data[1].data, "hello");
      assert.strictEqual(parsed.data[1].dtype, "String");
      assert.strictEqual(parsed.data[2].data, true);
      assert.strictEqual(parsed.data[2].dtype, "Bool");
      assert.ok(Math.abs(parsed.data[3].data - 3.14) < 0.001);
      assert.strictEqual(parsed.data[3].dtype, "Float64");
      assert.strictEqual(parsed.data[4].data, null);

      for await (const _ of query("DROP TABLE test_rb_dynamic", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });

    it("should insert Dynamic with explicit types", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_dynamic_explicit (id UInt32, data Dynamic) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "Dynamic" },
      ];
      // Use explicit {type, value} format
      const rows = [
        [1, { type: "UInt8", value: 255 }],
        [2, { type: "Int16", value: -1000 }],
        [3, { type: "Array(String)", value: ["a", "b", "c"] }],
      ];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_dynamic_explicit FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const result = await collectText(
        query(
          "SELECT id, data, dynamicType(data) as dtype FROM test_rb_dynamic_explicit ORDER BY id FORMAT JSON",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const parsed = JSON.parse(result);
      assert.strictEqual(parsed.data.length, 3);
      assert.strictEqual(parsed.data[0].dtype, "UInt8");
      assert.strictEqual(Number(parsed.data[0].data), 255);
      assert.strictEqual(parsed.data[1].dtype, "Int16");
      assert.strictEqual(Number(parsed.data[1].data), -1000);
      assert.strictEqual(parsed.data[2].dtype, "Array(String)");
      assert.deepStrictEqual(parsed.data[2].data, ["a", "b", "c"]);

      for await (const _ of query(
        "DROP TABLE test_rb_dynamic_explicit",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }
    });

    it("should round-trip Dynamic via RowBinaryWithNamesAndTypes", async () => {
      for await (const _ of query(
        "CREATE TABLE IF NOT EXISTS test_rb_dynamic_rt (id UInt32, data Dynamic) ENGINE = Memory",
        sessionId,
        { baseUrl, auth, compression: "none" },
      )) {
      }

      const columns: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "data", type: "Dynamic" },
      ];
      const rows = [
        [1, 42],
        [2, "hello"],
        [3, null],
      ];
      const encoded = encodeRowBinary(columns, rows);

      await insert(
        "INSERT INTO test_rb_dynamic_rt FORMAT RowBinaryWithNamesAndTypes",
        encoded,
        sessionId,
        { baseUrl, auth },
      );

      const data = await collectBytes(
        query(
          "SELECT * FROM test_rb_dynamic_rt ORDER BY id FORMAT RowBinaryWithNamesAndTypes",
          sessionId,
          { baseUrl, auth },
        ),
      );

      const decoded = decodeRowBinary(data);
      assert.strictEqual(decoded.rows.length, 3);
      // Dynamic values come back as {type, value}
      const row0data = decoded.rows[0][1] as { type: string; value: unknown };
      assert.strictEqual(row0data.type, "Int64");
      assert.strictEqual(row0data.value, 42n);
      const row1data = decoded.rows[1][1] as { type: string; value: unknown };
      assert.strictEqual(row1data.type, "String");
      assert.strictEqual(row1data.value, "hello");
      assert.strictEqual(decoded.rows[2][1], null);

      for await (const _ of query("DROP TABLE test_rb_dynamic_rt", sessionId, {
        baseUrl,
        auth,
        compression: "none",
      })) {
      }
    });
  });
});
