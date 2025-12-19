import { describe, it } from "node:test";
import assert from "node:assert";
import { RecordBatch, type ColumnDef } from "../../native/index.ts";
import { encodeNativeRows, decodeBatch, toArrayRows } from "../test_utils.ts";

describe("encodeNative", () => {
  it("encodes empty block", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const rows: unknown[][] = [];
    const encoded = encodeNativeRows(columns, rows);

    // Should have: 1 col, 0 rows, "id", "Int32", no data
    assert.ok(encoded.length > 0);

    const decoded = await decodeBatch(encoded);
    assert.ok(decoded instanceof RecordBatch);
    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 0);
  });

  it("encodes Int32 column", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const rows = [[1], [2], [3]];
    const encoded = encodeNativeRows(columns, rows);
    const table = await decodeBatch(encoded);

    assert.ok(table instanceof RecordBatch);
    assert.deepStrictEqual(table.columns, columns);

    // Test lazy iteration
    const resultRows = [];
    for (const row of table) {
      resultRows.push([row.id]);
    }
    assert.deepStrictEqual(resultRows, [[1], [2], [3]]);

    // Test toArrayRows helper
    assert.deepStrictEqual(toArrayRows(table), [[1], [2], [3]]);
  });

  it("encodes multiple columns", async () => {
    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "name", type: "String" },
      { name: "score", type: "Float64" },
    ];
    const rows = [
      [1, "alice", 1.5],
      [2, "bob", 2.5],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const table = await decodeBatch(encoded);

    assert.deepStrictEqual(table.columns, columns);

    // Test row proxy access
    const row0 = table.get(0);
    assert.strictEqual(row0.id, 1);
    assert.strictEqual(row0.name, "alice");
    assert.strictEqual(row0.score, 1.5);

    assert.deepStrictEqual(toArrayRows(table), rows);
  });

  it("encodes all integer types", async () => {
    const columns: ColumnDef[] = [
      { name: "i8", type: "Int8" },
      { name: "i16", type: "Int16" },
      { name: "i32", type: "Int32" },
      { name: "i64", type: "Int64" },
      { name: "u8", type: "UInt8" },
      { name: "u16", type: "UInt16" },
      { name: "u32", type: "UInt32" },
      { name: "u64", type: "UInt64" },
    ];
    const rows = [
      [-128, -32768, -2147483648, -9223372036854775808n, 255, 65535, 4294967295, 18446744073709551615n],
      [127, 32767, 2147483647, 9223372036854775807n, 0, 0, 0, 0n],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Float32 and Float64", async () => {
    const columns: ColumnDef[] = [
      { name: "f32", type: "Float32" },
      { name: "f64", type: "Float64" },
    ];
    const rows = [
      [3.14, 3.141592653589793],
      [-1.5, -1.5],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Float32 loses precision
    const decodedRows = toArrayRows(decoded);
    assert.strictEqual(typeof decodedRows[0][0], "number");
    assert.strictEqual(decodedRows[0][1], 3.141592653589793);
  });

  it("encodes String with unicode", async () => {
    const columns: ColumnDef[] = [{ name: "text", type: "String" }];
    const rows = [["hello"], ["世界"], ["🎉"], [""]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Nullable", async () => {
    const columns: ColumnDef[] = [{ name: "val", type: "Nullable(Int32)" }];
    const rows = [[1], [null], [3]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Array", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    const rows = [[[1, 2, 3]], [[]], [[42]]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Arrays of integers decode as TypedArrays for performance
    assert.deepStrictEqual([...decodedRows[0][0] as Int32Array], [1, 2, 3]);
    assert.deepStrictEqual([...decodedRows[1][0] as Int32Array], []);
    assert.deepStrictEqual([...decodedRows[2][0] as Int32Array], [42]);
  });

  it("encodes Map", async () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    const rows = [
      [{ a: 1, b: 2 }],
      [{}],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Maps decode as Map objects
    assert.ok(decodedRows[0][0] instanceof Map);
    assert.strictEqual((decodedRows[0][0] as Map<string, number>).get("a"), 1);
  });

  it("encodes Tuple", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, String)" }];
    const rows = [[[1, "a"]], [[2, "b"]]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes named Tuple", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(id Int32, name String)" }];
    const rows = [[{ id: 1, name: "alice" }], [{ id: 2, name: "bob" }]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(decodedRows[0][0], { id: 1, name: "alice" });
  });

  it("encodes UUID", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "UUID" }];
    const rows = [["550e8400-e29b-41d4-a716-446655440000"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "550e8400-e29b-41d4-a716-446655440000");
  });

  it("encodes Date and DateTime", async () => {
    const columns: ColumnDef[] = [
      { name: "d", type: "Date" },
      { name: "dt", type: "DateTime" },
    ];
    const date = new Date("2024-01-15");
    const datetime = new Date("2024-01-15T10:30:00Z");
    const rows = [[date, datetime]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.ok(decodedRows[0][0] instanceof Date);
    assert.ok(decodedRows[0][1] instanceof Date);
  });
});

describe("additional scalar types", () => {
  it("encodes FixedString", async () => {
    const columns: ColumnDef[] = [{ name: "fs", type: "FixedString(5)" }];
    const rows = [["hello"], ["world"], ["hi\0\0\0"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // FixedString decodes as Uint8Array
    const textDecoder = new TextDecoder();
    assert.strictEqual(textDecoder.decode(decodedRows[0][0] as Uint8Array), "hello");
    assert.strictEqual(textDecoder.decode(decodedRows[1][0] as Uint8Array), "world");
  });

  it("encodes Date32", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Date32" }];
    const date = new Date("2024-01-15");
    const rows = [[date]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.ok(decodedRows[0][0] instanceof Date);
  });

  it("encodes DateTime64", async () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const date = new Date("2024-01-15T10:30:00.123Z");
    const rows = [[date]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // DateTime64 returns ClickHouseDateTime64 wrapper
    const dt = decodedRows[0][0] as { toDate(): Date };
    assert.strictEqual(dt.toDate().getTime(), date.getTime());
  });

  it("encodes IPv4", async () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv4" }];
    const rows = [["192.168.1.1"], ["10.0.0.1"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "192.168.1.1");
    assert.strictEqual(decodedRows[1][0], "10.0.0.1");
  });

  it("encodes IPv6", async () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv6" }];
    const rows = [["2001:db8::1"], ["::1"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // IPv6 may be normalized
    assert.ok(typeof decodedRows[0][0] === "string");
  });

  it("encodes Enum8", async () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    const rows = [[1], [2], [1]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), [[1], [2], [1]]);
  });

  it("encodes Decimal64", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal64(4)" }];
    const rows = [["123.4567"], ["-999.9999"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "123.4567");
    assert.strictEqual(decodedRows[1][0], "-999.9999");
  });

  it("encodes Int128", async () => {
    const columns: ColumnDef[] = [{ name: "i", type: "Int128" }];
    const rows = [[170141183460469231731687303715884105727n], [-170141183460469231731687303715884105728n]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], 170141183460469231731687303715884105727n);
    assert.strictEqual(decodedRows[1][0], -170141183460469231731687303715884105728n);
  });

  it("encodes UInt256", async () => {
    const columns: ColumnDef[] = [{ name: "u", type: "UInt256" }];
    const maxU256 = (1n << 256n) - 1n;
    const rows = [[maxU256], [0n]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], maxU256);
    assert.strictEqual(decodedRows[1][0], 0n);
  });
});

describe("DateTime64 precision edge cases", () => {
  it("encodes DateTime64(1) - precision < 3 requires division", async () => {
    // DateTime64(1) = deciseconds (1/10 second)
    // Precision < 3 triggered: BigInt(10 ** (1-3)) = BigInt(0.01) which fails
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(1)" }];
    const date = new Date("2024-01-15T10:30:00.500Z"); // 500ms -> 5 deciseconds
    const rows = [[date]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const dt = decodedRows[0][0] as { toClosestDate(): Date };
    // 500ms truncated to deciseconds (5 * 100ms = 500ms)
    assert.strictEqual(dt.toClosestDate().getTime(), new Date("2024-01-15T10:30:00.500Z").getTime());
  });

  it("encodes DateTime64(2) - precision < 3 requires division", async () => {
    // DateTime64(2) = centiseconds (1/100 second)
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(2)" }];
    const date = new Date("2024-01-15T10:30:00.120Z"); // 120ms -> 12 centiseconds
    const rows = [[date]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const dt = decodedRows[0][0] as { toClosestDate(): Date };
    assert.strictEqual(dt.toClosestDate().getTime(), new Date("2024-01-15T10:30:00.120Z").getTime());
  });

  it("encodes DateTime64(0) - seconds only", async () => {
    // DateTime64(0) = seconds (precision < 3)
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(0)" }];
    const date = new Date("2024-01-15T10:30:00.999Z"); // 999ms -> truncated to 0
    const rows = [[date]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const dt = decodedRows[0][0] as { toClosestDate(): Date };
    // 999ms truncated to seconds
    assert.strictEqual(dt.toClosestDate().getTime(), new Date("2024-01-15T10:30:00.000Z").getTime());
  });
});
