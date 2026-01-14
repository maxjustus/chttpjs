import assert from "node:assert";
import { describe, it } from "node:test";
import { type ColumnDef, RecordBatch } from "../../native/index.ts";
import { parseEnumDefinition } from "../../native/types.ts";
import { decodeBatch, encodeNativeRows, toArrayRows } from "../test_utils.ts";

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
      [
        -128,
        -32768,
        -2147483648,
        -9223372036854775808n,
        255,
        65535,
        4294967295,
        18446744073709551615n,
      ],
      [127, 32767, 2147483647, 9223372036854775807n, 0, 0, 0, 0n],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("throws on integer overflow and non-integer values", () => {
    assert.throws(
      () => encodeNativeRows([{ name: "i8", type: "Int8" }], [[128]]),
      /Int8 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u8", type: "UInt8" }], [[-1]]),
      /UInt8 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "i32", type: "Int32" }], [[1.5]]),
      /expected integer/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u32", type: "UInt32" }], [[4294967296]]),
      /UInt32 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "i64", type: "Int64" }], [[9223372036854775808n]]),
      /Int64 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u64", type: "UInt64" }], [[-1n]]),
      /UInt64 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "i64", type: "Int64" }], [[9007199254740992]]),
      /cannot safely represent number/,
    );
  });

  it("encodes Float32 and Float64", async () => {
    const columns: ColumnDef[] = [
      { name: "f32", type: "Float32" },
      { name: "f64", type: "Float64" },
    ];
    const rows = [
      [3.14, Math.PI],
      [-1.5, -1.5],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Float32 loses precision
    const decodedRows = toArrayRows(decoded);
    assert.strictEqual(typeof decodedRows[0][0], "number");
    assert.strictEqual(decodedRows[0][1], Math.PI);
  });

  it("throws on Float Infinity and NaN", () => {
    assert.throws(
      () => encodeNativeRows([{ name: "f32", type: "Float32" }], [[Infinity]]),
      /must be finite/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "f32", type: "Float32" }], [[-Infinity]]),
      /must be finite/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "f32", type: "Float32" }], [[NaN]]),
      /must be finite/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "f64", type: "Float64" }], [[Infinity]]),
      /must be finite/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "f64", type: "Float64" }], [[NaN]]),
      /must be finite/,
    );
  });

  it("encodes String with unicode", async () => {
    const columns: ColumnDef[] = [{ name: "text", type: "String" }];
    const rows = [["hello"], ["世界"], ["🎉"], [""]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("coerces arrays/objects to JSON strings for String", async () => {
    const columns: ColumnDef[] = [{ name: "s", type: "String" }];
    const rows = [[{ a: 1 }], [[1, 2, 3]], [new Map([["x", 1]])], [1n], [new Uint8Array([1, 2])]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], '{"a":1}');
    assert.strictEqual(decodedRows[1][0], "[1,2,3]");
    assert.strictEqual(decodedRows[2][0], '{"x":1}');
    assert.strictEqual(decodedRows[3][0], "1");
    assert.strictEqual(decodedRows[4][0], "[1,2]");
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
    assert.deepStrictEqual([...(decodedRows[0][0] as Int32Array)], [1, 2, 3]);
    assert.deepStrictEqual([...(decodedRows[1][0] as Int32Array)], []);
    assert.deepStrictEqual([...(decodedRows[2][0] as Int32Array)], [42]);
  });

  it("treats null/undefined as defaults for container types", async () => {
    const columns: ColumnDef[] = [
      { name: "arr", type: "Array(Int32)" },
      { name: "m", type: "Map(String, Int32)" },
      { name: "t", type: "Tuple(Int32, String)" },
      { name: "e", type: "Enum8('a' = 2, 'b' = 1)" },
    ];
    const rows = [
      [null, null, null, null],
      [undefined, undefined, undefined, undefined],
      [[1, 2], { x: 1 }, [3, "z"], "b"],
    ];

    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded, { enumAsNumber: true });
    const decodedRows = toArrayRows(decoded);

    // Array defaults to empty array
    assert.deepStrictEqual([...(decodedRows[0][0] as Int32Array)], []);
    assert.deepStrictEqual([...(decodedRows[1][0] as Int32Array)], []);

    // Map defaults to empty map
    assert.strictEqual((decodedRows[0][1] as Map<unknown, unknown>).size, 0);
    assert.strictEqual((decodedRows[1][1] as Map<unknown, unknown>).size, 0);

    // Tuple defaults each element (Int32=0, String="")
    assert.deepStrictEqual(decodedRows[0][2], [0, ""]);
    assert.deepStrictEqual(decodedRows[1][2], [0, ""]);

    // Enum defaults to minimum mapped value for both null and undefined.
    // (ClickHouse Native protocol silently converts invalid values like 0 to min.)
    assert.strictEqual(decodedRows[0][3], 1);
    assert.strictEqual(decodedRows[1][3], 1);

    // Non-default row still round-trips
    assert.deepStrictEqual([...(decodedRows[2][0] as Int32Array)], [1, 2]);
    assert.strictEqual((decodedRows[2][1] as Map<string, number>).get("x"), 1);
    assert.deepStrictEqual(decodedRows[2][2], [3, "z"]);
    assert.strictEqual(decodedRows[2][3], 1);
  });

  it("encodes Map", async () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    const rows = [[{ a: 1, b: 2 }], [{}]];
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

  it("throws on Date/DateTime out of range", () => {
    assert.throws(
      () => encodeNativeRows([{ name: "d", type: "Date" }], [[new Date("2300-01-01")]]),
      /Date out of range/,
    );
    assert.throws(
      () =>
        encodeNativeRows([{ name: "dt", type: "DateTime" }], [[new Date("2200-01-01T00:00:00Z")]]),
      /DateTime out of range/,
    );
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

  it("encodes Date/DateTime from ISO strings", async () => {
    const columns: ColumnDef[] = [
      { name: "d", type: "Date" },
      { name: "dt", type: "DateTime" },
      { name: "d32", type: "Date32" },
    ];
    const isoFull = "2024-01-15T10:30:00.000Z";
    const isoWithZ = "2024-01-15T10:30:00Z";
    const isoDateOnly = "2024-01-15";
    const rows = [[isoFull, isoWithZ, isoDateOnly]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Date truncates to day precision
    assert.ok(decodedRows[0][0] instanceof Date);
    assert.strictEqual((decodedRows[0][0] as Date).toISOString().split("T")[0], "2024-01-15");
    // DateTime has second precision
    assert.ok(decodedRows[0][1] instanceof Date);
    assert.strictEqual((decodedRows[0][1] as Date).getUTCHours(), 10);
    // Date32 truncates to day precision
    assert.ok(decodedRows[0][2] instanceof Date);
    assert.strictEqual((decodedRows[0][2] as Date).toISOString().split("T")[0], "2024-01-15");
  });

  it("encodes DateTime64 from ISO strings", async () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const isoFull = "2024-01-15T10:30:00.123Z";
    const isoNoMs = "2024-01-15T10:30:00";
    const rows = [[isoFull], [isoNoMs]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const dt1 = decodedRows[0][0] as { toDate(): Date };
    assert.strictEqual(dt1.toDate().getTime(), new Date(isoFull).getTime());
    const dt2 = decodedRows[1][0] as { toDate(): Date };
    assert.strictEqual(dt2.toDate().getTime(), new Date(isoNoMs).getTime());
  });

  it("encodes DateTime64 from Unix timestamp number", async () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const timestampMs = 1705315800123; // 2024-01-15T10:30:00.123Z
    const rows = [[timestampMs]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const dt = decodedRows[0][0] as { toDate(): Date };
    assert.strictEqual(dt.toDate().getTime(), timestampMs);
  });

  it("coerces boolean to numeric types", async () => {
    const columns: ColumnDef[] = [
      { name: "i8", type: "Int8" },
      { name: "u32", type: "UInt32" },
      { name: "i64", type: "Int64" },
      { name: "f64", type: "Float64" },
    ];
    const rows = [
      [true, true, true, true],
      [false, false, false, false],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // true -> 1
    assert.strictEqual(decodedRows[0][0], 1);
    assert.strictEqual(decodedRows[0][1], 1);
    assert.strictEqual(decodedRows[0][2], 1n);
    assert.strictEqual(decodedRows[0][3], 1);
    // false -> 0
    assert.strictEqual(decodedRows[1][0], 0);
    assert.strictEqual(decodedRows[1][1], 0);
    assert.strictEqual(decodedRows[1][2], 0n);
    assert.strictEqual(decodedRows[1][3], 0);
  });

  it("throws on invalid string coercion to number", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Int32" }];
    const rows = [["not-a-number"]];
    assert.throws(
      () => encodeNativeRows(columns, rows),
      /Cannot coerce string "not-a-number" to number/,
    );
  });

  it("coerces valid numeric strings to numbers", async () => {
    const columns: ColumnDef[] = [
      { name: "i32", type: "Int32" },
      { name: "f64", type: "Float64" },
    ];
    const rows = [
      ["123", "3.14"],
      [" -456 ", " -2.5 "], // whitespace trimmed
      ["0", "0.0"],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);
    assert.strictEqual(decodedRows[0][0], 123);
    assert.strictEqual(decodedRows[0][1], 3.14);
    assert.strictEqual(decodedRows[1][0], -456);
    assert.strictEqual(decodedRows[1][1], -2.5);
    assert.strictEqual(decodedRows[2][0], 0);
    assert.strictEqual(decodedRows[2][1], 0.0);
  });

  it("throws on invalid string coercion to bigint", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Int64" }];
    const rows = [["not-a-number"]];
    assert.throws(
      () => encodeNativeRows(columns, rows),
      /Cannot coerce string "not-a-number" to bigint/,
    );
  });

  it("throws on invalid string coercion to DateTime", () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime" }];
    const rows = [["not-a-date"]];
    assert.throws(() => encodeNativeRows(columns, rows), /Cannot coerce "not-a-date" to DateTime/);
  });

  it("throws on invalid string coercion to DateTime64", () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const rows = [["not-a-date"]];
    assert.throws(
      () => encodeNativeRows(columns, rows),
      /Cannot coerce "not-a-date" to DateTime64/,
    );
  });

  it("coerces null/undefined to defaults without throwing", async () => {
    const columns: ColumnDef[] = [
      { name: "n", type: "Int32" },
      { name: "dt", type: "DateTime" },
    ];
    const rows = [[null, undefined]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], 0);
    assert.strictEqual((decodedRows[0][1] as Date).getTime(), 0);
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

  it("throws on invalid IPv4 address", () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv4" }];
    assert.throws(() => encodeNativeRows(columns, [["not-an-ip"]]), /Invalid IPv4 address/);
    assert.throws(
      () => encodeNativeRows(columns, [["256.1.1.1"]]),
      /Invalid IPv4 address.*octet 256 > 255/,
    );
  });

  it("throws on invalid IPv6 address", () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv6" }];
    assert.throws(() => encodeNativeRows(columns, [["not-an-ip"]]), /Invalid IPv6 address/);
    assert.throws(() => encodeNativeRows(columns, [[":::"]]), /Invalid IPv6 address/);
    assert.throws(() => encodeNativeRows(columns, [["2001:db8::1::2"]]), /Invalid IPv6 address/);
  });

  it("throws on invalid UUID", () => {
    const columns: ColumnDef[] = [{ name: "id", type: "UUID" }];
    assert.throws(() => encodeNativeRows(columns, [["not-a-uuid"]]), /Invalid UUID/);
    assert.throws(() => encodeNativeRows(columns, [["123"]]), /Invalid UUID/);
  });

  it("throws on invalid Decimal", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal64(2)" }];
    assert.throws(() => encodeNativeRows(columns, [["not-a-number"]]), /Invalid Decimal/);
    assert.throws(() => encodeNativeRows(columns, [["12.34.56"]]), /Invalid Decimal/);
  });

  it("encodes Enum8 and supports both decode modes", async () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    const rows = [[1], [2], [1]];
    const encoded = encodeNativeRows(columns, rows);

    const decodedStrings = await decodeBatch(encoded);
    assert.deepStrictEqual(decodedStrings.columns, columns);
    assert.deepStrictEqual(toArrayRows(decodedStrings), [["a"], ["b"], ["a"]]);

    const decodedNumbers = await decodeBatch(encoded, { enumAsNumber: true });
    assert.deepStrictEqual(decodedNumbers.columns, columns);
    assert.deepStrictEqual(toArrayRows(decodedNumbers), [[1], [2], [1]]);
  });

  it("encodes Enum8 with string values", async () => {
    const columns: ColumnDef[] = [
      { name: "e", type: "Enum8('pending' = 0, 'active' = 1, 'done' = 2)" },
    ];
    const rows = [["pending"], ["active"], ["done"], ["pending"]];
    const encoded = encodeNativeRows(columns, rows);

    const decodedStrings = await decodeBatch(encoded);
    assert.deepStrictEqual(toArrayRows(decodedStrings), [
      ["pending"],
      ["active"],
      ["done"],
      ["pending"],
    ]);

    const decodedNumbers = await decodeBatch(encoded, { enumAsNumber: true });
    assert.deepStrictEqual(toArrayRows(decodedNumbers), [[0], [1], [2], [0]]);
  });

  it("decodes Enum8 as numbers with enumAsNumber option", async () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    const rows = [[1], [2]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded, { enumAsNumber: true });

    assert.deepStrictEqual(toArrayRows(decoded), [[1], [2]]);
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

  it("encodes Decimal32, Decimal128, and Decimal256", async () => {
    const columns: ColumnDef[] = [
      { name: "d32", type: "Decimal32(2)" },
      { name: "d128", type: "Decimal128(6)" },
      { name: "d256", type: "Decimal256(10)" },
    ];
    const rows = [
      ["12.34", "12345.678901", "-1234567890.0123456789"],
      ["-0.01", "0.000001", "9999999999.9999999999"],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(decodedRows, rows);
  });

  it("throws on Decimal out of range", () => {
    assert.throws(
      () => encodeNativeRows([{ name: "d", type: "Decimal64(2)" }], [["9223372036854775807"]]),
      /Decimal64\(2\) out of range/,
    );
  });

  it("encodes Int128", async () => {
    const columns: ColumnDef[] = [{ name: "i", type: "Int128" }];
    const rows = [
      [170141183460469231731687303715884105727n],
      [-170141183460469231731687303715884105728n],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], 170141183460469231731687303715884105727n);
    assert.strictEqual(decodedRows[1][0], -170141183460469231731687303715884105728n);
  });

  it("encodes UInt128", async () => {
    const columns: ColumnDef[] = [{ name: "u", type: "UInt128" }];
    const maxU128 = (1n << 128n) - 1n;
    const rows = [[maxU128], [0n]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], maxU128);
    assert.strictEqual(decodedRows[1][0], 0n);
  });

  it("encodes Int256", async () => {
    const columns: ColumnDef[] = [{ name: "i", type: "Int256" }];
    const maxI256 = (1n << 255n) - 1n;
    const minI256 = -(1n << 255n);
    const rows = [[maxI256], [minI256]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], maxI256);
    assert.strictEqual(decodedRows[1][0], minI256);
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

  it("throws on out-of-range Int128/UInt128/Int256/UInt256", () => {
    assert.throws(
      () => encodeNativeRows([{ name: "i", type: "Int128" }], [[1n << 127n]]),
      /Int128 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u", type: "UInt128" }], [[-1n]]),
      /UInt128 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u", type: "UInt128" }], [[1n << 128n]]),
      /UInt128 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "i", type: "Int256" }], [[1n << 255n]]),
      /Int256 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u", type: "UInt256" }], [[-1n]]),
      /UInt256 out of range/,
    );
    assert.throws(
      () => encodeNativeRows([{ name: "u", type: "UInt256" }], [[1n << 256n]]),
      /UInt256 out of range/,
    );
  });

  it("throws on invalid FixedString input type", () => {
    const columns: ColumnDef[] = [{ name: "fs", type: "FixedString(5)" }];
    assert.throws(
      () => encodeNativeRows(columns, [[12345]]),
      /Cannot coerce number to FixedString/,
    );
    assert.throws(
      () => encodeNativeRows(columns, [[{ obj: true }]]),
      /Cannot coerce object to FixedString/,
    );
  });

  it("throws on non-array input for Array column", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    assert.throws(
      () => encodeNativeRows(columns, [["not-an-array"]]),
      /Expected array for Array\(Int32\), got string/,
    );
    assert.throws(
      () => encodeNativeRows(columns, [[123]]),
      /Expected array for Array\(Int32\), got number/,
    );
  });

  it("throws on invalid Map input", () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    assert.throws(
      () => encodeNativeRows(columns, [["not-a-map"]]),
      /Expected Map, Array, or object.*got string/,
    );
  });

  it("throws on invalid Tuple input", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, String)" }];
    assert.throws(
      () => encodeNativeRows(columns, [["not-a-tuple"]]),
      /Expected array for tuple.*got string/,
    );
  });

  it("throws on invalid named Tuple input", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(a Int32, b String)" }];
    assert.throws(
      () => encodeNativeRows(columns, [["not-an-object"]]),
      /Expected object for named tuple.*got string/,
    );
  });

  it("throws on invalid Enum value", () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    assert.throws(() => encodeNativeRows(columns, [[128]]), /Enum value out of range/);
    assert.throws(() => encodeNativeRows(columns, [[3]]), /Invalid enum value/);
  });

  it("throws on invalid Map array pairs", () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    // Array with invalid pair (not [key, value])
    assert.throws(
      () => encodeNativeRows(columns, [[[["a", 1], "invalid", ["b", 2]]]]),
      /Invalid Map entry at index 1.*expected \[key, value\] pair/,
    );
    // Array with wrong-length pair
    assert.throws(
      () => encodeNativeRows(columns, [[[["a", 1, "extra"]]]]),
      /Invalid Map entry at index 0.*expected \[key, value\] pair/,
    );
  });

  it("throws on wrong-sized Uint8Array for FixedString", () => {
    const columns: ColumnDef[] = [{ name: "fs", type: "FixedString(5)" }];
    assert.throws(
      () => encodeNativeRows(columns, [[new Uint8Array(3)]]),
      /FixedString\(5\) requires 5 bytes, got 3/,
    );
    assert.throws(
      () => encodeNativeRows(columns, [[new Uint8Array(10)]]),
      /FixedString\(5\) requires 5 bytes, got 10/,
    );
  });

  it("throws on too-long string for FixedString", () => {
    const columns: ColumnDef[] = [{ name: "fs", type: "FixedString(5)" }];
    assert.throws(
      () => encodeNativeRows(columns, [["toolong"]]),
      /FixedString\(5\) requires 5 bytes, got 7/,
    );
    // Multibyte: "世界" is 6 bytes in UTF-8.
    assert.throws(
      () => encodeNativeRows(columns, [["世界"]]),
      /FixedString\(5\) requires 5 bytes, got 6/,
    );
  });

  it("coerces Bool strings correctly", async () => {
    const columns: ColumnDef[] = [{ name: "b", type: "Bool" }];
    const rows = [
      ["true"],
      ["false"],
      ["1"],
      ["0"],
      ["TRUE"],
      ["FALSE"],
      [true],
      [false],
      [1],
      [0],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], 1); // "true"
    assert.strictEqual(decodedRows[1][0], 0); // "false"
    assert.strictEqual(decodedRows[2][0], 1); // "1"
    assert.strictEqual(decodedRows[3][0], 0); // "0"
    assert.strictEqual(decodedRows[4][0], 1); // "TRUE"
    assert.strictEqual(decodedRows[5][0], 0); // "FALSE"
    assert.strictEqual(decodedRows[6][0], 1); // true
    assert.strictEqual(decodedRows[7][0], 0); // false
    assert.strictEqual(decodedRows[8][0], 1); // 1
    assert.strictEqual(decodedRows[9][0], 0); // 0
  });

  it("throws on invalid Bool string", () => {
    const columns: ColumnDef[] = [{ name: "b", type: "Bool" }];
    assert.throws(() => encodeNativeRows(columns, [["yes"]]), /Cannot coerce string "yes" to Bool/);
    assert.throws(() => encodeNativeRows(columns, [["no"]]), /Cannot coerce string "no" to Bool/);
    assert.throws(() => encodeNativeRows(columns, [[{}]]), /Cannot coerce object to Bool/);
    // Empty string is not a valid Bool (matches CH input_format_json_empty_as_default=false)
    assert.throws(() => encodeNativeRows(columns, [[""]]), /Cannot coerce string "" to Bool/);
  });

  it("coerces Int128/UInt128 with toBigInt helper", async () => {
    const columns: ColumnDef[] = [
      { name: "i128", type: "Int128" },
      { name: "u128", type: "UInt128" },
    ];
    const rows = [
      [true, true], // boolean -> 1n
      [false, false], // boolean -> 0n
      [null, null], // null -> 0n
      ["123", "456"], // string -> bigint
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], 1n);
    assert.strictEqual(decodedRows[0][1], 1n);
    assert.strictEqual(decodedRows[1][0], 0n);
    assert.strictEqual(decodedRows[1][1], 0n);
    assert.strictEqual(decodedRows[2][0], 0n);
    assert.strictEqual(decodedRows[2][1], 0n);
    assert.strictEqual(decodedRows[3][0], 123n);
    assert.strictEqual(decodedRows[3][1], 456n);
  });

  it("throws on invalid Int128 input", () => {
    const columns: ColumnDef[] = [{ name: "i128", type: "Int128" }];
    assert.throws(
      () => encodeNativeRows(columns, [["not-a-number"]]),
      /Cannot coerce string "not-a-number" to bigint/,
    );
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
    assert.strictEqual(
      dt.toClosestDate().getTime(),
      new Date("2024-01-15T10:30:00.500Z").getTime(),
    );
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
    assert.strictEqual(
      dt.toClosestDate().getTime(),
      new Date("2024-01-15T10:30:00.120Z").getTime(),
    );
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
    assert.strictEqual(
      dt.toClosestDate().getTime(),
      new Date("2024-01-15T10:30:00.000Z").getTime(),
    );
  });
});

describe("parseEnumDefinition", () => {
  it("parses Enum8 with simple values", () => {
    const result = parseEnumDefinition("Enum8('a' = 1, 'b' = 2)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("a"), 1);
    assert.strictEqual(result.nameToValue.get("b"), 2);
    assert.strictEqual(result.valueToName.get(1), "a");
    assert.strictEqual(result.valueToName.get(2), "b");
  });

  it("parses Enum16 with negative values", () => {
    const result = parseEnumDefinition("Enum16('error' = -1, 'ok' = 0, 'pending' = 1)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("error"), -1);
    assert.strictEqual(result.nameToValue.get("ok"), 0);
    assert.strictEqual(result.nameToValue.get("pending"), 1);
  });

  it("parses enum with spaces in names", () => {
    const result = parseEnumDefinition("Enum8('hello world' = 1, 'foo bar' = 2)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("hello world"), 1);
    assert.strictEqual(result.nameToValue.get("foo bar"), 2);
  });

  it("parses enum with escaped quotes", () => {
    const result = parseEnumDefinition("Enum8('it\\'s' = 1, 'don\\'t' = 2)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("it's"), 1);
    assert.strictEqual(result.nameToValue.get("don't"), 2);
  });

  it("parses enum with backslash escapes from ClickHouse tests", () => {
    const result = parseEnumDefinition("Enum8('Hello' = -100, '\\\\' = 0, '\\t\\\\t' = 111)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("Hello"), -100);
    assert.strictEqual(result.nameToValue.get("\\"), 0);
    assert.strictEqual(result.nameToValue.get("\t\\t"), 111);
  });

  it("preserves unknown escapes but drops backslash for special cases", () => {
    const result = parseEnumDefinition("Enum8('a\\%b' = 1, 'a\\=b' = 2)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("a\\%b"), 1);
    assert.strictEqual(result.nameToValue.get("a=b"), 2);
  });

  it("parses hex escapes", () => {
    const result = parseEnumDefinition("Enum8('\\x41\\x42' = 1)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("AB"), 1);
  });

  it("parses explicit + sign", () => {
    const result = parseEnumDefinition("Enum8('a' = +1, 'b' = -2)");
    assert.ok(result);
    assert.strictEqual(result.nameToValue.get("a"), 1);
    assert.strictEqual(result.nameToValue.get("b"), -2);
  });

  it("returns null for invalid type strings", () => {
    assert.strictEqual(parseEnumDefinition("Int32"), null);
    assert.strictEqual(parseEnumDefinition("Enum8()"), null);
    assert.strictEqual(parseEnumDefinition("Enum8(invalid)"), null);
    assert.strictEqual(parseEnumDefinition("Enum8('unterminated = 1)"), null);
    assert.strictEqual(parseEnumDefinition("Enum8('\\x4G' = 1)"), null);
    assert.strictEqual(parseEnumDefinition("Enum8('dup' = 1, 'dup' = 2)"), null);
    assert.strictEqual(parseEnumDefinition("Enum8('a' = 1, 'b' = 1)"), null);
  });
});
