import { describe, it } from "node:test";
import assert from "node:assert";
import {
  encodeNative,
  decodeNative,
  Table,
  TableBuilder,
  streamEncodeNative,
  streamDecodeNative,
  toArrayRows,
  tableFromArrays,
  tableFromRows,
  tableFromCols,
  tableBuilder,
  makeBuilder,
  type ColumnDef,
} from "../formats/native/index.ts";

// Helper to encode rows via TableBuilder
function encodeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const builder = new TableBuilder(columns);
  for (const row of rows) builder.appendRow(row);
  return encodeNative(builder.finish());
}

// Helper to convert sync iterable to async
async function* toAsync<T>(iter: Iterable<T>): AsyncIterable<T> {
  for (const item of iter) yield item;
}

// Helper to collect async generator results
async function collect<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const results: T[] = [];
  for await (const item of gen) results.push(item);
  return results;
}

describe("encodeNative", () => {
  it("encodes empty block", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const rows: unknown[][] = [];
    const encoded = encodeRows(columns, rows);

    // Should have: 1 col, 0 rows, "id", "Int32", no data
    assert.ok(encoded.length > 0);

    const decoded = await decodeNative(encoded);
    assert.ok(decoded instanceof Table);
    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 0);
  });

  it("encodes Int32 column", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const rows = [[1], [2], [3]];
    const encoded = encodeRows(columns, rows);
    const table = await decodeNative(encoded);

    assert.ok(table instanceof Table);
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
    const encoded = encodeRows(columns, rows);
    const table = await decodeNative(encoded);

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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Float32 loses precision
    const decodedRows = toArrayRows(decoded);
    assert.strictEqual(typeof decodedRows[0][0], "number");
    assert.strictEqual(decodedRows[0][1], 3.141592653589793);
  });

  it("encodes String with unicode", async () => {
    const columns: ColumnDef[] = [{ name: "text", type: "String" }];
    const rows = [["hello"], ["世界"], ["🎉"], [""]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Nullable", async () => {
    const columns: ColumnDef[] = [{ name: "val", type: "Nullable(Int32)" }];
    const rows = [[1], [null], [3]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Array", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    const rows = [[[1, 2, 3]], [[]], [[42]]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Maps decode as Map objects
    assert.ok(decodedRows[0][0] instanceof Map);
    assert.strictEqual((decodedRows[0][0] as Map<string, number>).get("a"), 1);
  });

  it("encodes Tuple", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, String)" }];
    const rows = [[[1, "a"]], [[2, "b"]]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes named Tuple", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(id Int32, name String)" }];
    const rows = [[{ id: 1, name: "alice" }], [{ id: 2, name: "bob" }]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(decodedRows[0][0], { id: 1, name: "alice" });
  });

  it("encodes UUID", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "UUID" }];
    const rows = [["550e8400-e29b-41d4-a716-446655440000"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.ok(decodedRows[0][0] instanceof Date);
    assert.ok(decodedRows[0][1] instanceof Date);
  });
});

describe("streamEncodeNative", () => {
  it("streams tables", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];

    // Create tables to stream
    async function* generateTables() {
      yield Table.fromColumnar(columns, [new Int32Array([1, 2])]);
      yield Table.fromColumnar(columns, [new Int32Array([3, 4])]);
      yield Table.fromColumnar(columns, [new Int32Array([5])]);
    }

    const chunks = await collect(streamEncodeNative(generateTables()));

    assert.strictEqual(chunks.length, 3);

    // Decode each block
    const decoded1 = await decodeNative(chunks[0]);
    assert.deepStrictEqual(toArrayRows(decoded1), [[1], [2]]);

    const decoded2 = await decodeNative(chunks[1]);
    assert.deepStrictEqual(toArrayRows(decoded2), [[3], [4]]);

    const decoded3 = await decodeNative(chunks[2]);
    assert.deepStrictEqual(toArrayRows(decoded3), [[5]]);
  });
});

describe("streamDecodeNative", () => {
  it("decodes streamed blocks", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];

    // Create two separate blocks
    const block1 = encodeRows(columns, [[1], [2]]);
    const block2 = encodeRows(columns, [[3], [4]]);

    // Stream them
    const results = await collect(streamDecodeNative(toAsync([block1, block2])));

    assert.strictEqual(results.length, 2);
    assert.ok(results[0] instanceof Table);
    assert.ok(results[1] instanceof Table);
    assert.deepStrictEqual(toArrayRows(results[0]), [[1], [2]]);
    assert.deepStrictEqual(toArrayRows(results[1]), [[3], [4]]);
  });

  it("handles partial chunks", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const block = encodeRows(columns, [[1], [2], [3]]);

    // Split block into small chunks
    const chunk1 = block.subarray(0, 5);
    const chunk2 = block.subarray(5, 10);
    const chunk3 = block.subarray(10);

    const results = await collect(streamDecodeNative(toAsync([chunk1, chunk2, chunk3])));

    assert.strictEqual(results.length, 1);
    assert.ok(results[0] instanceof Table);
    assert.deepStrictEqual(toArrayRows(results[0]), [[1], [2], [3]]);
  });
});

describe("additional scalar types", () => {
  it("encodes FixedString", async () => {
    const columns: ColumnDef[] = [{ name: "fs", type: "FixedString(5)" }];
    const rows = [["hello"], ["world"], ["hi\0\0\0"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.ok(decodedRows[0][0] instanceof Date);
  });

  it("encodes DateTime64", async () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const date = new Date("2024-01-15T10:30:00.123Z");
    const rows = [[date]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // DateTime64 returns ClickHouseDateTime64 wrapper
    const dt = decodedRows[0][0] as { toDate(): Date };
    assert.strictEqual(dt.toDate().getTime(), date.getTime());
  });

  it("encodes IPv4", async () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv4" }];
    const rows = [["192.168.1.1"], ["10.0.0.1"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "192.168.1.1");
    assert.strictEqual(decodedRows[1][0], "10.0.0.1");
  });

  it("encodes IPv6", async () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv6" }];
    const rows = [["2001:db8::1"], ["::1"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // IPv6 may be normalized
    assert.ok(typeof decodedRows[0][0] === "string");
  });

  it("encodes Enum8", async () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    const rows = [[1], [2], [1]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), [[1], [2], [1]]);
  });

  it("encodes Decimal64", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal64(4)" }];
    const rows = [["123.4567"], ["-999.9999"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "123.4567");
    assert.strictEqual(decodedRows[1][0], "-999.9999");
  });

  it("encodes Int128", async () => {
    const columns: ColumnDef[] = [{ name: "i", type: "Int128" }];
    const rows = [[170141183460469231731687303715884105727n], [-170141183460469231731687303715884105728n]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], 170141183460469231731687303715884105727n);
    assert.strictEqual(decodedRows[1][0], -170141183460469231731687303715884105728n);
  });

  it("encodes UInt256", async () => {
    const columns: ColumnDef[] = [{ name: "u", type: "UInt256" }];
    const maxU256 = (1n << 256n) - 1n;
    const rows = [[maxU256], [0n]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], maxU256);
    assert.strictEqual(decodedRows[1][0], 0n);
  });
});

describe("LowCardinality", () => {
  it("encodes LowCardinality(String)", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(String)" }];
    const rows = [["a"], ["b"], ["a"], ["c"], ["b"], ["a"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes LowCardinality(Nullable(String))", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(Nullable(String))" }];
    const rows = [["a"], [null], ["b"], [null], ["a"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes LowCardinality(FixedString(3))", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(FixedString(3))" }];
    const rows = [["abc"], ["def"], ["abc"], ["ghi"]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // FixedString decodes as Uint8Array
    const textDecoder = new TextDecoder();
    assert.strictEqual(textDecoder.decode(decodedRows[0][0] as Uint8Array), "abc");
    assert.strictEqual(textDecoder.decode(decodedRows[1][0] as Uint8Array), "def");
  });

  it("handles empty LowCardinality", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(String)" }];
    const rows: unknown[][] = [];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 0);
  });

  it("encodes LowCardinality(Int32) with duplicate values", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(Int32)" }];
    const rows = [[42], [100], [42], [100], [42]]; // duplicates to test deduplication
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], 42);
    assert.strictEqual(decodedRows[1][0], 100);
    assert.strictEqual(decodedRows[2][0], 42);
    assert.strictEqual(decodedRows[3][0], 100);
    assert.strictEqual(decodedRows[4][0], 42);
  });

  it("encodes LowCardinality(Date) with duplicate dates", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(Date)" }];
    const d1 = new Date("2024-01-15");
    const d2 = new Date("2024-06-30");
    const d1dup = new Date("2024-01-15"); // same date, different object
    const rows = [[d1], [d2], [d1dup], [d2]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // Date decodes as Date object - compare time values
    assert.strictEqual((decodedRows[0][0] as Date).getTime(), d1.getTime());
    assert.strictEqual((decodedRows[1][0] as Date).getTime(), d2.getTime());
    assert.strictEqual((decodedRows[2][0] as Date).getTime(), d1.getTime());
    assert.strictEqual((decodedRows[3][0] as Date).getTime(), d2.getTime());
  });

  it("encodes LowCardinality(DateTime) with duplicate datetimes", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(DateTime)" }];
    const dt1 = new Date("2024-01-15T10:30:00Z");
    const dt2 = new Date("2024-06-30T15:45:00Z");
    const dt1dup = new Date("2024-01-15T10:30:00Z"); // same datetime, different object
    const rows = [[dt1], [dt2], [dt1dup]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual((decodedRows[0][0] as Date).getTime(), dt1.getTime());
    assert.strictEqual((decodedRows[1][0] as Date).getTime(), dt2.getTime());
    assert.strictEqual((decodedRows[2][0] as Date).getTime(), dt1.getTime());
  });
});

describe("Geo types", () => {
  it("encodes Point", async () => {
    const columns: ColumnDef[] = [{ name: "p", type: "Point" }];
    const rows = [[[1.5, 2.5]], [[3.0, 4.0]], [[-1.0, -2.0]]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(decodedRows[0][0], [1.5, 2.5]);
    assert.deepStrictEqual(decodedRows[1][0], [3.0, 4.0]);
    assert.deepStrictEqual(decodedRows[2][0], [-1.0, -2.0]);
  });

  it("encodes Ring (Array(Point))", async () => {
    const columns: ColumnDef[] = [{ name: "r", type: "Ring" }];
    // Ring = Array(Point), value is [[x,y], [x,y], ...]
    const rows = [
      [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],  // Square
      [[[0, 0], [2, 0], [1, 1], [0, 0]]],          // Triangle
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual((decodedRows[0][0] as unknown[]).length, 5);
    assert.strictEqual((decodedRows[1][0] as unknown[]).length, 4);
  });

  it("encodes Polygon (Array(Ring))", async () => {
    const columns: ColumnDef[] = [{ name: "poly", type: "Polygon" }];
    // Polygon = Array(Ring) = Array(Array(Point)), outer ring first, then holes
    const outerRing = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]];
    const hole = [[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]];
    const rows = [[[outerRing, hole]]];  // row 0, col 0 = [ring1, ring2]
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const polygon = decodedRows[0][0] as unknown[][];
    assert.strictEqual(polygon.length, 2); // outer + hole
  });

  it("encodes MultiPolygon (Array(Polygon))", async () => {
    const columns: ColumnDef[] = [{ name: "mp", type: "MultiPolygon" }];
    // MultiPolygon = Array(Polygon) = Array(Array(Ring)) = Array(Array(Array(Point)))
    const poly1 = [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]];  // simple square
    const poly2 = [[[5, 5], [6, 5], [6, 6], [5, 6], [5, 5]]];  // another square
    const rows = [[[poly1, poly2]]];  // row 0, col 0 = [polygon1, polygon2]
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const multiPoly = decodedRows[0][0] as unknown[][][];
    assert.strictEqual(multiPoly.length, 2); // 2 polygons
  });
});

describe("Variant", () => {
  it("encodes simple Variant(String, UInt64)", async () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(String, UInt64)" }];
    // Values are [discriminator, value] tuples
    const rows = [
      [[0, "hello"]],   // String (disc 0)
      [[1, 42n]],       // UInt64 (disc 1)
      [[0, "world"]],   // String (disc 0)
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(decodedRows[0][0], [0, "hello"]);
    assert.deepStrictEqual(decodedRows[1][0], [1, 42n]);
    assert.deepStrictEqual(decodedRows[2][0], [0, "world"]);
  });

  it("encodes Variant with nulls", async () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(String, UInt64)" }];
    const rows = [
      [[0, "test"]],
      [null],           // null discriminator (0xFF)
      [[1, 123n]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decodedRows[0][0], [0, "test"]);
    assert.strictEqual(decodedRows[1][0], null);
    assert.deepStrictEqual(decodedRows[2][0], [1, 123n]);
  });

  it("encodes Variant with all nulls", async () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(String, Int32)" }];
    const rows = [[null], [null], [null]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.strictEqual(decodedRows[0][0], null);
    assert.strictEqual(decodedRows[1][0], null);
    assert.strictEqual(decodedRows[2][0], null);
  });

  it("encodes Variant with complex nested types", async () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(Array(Int32), String)" }];
    const rows = [
      [[0, [1, 2, 3]]],   // Array(Int32)
      [[1, "test"]],      // String
      [[0, []]],          // Empty array
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    // Arrays return plain arrays of values
    assert.deepStrictEqual(decodedRows[0][0], [0, [1, 2, 3]]);
    assert.deepStrictEqual(decodedRows[1][0], [1, "test"]);
    assert.deepStrictEqual(decodedRows[2][0], [0, []]);
  });
});

describe("Dynamic", () => {
  it("encodes simple Dynamic with mixed types", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Dynamic" }];
    // Raw values - types are inferred (integers become Int64 = bigint)
    const rows = [
      ["hello"],        // String
      [42],             // Int64 (inferred)
      ["world"],        // String
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "hello");
    assert.strictEqual(decodedRows[1][0], 42n);  // Int64 decoded as bigint
    assert.strictEqual(decodedRows[2][0], "world");
  });

  it("encodes Dynamic with nulls", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Dynamic" }];
    const rows = [
      ["test"],
      [null],
      [123],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], "test");
    assert.strictEqual(decodedRows[1][0], null);
    assert.strictEqual(decodedRows[2][0], 123n);  // Int64 decoded as bigint
  });

  it("encodes Dynamic with all nulls", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Dynamic" }];
    const rows = [[null], [null], [null]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.strictEqual(decodedRows[0][0], null);
    assert.strictEqual(decodedRows[1][0], null);
    assert.strictEqual(decodedRows[2][0], null);
  });

  it("encodes Dynamic with bigint", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Dynamic" }];
    const rows = [
      [100n],
      ["text"],
      [200n],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    // BigInt is encoded as Int64
    assert.strictEqual(decodedRows[0][0], 100n);
    assert.strictEqual(decodedRows[1][0], "text");
    assert.strictEqual(decodedRows[2][0], 200n);
  });
});

describe("JSON", () => {
  it("encodes simple JSON objects", async () => {
    const columns: ColumnDef[] = [{ name: "j", type: "JSON" }];
    const rows = [
      [{ name: "alice", age: 30 }],
      [{ name: "bob", age: 25 }],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const obj0 = decodedRows[0][0] as Record<string, unknown>;
    const obj1 = decodedRows[1][0] as Record<string, unknown>;
    assert.strictEqual(obj0.name, "alice");
    assert.strictEqual(obj0.age, 30n);  // V3 encoding uses Int64 -> bigint
    assert.strictEqual(obj1.name, "bob");
    assert.strictEqual(obj1.age, 25n);
  });

  it("encodes JSON with missing keys", async () => {
    const columns: ColumnDef[] = [{ name: "j", type: "JSON" }];
    const rows = [
      [{ name: "alice", age: 30 }],
      [{ name: "bob" }],  // missing age
      [{ age: 40 }],      // missing name
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    const obj0 = decodedRows[0][0] as Record<string, unknown>;
    const obj1 = decodedRows[1][0] as Record<string, unknown>;
    const obj2 = decodedRows[2][0] as Record<string, unknown>;

    assert.strictEqual(obj0.name, "alice");
    assert.strictEqual(obj0.age, 30n);  // V3 encoding uses Int64 -> bigint
    assert.strictEqual(obj1.name, "bob");
    assert.strictEqual(obj1.age, undefined);  // Missing key not in object
    assert.strictEqual(obj2.name, undefined);
    assert.strictEqual(obj2.age, 40n);
  });

  it("encodes empty JSON objects", async () => {
    const columns: ColumnDef[] = [{ name: "j", type: "JSON" }];
    const rows = [[{}], [{}]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decodedRows[0][0], {});
    assert.deepStrictEqual(decodedRows[1][0], {});
  });
});

describe("round-trip with complex nested types", () => {
  it("Array of Nullable", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Nullable(Int32))" }];
    const rows = [[[1, null, 3]], [[null, null]], [[]]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("Tuple with Array", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Array(Int32), String)" }];
    const rows = [[[[1, 2], "a"]], [[[3], "b"]]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    // Arrays decode as TypedArrays
    const t0 = decodedRows[0][0] as [Int32Array, string];
    assert.deepStrictEqual([...t0[0]], [1, 2]);
    assert.strictEqual(t0[1], "a");

    const t1 = decodedRows[1][0] as [Int32Array, string];
    assert.deepStrictEqual([...t1[0]], [3]);
    assert.strictEqual(t1[1], "b");
  });

  it("Map with Array values", async () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Array(Int32))" }];
    const rows = [[{ a: [1, 2], b: [3] }]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    const map = decodedRows[0][0] as Map<string, Int32Array>;
    assert.deepStrictEqual([...map.get("a")!], [1, 2]);
    assert.deepStrictEqual([...map.get("b")!], [3]);
  });
});

// ============================================================================
// Edge case regression tests (from fuzz testing failures)
// ============================================================================

describe("DateTime64 precision edge cases", () => {
  it("encodes DateTime64(1) - precision < 3 requires division", async () => {
    // DateTime64(1) = deciseconds (1/10 second)
    // Precision < 3 triggered: BigInt(10 ** (1-3)) = BigInt(0.01) which fails
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(1)" }];
    const date = new Date("2024-01-15T10:30:00.500Z"); // 500ms -> 5 deciseconds
    const rows = [[date]];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
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
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const dt = decodedRows[0][0] as { toClosestDate(): Date };
    // 999ms truncated to seconds
    assert.strictEqual(dt.toClosestDate().getTime(), new Date("2024-01-15T10:30:00.000Z").getTime());
  });
});

describe("LowCardinality empty values edge cases", () => {
  it("encodes Array(Map(LowCardinality(String), Int64)) with empty maps", async () => {
    // Empty LowCardinality arrays inside nested structures should write 0 bytes
    // Previously wrote 24 bytes (flags + dict size + count) even for empty
    const columns: ColumnDef[] = [{ name: "m", type: "Array(Map(LowCardinality(String), Int64))" }];
    const rows = [
      [[[["a", 1n]], [["b", 2n]], []]],  // Last map is empty
      [[[], [], []]],                      // All maps empty
      [[[["c", 3n]]]],                     // No empty maps
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded, { mapAsArray: true });
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 3);
    // Verify structure is preserved
    const row0 = decodedRows[0][0] as [string, bigint][][];
    assert.strictEqual(row0.length, 3);
    assert.deepStrictEqual(row0[2], []); // Empty map preserved
  });

  it("encodes Map(LowCardinality(String), Int64) with some empty rows", async () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(LowCardinality(String), Int64)" }];
    const rows = [
      [[["a", 1n], ["b", 2n]]],
      [[]],  // Empty map
      [[["c", 3n]]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded, { mapAsArray: true });
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 3);
    assert.deepStrictEqual(decodedRows[1][0], []); // Empty map preserved
  });
});

describe("Deep nested structure edge cases", () => {
  it("encodes Array(Nested(Nested(...Map(LowCardinality)))) - the complex failing case", async () => {
    // This structure caused multiple issues:
    // 1. LowCardinality prefix counting
    // 2. Empty values writing extra bytes
    // Structure: Array(Nested(e1 Int32, e2 Nested(e3 Int64, e4 Map(LowCardinality(String), Int64))))
    const columns: ColumnDef[] = [{
      name: "c1",
      type: "Array(Nested(e1 Int32, e2 Nested(e3 Int64, e4 Map(LowCardinality(String), Int64))))"
    }];

    // Create test data with varying nesting depths and empty arrays
    const rows = [
      // Row with nested data
      [[[
        { e1: 1, e2: [{ e3: 10n, e4: [["k1", 100n]] }] },
        { e1: 2, e2: [] }  // Empty inner nested
      ]]],
      // Row with empty outer array
      [[[]]],
      // Row with multiple levels of data
      [[[
        { e1: 3, e2: [{ e3: 20n, e4: [] }, { e3: 30n, e4: [["k2", 200n], ["k3", 300n]] }] }
      ]]],
    ];

    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded, { mapAsArray: true });
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 3);

    // Verify nested structure is preserved
    const row0 = decodedRows[0][0] as any[];
    assert.strictEqual(row0.length, 1);
    assert.strictEqual(row0[0].length, 2);
    assert.strictEqual(row0[0][0].e1, 1);
    assert.strictEqual(row0[0][0].e2.length, 1);
    assert.deepStrictEqual(row0[0][1].e2, []); // Empty inner preserved
  });

  it("encodes Nested with LowCardinality in Map inside Array", async () => {
    // Simpler version of the complex case
    const columns: ColumnDef[] = [{
      name: "data",
      type: "Nested(id Int32, tags Map(LowCardinality(String), Int64))"
    }];

    const rows = [
      [[{ id: 1, tags: [["a", 1n]] }, { id: 2, tags: [] }]],
      [[]],
      [[{ id: 3, tags: [["b", 2n], ["c", 3n]] }]],
    ];

    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded, { mapAsArray: true });
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.deepStrictEqual(decodedRows[1][0], []); // Empty array preserved
  });
});

// Tests for ArrayCodec code paths (fast path vs converter/NaN paths)
describe("ArrayCodec code paths", () => {
  // Fast path: Array of integers with TypedArray input
  it("Array(Int32) with Int32Array input (fast path)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    const rows = [
      [new Int32Array([1, 2, 3])],
      [new Int32Array([])],
      [new Int32Array([-2147483648, 0, 2147483647])],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as Int32Array), [1, 2, 3]);
    assert.deepStrictEqual(Array.from(decodedRows[1][0] as Int32Array), []);
    assert.deepStrictEqual(Array.from(decodedRows[2][0] as Int32Array), [-2147483648, 0, 2147483647]);
  });

  it("Array(UInt32) with Uint32Array input (fast path)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(UInt32)" }];
    const rows = [
      [new Uint32Array([0, 100, 4294967295])],
      [new Uint32Array([42])],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as Uint32Array), [0, 100, 4294967295]);
    assert.deepStrictEqual(Array.from(decodedRows[1][0] as Uint32Array), [42]);
  });

  it("Array(Int16) with regular array (fast path, non-TypedArray input)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int16)" }];
    const rows = [
      [[-32768, 0, 32767]],
      [[1, 2, 3]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as Int16Array), [-32768, 0, 32767]);
    assert.deepStrictEqual(Array.from(decodedRows[1][0] as Int16Array), [1, 2, 3]);
  });

  // Converter path: Array(Int64) requires BigInt conversion
  it("Array(Int64) with BigInt values (converter path)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int64)" }];
    const rows = [
      [[1n, 2n, 3n]],
      [[-9223372036854775808n, 0n, 9223372036854775807n]],
      [[]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as BigInt64Array), [1n, 2n, 3n]);
    assert.deepStrictEqual(
      Array.from(decodedRows[1][0] as BigInt64Array),
      [-9223372036854775808n, 0n, 9223372036854775807n]
    );
    assert.deepStrictEqual(Array.from(decodedRows[2][0] as BigInt64Array), []);
  });

  it("Array(UInt64) with BigInt values (converter path)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(UInt64)" }];
    const rows = [
      [[0n, 18446744073709551615n]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as BigUint64Array), [0n, 18446744073709551615n]);
  });

  // Converter path: Array(Bool) requires boolean to number conversion
  it("Array(Bool) with boolean values (converter path)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Bool)" }];
    const rows = [
      [[true, false, true]],
      [[false]],
      [[]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    // Bool decodes to Uint8Array with 0/1 values
    assert.deepStrictEqual(Array.from(decodedRows[0][0] as Uint8Array), [1, 0, 1]);
    assert.deepStrictEqual(Array.from(decodedRows[1][0] as Uint8Array), [0]);
    assert.deepStrictEqual(Array.from(decodedRows[2][0] as Uint8Array), []);
  });

  // NaN path: Array(Float64) requires NaN bit pattern preservation
  it("Array(Float64) with regular floats (NaN path, no actual NaN)", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Float64)" }];
    const rows = [
      [[1.5, -2.5, 0, Infinity, -Infinity]],
      [new Float64Array([3.14, 2.718])],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as Float64Array), [1.5, -2.5, 0, Infinity, -Infinity]);
    assert.deepStrictEqual(Array.from(decodedRows[1][0] as Float64Array), [3.14, 2.718]);
  });

  it("Array(Float64) with NaN values", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Float64)" }];
    const rows = [
      [[1.0, NaN, 2.0]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    // NaN is normalized to canonical form and round-trips correctly
    const arr = decodedRows[0][0] as number[];
    assert.strictEqual(arr[0], 1.0);
    assert.ok(Number.isNaN(arr[1]), "NaN should round-trip as NaN");
    assert.strictEqual(arr[2], 2.0);
  });

  it("Array(Float32) with NaN values", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Float32)" }];
    const rows = [
      [new Float32Array([1.5, -2.5, 0])],
      [[3.14, NaN, Infinity]],
    ];
    const encoded = encodeRows(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    const arr0 = decodedRows[0][0] as number[];
    assert.strictEqual(arr0.length, 3);
    assert.ok(Math.abs(arr0[0] - 1.5) < 0.0001);
    assert.ok(Math.abs(arr0[1] - (-2.5)) < 0.0001);
    assert.strictEqual(arr0[2], 0);

    const arr1 = decodedRows[1][0] as number[];
    assert.ok(Math.abs(arr1[0] - 3.14) < 0.01);
    assert.ok(Number.isNaN(arr1[1]), "NaN should round-trip as NaN");
    assert.strictEqual(arr1[2], Infinity);
  });
});

describe("Arrow-style factory functions", () => {
  it("tableFromArrays creates table from named columns", async () => {
    const schema: ColumnDef[] = [
      { name: "id", type: "UInt32" },
      { name: "name", type: "String" },
    ];
    const table = tableFromArrays(schema, {
      id: new Uint32Array([1, 2, 3]),
      name: ["alice", "bob", "charlie"],
    });

    assert.strictEqual(table.length, 3);
    assert.strictEqual(table.numCols, 2);
    assert.deepStrictEqual(table.columnNames, ["id", "name"]);

    const rows = toArrayRows(table);
    assert.deepStrictEqual(rows[0], [1, "alice"]);
    assert.deepStrictEqual(rows[1], [2, "bob"]);
    assert.deepStrictEqual(rows[2], [3, "charlie"]);

    // Round-trip through encode/decode
    const encoded = encodeNative(table);
    const decoded = await decodeNative(encoded);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("tableFromRows creates table from row arrays", async () => {
    const schema: ColumnDef[] = [
      { name: "id", type: "UInt32" },
      { name: "value", type: "Float64" },
    ];
    const table = tableFromRows(schema, [
      [1, 1.5],
      [2, 2.5],
      [3, 3.5],
    ]);

    assert.strictEqual(table.length, 3);
    const rows = toArrayRows(table);
    assert.deepStrictEqual(rows[0], [1, 1.5]);
    assert.deepStrictEqual(rows[2], [3, 3.5]);
  });

  it("tableFromCols creates table from pre-built columns", async () => {
    const idCol = makeBuilder("UInt32").append(1).append(2).append(3).finish();
    const nameCol = makeBuilder("String").append("alice").append("bob").append("charlie").finish();

    const table = tableFromCols({ id: idCol, name: nameCol });

    assert.strictEqual(table.length, 3);
    assert.deepStrictEqual(table.columnNames, ["id", "name"]);

    // Columns should have correct types
    assert.strictEqual(table.getColumn("id")!.type, "UInt32");
    assert.strictEqual(table.getColumn("name")!.type, "String");

    const rows = toArrayRows(table);
    assert.deepStrictEqual(rows[0], [1, "alice"]);
  });

  it("tableBuilder creates row-by-row builder", async () => {
    const schema: ColumnDef[] = [
      { name: "x", type: "Int32" },
      { name: "y", type: "Int32" },
    ];
    const builder = tableBuilder(schema);
    builder.appendRow([1, 2]);
    builder.appendRow([3, 4]);
    builder.appendRow([5, 6]);

    const table = builder.finish();
    assert.strictEqual(table.length, 3);
    assert.deepStrictEqual(toArrayRows(table), [[1, 2], [3, 4], [5, 6]]);
  });
});

describe("makeBuilder", () => {
  it("supports chainable append", () => {
    const col = makeBuilder("Int32")
      .append(1)
      .append(2)
      .append(3)
      .finish();

    assert.strictEqual(col.length, 3);
    assert.strictEqual(col.type, "Int32");
    assert.strictEqual(col.get(0), 1);
    assert.strictEqual(col.get(1), 2);
    assert.strictEqual(col.get(2), 3);
  });

  it("makeBuilder works with complex types", async () => {
    // Array(Int32)
    const arrCol = makeBuilder("Array(Int32)")
      .append([1, 2, 3])
      .append([4, 5])
      .append([6])
      .finish();
    assert.strictEqual(arrCol.type, "Array(Int32)");
    assert.deepStrictEqual(arrCol.get(0), [1, 2, 3]);
    assert.deepStrictEqual(arrCol.get(2), [6]);

    // Tuple(Float64, Float64)
    const tupleCol = makeBuilder("Tuple(Float64, Float64)")
      .append([1.0, 2.0])
      .append([3.0, 4.0])
      .finish();
    assert.strictEqual(tupleCol.type, "Tuple(Float64, Float64)");
    assert.deepStrictEqual(tupleCol.get(0), [1.0, 2.0]);

    // Named Tuple
    const namedTupleCol = makeBuilder("Tuple(x Float64, y Float64)")
      .append({ x: 1.0, y: 2.0 })
      .append({ x: 3.0, y: 4.0 })
      .finish();
    assert.deepStrictEqual(namedTupleCol.get(0), { x: 1.0, y: 2.0 });

    // Nullable(String)
    const nullableCol = makeBuilder("Nullable(String)")
      .append("hello")
      .append(null)
      .append("world")
      .finish();
    assert.strictEqual(nullableCol.get(0), "hello");
    assert.strictEqual(nullableCol.get(1), null);
    assert.strictEqual(nullableCol.get(2), "world");
  });

  it("columns carry their type for tableFromCols", () => {
    const pointCol = makeBuilder("Tuple(Float64, Float64)")
      .append([1.0, 2.0])
      .append([3.0, 4.0])
      .finish();

    const table = tableFromCols({ point: pointCol });

    // Schema derived from column type
    assert.deepStrictEqual(table.schema, [{ name: "point", type: "Tuple(Float64, Float64)" }]);
  });
});

describe("Column type property", () => {
  it("decoded columns have correct type strings", async () => {
    const schema: ColumnDef[] = [
      { name: "i", type: "Int32" },
      { name: "s", type: "String" },
      { name: "arr", type: "Array(UInt64)" },
      { name: "n", type: "Nullable(Float64)" },
    ];
    const table = tableFromRows(schema, [
      [1, "hello", [1n, 2n], 1.5],
      [2, "world", [3n], null],
    ]);

    // Columns have type property
    assert.strictEqual(table.getColumn("i")!.type, "Int32");
    assert.strictEqual(table.getColumn("s")!.type, "String");
    assert.strictEqual(table.getColumn("arr")!.type, "Array(UInt64)");
    assert.strictEqual(table.getColumn("n")!.type, "Nullable(Float64)");

    // Round-trip preserves types
    const encoded = encodeNative(table);
    const decoded = await decodeNative(encoded);
    assert.strictEqual(decoded.getColumn("i")!.type, "Int32");
    assert.strictEqual(decoded.getColumn("s")!.type, "String");
    assert.strictEqual(decoded.getColumn("arr")!.type, "Array(UInt64)");
    assert.strictEqual(decoded.getColumn("n")!.type, "Nullable(Float64)");
  });
});

describe("Complex types via tableFromArrays", () => {
  it("Array(Int32)", async () => {
    const table = tableFromArrays(
      [{ name: "tags", type: "Array(Int32)" }],
      { tags: [[1, 2], [3, 4, 5], [6]] }
    );
    assert.strictEqual(table.length, 3);
    assert.deepStrictEqual(table.getColumn("tags")!.get(0), [1, 2]);
    assert.deepStrictEqual(table.getColumn("tags")!.get(1), [3, 4, 5]);
    assert.deepStrictEqual(table.getColumn("tags")!.get(2), [6]);

    // Round-trip
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(Float64, Float64) - positional", async () => {
    const table = tableFromArrays(
      [{ name: "point", type: "Tuple(Float64, Float64)" }],
      { point: [[1.0, 2.0], [3.0, 4.0]] }
    );
    assert.deepStrictEqual(table.getColumn("point")!.get(0), [1.0, 2.0]);
    assert.deepStrictEqual(table.getColumn("point")!.get(1), [3.0, 4.0]);

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(x Float64, y Float64) - named", async () => {
    const table = tableFromArrays(
      [{ name: "point", type: "Tuple(x Float64, y Float64)" }],
      { point: [{ x: 1.0, y: 2.0 }, { x: 3.0, y: 4.0 }] }
    );
    assert.deepStrictEqual(table.getColumn("point")!.get(0), { x: 1.0, y: 2.0 });
    assert.deepStrictEqual(table.getColumn("point")!.get(1), { x: 3.0, y: 4.0 });

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Map(String, Int32)", async () => {
    const table = tableFromArrays(
      [{ name: "meta", type: "Map(String, Int32)" }],
      { meta: [{ a: 1, b: 2 }, new Map([["c", 3]])] }
    );
    assert.deepStrictEqual(table.getColumn("meta")!.get(0), new Map([["a", 1], ["b", 2]]));
    assert.deepStrictEqual(table.getColumn("meta")!.get(1), new Map([["c", 3]]));

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Nullable(String)", async () => {
    const table = tableFromArrays(
      [{ name: "note", type: "Nullable(String)" }],
      { note: ["hello", null, "world"] }
    );
    assert.strictEqual(table.getColumn("note")!.get(0), "hello");
    assert.strictEqual(table.getColumn("note")!.get(1), null);
    assert.strictEqual(table.getColumn("note")!.get(2), "world");

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Variant(String, Int64, Bool) - type inferred", async () => {
    const table = tableFromArrays(
      [{ name: "val", type: "Variant(String, Int64, Bool)" }],
      { val: ["hello", 42n, true, null] }
    );
    // Type inference: string->0, bigint->1, bool->2
    assert.deepStrictEqual(table.getColumn("val")!.get(0), [0, "hello"]);
    assert.deepStrictEqual(table.getColumn("val")!.get(1), [1, 42n]);
    // Bool stores as 1/0
    assert.deepStrictEqual(table.getColumn("val")!.get(2), [2, 1]);
    assert.strictEqual(table.getColumn("val")!.get(3), null);

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Variant(String, Int64, Bool) - explicit discriminators", async () => {
    const table = tableFromArrays(
      [{ name: "val", type: "Variant(String, Int64, Bool)" }],
      { val: [[0, "hello"], [1, 42n], [2, true], null] }
    );
    assert.deepStrictEqual(table.getColumn("val")!.get(0), [0, "hello"]);
    assert.deepStrictEqual(table.getColumn("val")!.get(1), [1, 42n]);
    assert.deepStrictEqual(table.getColumn("val")!.get(2), [2, 1]);
    assert.strictEqual(table.getColumn("val")!.get(3), null);

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Dynamic", async () => {
    const table = tableFromArrays(
      [{ name: "dyn", type: "Dynamic" }],
      { dyn: ["hello", 42, true, [1, 2, 3], null] }
    );
    assert.strictEqual(table.getColumn("dyn")!.get(0), "hello");
    assert.strictEqual(table.getColumn("dyn")!.get(1), 42n); // integers become Int64
    assert.strictEqual(table.getColumn("dyn")!.get(2), 1);   // bool becomes 1/0
    assert.deepStrictEqual(table.getColumn("dyn")!.get(3), [1n, 2n, 3n]); // array of Int64
    assert.strictEqual(table.getColumn("dyn")!.get(4), null);

    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("JSON", async () => {
    const table = tableFromArrays(
      [{ name: "data", type: "JSON" }],
      { data: [{ a: 1, b: "x" }, { a: 2, c: true }] }
    );
    // JSON returns objects with dynamic values per path
    // Missing keys are omitted from the object (not set to null)
    const row0 = table.getColumn("data")!.get(0) as Record<string, unknown>;
    const row1 = table.getColumn("data")!.get(1) as Record<string, unknown>;
    assert.strictEqual(row0.a, 1n); // integers become Int64
    assert.strictEqual(row0.b, "x");
    assert.ok(!("c" in row0)); // missing keys are omitted
    assert.strictEqual(row1.a, 2n);
    assert.ok(!("b" in row1)); // missing keys are omitted
    assert.strictEqual(row1.c, 1); // bool becomes 1/0

    const decoded = await decodeNative(encodeNative(table));
    // After round-trip, compare
    const decodedRow0 = decoded.getColumn("data")!.get(0) as Record<string, unknown>;
    const decodedRow1 = decoded.getColumn("data")!.get(1) as Record<string, unknown>;
    assert.strictEqual(decodedRow0.a, 1n);
    assert.strictEqual(decodedRow0.b, "x");
    assert.strictEqual(decodedRow1.a, 2n);
    assert.strictEqual(decodedRow1.c, 1);
  });
});

describe("Complex types via makeBuilder", () => {
  it("Array(Int32)", async () => {
    const col = makeBuilder("Array(Int32)")
      .append([1, 2])
      .append([3, 4, 5])
      .append([6])
      .finish();
    assert.strictEqual(col.type, "Array(Int32)");
    assert.deepStrictEqual(col.get(0), [1, 2]);
    assert.deepStrictEqual(col.get(1), [3, 4, 5]);

    const table = tableFromCols({ tags: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(Float64, Float64) - positional", async () => {
    const col = makeBuilder("Tuple(Float64, Float64)")
      .append([1.0, 2.0])
      .append([3.0, 4.0])
      .finish();
    assert.strictEqual(col.type, "Tuple(Float64, Float64)");
    assert.deepStrictEqual(col.get(0), [1.0, 2.0]);

    const table = tableFromCols({ point: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(x Float64, y Float64) - named", async () => {
    const col = makeBuilder("Tuple(x Float64, y Float64)")
      .append({ x: 1.0, y: 2.0 })
      .append({ x: 3.0, y: 4.0 })
      .finish();
    assert.strictEqual(col.type, "Tuple(x Float64, y Float64)");
    assert.deepStrictEqual(col.get(0), { x: 1.0, y: 2.0 });

    const table = tableFromCols({ point: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Map(String, Int32)", async () => {
    const col = makeBuilder("Map(String, Int32)")
      .append({ a: 1, b: 2 })
      .append(new Map([["c", 3]]))
      .finish();
    assert.strictEqual(col.type, "Map(String, Int32)");
    assert.deepStrictEqual(col.get(0), new Map([["a", 1], ["b", 2]]));
    assert.deepStrictEqual(col.get(1), new Map([["c", 3]]));

    const table = tableFromCols({ meta: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Nullable(String)", async () => {
    const col = makeBuilder("Nullable(String)")
      .append("hello")
      .append(null)
      .append("world")
      .finish();
    assert.strictEqual(col.type, "Nullable(String)");
    assert.strictEqual(col.get(0), "hello");
    assert.strictEqual(col.get(1), null);
    assert.strictEqual(col.get(2), "world");

    const table = tableFromCols({ note: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Variant(String, Int64, Bool) - type inferred", async () => {
    const col = makeBuilder("Variant(String, Int64, Bool)")
      .append("hello")
      .append(42n)
      .append(true)
      .append(null)
      .finish();
    assert.strictEqual(col.type, "Variant(String, Int64, Bool)");
    assert.deepStrictEqual(col.get(0), [0, "hello"]);
    assert.deepStrictEqual(col.get(1), [1, 42n]);
    assert.deepStrictEqual(col.get(2), [2, 1]); // bool becomes 1/0
    assert.strictEqual(col.get(3), null);

    const table = tableFromCols({ val: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Variant(String, Int64, Bool) - explicit discriminators", async () => {
    const col = makeBuilder("Variant(String, Int64, Bool)")
      .append([0, "hello"])
      .append([1, 42n])
      .append([2, true])
      .append(null)
      .finish();
    assert.strictEqual(col.type, "Variant(String, Int64, Bool)");
    assert.deepStrictEqual(col.get(0), [0, "hello"]);
    assert.deepStrictEqual(col.get(1), [1, 42n]);
    assert.strictEqual(col.get(3), null);

    const table = tableFromCols({ val: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Dynamic", async () => {
    const col = makeBuilder("Dynamic")
      .append("hello")
      .append(42)
      .append(true)
      .append([1, 2, 3])
      .append(null)
      .finish();
    assert.strictEqual(col.type, "Dynamic");
    assert.strictEqual(col.get(0), "hello");
    assert.strictEqual(col.get(1), 42n); // integers become Int64
    assert.strictEqual(col.get(2), 1);   // bool becomes 1/0
    assert.deepStrictEqual(col.get(3), [1n, 2n, 3n]);
    assert.strictEqual(col.get(4), null);

    const table = tableFromCols({ dyn: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("JSON", async () => {
    const col = makeBuilder("JSON")
      .append({ a: 1, b: "x" })
      .append({ a: 2, c: true })
      .finish();
    assert.strictEqual(col.type, "JSON");

    const row0 = col.get(0) as Record<string, unknown>;
    const row1 = col.get(1) as Record<string, unknown>;
    assert.strictEqual(row0.a, 1n);
    assert.strictEqual(row0.b, "x");
    assert.strictEqual(row1.a, 2n);
    assert.strictEqual(row1.c, 1); // bool becomes 1/0

    const table = tableFromCols({ data: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("nested: Array(Tuple(String, Int32))", async () => {
    const col = makeBuilder("Array(Tuple(String, Int32))")
      .append([["a", 1], ["b", 2]])
      .append([["c", 3]])
      .append([])
      .finish();
    assert.strictEqual(col.type, "Array(Tuple(String, Int32))");
    assert.deepStrictEqual(col.get(0), [["a", 1], ["b", 2]]);
    assert.deepStrictEqual(col.get(1), [["c", 3]]);
    assert.deepStrictEqual(col.get(2), []);

    const table = tableFromCols({ nested: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("nested: Map(String, Array(Int32))", async () => {
    const col = makeBuilder("Map(String, Array(Int32))")
      .append({ x: [1, 2], y: [3, 4, 5] })
      .append(new Map([["z", [6]]]))
      .finish();
    assert.strictEqual(col.type, "Map(String, Array(Int32))");

    const table = tableFromCols({ nested: col });
    const decoded = await decodeNative(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });
});
