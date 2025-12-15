import { describe, it } from "node:test";
import assert from "node:assert";
import {
  encodeNative,
  decodeNative,
  streamEncodeNative,
  streamDecodeNative,
  toArrayRows,
  type ColumnDef,
} from "../native.ts";

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
    const encoded = encodeNative(columns, rows);

    // Should have: 1 col, 0 rows, "id", "Int32", no data
    assert.ok(encoded.length > 0);

    const decoded = await decodeNative(encoded);
    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 0);
  });

  it("encodes Int32 column", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const rows = [[1], [2], [3]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), [[1], [2], [3]]);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Nullable", async () => {
    const columns: ColumnDef[] = [{ name: "val", type: "Nullable(Int32)" }];
    const rows = [[1], [null], [3]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes Array", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    const rows = [[[1, 2, 3]], [[]], [[42]]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes named Tuple", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(id Int32, name String)" }];
    const rows = [[{ id: 1, name: "alice" }], [{ id: 2, name: "bob" }]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(decodedRows[0][0], { id: 1, name: "alice" });
  });

  it("encodes UUID", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "UUID" }];
    const rows = [["550e8400-e29b-41d4-a716-446655440000"]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.ok(decodedRows[0][0] instanceof Date);
    assert.ok(decodedRows[0][1] instanceof Date);
  });
});

describe("streamEncodeNative", () => {
  it("streams in blocks", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const rows = [[1], [2], [3], [4], [5]];

    const chunks = await collect(streamEncodeNative(columns, rows, { blockSize: 2 }));

    // Should produce 3 blocks: [1,2], [3,4], [5]
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
    const block1 = encodeNative(columns, [[1], [2]]);
    const block2 = encodeNative(columns, [[3], [4]]);

    // Stream them
    const results = await collect(streamDecodeNative(toAsync([block1, block2])));

    assert.strictEqual(results.length, 2);
    assert.deepStrictEqual(toArrayRows(results[0]), [[1], [2]]);
    assert.deepStrictEqual(toArrayRows(results[1]), [[3], [4]]);
  });

  it("handles partial chunks", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const block = encodeNative(columns, [[1], [2], [3]]);

    // Split block into small chunks
    const chunk1 = block.subarray(0, 5);
    const chunk2 = block.subarray(5, 10);
    const chunk3 = block.subarray(10);

    const results = await collect(streamDecodeNative(toAsync([chunk1, chunk2, chunk3])));

    assert.strictEqual(results.length, 1);
    assert.deepStrictEqual(toArrayRows(results[0]), [[1], [2], [3]]);
  });
});

describe("additional scalar types", () => {
  it("encodes FixedString", async () => {
    const columns: ColumnDef[] = [{ name: "fs", type: "FixedString(5)" }];
    const rows = [["hello"], ["world"], ["hi\0\0\0"]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.ok(decodedRows[0][0] instanceof Date);
  });

  it("encodes DateTime64", async () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const date = new Date("2024-01-15T10:30:00.123Z");
    const rows = [[date]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "192.168.1.1");
    assert.strictEqual(decodedRows[1][0], "10.0.0.1");
  });

  it("encodes IPv6", async () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv6" }];
    const rows = [["2001:db8::1"], ["::1"]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    // IPv6 may be normalized
    assert.ok(typeof decodedRows[0][0] === "string");
  });

  it("encodes Enum8", async () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    const rows = [[1], [2], [1]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), [[1], [2], [1]]);
  });

  it("encodes Decimal64", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal64(4)" }];
    const rows = [["123.4567"], ["-999.9999"]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decodedRows[0][0], "123.4567");
    assert.strictEqual(decodedRows[1][0], "-999.9999");
  });

  it("encodes Int128", async () => {
    const columns: ColumnDef[] = [{ name: "i", type: "Int128" }];
    const rows = [[170141183460469231731687303715884105727n], [-170141183460469231731687303715884105728n]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes LowCardinality(Nullable(String))", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(Nullable(String))" }];
    const rows = [["a"], [null], ["b"], [null], ["a"]];
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes LowCardinality(FixedString(3))", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(FixedString(3))" }];
    const rows = [["abc"], ["def"], ["abc"], ["ghi"]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 0);
  });
});

describe("Geo types", () => {
  it("encodes Point", async () => {
    const columns: ColumnDef[] = [{ name: "p", type: "Point" }];
    const rows = [[[1.5, 2.5]], [[3.0, 4.0]], [[-1.0, -2.0]]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decodedRows[0][0], [0, "test"]);
    assert.strictEqual(decodedRows[1][0], null);
    assert.deepStrictEqual(decodedRows[2][0], [1, 123n]);
  });

  it("encodes Variant with all nulls", async () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(String, Int32)" }];
    const rows = [[null], [null], [null]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], "test");
    assert.strictEqual(decodedRows[1][0], null);
    assert.strictEqual(decodedRows[2][0], 123n);  // Int64 decoded as bigint
  });

  it("encodes Dynamic with all nulls", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Dynamic" }];
    const rows = [[null], [null], [null]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("Tuple with Array", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Array(Int32), String)" }];
    const rows = [[[[1, 2], "a"]], [[[3], "b"]]];
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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
    const encoded = encodeNative(columns, rows);
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

    const encoded = encodeNative(columns, rows);
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

    const encoded = encodeNative(columns, rows);
    const decoded = await decodeNative(encoded, { mapAsArray: true });
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decoded.rowCount, 3);
    assert.deepStrictEqual(decodedRows[1][0], []); // Empty array preserved
  });
});
