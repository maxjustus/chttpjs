import { describe, it } from "node:test";
import assert from "node:assert";
import {
  type ColumnDef,
} from "../../native/index.ts";
import { encodeNativeRows, decodeBatch, toArrayRows } from "../test_utils.ts";

describe("LowCardinality", () => {
  it("encodes LowCardinality(String)", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(String)" }];
    const rows = [["a"], ["b"], ["a"], ["c"], ["b"], ["a"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes LowCardinality(Nullable(String))", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(Nullable(String))" }];
    const rows = [["a"], [null], ["b"], [null], ["a"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("encodes LowCardinality(FixedString(3))", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(FixedString(3))" }];
    const rows = [["abc"], ["def"], ["abc"], ["ghi"]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);

    assert.deepStrictEqual(decoded.columns, columns);
    assert.strictEqual(decoded.rowCount, 0);
  });

  it("encodes LowCardinality(Int32) with duplicate values", async () => {
    const columns: ColumnDef[] = [{ name: "lc", type: "LowCardinality(Int32)" }];
    const rows = [[42], [100], [42], [100], [42]]; // duplicates to test deduplication
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decodedRows[0][0], [0, "test"]);
    assert.strictEqual(decodedRows[1][0], null);
    assert.deepStrictEqual(decodedRows[2][0], [1, 123n]);
  });

  it("encodes Variant with all nulls", async () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(String, Int32)" }];
    const rows = [[null], [null], [null]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual(decodedRows[0][0], "test");
    assert.strictEqual(decodedRows[1][0], null);
    assert.strictEqual(decodedRows[2][0], 123n);  // Int64 decoded as bigint
  });

  it("encodes Dynamic with all nulls", async () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Dynamic" }];
    const rows = [[null], [null], [null]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decodedRows[0][0], {});
    assert.deepStrictEqual(decodedRows[1][0], {});
  });

  it("parses JSON with typed paths from type string", async () => {
    // Tests that the type parser correctly extracts typed paths
    const columns: ColumnDef[] = [
      { name: "j", type: "JSON(max_dynamic_paths=128, currency String, amount Int64)" }
    ];
    const rows = [
      [{ currency: "USD", amount: 100 }],
      [{ currency: "EUR", amount: 200 }],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(decoded.columns, columns);
    const obj0 = decodedRows[0][0] as Record<string, unknown>;
    const obj1 = decodedRows[1][0] as Record<string, unknown>;
    assert.strictEqual(obj0.currency, "USD");
    assert.strictEqual(obj0.amount, 100n);
    assert.strictEqual(obj1.currency, "EUR");
    assert.strictEqual(obj1.amount, 200n);
  });

  it("handles JSON with typed paths and dynamic paths together", async () => {
    const columns: ColumnDef[] = [
      { name: "j", type: "JSON(currency String, amount Int64)" }
    ];
    const rows = [
      [{ currency: "USD", amount: 100, extra: "foo" }],
      [{ currency: "EUR", amount: 200, extra: "bar" }],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    const obj0 = decodedRows[0][0] as Record<string, unknown>;
    const obj1 = decodedRows[1][0] as Record<string, unknown>;
    assert.strictEqual(obj0.currency, "USD");
    assert.strictEqual(obj0.amount, 100n);
    assert.strictEqual(obj0.extra, "foo");
    assert.strictEqual(obj1.currency, "EUR");
    assert.strictEqual(obj1.amount, 200n);
    assert.strictEqual(obj1.extra, "bar");
  });

  it("handles JSON with LowCardinality typed paths", async () => {
    const columns: ColumnDef[] = [
      { name: "j", type: "JSON(status LowCardinality(String))" }
    ];
    const rows = [
      [{ status: "active" }],
      [{ status: "inactive" }],
      [{ status: "active" }],  // Repeated value for LC
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.strictEqual((decodedRows[0][0] as any).status, "active");
    assert.strictEqual((decodedRows[1][0] as any).status, "inactive");
    assert.strictEqual((decodedRows[2][0] as any).status, "active");
  });

  it("handles JSON with Nullable typed paths", async () => {
    const columns: ColumnDef[] = [
      { name: "j", type: "JSON(name Nullable(String), age Int32)" }
    ];
    const rows = [
      [{ name: "alice", age: 30 }],
      [{ name: null, age: 25 }],
      [{ age: 40 }],  // missing name treated as null
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    const obj0 = decodedRows[0][0] as Record<string, unknown>;
    const obj1 = decodedRows[1][0] as Record<string, unknown>;
    const obj2 = decodedRows[2][0] as Record<string, unknown>;
    assert.strictEqual(obj0.name, "alice");
    assert.strictEqual(obj0.age, 30);
    // JsonColumn.get() omits null values from the object (undefined means absent)
    assert.strictEqual(obj1.name, undefined);
    assert.strictEqual(obj1.age, 25);
    assert.strictEqual(obj2.name, undefined);
    assert.strictEqual(obj2.age, 40);
  });

  it("handles JSON with Array typed paths", async () => {
    const columns: ColumnDef[] = [
      { name: "j", type: "JSON(tags Array(String))" }
    ];
    const rows = [
      [{ tags: ["a", "b", "c"] }],
      [{ tags: [] }],
      [{ tags: ["x"] }],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual((decodedRows[0][0] as any).tags, ["a", "b", "c"]);
    assert.deepStrictEqual((decodedRows[1][0] as any).tags, []);
    assert.deepStrictEqual((decodedRows[2][0] as any).tags, ["x"]);
  });
});

describe("round-trip with complex nested types", () => {
  it("Array of Nullable", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Nullable(Int32))" }];
    const rows = [[[1, null, 3]], [[null, null]], [[]]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("Tuple with Array", async () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Array(Int32), String)" }];
    const rows = [[[[1, 2], "a"]], [[[3], "b"]]];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    const map = decodedRows[0][0] as Map<string, Int32Array>;
    assert.deepStrictEqual([...map.get("a")!], [1, 2]);
    assert.deepStrictEqual([...map.get("b")!], [3]);
  });
});

// ============================================================================
// Edge case regression tests (from fuzz testing failures)
// ============================================================================

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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded, { mapAsArray: true });
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded, { mapAsArray: true });
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

    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded, { mapAsArray: true });
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

    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded, { mapAsArray: true });
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
    const decodedRows = toArrayRows(decoded);

    assert.deepStrictEqual(Array.from(decodedRows[0][0] as Float64Array), [1.5, -2.5, 0, Infinity, -Infinity]);
    assert.deepStrictEqual(Array.from(decodedRows[1][0] as Float64Array), [3.14, 2.718]);
  });

  it("Array(Float64) with NaN values", async () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Float64)" }];
    const rows = [
      [[1.0, NaN, 2.0]],
    ];
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
    const encoded = encodeNativeRows(columns, rows);
    const decoded = await decodeBatch(encoded);
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
