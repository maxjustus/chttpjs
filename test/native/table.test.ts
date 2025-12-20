import { describe, it } from "node:test";
import assert from "node:assert";
import {
  encodeNative,
  RecordBatch,
  streamEncodeNative,
  streamDecodeNative,
  batchFromArrays,
  batchFromRows,
  batchFromCols,
  batchBuilder,
  makeBuilder,
  type ColumnDef,
} from "../../native/index.ts";
import { encodeNativeRows, toAsync, collect, toArrayRows, decodeBatch } from "../test_utils.ts";

describe("streamEncodeNative", () => {
  it("streams tables", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];

    // Create tables to stream
    async function* generateTables() {
      yield RecordBatch.fromColumnar(columns, [new Int32Array([1, 2])]);
      yield RecordBatch.fromColumnar(columns, [new Int32Array([3, 4])]);
      yield RecordBatch.fromColumnar(columns, [new Int32Array([5])]);
    }

    const chunks = await collect(streamEncodeNative(generateTables()));

    assert.strictEqual(chunks.length, 3);

    // Decode each block
    const decoded1 = await decodeBatch(chunks[0]);
    assert.deepStrictEqual(toArrayRows(decoded1), [[1], [2]]);

    const decoded2 = await decodeBatch(chunks[1]);
    assert.deepStrictEqual(toArrayRows(decoded2), [[3], [4]]);

    const decoded3 = await decodeBatch(chunks[2]);
    assert.deepStrictEqual(toArrayRows(decoded3), [[5]]);
  });
});

describe("streamDecodeNative", () => {
  it("decodes streamed blocks", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];

    // Create two separate blocks
    const block1 = encodeNativeRows(columns, [[1], [2]]);
    const block2 = encodeNativeRows(columns, [[3], [4]]);

    // Stream them
    const results = await collect(streamDecodeNative(toAsync([block1, block2])));

    assert.strictEqual(results.length, 2);
    assert.ok(results[0] instanceof RecordBatch);
    assert.ok(results[1] instanceof RecordBatch);
    assert.deepStrictEqual(toArrayRows(results[0]), [[1], [2]]);
    assert.deepStrictEqual(toArrayRows(results[1]), [[3], [4]]);
  });

  it("handles partial chunks", async () => {
    const columns: ColumnDef[] = [{ name: "id", type: "Int32" }];
    const block = encodeNativeRows(columns, [[1], [2], [3]]);

    // Split block into small chunks
    const chunk1 = block.subarray(0, 5);
    const chunk2 = block.subarray(5, 10);
    const chunk3 = block.subarray(10);

    const results = await collect(streamDecodeNative(toAsync([chunk1, chunk2, chunk3])));

    assert.strictEqual(results.length, 1);
    assert.ok(results[0] instanceof RecordBatch);
    assert.deepStrictEqual(toArrayRows(results[0]), [[1], [2], [3]]);
  });

  it("RecordBatch iteration yields stable row objects that can be collected", async () => {
    const schema: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "name", type: "String" },
    ];

    const batch = batchFromRows(schema, [
      [1, "alice"],
      [2, "bob"],
      [3, "charlie"],
    ]);

    const collected = [...batch];

    // Each element should be a distinct row reference (not a single reused view)
    assert.notStrictEqual(collected[0], collected[1]);
    assert.notStrictEqual(collected[1], collected[2]);

    // Values should remain correct after collection
    assert.strictEqual(collected[0].id, 1);
    assert.strictEqual(collected[0].name, "alice");
    assert.strictEqual(collected[1].id, 2);
    assert.strictEqual(collected[1].name, "bob");
    assert.strictEqual(collected[2].id, 3);
    assert.strictEqual(collected[2].name, "charlie");

    // Materialization helpers should also be stable
    assert.deepStrictEqual(collected[0].toObject(), { id: 1, name: "alice" });
    assert.deepStrictEqual(collected[2].toArray(), [3, "charlie"]);
  });
});

describe("Arrow-style factory functions", () => {
  it("batchFromArrays creates table from named columns", async () => {
    const schema: ColumnDef[] = [
      { name: "id", type: "UInt32" },
      { name: "name", type: "String" },
    ];
    const table = batchFromArrays(schema, {
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
    const decoded = await decodeBatch(encoded);
    assert.deepStrictEqual(toArrayRows(decoded), rows);
  });

  it("batchFromRows creates table from row arrays", async () => {
    const schema: ColumnDef[] = [
      { name: "id", type: "UInt32" },
      { name: "value", type: "Float64" },
    ];
    const table = batchFromRows(schema, [
      [1, 1.5],
      [2, 2.5],
      [3, 3.5],
    ]);

    assert.strictEqual(table.length, 3);
    const rows = toArrayRows(table);
    assert.deepStrictEqual(rows[0], [1, 1.5]);
    assert.deepStrictEqual(rows[2], [3, 3.5]);
  });

  it("batchFromCols creates table from pre-built columns", async () => {
    const idCol = makeBuilder("UInt32").append(1).append(2).append(3).finish();
    const nameCol = makeBuilder("String").append("alice").append("bob").append("charlie").finish();

    const table = batchFromCols({ id: idCol, name: nameCol });

    assert.strictEqual(table.length, 3);
    assert.deepStrictEqual(table.columnNames, ["id", "name"]);

    // Columns should have correct types
    assert.strictEqual(table.getColumn("id")!.type, "UInt32");
    assert.strictEqual(table.getColumn("name")!.type, "String");

    const rows = toArrayRows(table);
    assert.deepStrictEqual(rows[0], [1, "alice"]);
  });

  it("batchBuilder creates row-by-row builder", async () => {
    const schema: ColumnDef[] = [
      { name: "x", type: "Int32" },
      { name: "y", type: "Int32" },
    ];
    const builder = batchBuilder(schema);
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

  it("columns carry their type for batchFromCols", () => {
    const pointCol = makeBuilder("Tuple(Float64, Float64)")
      .append([1.0, 2.0])
      .append([3.0, 4.0])
      .finish();

    const table = batchFromCols({ point: pointCol });

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
    const table = batchFromRows(schema, [
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
    const decoded = await decodeBatch(encoded);
    assert.strictEqual(decoded.getColumn("i")!.type, "Int32");
    assert.strictEqual(decoded.getColumn("s")!.type, "String");
    assert.strictEqual(decoded.getColumn("arr")!.type, "Array(UInt64)");
    assert.strictEqual(decoded.getColumn("n")!.type, "Nullable(Float64)");
  });
});

describe("Complex types via batchFromArrays", () => {
  it("Array(Int32)", async () => {
    const table = batchFromArrays(
      [{ name: "tags", type: "Array(Int32)" }],
      { tags: [[1, 2], [3, 4, 5], [6]] }
    );
    assert.strictEqual(table.length, 3);
    assert.deepStrictEqual(table.getColumn("tags")!.get(0), [1, 2]);
    assert.deepStrictEqual(table.getColumn("tags")!.get(1), [3, 4, 5]);
    assert.deepStrictEqual(table.getColumn("tags")!.get(2), [6]);

    // Round-trip
    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(Float64, Float64) - positional", async () => {
    const table = batchFromArrays(
      [{ name: "point", type: "Tuple(Float64, Float64)" }],
      { point: [[1.0, 2.0], [3.0, 4.0]] }
    );
    assert.deepStrictEqual(table.getColumn("point")!.get(0), [1.0, 2.0]);
    assert.deepStrictEqual(table.getColumn("point")!.get(1), [3.0, 4.0]);

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(x Float64, y Float64) - named", async () => {
    const table = batchFromArrays(
      [{ name: "point", type: "Tuple(x Float64, y Float64)" }],
      { point: [{ x: 1.0, y: 2.0 }, { x: 3.0, y: 4.0 }] }
    );
    assert.deepStrictEqual(table.getColumn("point")!.get(0), { x: 1.0, y: 2.0 });
    assert.deepStrictEqual(table.getColumn("point")!.get(1), { x: 3.0, y: 4.0 });

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Map(String, Int32)", async () => {
    const table = batchFromArrays(
      [{ name: "meta", type: "Map(String, Int32)" }],
      { meta: [{ a: 1, b: 2 }, new Map([["c", 3]])] }
    );
    assert.deepStrictEqual(table.getColumn("meta")!.get(0), new Map([["a", 1], ["b", 2]]));
    assert.deepStrictEqual(table.getColumn("meta")!.get(1), new Map([["c", 3]]));

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Nullable(String)", async () => {
    const table = batchFromArrays(
      [{ name: "note", type: "Nullable(String)" }],
      { note: ["hello", null, "world"] }
    );
    assert.strictEqual(table.getColumn("note")!.get(0), "hello");
    assert.strictEqual(table.getColumn("note")!.get(1), null);
    assert.strictEqual(table.getColumn("note")!.get(2), "world");

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Variant(String, Int64, Bool) - type inferred", async () => {
    const table = batchFromArrays(
      [{ name: "val", type: "Variant(String, Int64, Bool)" }],
      { val: ["hello", 42n, true, null] }
    );
    // Type inference: string->0, bigint->1, bool->2
    assert.deepStrictEqual(table.getColumn("val")!.get(0), [0, "hello"]);
    assert.deepStrictEqual(table.getColumn("val")!.get(1), [1, 42n]);
    // Bool stores as 1/0
    assert.deepStrictEqual(table.getColumn("val")!.get(2), [2, 1]);
    assert.strictEqual(table.getColumn("val")!.get(3), null);

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Variant(String, Int64, Bool) - explicit discriminators", async () => {
    const table = batchFromArrays(
      [{ name: "val", type: "Variant(String, Int64, Bool)" }],
      { val: [[0, "hello"], [1, 42n], [2, true], null] }
    );
    assert.deepStrictEqual(table.getColumn("val")!.get(0), [0, "hello"]);
    assert.deepStrictEqual(table.getColumn("val")!.get(1), [1, 42n]);
    assert.deepStrictEqual(table.getColumn("val")!.get(2), [2, 1]);
    assert.strictEqual(table.getColumn("val")!.get(3), null);

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Dynamic", async () => {
    const table = batchFromArrays(
      [{ name: "dyn", type: "Dynamic" }],
      { dyn: ["hello", 42, true, [1, 2, 3], null] }
    );
    assert.strictEqual(table.getColumn("dyn")!.get(0), "hello");
    assert.strictEqual(table.getColumn("dyn")!.get(1), 42n); // integers become Int64
    assert.strictEqual(table.getColumn("dyn")!.get(2), 1);   // bool becomes 1/0
    assert.deepStrictEqual(table.getColumn("dyn")!.get(3), [1n, 2n, 3n]); // array of Int64
    assert.strictEqual(table.getColumn("dyn")!.get(4), null);

    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("JSON", async () => {
    const table = batchFromArrays(
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

    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ tags: col });
    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(Float64, Float64) - positional", async () => {
    const col = makeBuilder("Tuple(Float64, Float64)")
      .append([1.0, 2.0])
      .append([3.0, 4.0])
      .finish();
    assert.strictEqual(col.type, "Tuple(Float64, Float64)");
    assert.deepStrictEqual(col.get(0), [1.0, 2.0]);

    const table = batchFromCols({ point: col });
    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("Tuple(x Float64, y Float64) - named", async () => {
    const col = makeBuilder("Tuple(x Float64, y Float64)")
      .append({ x: 1.0, y: 2.0 })
      .append({ x: 3.0, y: 4.0 })
      .finish();
    assert.strictEqual(col.type, "Tuple(x Float64, y Float64)");
    assert.deepStrictEqual(col.get(0), { x: 1.0, y: 2.0 });

    const table = batchFromCols({ point: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ meta: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ note: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ val: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ val: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ dyn: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ data: col });
    const decoded = await decodeBatch(encodeNative(table));
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

    const table = batchFromCols({ nested: col });
    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });

  it("nested: Map(String, Array(Int32))", async () => {
    const col = makeBuilder("Map(String, Array(Int32))")
      .append({ x: [1, 2], y: [3, 4, 5] })
      .append(new Map([["z", [6]]]))
      .finish();
    assert.strictEqual(col.type, "Map(String, Array(Int32))");

    const table = batchFromCols({ nested: col });
    const decoded = await decodeBatch(encodeNative(table));
    assert.deepStrictEqual(toArrayRows(decoded), toArrayRows(table));
  });
});
