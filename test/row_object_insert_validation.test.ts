import assert from "node:assert";
import { describe, it } from "node:test";
import { transposeRowObjectsToColumns } from "../tcp_client/row_object_insert.ts";

describe("row object insert validation", () => {
  it("transposes valid rows", () => {
    const schema = [{ name: "a" }, { name: "b" }];
    const rows = [
      { a: 1, b: "x" },
      { a: 2, b: "y" },
    ];
    const cols = transposeRowObjectsToColumns(schema, rows);
    assert.deepStrictEqual(cols, [
      [1, 2],
      ["x", "y"],
    ]);
  });

  it("treats missing keys as omitted (undefined)", () => {
    const schema = [{ name: "a" }, { name: "b" }];
    const cols = transposeRowObjectsToColumns(schema, [{ a: 1 } as any]);
    assert.deepStrictEqual(cols, [[1], [undefined]]);
  });

  it("treats undefined values as omitted (undefined)", () => {
    const schema = [{ name: "a" }, { name: "b" }];
    const cols = transposeRowObjectsToColumns(schema, [{ a: 1, b: undefined }]);
    assert.deepStrictEqual(cols, [[1], [undefined]]);
  });

  it("ignores extra keys", () => {
    const schema = [{ name: "a" }, { name: "b" }];
    const cols = transposeRowObjectsToColumns(schema, [{ a: 1, b: "x", c: 3 } as any]);
    assert.deepStrictEqual(cols, [[1], ["x"]]);
  });

  it("throws on non-object rows", () => {
    const schema = [{ name: "a" }];
    assert.throws(
      () => transposeRowObjectsToColumns(schema, [null as any]),
      /must be an object, got null/,
    );
    assert.throws(
      () => transposeRowObjectsToColumns(schema, [123 as any]),
      /must be an object, got number/,
    );
    assert.throws(
      () => transposeRowObjectsToColumns(schema, [[] as any]),
      /must be an object, got object/,
    );
  });
});
