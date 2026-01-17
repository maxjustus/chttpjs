/**
 * Tests for deeply nested type handling.
 *
 * Covers:
 * - Deeply nested Arrays
 * - Deeply nested Tuples
 * - Mixed nesting (Array of Tuple of Array)
 * - Nullable with nested types
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { getCodec } from "../../native/codecs.ts";
import { BufferReader } from "../../native/io.ts";
import { defaultDeserializerState } from "../../native/codecs.ts";

describe("deep nesting edge cases", () => {
  describe("nested Arrays", () => {
    it("handles 2D array", () => {
      const codec = getCodec("Array(Array(Int32))");
      const values = [
        [
          [1, 2],
          [3, 4, 5],
        ],
        [[6]],
        [],
      ];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [
        [1, 2],
        [3, 4, 5],
      ]);
      assert.deepStrictEqual(col.get(1), [[6]]);
      assert.deepStrictEqual(col.get(2), []);
    });

    it("handles 3D array", () => {
      const codec = getCodec("Array(Array(Array(UInt8)))");
      const values = [[[[1, 2], [3]], [[4, 5, 6]]], [], [[[], [7]]]];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [[[1, 2], [3]], [[4, 5, 6]]]);
      assert.deepStrictEqual(col.get(1), []);
      assert.deepStrictEqual(col.get(2), [[[], [7]]]);
    });

    it("handles 4D array", () => {
      const codec = getCodec("Array(Array(Array(Array(Int16))))");
      const values = [[[[[1, 2]]]], [[[[3], [4, 5]]], [[[6, 7, 8]]]]];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [[[[1, 2]]]]);
      assert.deepStrictEqual(col.get(1), [[[[3], [4, 5]]], [[[6, 7, 8]]]]);
    });
  });

  describe("nested Tuples", () => {
    it("handles 2-level nested tuple", () => {
      const codec = getCodec("Tuple(Tuple(Int32, String), Float64)");
      const values = [
        [[42, "hello"], 3.14],
        [[0, ""], 0.0],
      ];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [[42, "hello"], 3.14]);
      assert.deepStrictEqual(col.get(1), [[0, ""], 0.0]);
    });

    it("handles 3-level nested tuple", () => {
      const codec = getCodec("Tuple(Tuple(Tuple(Int32, Int32), Int32), Int32)");
      const values = [
        [[[1, 2], 3], 4],
        [[[5, 6], 7], 8],
      ];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [[[1, 2], 3], 4]);
      assert.deepStrictEqual(col.get(1), [[[5, 6], 7], 8]);
    });

    it("handles named nested tuples", () => {
      const codec = getCodec("Tuple(inner Tuple(x Int32, y Int32), z Int32)");
      const values = [
        { inner: { x: 1, y: 2 }, z: 3 },
        { inner: { x: 4, y: 5 }, z: 6 },
      ];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), { inner: { x: 1, y: 2 }, z: 3 });
      assert.deepStrictEqual(col.get(1), { inner: { x: 4, y: 5 }, z: 6 });
    });
  });

  describe("mixed nesting", () => {
    it("handles Array of Tuple", () => {
      const codec = getCodec("Array(Tuple(Int32, String))");
      const values = [
        [
          [1, "a"],
          [2, "b"],
        ],
        [[3, "c"]],
        [],
      ];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [
        [1, "a"],
        [2, "b"],
      ]);
      assert.deepStrictEqual(col.get(1), [[3, "c"]]);
      assert.deepStrictEqual(col.get(2), []);
    });

    it("handles Tuple of Arrays", () => {
      const codec = getCodec("Tuple(Array(Int32), Array(String))");
      const values = [
        [
          [1, 2, 3],
          ["a", "b"],
        ],
        [[], ["single"]],
      ];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [
        [1, 2, 3],
        ["a", "b"],
      ]);
      assert.deepStrictEqual(col.get(1), [[], ["single"]]);
    });

    it("handles Map inside Array", () => {
      const codec = getCodec("Array(Map(String, Int32))");
      const values = [[{ a: 1, b: 2 }, { c: 3 }], [{}], []];
      const col = codec.fromValues(values);

      const v0 = col.get(0) as Map<string, number>[];
      assert.ok(Array.isArray(v0));
      assert.strictEqual(v0.length, 2);
      assert.strictEqual(v0[0].get("a"), 1);
      assert.strictEqual(v0[0].get("b"), 2);
      assert.strictEqual(v0[1].get("c"), 3);
    });

    it("handles Array inside Map", () => {
      const codec = getCodec("Map(String, Array(Int32))");
      const values = [{ nums: [1, 2, 3], more: [4, 5] }, { empty: [] }];
      const col = codec.fromValues(values);

      const v0 = col.get(0) as Map<string, number[]>;
      assert.ok(v0 instanceof Map);
      assert.deepStrictEqual(v0.get("nums"), [1, 2, 3]);
      assert.deepStrictEqual(v0.get("more"), [4, 5]);
    });
  });

  describe("Nullable nesting", () => {
    it("handles Nullable Array", () => {
      const codec = getCodec("Nullable(Array(Int32))");
      const values = [[1, 2, 3], null, []];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [1, 2, 3]);
      assert.strictEqual(col.get(1), null);
      assert.deepStrictEqual(col.get(2), []);
    });

    it("handles Array of Nullable", () => {
      const codec = getCodec("Array(Nullable(Int32))");
      const values = [[1, null, 3], [null, null], []];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [1, null, 3]);
      assert.deepStrictEqual(col.get(1), [null, null]);
      assert.deepStrictEqual(col.get(2), []);
    });

    it("handles Nullable Tuple", () => {
      const codec = getCodec("Nullable(Tuple(Int32, String))");
      const values = [[42, "hello"], null];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [42, "hello"]);
      assert.strictEqual(col.get(1), null);
    });

    it("handles deeply nested with Nullable", () => {
      const codec = getCodec("Array(Nullable(Tuple(Int32, Nullable(String))))");
      const values = [[[1, "a"], null, [2, null]], []];
      const col = codec.fromValues(values);

      assert.deepStrictEqual(col.get(0), [[1, "a"], null, [2, null]]);
      assert.deepStrictEqual(col.get(1), []);
    });
  });

  describe("encode/decode round-trip", () => {
    it("round-trips complex nested structure", () => {
      const codec = getCodec("Array(Tuple(id UInt32, values Array(Float64)))");
      const original = [
        [
          { id: 1, values: [1.1, 2.2] },
          { id: 2, values: [] },
        ],
        [],
        [{ id: 3, values: [3.3] }],
      ];
      const col = codec.fromValues(original);
      const encoded = codec.encode(col);

      // Re-decode
      const reader = new BufferReader(encoded);
      const decoded = codec.decode(reader, original.length, defaultDeserializerState());

      assert.deepStrictEqual(decoded.get(0), original[0]);
      assert.deepStrictEqual(decoded.get(1), original[1]);
      assert.deepStrictEqual(decoded.get(2), original[2]);
    });
  });
});
