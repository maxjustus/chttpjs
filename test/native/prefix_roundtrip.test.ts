/**
 * Tests for prefix encoding round-trips in complex codecs.
 *
 * Several codecs use writePrefix/readPrefix for format metadata:
 * - LowCardinality: version number
 * - Variant: encoding mode
 * - Dynamic: version + type list
 *
 * These tests verify correct prefix handling through encode/decode cycles.
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { decodeNativeBlock } from "../../native/index.ts";
import { BufferWriter } from "../../native/io.ts";
import { getCodec } from "../../native/codecs.ts";
import { BlockInfoField, LowCardinality as LC, Dynamic, Variant } from "../../native/constants.ts";

/**
 * Build a Native block with given type, prefix, and encoded data.
 */
function buildBlock(
  colName: string,
  colType: string,
  prefixBuilder: (w: BufferWriter) => void,
  dataBuilder: (w: BufferWriter) => void,
  rows: number,
): Uint8Array {
  const writer = new BufferWriter(4096);

  // Block info
  writer.writeVarint(BlockInfoField.End);

  // Header
  writer.writeVarint(1);
  writer.writeVarint(rows);

  // Column
  writer.writeString(colName);
  writer.writeString(colType);
  writer.writeU8(0); // no custom serialization

  // Prefix
  prefixBuilder(writer);

  // Data
  dataBuilder(writer);

  return writer.finish();
}

describe("prefix round-trip tests", () => {
  describe("LowCardinality", () => {
    it("round-trips through encode/decode", () => {
      const codec = getCodec("LowCardinality(String)");
      const values = ["apple", "banana", "apple", "cherry", "banana", "apple"];
      const col = codec.fromValues(values);

      // Encode
      const prefixWriter = new BufferWriter(64);
      codec.writePrefix!(prefixWriter, col);
      const encoded = codec.encode(col);

      // Decode through full block
      const fullWriter = new BufferWriter(1024);
      fullWriter.writeVarint(BlockInfoField.End);
      fullWriter.writeVarint(1);
      fullWriter.writeVarint(6);
      fullWriter.writeString("val");
      fullWriter.writeString("LowCardinality(String)");
      fullWriter.writeU8(0);
      fullWriter.write(prefixWriter.finish());
      fullWriter.write(encoded);

      const result = decodeNativeBlock(fullWriter.finish(), 0, { clientVersion: 54454 });
      assert.strictEqual(result.rowCount, 6);

      for (let i = 0; i < values.length; i++) {
        assert.strictEqual(result.columnData[0].get(i), values[i]);
      }
    });

    it("handles version prefix correctly", () => {
      // LowCardinality prefix is just VERSION (UInt64)
      const data = buildBlock(
        "val",
        "LowCardinality(String)",
        (w) => w.writeU64LE(LC.VERSION),
        (w) => {
          // flags: FLAG_ADDITIONAL_KEYS | INDEX_U8
          w.writeU64LE(LC.FLAG_ADDITIONAL_KEYS | LC.INDEX_U8);
          // dict size
          w.writeU64LE(2n);
          // dict values: ["a", "b"]
          w.writeVarint(1);
          w.write(new Uint8Array([97]));
          w.writeVarint(1);
          w.write(new Uint8Array([98]));
          // row count
          w.writeU64LE(3n);
          // indices: [0, 1, 0]
          w.write(new Uint8Array([0, 1, 0]));
        },
        3,
      );

      const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });
      assert.strictEqual(result.rowCount, 3);
      assert.strictEqual(result.columnData[0].get(0), "a");
      assert.strictEqual(result.columnData[0].get(1), "b");
      assert.strictEqual(result.columnData[0].get(2), "a");
    });
  });

  describe("Variant", () => {
    it("round-trips through encode/decode", () => {
      const codec = getCodec("Variant(String, UInt64)");
      // Variant values are [discriminator, value]
      const values = [
        [0, "hello"],
        [1, 42n],
        [0, "world"],
      ];
      const col = codec.fromValues(values);

      // Encode
      const prefixWriter = new BufferWriter(64);
      codec.writePrefix!(prefixWriter, col);
      const encoded = codec.encode(col);

      // Decode through full block
      const fullWriter = new BufferWriter(1024);
      fullWriter.writeVarint(BlockInfoField.End);
      fullWriter.writeVarint(1);
      fullWriter.writeVarint(3);
      fullWriter.writeString("val");
      fullWriter.writeString("Variant(String, UInt64)");
      fullWriter.writeU8(0);
      fullWriter.write(prefixWriter.finish());
      fullWriter.write(encoded);

      const result = decodeNativeBlock(fullWriter.finish(), 0, { clientVersion: 54454 });
      assert.strictEqual(result.rowCount, 3);

      const col0 = result.columnData[0];
      assert.deepStrictEqual(col0.get(0), [0, "hello"]);
      assert.deepStrictEqual(col0.get(1), [1, 42n]);
      assert.deepStrictEqual(col0.get(2), [0, "world"]);
    });

    it("handles mode prefix correctly", () => {
      // Variant prefix is MODE_BASIC (UInt64)
      const data = buildBlock(
        "val",
        "Variant(String, UInt64)",
        (w) => w.writeU64LE(Variant.MODE_BASIC),
        (w) => {
          // discriminators: [0, 1, 0]
          w.write(new Uint8Array([0, 1, 0]));
          // String group (2 values: "a", "b")
          w.writeVarint(1);
          w.write(new Uint8Array([97]));
          w.writeVarint(1);
          w.write(new Uint8Array([98]));
          // UInt64 group (1 value: 123)
          w.writeU64LE(123n);
        },
        3,
      );

      const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });
      assert.strictEqual(result.rowCount, 3);
      assert.deepStrictEqual(result.columnData[0].get(0), [0, "a"]);
      assert.deepStrictEqual(result.columnData[0].get(1), [1, 123n]);
      assert.deepStrictEqual(result.columnData[0].get(2), [0, "b"]);
    });
  });

  describe("Dynamic", () => {
    it("handles version and type list prefix", () => {
      // Dynamic prefix: VERSION_V3 + type count + type names
      const data = buildBlock(
        "val",
        "Dynamic",
        (w) => {
          w.writeU64LE(Dynamic.VERSION_V3);
          w.writeVarint(2); // 2 types
          w.writeString("String");
          w.writeString("UInt64");
          // No inner codec prefixes for String/UInt64
        },
        (w) => {
          // discriminators: [0, 1, 2, 0] (2 = null discriminator for Dynamic with 2 types)
          w.write(new Uint8Array([0, 1, 2, 0]));
          // String group (2 values)
          w.writeVarint(1);
          w.write(new Uint8Array([97])); // "a"
          w.writeVarint(1);
          w.write(new Uint8Array([98])); // "b"
          // UInt64 group (1 value)
          w.writeU64LE(42n);
        },
        4,
      );

      const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });
      assert.strictEqual(result.rowCount, 4);

      // Dynamic returns unwrapped values (unlike Variant which returns [disc, value])
      const col = result.columnData[0];
      assert.strictEqual(col.get(0), "a");
      assert.strictEqual(col.get(1), 42n);
      assert.strictEqual(col.get(2), null);
      assert.strictEqual(col.get(3), "b");
    });
  });
});
