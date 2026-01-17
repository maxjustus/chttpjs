/**
 * Tests for Enum parsing and encoding error cases.
 *
 * Covers:
 * - parseEnumDefinition failure modes
 * - EnumCodec.toEnumValue validation errors
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { getCodec } from "../../native/codecs.ts";
import { parseEnumDefinition } from "../../native/types.ts";

describe("Enum parsing errors", () => {
  describe("parseEnumDefinition", () => {
    it("returns null for non-enum type", () => {
      assert.strictEqual(parseEnumDefinition("String"), null);
      assert.strictEqual(parseEnumDefinition("Int32"), null);
    });

    it("returns null for missing closing paren", () => {
      assert.strictEqual(parseEnumDefinition("Enum8('a' = 1"), null);
    });

    it("returns null for empty definition", () => {
      assert.strictEqual(parseEnumDefinition("Enum8()"), null);
    });

    it("returns null for malformed quoted string", () => {
      // Unclosed quote
      assert.strictEqual(parseEnumDefinition("Enum8('a = 1)"), null);
    });

    it("returns null for missing equals sign", () => {
      assert.strictEqual(parseEnumDefinition("Enum8('a' 1)"), null);
    });

    it("returns null for missing value", () => {
      assert.strictEqual(parseEnumDefinition("Enum8('a' = )"), null);
    });

    it("returns null for value out of Enum8 range", () => {
      // Enum8 range: -128 to 127
      assert.strictEqual(parseEnumDefinition("Enum8('a' = 128)"), null);
      assert.strictEqual(parseEnumDefinition("Enum8('a' = -129)"), null);
    });

    it("returns null for value out of Enum16 range", () => {
      // Enum16 range: -32768 to 32767
      assert.strictEqual(parseEnumDefinition("Enum16('a' = 32768)"), null);
      assert.strictEqual(parseEnumDefinition("Enum16('a' = -32769)"), null);
    });

    it("returns null for duplicate name", () => {
      assert.strictEqual(parseEnumDefinition("Enum8('a' = 1, 'a' = 2)"), null);
    });

    it("returns null for duplicate value", () => {
      assert.strictEqual(parseEnumDefinition("Enum8('a' = 1, 'b' = 1)"), null);
    });

    it("parses valid enum definitions", () => {
      const result = parseEnumDefinition("Enum8('a' = 1, 'b' = 2)");
      assert.ok(result !== null);
      assert.strictEqual(result.nameToValue.get("a"), 1);
      assert.strictEqual(result.nameToValue.get("b"), 2);
    });

    it("handles negative values", () => {
      const result = parseEnumDefinition("Enum8('neg' = -1, 'pos' = 1)");
      assert.ok(result !== null);
      assert.strictEqual(result.nameToValue.get("neg"), -1);
      assert.strictEqual(result.nameToValue.get("pos"), 1);
    });

    it("handles escaped characters in names", () => {
      const result = parseEnumDefinition("Enum8('it\\'s' = 1)");
      assert.ok(result !== null);
      assert.strictEqual(result.nameToValue.get("it's"), 1);
    });
  });

  describe("EnumCodec construction errors", () => {
    it("throws on invalid enum definition", () => {
      assert.throws(() => getCodec("Enum8("), /Failed to parse enum definition/);
    });

    it("throws on empty enum", () => {
      assert.throws(() => getCodec("Enum8()"), /Failed to parse enum definition/);
    });
  });

  describe("EnumCodec.fromValues errors", () => {
    it("throws on invalid string value", () => {
      const codec = getCodec("Enum8('a' = 1, 'b' = 2)");
      assert.throws(() => codec.fromValues(["c"]), /Invalid enum value: c/);
    });

    it("throws on out of range bigint", () => {
      const codec = getCodec("Enum8('a' = 1)");
      assert.throws(() => codec.fromValues([200n]), /Enum value out of range: 200/);
    });

    it("throws on non-integer number", () => {
      const codec = getCodec("Enum8('a' = 1)");
      assert.throws(() => codec.fromValues([1.5]), /Invalid enum value: 1\.5/);
    });

    it("throws on number out of range", () => {
      const codec = getCodec("Enum8('a' = 1)");
      assert.throws(() => codec.fromValues([200]), /Enum value out of range: 200/);
    });

    it("throws on number not in mapping", () => {
      const codec = getCodec("Enum8('a' = 1, 'b' = 3)");
      assert.throws(() => codec.fromValues([2]), /Invalid enum value: 2/);
    });

    it("throws on invalid type", () => {
      const codec = getCodec("Enum8('a' = 1)");
      assert.throws(() => codec.fromValues([{ invalid: true }]), /Invalid enum value/);
    });

    it("handles null/undefined as default value", () => {
      const codec = getCodec("Enum8('a' = 1, 'b' = 2)");
      // Should use minimum valid value (1) for null/undefined
      const col = codec.fromValues([null, undefined, "b"]);
      assert.strictEqual(col.get(0), "a");
      assert.strictEqual(col.get(1), "a");
      assert.strictEqual(col.get(2), "b");
    });
  });
});
