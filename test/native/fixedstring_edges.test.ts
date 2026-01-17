/**
 * Tests for FixedString edge cases.
 *
 * Covers:
 * - Exact length matches
 * - Zero-padding for short strings
 * - Truncation errors for long strings
 * - Multi-byte UTF-8 character handling
 * - Uint8Array input validation
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { getCodec } from "../../native/codecs.ts";

describe("FixedString edge cases", () => {
  describe("string input", () => {
    it("accepts exact length ASCII string", () => {
      const codec = getCodec("FixedString(5)");
      const col = codec.fromValues(["hello"]);
      const bytes = col.get(0) as Uint8Array;
      assert.deepStrictEqual(bytes, new TextEncoder().encode("hello"));
    });

    it("pads short string with zeros", () => {
      const codec = getCodec("FixedString(8)");
      const col = codec.fromValues(["hi"]);
      const bytes = col.get(0) as Uint8Array;
      assert.strictEqual(bytes.length, 8);
      assert.strictEqual(bytes[0], 104); // 'h'
      assert.strictEqual(bytes[1], 105); // 'i'
      for (let i = 2; i < 8; i++) {
        assert.strictEqual(bytes[i], 0, `byte ${i} should be 0`);
      }
    });

    it("throws on string too long in bytes", () => {
      const codec = getCodec("FixedString(3)");
      assert.throws(() => codec.fromValues(["hello"]), /FixedString\(3\) requires 3 bytes, got 5/);
    });

    it("handles multi-byte UTF-8 characters", () => {
      const codec = getCodec("FixedString(6)");
      // "é" is 2 bytes in UTF-8
      const col = codec.fromValues(["café"]);
      const bytes = col.get(0) as Uint8Array;
      assert.strictEqual(bytes.length, 6);
      // "café" = 5 bytes: c(1) a(1) f(1) é(2) = 5 bytes
    });

    it("throws when UTF-8 bytes exceed length", () => {
      const codec = getCodec("FixedString(3)");
      // "日本" = 6 bytes in UTF-8 (3 bytes per character)
      assert.throws(() => codec.fromValues(["日本"]), /FixedString\(3\) requires 3 bytes, got 6/);
    });

    it("accepts empty string (pads with zeros)", () => {
      const codec = getCodec("FixedString(4)");
      const col = codec.fromValues([""]);
      const bytes = col.get(0) as Uint8Array;
      assert.deepStrictEqual(bytes, new Uint8Array(4));
    });
  });

  describe("Uint8Array input", () => {
    it("accepts exact length Uint8Array", () => {
      const codec = getCodec("FixedString(4)");
      const input = new Uint8Array([1, 2, 3, 4]);
      const col = codec.fromValues([input]);
      assert.deepStrictEqual(col.get(0), input);
    });

    it("throws on short Uint8Array", () => {
      const codec = getCodec("FixedString(4)");
      assert.throws(
        () => codec.fromValues([new Uint8Array([1, 2])]),
        /FixedString\(4\) requires 4 bytes, got 2/,
      );
    });

    it("throws on long Uint8Array", () => {
      const codec = getCodec("FixedString(4)");
      assert.throws(
        () => codec.fromValues([new Uint8Array([1, 2, 3, 4, 5])]),
        /FixedString\(4\) requires 4 bytes, got 5/,
      );
    });

    it("accepts binary data with embedded nulls", () => {
      const codec = getCodec("FixedString(5)");
      const input = new Uint8Array([65, 0, 66, 0, 67]);
      const col = codec.fromValues([input]);
      assert.deepStrictEqual(col.get(0), input);
    });
  });

  describe("null handling", () => {
    it("converts null to zero-filled buffer", () => {
      const codec = getCodec("FixedString(3)");
      const col = codec.fromValues([null]);
      assert.deepStrictEqual(col.get(0), new Uint8Array(3));
    });

    it("converts undefined to zero-filled buffer", () => {
      const codec = getCodec("FixedString(3)");
      const col = codec.fromValues([undefined]);
      assert.deepStrictEqual(col.get(0), new Uint8Array(3));
    });
  });

  describe("type errors", () => {
    it("throws on number input", () => {
      const codec = getCodec("FixedString(4)");
      assert.throws(() => codec.fromValues([42]), /Cannot coerce number to FixedString/);
    });

    it("throws on object input", () => {
      const codec = getCodec("FixedString(4)");
      assert.throws(
        () => codec.fromValues([{ key: "value" }]),
        /Cannot coerce object to FixedString/,
      );
    });

    it("throws on array input", () => {
      const codec = getCodec("FixedString(4)");
      assert.throws(() => codec.fromValues([["a", "b"]]), /Cannot coerce object to FixedString/);
    });
  });

  describe("zeroValue", () => {
    it("returns zero-filled buffer of correct length", () => {
      const codec = getCodec("FixedString(7)");
      const zero = codec.zeroValue();
      assert.ok(zero instanceof Uint8Array);
      assert.strictEqual((zero as Uint8Array).length, 7);
      assert.deepStrictEqual(zero, new Uint8Array(7));
    });
  });

  describe("toLiteral", () => {
    it("trims trailing nulls for display", () => {
      const codec = getCodec("FixedString(8)");
      const buf = new Uint8Array(8);
      buf.set(new TextEncoder().encode("test"));
      const literal = codec.toLiteral(buf, true);
      assert.strictEqual(literal, "'test'");
    });

    it("escapes special characters", () => {
      const codec = getCodec("FixedString(10)");
      const buf = new Uint8Array(10);
      buf.set(new TextEncoder().encode("it's"));
      const literal = codec.toLiteral(buf, true);
      assert.strictEqual(literal, "'it\\'s'");
    });
  });
});
