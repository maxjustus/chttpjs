/**
 * Tests for BigInt boundary cases in 64/128/256-bit integer codecs.
 *
 * Covers:
 * - Valid min/max values at type boundaries
 * - Out of range errors
 * - Non-integer and non-finite number errors
 * - Unsafe integer warnings
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { getCodec } from "../../native/codecs.ts";
import {
  INT64_MIN,
  INT64_MAX,
  INT128_MIN,
  INT128_MAX,
  UINT128_MAX,
  INT256_MIN,
  INT256_MAX,
  UINT256_MAX,
  toBigIntInRange,
} from "../../native/coercion.ts";

describe("BigInt boundary tests", () => {
  describe("Int64 boundaries", () => {
    const codec = getCodec("Int64");

    it("accepts MIN value", () => {
      const col = codec.fromValues([INT64_MIN]);
      assert.strictEqual(col.get(0), INT64_MIN);
    });

    it("accepts MAX value", () => {
      const col = codec.fromValues([INT64_MAX]);
      assert.strictEqual(col.get(0), INT64_MAX);
    });

    it("throws on overflow", () => {
      assert.throws(() => codec.fromValues([INT64_MAX + 1n]), /Int64 out of range/);
    });

    it("throws on underflow", () => {
      assert.throws(() => codec.fromValues([INT64_MIN - 1n]), /Int64 out of range/);
    });
  });

  describe("UInt64 boundaries", () => {
    const codec = getCodec("UInt64");
    const UINT64_MAX = (1n << 64n) - 1n;

    it("accepts 0", () => {
      const col = codec.fromValues([0n]);
      assert.strictEqual(col.get(0), 0n);
    });

    it("accepts MAX value", () => {
      const col = codec.fromValues([UINT64_MAX]);
      assert.strictEqual(col.get(0), UINT64_MAX);
    });

    it("throws on negative", () => {
      assert.throws(() => codec.fromValues([-1n]), /UInt64 out of range/);
    });

    it("throws on overflow", () => {
      assert.throws(() => codec.fromValues([UINT64_MAX + 1n]), /UInt64 out of range/);
    });
  });

  describe("Int128 boundaries", () => {
    const codec = getCodec("Int128");

    it("accepts MIN value", () => {
      const col = codec.fromValues([INT128_MIN]);
      assert.strictEqual(col.get(0), INT128_MIN);
    });

    it("accepts MAX value", () => {
      const col = codec.fromValues([INT128_MAX]);
      assert.strictEqual(col.get(0), INT128_MAX);
    });

    it("throws on overflow", () => {
      assert.throws(() => codec.fromValues([INT128_MAX + 1n]), /Int128 out of range/);
    });

    it("throws on underflow", () => {
      assert.throws(() => codec.fromValues([INT128_MIN - 1n]), /Int128 out of range/);
    });
  });

  describe("UInt128 boundaries", () => {
    const codec = getCodec("UInt128");

    it("accepts 0", () => {
      const col = codec.fromValues([0n]);
      assert.strictEqual(col.get(0), 0n);
    });

    it("accepts MAX value", () => {
      const col = codec.fromValues([UINT128_MAX]);
      assert.strictEqual(col.get(0), UINT128_MAX);
    });

    it("throws on negative", () => {
      assert.throws(() => codec.fromValues([-1n]), /UInt128 out of range/);
    });

    it("throws on overflow", () => {
      assert.throws(() => codec.fromValues([UINT128_MAX + 1n]), /UInt128 out of range/);
    });
  });

  describe("Int256 boundaries", () => {
    const codec = getCodec("Int256");

    it("accepts MIN value", () => {
      const col = codec.fromValues([INT256_MIN]);
      assert.strictEqual(col.get(0), INT256_MIN);
    });

    it("accepts MAX value", () => {
      const col = codec.fromValues([INT256_MAX]);
      assert.strictEqual(col.get(0), INT256_MAX);
    });

    it("throws on overflow", () => {
      assert.throws(() => codec.fromValues([INT256_MAX + 1n]), /Int256 out of range/);
    });

    it("throws on underflow", () => {
      assert.throws(() => codec.fromValues([INT256_MIN - 1n]), /Int256 out of range/);
    });
  });

  describe("UInt256 boundaries", () => {
    const codec = getCodec("UInt256");

    it("accepts 0", () => {
      const col = codec.fromValues([0n]);
      assert.strictEqual(col.get(0), 0n);
    });

    it("accepts MAX value", () => {
      const col = codec.fromValues([UINT256_MAX]);
      assert.strictEqual(col.get(0), UINT256_MAX);
    });

    it("throws on negative", () => {
      assert.throws(() => codec.fromValues([-1n]), /UInt256 out of range/);
    });

    it("throws on overflow", () => {
      assert.throws(() => codec.fromValues([UINT256_MAX + 1n]), /UInt256 out of range/);
    });
  });

  describe("toBigIntInRange coercion", () => {
    it("throws on NaN", () => {
      assert.throws(
        () => toBigIntInRange(NaN, "Int64", INT64_MIN, INT64_MAX),
        /Cannot coerce number "NaN" to Int64/,
      );
    });

    it("throws on Infinity", () => {
      assert.throws(
        () => toBigIntInRange(Infinity, "Int64", INT64_MIN, INT64_MAX),
        /Cannot coerce number "Infinity" to Int64/,
      );
    });

    it("throws on -Infinity", () => {
      assert.throws(
        () => toBigIntInRange(-Infinity, "Int64", INT64_MIN, INT64_MAX),
        /Cannot coerce number "-Infinity" to Int64/,
      );
    });

    it("throws on non-integer float", () => {
      assert.throws(() => toBigIntInRange(3.14, "Int64", INT64_MIN, INT64_MAX), /expected integer/);
    });

    it("throws on unsafe integer", () => {
      // Number.MAX_SAFE_INTEGER + 1 is not safe
      const unsafe = Number.MAX_SAFE_INTEGER + 1;
      assert.throws(
        () => toBigIntInRange(unsafe, "Int64", INT64_MIN, INT64_MAX),
        /cannot safely represent/,
      );
    });

    it("accepts string representations", () => {
      const result = toBigIntInRange("12345678901234567890", "UInt128", 0n, UINT128_MAX);
      assert.strictEqual(result, 12345678901234567890n);
    });

    it("accepts safe integers as numbers", () => {
      const result = toBigIntInRange(42, "Int64", INT64_MIN, INT64_MAX);
      assert.strictEqual(result, 42n);
    });
  });
});
