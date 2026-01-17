/**
 * Tests for Nullable with complex inner types.
 *
 * Covers:
 * - Nullable with LowCardinality (special handling)
 * - Nullable with Decimal types
 * - Nullable with DateTime64
 * - Nullable with UUID/IPv4/IPv6
 * - Null flag encoding/decoding patterns
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { getCodec } from "../../native/codecs.ts";
import { BufferReader } from "../../native/io.ts";
import { defaultDeserializerState } from "../../native/codecs.ts";

describe("Nullable with complex inner types", () => {
  describe("Nullable(LowCardinality(String))", () => {
    it("handles mix of values and nulls", () => {
      const codec = getCodec("Nullable(LowCardinality(String))");
      const values = ["apple", null, "banana", null, "apple"];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), "apple");
      assert.strictEqual(col.get(1), null);
      assert.strictEqual(col.get(2), "banana");
      assert.strictEqual(col.get(3), null);
      assert.strictEqual(col.get(4), "apple");
    });

    it("handles all nulls", () => {
      const codec = getCodec("Nullable(LowCardinality(String))");
      const values = [null, null, null];
      const col = codec.fromValues(values);

      for (let i = 0; i < 3; i++) {
        assert.strictEqual(col.get(i), null);
      }
    });

    it("handles no nulls", () => {
      const codec = getCodec("Nullable(LowCardinality(String))");
      const values = ["a", "b", "c"];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), "a");
      assert.strictEqual(col.get(1), "b");
      assert.strictEqual(col.get(2), "c");
    });
  });

  describe("Nullable(Decimal)", () => {
    it("handles Nullable Decimal(18, 4)", () => {
      const codec = getCodec("Nullable(Decimal(18, 4))");
      const values = ["123.4567", null, "0.0001", "-999.9999"];
      const col = codec.fromValues(values);

      // Decimal returns strings (preserves precision)
      assert.strictEqual(col.get(0), "123.4567");
      assert.strictEqual(col.get(1), null);
      assert.strictEqual(col.get(2), "0.0001");
      assert.strictEqual(col.get(3), "-999.9999");
    });

    it("handles Nullable Decimal(38, 10)", () => {
      const codec = getCodec("Nullable(Decimal(38, 10))");
      const values = [null, "1234567890.1234567890"];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), null);
      // Decimal returns strings
      assert.strictEqual(typeof col.get(1), "string");
    });
  });

  describe("Nullable(DateTime64)", () => {
    it("handles Nullable DateTime64(3)", () => {
      const codec = getCodec("Nullable(DateTime64(3))");
      const date1 = new Date("2024-06-15T10:30:45.123Z");
      const date2 = new Date("2000-01-01T00:00:00.000Z");
      const values = [date1, null, date2];
      const col = codec.fromValues(values);

      // DateTime64 returns ClickHouseDateTime64 objects with ticks
      const v0 = col.get(0) as { ticks: bigint };
      assert.ok(v0 !== null && typeof v0 === "object");
      assert.ok("ticks" in v0);
      assert.strictEqual(v0.ticks, BigInt(date1.getTime()));

      assert.strictEqual(col.get(1), null);

      const v2 = col.get(2) as { ticks: bigint };
      assert.ok(v2 !== null && typeof v2 === "object");
      assert.strictEqual(v2.ticks, BigInt(date2.getTime()));
    });

    it("handles Nullable DateTime64(6)", () => {
      const codec = getCodec("Nullable(DateTime64(6))");
      const date = new Date("2024-06-15T10:30:45.123Z");
      const values = [null, date, null];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), null);
      const v1 = col.get(1) as { ticks: bigint };
      assert.ok(v1 !== null && typeof v1 === "object");
      assert.ok("ticks" in v1);
      assert.strictEqual(col.get(2), null);
    });
  });

  describe("Nullable(UUID)", () => {
    it("handles mix of UUIDs and nulls", () => {
      const codec = getCodec("Nullable(UUID)");
      const uuid1 = "550e8400-e29b-41d4-a716-446655440000";
      const uuid2 = "00000000-0000-0000-0000-000000000000";
      const values = [uuid1, null, uuid2];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), uuid1);
      assert.strictEqual(col.get(1), null);
      assert.strictEqual(col.get(2), uuid2);
    });
  });

  describe("Nullable(IPv4)", () => {
    it("handles mix of IPs and nulls", () => {
      const codec = getCodec("Nullable(IPv4)");
      const values = ["192.168.1.1", null, "10.0.0.1", null];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), "192.168.1.1");
      assert.strictEqual(col.get(1), null);
      assert.strictEqual(col.get(2), "10.0.0.1");
      assert.strictEqual(col.get(3), null);
    });
  });

  describe("Nullable(IPv6)", () => {
    it("handles mix of IPv6 and nulls", () => {
      const codec = getCodec("Nullable(IPv6)");
      const values = ["::1", null, "2001:db8::1"];
      const col = codec.fromValues(values);

      assert.ok(String(col.get(0)).includes("1"));
      assert.strictEqual(col.get(1), null);
      assert.ok(String(col.get(2)).includes("2001:db8"));
    });
  });

  describe("Nullable(FixedString)", () => {
    it("handles mix of FixedString and nulls", () => {
      const codec = getCodec("Nullable(FixedString(4))");
      const values = ["test", null, "abcd"];
      const col = codec.fromValues(values);

      const v0 = col.get(0) as Uint8Array;
      assert.deepStrictEqual(v0, new TextEncoder().encode("test"));
      assert.strictEqual(col.get(1), null);
      const v2 = col.get(2) as Uint8Array;
      assert.deepStrictEqual(v2, new TextEncoder().encode("abcd"));
    });
  });

  describe("Nullable(Enum8)", () => {
    it("handles mix of enum values and nulls", () => {
      // Note: Enum must include 0 as a valid value for Nullable to work correctly
      // (EnumCodec.zeroValue returns 0, which NullableCodec uses for null positions)
      const codec = getCodec("Nullable(Enum8('zero' = 0, 'one' = 1, 'two' = 2))");
      const values = ["one", null, "two", null];
      const col = codec.fromValues(values);

      assert.strictEqual(col.get(0), "one");
      assert.strictEqual(col.get(1), null);
      assert.strictEqual(col.get(2), "two");
      assert.strictEqual(col.get(3), null);
    });
  });

  describe("null flag patterns", () => {
    it("correctly encodes null flags", () => {
      const codec = getCodec("Nullable(Int32)");
      const values = [null, 42, null, null, 100];
      const col = codec.fromValues(values);
      const encoded = codec.encode(col);

      // Nullable encoding: null flags (1 byte each) + inner data
      // First 5 bytes should be null flags: 1, 0, 1, 1, 0
      assert.strictEqual(encoded[0], 1); // null
      assert.strictEqual(encoded[1], 0); // not null
      assert.strictEqual(encoded[2], 1); // null
      assert.strictEqual(encoded[3], 1); // null
      assert.strictEqual(encoded[4], 0); // not null
    });

    it("round-trips through encode/decode", () => {
      const codec = getCodec("Nullable(Int64)");
      const values = [123n, null, -456n, null, 0n];
      const col = codec.fromValues(values);
      const encoded = codec.encode(col);

      const reader = new BufferReader(encoded);
      const decoded = codec.decode(reader, values.length, defaultDeserializerState());

      for (let i = 0; i < values.length; i++) {
        assert.strictEqual(decoded.get(i), values[i], `mismatch at index ${i}`);
      }
    });
  });
});
