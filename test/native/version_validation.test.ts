/**
 * Tests for version validation in Dynamic and JSON codecs.
 *
 * These types use readPrefix() which validates the format version.
 * ClickHouse uses V3 format for both Dynamic and JSON types.
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { decodeNativeBlock } from "../../native/index.ts";
import { BufferWriter } from "../../native/io.ts";
import { BlockInfoField, Dynamic, JSONFormat } from "../../native/constants.ts";

/**
 * Build a Native block with Dynamic type and specified version.
 */
function buildDynamicBlock(version: bigint, rows: number): Uint8Array {
  const writer = new BufferWriter(256);

  // Block info
  writer.writeVarint(BlockInfoField.End);

  // Header: 1 column, N rows
  writer.writeVarint(1);
  writer.writeVarint(rows);

  // Column
  writer.writeString("d");
  writer.writeString("Dynamic");
  writer.writeU8(0); // no custom serialization

  // Dynamic prefix: version, type count
  writer.writeU64LE(version);
  writer.writeVarint(0); // no types

  // Data: all NULLs (discriminator = type count = 0)
  for (let i = 0; i < rows; i++) {
    writer.writeU8(0);
  }

  return writer.finish();
}

/**
 * Build a Native block with JSON type and specified version.
 */
function buildJSONBlock(version: bigint, rows: number): Uint8Array {
  const writer = new BufferWriter(256);

  // Block info
  writer.writeVarint(BlockInfoField.End);

  // Header: 1 column, N rows
  writer.writeVarint(1);
  writer.writeVarint(rows);

  // Column
  writer.writeString("j");
  writer.writeString("JSON");
  writer.writeU8(0); // no custom serialization

  // JSON prefix: version, path count
  writer.writeU64LE(version);
  writer.writeVarint(0); // no paths

  // Data: empty JSON objects (no typed paths, no dynamic paths to read)
  // JSON with no paths = just prefix, no data bytes per row

  return writer.finish();
}

describe("version validation tests", () => {
  describe("Dynamic codec", () => {
    it("decodes V3 format successfully", () => {
      const data = buildDynamicBlock(Dynamic.VERSION_V3, 5);
      const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

      assert.strictEqual(result.rowCount, 5);
      assert.strictEqual(result.columns[0].type, "Dynamic");
    });

    it("throws on V1 format", () => {
      const data = buildDynamicBlock(1n, 5);

      assert.throws(
        () => decodeNativeBlock(data, 0, { clientVersion: 54454 }),
        /Dynamic: only V3 supported, got V1/,
      );
    });

    it("throws on V2 format", () => {
      const data = buildDynamicBlock(2n, 5);

      assert.throws(
        () => decodeNativeBlock(data, 0, { clientVersion: 54454 }),
        /Dynamic: only V3 supported, got V2/,
      );
    });

    it("throws on unknown version", () => {
      const data = buildDynamicBlock(99n, 5);

      assert.throws(
        () => decodeNativeBlock(data, 0, { clientVersion: 54454 }),
        /Dynamic: only V3 supported, got V99/,
      );
    });
  });

  describe("JSON codec", () => {
    it("decodes V3 format successfully", () => {
      const data = buildJSONBlock(JSONFormat.VERSION_V3, 5);
      const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

      assert.strictEqual(result.rowCount, 5);
      assert.strictEqual(result.columns[0].type, "JSON");
    });

    it("throws on V1 format", () => {
      const data = buildJSONBlock(1n, 5);

      assert.throws(
        () => decodeNativeBlock(data, 0, { clientVersion: 54454 }),
        /JSON: only V3 supported, got V1/,
      );
    });

    it("throws on V2 format", () => {
      const data = buildJSONBlock(2n, 5);

      assert.throws(
        () => decodeNativeBlock(data, 0, { clientVersion: 54454 }),
        /JSON: only V3 supported, got V2/,
      );
    });

    it("throws on unknown version", () => {
      const data = buildJSONBlock(42n, 5);

      assert.throws(
        () => decodeNativeBlock(data, 0, { clientVersion: 54454 }),
        /JSON: only V3 supported, got V42/,
      );
    });
  });
});
