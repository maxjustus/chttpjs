/**
 * Unit tests for sparse serialization decoding.
 *
 * Sparse serialization encodes columns with many default values by storing
 * only the positions of non-default values. Format:
 * - Offset stream: VarInt gaps between non-default values
 * - END_OF_GRANULE_FLAG (1n << 62n) marks last offset
 * - Value stream: Dense-encoded non-default values
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { decodeNativeBlock } from "../../native/index.ts";
import { BufferWriter } from "../../native/io.ts";
import { Sparse, SerializationKind, BlockInfoField } from "../../native/constants.ts";

/**
 * Build a sparse-encoded Native block with UInt64 data.
 *
 * Sparse format:
 * - For each non-default: VarInt = gap (defaults before this value)
 * - After all values: VarInt with trailing defaults + END_OF_GRANULE_FLAG
 *
 * @param nonDefaults - Array of {index, value} for non-default positions
 * @param totalRows - Total row count (remaining are zeros)
 */
function buildSparseUInt64Block(
  nonDefaults: Array<{ index: number; value: bigint }>,
  totalRows: number,
): Uint8Array {
  const writer = new BufferWriter(4096);

  // Block info (required when clientVersion > 0): just end marker
  writer.writeVarint(BlockInfoField.End);

  // Block header: 1 column, N rows
  writer.writeVarint(1);
  writer.writeVarint(totalRows);

  // Column: name, type
  writer.writeString("val");
  writer.writeString("UInt64");

  // Custom serialization: has_custom=1, kind=Sparse
  writer.writeU8(1); // hasCustomSerialization
  writer.writeU8(SerializationKind.Sparse); // kind

  // Sparse offset stream
  let lastIndex = -1;
  for (const { index } of nonDefaults) {
    const gap = index - lastIndex - 1;
    writer.writeVarint(gap);
    lastIndex = index;
  }

  // Trailing defaults with END flag
  const trailingDefaults =
    nonDefaults.length > 0 ? totalRows - nonDefaults[nonDefaults.length - 1].index - 1 : totalRows;
  writer.writeVarint(BigInt(trailingDefaults) | Sparse.END_OF_GRANULE_FLAG);

  // Dense-encoded values (UInt64 little-endian)
  for (const { value } of nonDefaults) {
    writer.writeU64LE(value);
  }

  return writer.finish();
}

describe("sparse serialization unit tests", () => {
  it("decodes sparse column with single non-default value", () => {
    // 10 rows, only index 5 has value 42
    const data = buildSparseUInt64Block([{ index: 5, value: 42n }], 10);

    const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

    assert.strictEqual(result.rowCount, 10);
    assert.strictEqual(result.columnData.length, 1);

    const col = result.columnData[0];
    for (let i = 0; i < 10; i++) {
      if (i === 5) {
        assert.strictEqual(col.get(i), 42n, `index ${i} should be 42n`);
      } else {
        assert.strictEqual(col.get(i), 0n, `index ${i} should be 0n`);
      }
    }
  });

  it("decodes sparse column with multiple non-default values", () => {
    // 100 rows with values at indices 10, 50, 99
    const nonDefaults = [
      { index: 10, value: 100n },
      { index: 50, value: 500n },
      { index: 99, value: 999n },
    ];
    const data = buildSparseUInt64Block(nonDefaults, 100);

    const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

    assert.strictEqual(result.rowCount, 100);
    const col = result.columnData[0];

    assert.strictEqual(col.get(10), 100n);
    assert.strictEqual(col.get(50), 500n);
    assert.strictEqual(col.get(99), 999n);
    // Spot check zeros
    assert.strictEqual(col.get(0), 0n);
    assert.strictEqual(col.get(25), 0n);
    assert.strictEqual(col.get(98), 0n);
  });

  it("decodes sparse column with all default values", () => {
    // 50 rows, all zeros
    const data = buildSparseUInt64Block([], 50);

    const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

    assert.strictEqual(result.rowCount, 50);
    const col = result.columnData[0];
    for (let i = 0; i < 50; i++) {
      assert.strictEqual(col.get(i), 0n, `index ${i} should be 0n`);
    }
  });

  it("decodes sparse column with consecutive non-default values", () => {
    // 20 rows, indices 5,6,7 have values (gap=0 between them)
    const nonDefaults = [
      { index: 5, value: 1n },
      { index: 6, value: 2n },
      { index: 7, value: 3n },
    ];
    const data = buildSparseUInt64Block(nonDefaults, 20);

    const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

    assert.strictEqual(result.rowCount, 20);
    const col = result.columnData[0];

    assert.strictEqual(col.get(4), 0n);
    assert.strictEqual(col.get(5), 1n);
    assert.strictEqual(col.get(6), 2n);
    assert.strictEqual(col.get(7), 3n);
    assert.strictEqual(col.get(8), 0n);
  });

  it("decodes sparse column with value at index 0", () => {
    // 10 rows, first row has value
    const data = buildSparseUInt64Block([{ index: 0, value: 999n }], 10);

    const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

    assert.strictEqual(result.rowCount, 10);
    const col = result.columnData[0];
    assert.strictEqual(col.get(0), 999n);
    assert.strictEqual(col.get(1), 0n);
  });

  it("decodes sparse column with value at last index", () => {
    // 10 rows, last row has value
    const data = buildSparseUInt64Block([{ index: 9, value: 123n }], 10);

    const result = decodeNativeBlock(data, 0, { clientVersion: 54454 });

    assert.strictEqual(result.rowCount, 10);
    const col = result.columnData[0];
    assert.strictEqual(col.get(8), 0n);
    assert.strictEqual(col.get(9), 123n);
  });
});
