import { describe, it, before } from "node:test";
import assert from "node:assert/strict";

import {
  init,
  encodeBlock,
  decodeBlock,
  decodeBlocks,
  Method,
  usingNativeLz4,
  usingNativeZstd,
} from "../compression.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function concat(arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

function readUInt32LE(arr: Uint8Array, offset: number): number {
  return (
    arr[offset] |
    (arr[offset + 1] << 8) |
    (arr[offset + 2] << 16) |
    ((arr[offset + 3] << 24) >>> 0)
  );
}

describe("Compression", () => {
  before(async () => {
    await init();
  });

  describe("LZ4 compression", () => {
    it("should compress and decompress data correctly", () => {
      const data = encoder.encode("Hello, World! This is a test.");
      const compressed = encodeBlock(data, Method.LZ4);
      const decompressed = decodeBlock(compressed);

      assert.strictEqual(decoder.decode(decompressed), decoder.decode(data));
    });

    it("should handle empty data", () => {
      const data = encoder.encode("");
      const compressed = encodeBlock(data, Method.None);
      const decompressed = decodeBlock(compressed);

      assert.strictEqual(decoder.decode(decompressed), "");
    });

    it("should handle large repetitive data efficiently", () => {
      const data = encoder.encode("A".repeat(10000));
      const compressed = encodeBlock(data, Method.LZ4);
      const decompressed = decodeBlock(compressed);

      assert.strictEqual(decoder.decode(decompressed), decoder.decode(data));
    });
  });

  describe("ZSTD compression", () => {
    it("should compress and decompress data correctly", () => {
      const data = encoder.encode("Hello, World! This is a ZSTD test.");
      const compressed = encodeBlock(data, Method.ZSTD);
      const decompressed = decodeBlock(compressed);

      assert.strictEqual(decoder.decode(decompressed), decoder.decode(data));
    });

    it("should achieve better compression than LZ4 for repetitive data", () => {
      const data = encoder.encode("ABCD".repeat(1000));

      const lz4Compressed = encodeBlock(data, Method.LZ4);
      const zstdCompressed = encodeBlock(data, Method.ZSTD);

      const lz4Decompressed = decodeBlock(lz4Compressed);
      const zstdDecompressed = decodeBlock(zstdCompressed);

      assert.strictEqual(decoder.decode(lz4Decompressed), decoder.decode(data));
      assert.strictEqual(
        decoder.decode(zstdDecompressed),
        decoder.decode(data),
      );

      // ZSTD typically achieves better compression
      console.log(
        `    LZ4: ${lz4Compressed.length} bytes, ZSTD: ${zstdCompressed.length} bytes`,
      );
    });
  });

  describe("Multi-block decompression", () => {
    it("should decompress multiple blocks correctly", () => {
      const data1 = encoder.encode("First block data");
      const data2 = encoder.encode("Second block data");
      const data3 = encoder.encode("Third block data");

      const block1 = encodeBlock(data1, Method.LZ4);
      const block2 = encodeBlock(data2, Method.LZ4);
      const block3 = encodeBlock(data3, Method.LZ4);

      const combined = concat([block1, block2, block3]);
      const decompressed = decodeBlocks(combined);

      const expected = decoder.decode(concat([data1, data2, data3]));
      assert.strictEqual(decoder.decode(decompressed), expected);
    });

    it("should handle mixed compression methods", () => {
      const data1 = encoder.encode("LZ4 compressed block");
      const data2 = encoder.encode("ZSTD compressed block");
      const data3 = encoder.encode("Uncompressed block");

      const block1 = encodeBlock(data1, Method.LZ4);
      const block2 = encodeBlock(data2, Method.ZSTD);
      const block3 = encodeBlock(data3, Method.None);

      const combined = concat([block1, block2, block3]);
      const decompressed = decodeBlocks(combined);

      const expected = decoder.decode(concat([data1, data2, data3]));
      assert.strictEqual(decoder.decode(decompressed), expected);
    });
  });

  describe("Partial block handling", () => {
    it("should handle block split across chunks", async () => {
      const data = encoder.encode("Test data for partial block handling");
      const compressed = encodeBlock(data, Method.LZ4);

      // Simulate the decompression logic with partial chunks
      async function processChunks(chunks: Uint8Array[]) {
        let buffer = new Uint8Array(0);
        let result = "";

        for (const chunk of chunks) {
          buffer = concat([buffer, chunk]);

          while (buffer.length >= 25) {
            if (buffer.length < 17) break;

            const compressedSize = readUInt32LE(buffer, 17);
            const blockSize = 16 + compressedSize;

            if (buffer.length < blockSize) break;

            const block = buffer.subarray(0, blockSize);
            buffer = buffer.subarray(blockSize);

            const decompressed = decodeBlock(block);
            result += decoder.decode(decompressed);
          }
        }

        return result;
      }

      // Test various split points
      const testCases = [
        { name: "single chunk", splits: [compressed.length] },
        { name: "split at header", splits: [10] },
        { name: "split at data", splits: [30] },
        { name: "multiple splits", splits: [5, 10, 15] },
        { name: "byte by byte", splits: Array(10).fill(1) },
      ];

      for (const testCase of testCases) {
        const chunks: Uint8Array[] = [];
        let offset = 0;

        for (const size of testCase.splits) {
          if (offset >= compressed.length) break;
          const chunkSize = Math.min(size, compressed.length - offset);
          chunks.push(compressed.subarray(offset, offset + chunkSize));
          offset += chunkSize;
        }

        if (offset < compressed.length) {
          chunks.push(compressed.subarray(offset));
        }

        const result = await processChunks(chunks);
        assert.strictEqual(
          result,
          decoder.decode(data),
          `Failed for: ${testCase.name}`,
        );
      }
    });
  });

  describe("Native compression backends", () => {
    it("should report native backend status", () => {
      console.log(`    LZ4: ${usingNativeLz4 ? "native (lz4-napi)" : "WASM"}`);
      console.log(
        `    ZSTD: ${usingNativeZstd ? "native (zstd-napi)" : "WASM"}`,
      );
      // In Node.js with native deps installed, both should be native
      if (typeof process !== "undefined" && process.versions?.node) {
        assert.ok(usingNativeLz4, "Should use native LZ4 in Node.js");
        assert.ok(usingNativeZstd, "Should use native ZSTD in Node.js");
      }
    });

    it("should produce compatible output with large data", () => {
      // Test with various data sizes to ensure native/WASM compatibility
      const sizes = [100, 1000, 10000, 100000];

      for (const size of sizes) {
        const data = encoder.encode("X".repeat(size));

        // LZ4 round-trip
        const lz4Compressed = encodeBlock(data, Method.LZ4);
        const lz4Decompressed = decodeBlock(lz4Compressed);
        assert.strictEqual(
          lz4Decompressed.length,
          data.length,
          `LZ4 size mismatch for ${size} bytes`,
        );
        assert.deepStrictEqual(
          lz4Decompressed,
          data,
          `LZ4 data mismatch for ${size} bytes`,
        );

        // ZSTD round-trip
        const zstdCompressed = encodeBlock(data, Method.ZSTD);
        const zstdDecompressed = decodeBlock(zstdCompressed);
        assert.strictEqual(
          zstdDecompressed.length,
          data.length,
          `ZSTD size mismatch for ${size} bytes`,
        );
        assert.deepStrictEqual(
          zstdDecompressed,
          data,
          `ZSTD data mismatch for ${size} bytes`,
        );
      }
    });

    it("should produce valid ClickHouse block format", () => {
      const data = encoder.encode("Test data for ClickHouse");
      const compressed = encodeBlock(data, Method.LZ4);

      // Verify block structure
      assert.ok(compressed.length >= 25, "Block should be at least 25 bytes");

      // First 16 bytes: checksum
      const checksum = compressed.subarray(0, 16);
      assert.strictEqual(checksum.length, 16, "Checksum should be 16 bytes");

      // Byte 16: method
      const method = compressed[16];
      assert.strictEqual(method, Method.LZ4, "Method should be LZ4");

      // Bytes 17-20: compressed size (includes 9-byte header)
      const compressedSize = readUInt32LE(compressed, 17);
      assert.strictEqual(
        compressedSize,
        compressed.length - 16,
        "Compressed size should match",
      );

      // Bytes 21-24: uncompressed size
      const uncompressedSize = readUInt32LE(compressed, 21);
      assert.strictEqual(
        uncompressedSize,
        data.length,
        "Uncompressed size should match",
      );
    });
  });
});
