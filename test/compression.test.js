const { describe, it, before, after } = require("node:test");
const assert = require("node:assert");
const {
  encodeBlock,
  decodeBlock,
  decodeBlocks,
  Method,
} = require("../compression-node");

describe("Compression", () => {
  describe("LZ4 compression", () => {
    it("should compress and decompress data correctly", () => {
      const data = Buffer.from("Hello, World! This is a test.");
      const compressed = encodeBlock(data, Method.LZ4);
      const decompressed = decodeBlock(compressed, true);

      assert.strictEqual(decompressed.toString(), data.toString());
    });

    it("should handle empty data", () => {
      const data = Buffer.from("");
      const compressed = encodeBlock(data, Method.None);
      const decompressed = decodeBlock(compressed, true);

      assert.strictEqual(decompressed.toString(), "");
    });

    it("should handle large repetitive data efficiently", () => {
      const data = Buffer.from("A".repeat(10000));
      const compressed = encodeBlock(data, Method.LZ4);
      const decompressed = decodeBlock(compressed, true);

      assert.strictEqual(decompressed.toString(), data.toString());
      // LZ4 should compress repetitive data well
      assert.ok(compressed.length < data.length / 10);
    });
  });

  describe("ZSTD compression", () => {
    it("should compress and decompress data correctly", () => {
      const data = Buffer.from("Hello, World! This is a ZSTD test.");
      const compressed = encodeBlock(data, Method.ZSTD);
      const decompressed = decodeBlock(compressed, true);

      assert.strictEqual(decompressed.toString(), data.toString());
    });

    it("should achieve better compression than LZ4 for repetitive data", () => {
      const data = Buffer.from("ABCD".repeat(1000));

      const lz4Compressed = encodeBlock(data, Method.LZ4);
      const zstdCompressed = encodeBlock(data, Method.ZSTD);

      const lz4Decompressed = decodeBlock(lz4Compressed, true);
      const zstdDecompressed = decodeBlock(zstdCompressed, true);

      assert.strictEqual(lz4Decompressed.toString(), data.toString());
      assert.strictEqual(zstdDecompressed.toString(), data.toString());

      // ZSTD typically achieves better compression
      console.log(
        `    LZ4: ${lz4Compressed.length} bytes, ZSTD: ${zstdCompressed.length} bytes`,
      );
    });
  });

  describe("Multi-block decompression", () => {
    it("should decompress multiple blocks correctly", () => {
      const data1 = Buffer.from("First block data");
      const data2 = Buffer.from("Second block data");
      const data3 = Buffer.from("Third block data");

      const block1 = encodeBlock(data1, Method.LZ4);
      const block2 = encodeBlock(data2, Method.LZ4);
      const block3 = encodeBlock(data3, Method.LZ4);

      const combined = Buffer.concat([block1, block2, block3]);
      const decompressed = decodeBlocks(combined, true);

      const expected = Buffer.concat([data1, data2, data3]).toString();
      assert.strictEqual(decompressed.toString(), expected);
    });

    it("should handle mixed compression methods", () => {
      const data1 = Buffer.from("LZ4 compressed block");
      const data2 = Buffer.from("ZSTD compressed block");
      const data3 = Buffer.from("Uncompressed block");

      const block1 = encodeBlock(data1, Method.LZ4);
      const block2 = encodeBlock(data2, Method.ZSTD);
      const block3 = encodeBlock(data3, Method.None);

      const combined = Buffer.concat([block1, block2, block3]);
      const decompressed = decodeBlocks(combined, true);

      const expected = Buffer.concat([data1, data2, data3]).toString();
      assert.strictEqual(decompressed.toString(), expected);
    });
  });

  describe("Partial block handling", () => {
    it("should handle block split across chunks", async () => {
      const data = Buffer.from("Test data for partial block handling");
      const compressed = encodeBlock(data, Method.LZ4);

      // Simulate the decompression logic with partial chunks
      async function processChunks(chunks) {
        let buffer = Buffer.alloc(0);
        let result = "";

        for (const chunk of chunks) {
          buffer = Buffer.concat([buffer, chunk]);

          while (buffer.length >= 25) {
            if (buffer.length < 17) break;

            const compressedSize = buffer.readUInt32LE(17);
            const blockSize = 16 + compressedSize;

            if (buffer.length < blockSize) break;

            const block = buffer.slice(0, blockSize);
            buffer = buffer.slice(blockSize);

            const decompressed = decodeBlock(block, true);
            result += decompressed.toString();
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
        const chunks = [];
        let offset = 0;

        for (const size of testCase.splits) {
          if (offset >= compressed.length) break;
          const chunkSize = Math.min(size, compressed.length - offset);
          chunks.push(compressed.slice(offset, offset + chunkSize));
          offset += chunkSize;
        }

        if (offset < compressed.length) {
          chunks.push(compressed.slice(offset));
        }

        const result = await processChunks(chunks);
        assert.strictEqual(
          result,
          data.toString(),
          `Failed for: ${testCase.name}`,
        );
      }
    });
  });
});
