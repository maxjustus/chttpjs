// Node.js version of ClickHouse compression with proper CityHash128
const bling = require('bling-hashes');
const lz4 = require('lz4');
const zstd = require('zstd-napi');

// ClickHouse uses a custom compression format with the following structure:
// - 16-byte CityHash128 checksum (v1.0.2)
// - 1-byte magic number (0x82 for LZ4, 0x90 for ZSTD)
// - 4-byte compressed size (little-endian, includes 9-byte header)
// - 4-byte uncompressed size (little-endian)
// - Raw compressed data

const Method = {
  None: 0x02,
  LZ4: 0x82,
  ZSTD: 0x90
};

// CityHash128 with 64-bit rotation for ClickHouse
function cityHash128LE(bytes) {
  // bling-hashes city128 returns a City128Value object
  const hashObj = bling.city128(bytes);
  
  // Get the two 8-byte buffers
  const [loBuf, hiBuf] = hashObj.toBuffers();
  
  // Rotate right 64 bits: swap the two 8-byte halves for ClickHouse
  const rotated = Buffer.concat([hiBuf, loBuf]);
  
  return rotated;
}

// LZ4 compression for ClickHouse (raw block without size prefix)
function lz4CompressCH(raw) {
  const maxSize = lz4.encodeBound(raw.length);
  const compressed = Buffer.alloc(maxSize);
  const compressedSize = lz4.encodeBlock(raw, compressed);
  return compressed.slice(0, compressedSize);
}

// LZ4 decompression for ClickHouse
function lz4DecompressCH(compressed, uncompressedSize) {
  const output = Buffer.alloc(uncompressedSize);
  lz4.decodeBlock(compressed, output);
  return output;
}

// ZSTD compression for ClickHouse (raw block)
function zstdCompressCH(raw, level = 3) {
  return zstd.compress(raw, level);
}

// ZSTD decompression for ClickHouse
function zstdDecompressCH(compressed) {
  return zstd.decompress(compressed);
}

// Encode a block in ClickHouse format
function encodeBlock(raw, mode = Method.LZ4) {
  let compressed;
  let compressedDataSize;
  
  if (mode === Method.LZ4) {
    compressed = lz4CompressCH(raw);
    compressedDataSize = compressed.length;
  } else if (mode === Method.ZSTD) {
    compressed = zstdCompressCH(raw);
    compressedDataSize = compressed.length;
  } else if (mode === Method.None) {
    compressed = raw;
    compressedDataSize = compressed.length;
  } else {
    throw new Error(`Unsupported compression method 0x${mode.toString(16)}`);
  }

  // Create metadata: magic(1) + compressed_size(4) + uncompressed_size(4)
  // Note: compressed_size includes the 9-byte header size
  const metadata = Buffer.alloc(9);
  metadata[0] = mode;  // magic byte (0x82 for LZ4, 0x90 for ZSTD)
  metadata.writeUInt32LE(9 + compressedDataSize, 1);  // compressed_size (header + data)
  metadata.writeUInt32LE(raw.length, 5);               // uncompressed_size

  // Hash metadata + compressed data
  const toHash = Buffer.concat([metadata, compressed]);
  const checksum = cityHash128LE(toHash);

  // Assemble final block: checksum + metadata + compressed
  return Buffer.concat([checksum, metadata, compressed]);
}

// Decode a single block from ClickHouse format
function decodeBlock(block, skipChecksumVerification = false) {
  if (block.length < 25) throw new Error("block too small");
  
  const checksum = block.slice(0, 16);
  const metadata = block.slice(16, 25);
  const compressed = block.slice(25);

  // Verify checksum (skip if using different CityHash implementation)
  if (!skipChecksumVerification) {
    const toHash = Buffer.concat([metadata, compressed]);
    const expected = cityHash128LE(toHash);
    if (!checksum.equals(expected)) {
      throw new Error("checksum mismatch");
    }
  }

  // Parse metadata
  const mode = metadata[0];
  const compressedSize = metadata.readUInt32LE(1);
  const uncompressedSize = metadata.readUInt32LE(5);
  
  if (compressedSize !== 9 + compressed.length) {
    throw new Error(`compressed_size mismatch: expected ${compressedSize}, got ${9 + compressed.length}`);
  }

  if (mode === Method.None) return compressed;
  if (mode === Method.LZ4) return lz4DecompressCH(compressed, uncompressedSize);
  if (mode === Method.ZSTD) return zstdDecompressCH(compressed);
  throw new Error(`Unsupported compression method 0x${mode.toString(16)}`);
}

// Decode multiple blocks from ClickHouse response
function decodeBlocks(data, skipChecksumVerification = false) {
  const blocks = [];
  let offset = 0;
  
  while (offset < data.length) {
    if (data.length - offset < 25) {
      break; // Not enough data for another block
    }
    
    // Read the compressed size from metadata to know how big this block is
    const compressedSize = data.readUInt32LE(offset + 17);
    const blockSize = 16 + compressedSize; // checksum + metadata + compressed data
    
    if (offset + blockSize > data.length) {
      break; // Not enough data for complete block
    }
    
    const block = data.slice(offset, offset + blockSize);
    const decompressed = decodeBlock(block, skipChecksumVerification);
    blocks.push(decompressed);
    
    offset += blockSize;
  }
  
  return Buffer.concat(blocks);
}

module.exports = {
  Method,
  encodeBlock,
  decodeBlock,
  decodeBlocks,
  cityHash128LE
};
