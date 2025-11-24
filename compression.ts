import { createChCity } from "./ch-city.js";
import { compressSync, uncompressSync } from "lz4-napi";
import * as zstd from "zstd-napi";

const chCity = await createChCity();

const CHECKSUM_SIZE = 16;
const HEADER_SIZE = 9;
const MAGIC_OFFSET = 0;
const COMPRESSED_SIZE_OFFSET = 1;
const UNCOMPRESSED_SIZE_OFFSET = 5;

export const Method = {
  None: 0x02,
  LZ4: 0x82,
  ZSTD: 0x90,
} as const;

export type MethodCode = (typeof Method)[keyof typeof Method];

export function cityHash128LE(bytes: Buffer): Buffer {
  const hash = Buffer.from(chCity.cityhash102(bytes));
  // Swap hi/lo 8-byte halves to match ClickHouse's expected byte order
  return Buffer.concat([hash.subarray(8, 16), hash.subarray(0, 8)]);
}

function lz4Compress(raw: Buffer): Buffer {
  // lz4-napi prepends 4-byte uncompressed size, but ClickHouse expects raw block data
  const withPrefix = compressSync(raw);
  return withPrefix.subarray(4);
}

function lz4Decompress(compressed: Buffer, uncompressedSize: number): Buffer {
  // lz4-napi expects 4-byte uncompressed size prefix
  const prefix = Buffer.alloc(4);
  prefix.writeUInt32LE(uncompressedSize, 0);
  return uncompressSync(Buffer.concat([prefix, compressed]));
}

function zstdCompress(raw: Buffer, level = 3): Buffer {
  return zstd.compress(raw, level);
}

function zstdDecompress(compressed: Buffer): Buffer {
  return zstd.decompress(compressed);
}

export function encodeBlock(
  raw: Buffer,
  mode: MethodCode = Method.LZ4,
): Buffer {
  let compressed: Buffer;

  switch (mode) {
    case Method.LZ4:
      compressed = lz4Compress(raw);
      break;
    case Method.ZSTD:
      compressed = zstdCompress(raw);
      break;
    case Method.None:
      compressed = raw;
      break;
    default:
      throw new Error(`Unsupported compression method 0x${mode.toString(16)}`);
  }

  const metadata = Buffer.alloc(HEADER_SIZE);
  metadata[MAGIC_OFFSET] = mode;
  metadata.writeUInt32LE(HEADER_SIZE + compressed.length, COMPRESSED_SIZE_OFFSET);
  metadata.writeUInt32LE(raw.length, UNCOMPRESSED_SIZE_OFFSET);

  const checksum = cityHash128LE(Buffer.concat([metadata, compressed]));
  return Buffer.concat([checksum, metadata, compressed]);
}

export function decodeBlock(
  block: Buffer,
  skipChecksumVerification = false,
): Buffer {
  if (block.length < CHECKSUM_SIZE + HEADER_SIZE) {
    throw new Error("block too small");
  }

  const checksum = block.subarray(0, CHECKSUM_SIZE);
  const metadata = block.subarray(CHECKSUM_SIZE, CHECKSUM_SIZE + HEADER_SIZE);
  const compressed = block.subarray(CHECKSUM_SIZE + HEADER_SIZE);

  if (!skipChecksumVerification) {
    const expected = cityHash128LE(Buffer.concat([metadata, compressed]));
    if (!checksum.equals(expected)) {
      throw new Error("checksum mismatch");
    }
  }

  const mode = metadata[MAGIC_OFFSET] as MethodCode;
  const compressedSize = metadata.readUInt32LE(COMPRESSED_SIZE_OFFSET);
  const uncompressedSize = metadata.readUInt32LE(UNCOMPRESSED_SIZE_OFFSET);

  if (compressedSize !== HEADER_SIZE + compressed.length) {
    throw new Error(
      `compressed_size mismatch: expected ${compressedSize}, got ${HEADER_SIZE + compressed.length}`,
    );
  }

  switch (mode) {
    case Method.None:
      return compressed;
    case Method.LZ4:
      return lz4Decompress(compressed, uncompressedSize);
    case Method.ZSTD:
      return zstdDecompress(compressed);
    default:
      throw new Error(`Unsupported compression method 0x${mode.toString(16)}`);
  }
}

export function decodeBlocks(
  data: Buffer,
  skipChecksumVerification = false,
): Buffer {
  const blocks: Buffer[] = [];
  let offset = 0;

  while (offset + CHECKSUM_SIZE + HEADER_SIZE <= data.length) {
    const metadataOffset = offset + CHECKSUM_SIZE;
    const compressedSize = data.readUInt32LE(
      metadataOffset + COMPRESSED_SIZE_OFFSET,
    );
    const blockSize = CHECKSUM_SIZE + compressedSize;

    if (offset + blockSize > data.length) {
      break;
    }

    const block = data.subarray(offset, offset + blockSize);
    const decompressed = decodeBlock(block, skipChecksumVerification);
    blocks.push(decompressed);

    offset += blockSize;
  }

  return Buffer.concat(blocks);
}
