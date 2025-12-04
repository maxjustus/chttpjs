import { cityhash_102_128 } from "ch-city-wasm";
import { compress as lz4CompressRaw, decompress as lz4DecompressRaw } from "./vendor/lz4/lz4.js";

// Build-time constant set by esbuild --define
// When bundled: replaced with true/false literal, enabling dead-code elimination
// When unbundled (dev): undefined, so we default to true
declare const BUILD_WITH_ZSTD: boolean | undefined;

// Conditional ZSTD imports - tree-shaken when BUILD_WITH_ZSTD=false
let zstdCompressFn: ((source: Uint8Array, level: number) => Uint8Array) | undefined;
let zstdDecompressFn: ((source: Uint8Array) => Uint8Array) | undefined;

// Module state - initialized by init()
let initialized = false;

/** True if using native zstd-napi, false if using WASM */
export let usingNativeZstd = false;

async function initZstd(): Promise<void> {
  // Try native zstd-napi first in Node.js
  if (typeof process !== "undefined" && process.versions?.node) {
    try {
      const native = await import("zstd-napi");
      zstdCompressFn = (d, level) => new Uint8Array(native.compress(d, level));
      zstdDecompressFn = (d) => new Uint8Array(native.decompress(d));
      usingNativeZstd = true;
      return;
    } catch {
      // Native not available, fall through to WASM
    }
  }

  // WASM fallback
  const wasm = await import("@bokuweb/zstd-wasm");
  await wasm.init();
  zstdCompressFn = wasm.compress;
  zstdDecompressFn = wasm.decompress;
}

export async function init(): Promise<void> {
  if (initialized) return;

  // Use BUILD_WITH_ZSTD directly for tree-shaking, fallback to true for dev
  if (typeof BUILD_WITH_ZSTD === "undefined" || BUILD_WITH_ZSTD) {
    await initZstd();
  }

  initialized = true;
}

// Uint8Array helpers
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
  return arr[offset] | (arr[offset + 1] << 8) | (arr[offset + 2] << 16) | (arr[offset + 3] << 24) >>> 0;
}

function writeUInt32LE(arr: Uint8Array, value: number, offset: number): void {
  arr[offset] = value & 0xff;
  arr[offset + 1] = (value >> 8) & 0xff;
  arr[offset + 2] = (value >> 16) & 0xff;
  arr[offset + 3] = (value >> 24) & 0xff;
}

function equals(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

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

export function cityHash128LE(bytes: Uint8Array): Uint8Array {
  const hash = cityhash_102_128(bytes);
  // Swap hi/lo 8-byte halves to match ClickHouse's expected byte order
  return concat([hash.subarray(8, 16), hash.subarray(0, 8)]);
}

function lz4Compress(raw: Uint8Array): Uint8Array {
  // @nick/lz4 prepends 4-byte uncompressed size, but ClickHouse expects raw block data
  const withPrefix = lz4CompressRaw(raw);
  return withPrefix.subarray(4);
}

function lz4Decompress(compressed: Uint8Array, uncompressedSize: number): Uint8Array {
  // @nick/lz4 expects 4-byte uncompressed size prefix
  const prefix = new Uint8Array(4);
  writeUInt32LE(prefix, uncompressedSize, 0);
  return lz4DecompressRaw(concat([prefix, compressed]));
}

function zstdCompress(raw: Uint8Array, level = 3): Uint8Array {
  if (!zstdCompressFn) {
    throw new Error("ZSTD compression not available in this build variant");
  }
  return zstdCompressFn(raw, level);
}

function zstdDecompress(compressed: Uint8Array): Uint8Array {
  if (!zstdDecompressFn) {
    throw new Error("ZSTD decompression not available in this build variant");
  }
  return zstdDecompressFn(compressed);
}

export function encodeBlock(
  raw: Uint8Array,
  mode: MethodCode = Method.LZ4,
): Uint8Array {
  let compressed: Uint8Array;

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
    default: {
      const _exhaustive: never = mode;
      throw new Error(`Unsupported compression method 0x${(mode as number).toString(16)}`);
    }
  }

  const metadata = new Uint8Array(HEADER_SIZE);
  metadata[MAGIC_OFFSET] = mode;
  writeUInt32LE(metadata, HEADER_SIZE + compressed.length, COMPRESSED_SIZE_OFFSET);
  writeUInt32LE(metadata, raw.length, UNCOMPRESSED_SIZE_OFFSET);

  const checksum = cityHash128LE(concat([metadata, compressed]));
  return concat([checksum, metadata, compressed]);
}

export function decodeBlock(
  block: Uint8Array,
  skipChecksumVerification = false,
): Uint8Array {
  if (block.length < CHECKSUM_SIZE + HEADER_SIZE) {
    throw new Error("block too small");
  }

  const checksum = block.subarray(0, CHECKSUM_SIZE);
  const metadata = block.subarray(CHECKSUM_SIZE, CHECKSUM_SIZE + HEADER_SIZE);
  const compressed = block.subarray(CHECKSUM_SIZE + HEADER_SIZE);

  if (!skipChecksumVerification) {
    const expected = cityHash128LE(concat([metadata, compressed]));
    if (!equals(checksum, expected)) {
      throw new Error("checksum mismatch");
    }
  }

  const mode = metadata[MAGIC_OFFSET] as MethodCode;
  const compressedSize = readUInt32LE(metadata, COMPRESSED_SIZE_OFFSET);
  const uncompressedSize = readUInt32LE(metadata, UNCOMPRESSED_SIZE_OFFSET);

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
    default: {
      const _exhaustive: never = mode;
      throw new Error(`Unsupported compression method 0x${(mode as number).toString(16)}`);
    }
  }
}

export function decodeBlocks(
  data: Uint8Array,
  skipChecksumVerification = false,
): Uint8Array {
  const blocks: Uint8Array[] = [];
  let offset = 0;

  while (offset + CHECKSUM_SIZE + HEADER_SIZE <= data.length) {
    const metadataOffset = offset + CHECKSUM_SIZE;
    const compressedSize = readUInt32LE(data, metadataOffset + COMPRESSED_SIZE_OFFSET);
    const blockSize = CHECKSUM_SIZE + compressedSize;

    if (offset + blockSize > data.length) {
      break;
    }

    const block = data.subarray(offset, offset + blockSize);
    const decompressed = decodeBlock(block, skipChecksumVerification);
    blocks.push(decompressed);

    offset += blockSize;
  }

  return concat(blocks);
}
