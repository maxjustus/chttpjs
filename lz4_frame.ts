import { concat, readUInt32LE } from "./compression.ts";
import { Xxhash32Stream, xxhash32 } from "./xxhash32.ts";

const MAGIC = 0x184d2204;
const UNCOMPRESSED_BLOCK = 0x80000000;
/** An LZ4 match reaches at most 64 KiB back, so that much history must survive. */
const WINDOW_SIZE = 65536;
const BLOCK_MAX_SIZES: Record<number, number> = {
  4: 64 * 1024,
  5: 256 * 1024,
  6: 1024 * 1024,
  7: 4 * 1024 * 1024,
};

export interface Lz4FrameDecoder {
  /** Feed frame bytes. Returns whatever became decodable, possibly empty. */
  push(chunk: Uint8Array): Uint8Array;
}

function decodeSequences(src: Uint8Array, dst: Uint8Array, start: number): number {
  let pos = start;
  let i = 0;

  while (i < src.length) {
    const token = src[i++]!;

    let literalLength = token >> 4;
    if (literalLength === 15) {
      let extra: number;
      do {
        if (i >= src.length) throw new Error("LZ4 block truncated in literal length");
        extra = src[i++]!;
        literalLength += extra;
      } while (extra === 255);
    }

    if (i + literalLength > src.length) throw new Error("LZ4 literal run past end of block");
    if (pos + literalLength > dst.length) throw new Error("LZ4 block exceeds max block size");
    dst.set(src.subarray(i, i + literalLength), pos);
    i += literalLength;
    pos += literalLength;

    // The final sequence of a block carries literals only, with no match.
    if (i === src.length) break;
    if (i + 2 > src.length) throw new Error("LZ4 block truncated in match offset");

    const offset = src[i]! | (src[i + 1]! << 8);
    i += 2;

    let matchLength = (token & 15) + 4;
    if ((token & 15) === 15) {
      let extra: number;
      do {
        if (i >= src.length) throw new Error("LZ4 block truncated in match length");
        extra = src[i++]!;
        matchLength += extra;
      } while (extra === 255);
    }

    let from = pos - offset;
    if (offset === 0 || from < 0) throw new Error(`LZ4 match offset ${offset} out of range`);
    if (pos + matchLength > dst.length) throw new Error("LZ4 block exceeds max block size");
    // Overlapping matches are legal and encode runs, so copy one byte at a time.
    for (let k = 0; k < matchLength; k++) dst[pos++] = dst[from++]!;
  }

  return pos;
}

export function createLz4FrameDecoder(): Lz4FrameDecoder {
  let pending: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let history: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let blockMaxSize = 0;
  let blockChecksum = false;
  let contentChecksum = false;
  let headerRead = false;
  let awaitingContentChecksum = false;
  let finished = false;
  const contentHash = new Xxhash32Stream();

  function readHeader(): boolean {
    if (pending.length < 7) return false;
    if (readUInt32LE(pending, 0) !== MAGIC) {
      throw new Error("Not an LZ4 frame: bad magic number");
    }

    const flg = pending[4]!;
    if (flg >> 6 !== 1) throw new Error(`Unsupported LZ4 frame version ${flg >> 6}`);
    if (flg & 0x01) throw new Error("LZ4 frame dictionaries are not supported");

    // Header: magic(4) FLG(1) BD(1) [content size(8)] HC(1). Dict ID (rejected above)
    // is the only optional field that shifts HC; content checksum lives after the frame.
    const headerLength = 7 + (flg & 0x08 ? 8 : 0);
    if (pending.length < headerLength) return false;

    const sizeCode = (pending[5]! >> 4) & 7;
    const maxSize = BLOCK_MAX_SIZES[sizeCode];
    if (!maxSize) throw new Error(`Unsupported LZ4 block size code ${sizeCode}`);
    blockMaxSize = maxSize;

    blockChecksum = (flg & 0x10) !== 0;
    contentChecksum = (flg & 0x04) !== 0;
    pending = pending.subarray(headerLength);
    headerRead = true;
    return true;
  }

  function decodeBlock(block: Uint8Array, uncompressed: boolean): Uint8Array {
    // Decode behind the retained history so cross-block matches resolve inline.
    const scratch = new Uint8Array(history.length + blockMaxSize);
    scratch.set(history, 0);
    const start = history.length;

    let end: number;
    if (uncompressed) {
      if (start + block.length > scratch.length) throw new Error("LZ4 block exceeds max size");
      scratch.set(block, start);
      end = start + block.length;
    } else {
      end = decodeSequences(block, scratch, start);
    }

    history = scratch.slice(Math.max(0, end - WINDOW_SIZE), end);
    return scratch.slice(start, end);
  }

  function verifyContentChecksum(): void {
    if (pending.length < 4) return;
    const expected = readUInt32LE(pending, 0);
    const actual = contentHash.digest();
    if (actual !== expected) {
      throw new Error(`LZ4 content checksum mismatch: got ${actual}, expected ${expected}`);
    }
    pending = pending.subarray(4);
    awaitingContentChecksum = false;
    finished = true;
  }

  return {
    push(chunk: Uint8Array): Uint8Array {
      if (finished || chunk.length === 0) return new Uint8Array(0);
      pending = pending.length === 0 ? chunk : concat([pending, chunk]);

      if (awaitingContentChecksum) {
        verifyContentChecksum();
        return new Uint8Array(0);
      }
      if (!headerRead && !readHeader()) return new Uint8Array(0);

      const produced: Uint8Array[] = [];
      while (pending.length >= 4) {
        const marker = readUInt32LE(pending, 0);
        if (marker === 0) {
          pending = pending.subarray(4);
          if (contentChecksum) {
            awaitingContentChecksum = true;
            verifyContentChecksum();
          } else {
            finished = true;
          }
          break;
        }

        const size = marker & ~UNCOMPRESSED_BLOCK;
        const total = 4 + size + (blockChecksum ? 4 : 0);
        if (pending.length < total) break;

        const block = pending.subarray(4, 4 + size);
        if (blockChecksum) {
          const expected = readUInt32LE(pending, 4 + size);
          const actual = xxhash32(block);
          if (actual !== expected) {
            throw new Error(`LZ4 block checksum mismatch: got ${actual}, expected ${expected}`);
          }
        }

        const decoded = decodeBlock(block, (marker & UNCOMPRESSED_BLOCK) !== 0);
        if (contentChecksum) contentHash.update(decoded);
        produced.push(decoded);
        pending = pending.subarray(total);
      }

      return concat(produced);
    },
  };
}
