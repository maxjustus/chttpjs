import { concat, readUInt32LE } from "./compression.ts";

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

function decodeSequences(src: Uint8Array, dst: Uint8Array, start: number, limit: number): number {
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
    if (pos + literalLength > limit) throw new Error("LZ4 block exceeds max block size");
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
    if (pos + matchLength > limit) throw new Error("LZ4 block exceeds max block size");
    // Overlapping matches are legal and encode runs, so copy one byte at a time.
    for (let k = 0; k < matchLength; k++) dst[pos++] = dst[from++]!;
  }

  return pos;
}

export function createLz4FrameDecoder(): Lz4FrameDecoder {
  // Unparsed frame bytes live in one flat buffer between head and tail. A push
  // copies its chunk in and parsing consumes from the front, so pending bytes
  // move only when the buffer grows, never per push.
  let pending: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let head = 0;
  let tail = 0;
  // One buffer for the whole frame: retained history at the front, the block
  // under decode behind it. Reallocating per block moved history + blockMaxSize
  // bytes each time, which dominates when the server flushes small blocks.
  let scratch: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  let historyLen = 0;
  let blockMaxSize = 0;
  let blockChecksum = false;
  let headerRead = false;
  let finished = false;

  function readHeader(): boolean {
    if (tail - head < 7) return false;
    if (readUInt32LE(pending, head) !== MAGIC) {
      throw new Error("Not an LZ4 frame: bad magic number");
    }

    const flg = pending[head + 4]!;
    if (flg >> 6 !== 1) throw new Error(`Unsupported LZ4 frame version ${flg >> 6}`);
    if (flg & 0x01) throw new Error("LZ4 frame dictionaries are not supported");

    // Header: magic(4) FLG(1) BD(1) [content size(8)] HC(1). Dict ID (rejected above)
    // is the only optional field that shifts HC.
    const headerLength = 7 + (flg & 0x08 ? 8 : 0);
    if (tail - head < headerLength) return false;

    const sizeCode = (pending[head + 5]! >> 4) & 7;
    const maxSize = BLOCK_MAX_SIZES[sizeCode];
    if (!maxSize) throw new Error(`Unsupported LZ4 block size code ${sizeCode}`);
    blockMaxSize = maxSize;

    blockChecksum = (flg & 0x10) !== 0;
    scratch = new Uint8Array(WINDOW_SIZE + blockMaxSize);
    head += headerLength;
    headerRead = true;
    return true;
  }

  function decodeBlock(block: Uint8Array, uncompressed: boolean): Uint8Array {
    // Decode behind the retained history so cross-block matches resolve inline.
    const start = historyLen;
    const limit = start + blockMaxSize;

    let end: number;
    if (uncompressed) {
      if (start + block.length > limit) throw new Error("LZ4 block exceeds max size");
      scratch.set(block, start);
      end = start + block.length;
    } else {
      end = decodeSequences(block, scratch, start, limit);
    }

    const decoded = scratch.slice(start, end);
    historyLen = Math.min(end, WINDOW_SIZE);
    scratch.copyWithin(0, end - historyLen, end);
    return decoded;
  }

  function appendPending(chunk: Uint8Array): void {
    if (tail + chunk.length > pending.length) {
      const used = tail - head;
      const next = new Uint8Array(Math.max(used + chunk.length, pending.length * 2, 1024));
      next.set(pending.subarray(head, tail), 0);
      pending = next;
      head = 0;
      tail = used;
    }
    pending.set(chunk, tail);
    tail += chunk.length;
  }

  return {
    push(chunk: Uint8Array): Uint8Array {
      if (finished || chunk.length === 0) return new Uint8Array(0);
      appendPending(chunk);
      if (!headerRead && !readHeader()) return new Uint8Array(0);

      const produced: Uint8Array[] = [];
      while (tail - head >= 4) {
        const marker = readUInt32LE(pending, head);
        if (marker === 0) {
          // A trailing content checksum, if any, is not validated: no producer
          // emits one.
          finished = true;
          break;
        }

        const size = marker & ~UNCOMPRESSED_BLOCK;
        const total = 4 + size + (blockChecksum ? 4 : 0);
        if (tail - head < total) break;

        const block = pending.subarray(head + 4, head + 4 + size);
        const decoded = decodeBlock(block, (marker & UNCOMPRESSED_BLOCK) !== 0);
        produced.push(decoded);
        head += total;
      }
      if (head === tail) {
        head = 0;
        tail = 0;
      }

      return concat(produced);
    },
  };
}
