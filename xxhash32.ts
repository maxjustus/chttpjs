const PRIME32_1 = 2654435761;
const PRIME32_2 = 2246822519;
const PRIME32_3 = 3266489917;
const PRIME32_4 = 668265263;
const PRIME32_5 = 374761393;

function rotl(x: number, r: number): number {
  return (x << r) | (x >>> (32 - r));
}

function round(acc: number, input: number): number {
  acc = (acc + Math.imul(input, PRIME32_2)) | 0;
  acc = rotl(acc, 13);
  return Math.imul(acc, PRIME32_1);
}

function readU32LE(data: Uint8Array, i: number): number {
  return data[i]! | (data[i + 1]! << 8) | (data[i + 2]! << 16) | (data[i + 3]! << 24) | 0;
}

/** xxHash-32 (https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md), one-shot. */
export function xxhash32(data: Uint8Array, seed = 0): number {
  let i = 0;
  let h: number;

  if (data.length >= 16) {
    let v1 = (seed + PRIME32_1 + PRIME32_2) | 0;
    let v2 = (seed + PRIME32_2) | 0;
    let v3 = seed | 0;
    let v4 = (seed - PRIME32_1) | 0;
    const limit = data.length - 16;
    while (i <= limit) {
      v1 = round(v1, readU32LE(data, i));
      v2 = round(v2, readU32LE(data, i + 4));
      v3 = round(v3, readU32LE(data, i + 8));
      v4 = round(v4, readU32LE(data, i + 12));
      i += 16;
    }
    h = (rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)) | 0;
  } else {
    h = (seed + PRIME32_5) | 0;
  }

  h = (h + data.length) | 0;

  while (i + 4 <= data.length) {
    h = (h + Math.imul(readU32LE(data, i), PRIME32_3)) | 0;
    h = Math.imul(rotl(h, 17), PRIME32_4);
    i += 4;
  }
  while (i < data.length) {
    h = (h + Math.imul(data[i]!, PRIME32_5)) | 0;
    h = Math.imul(rotl(h, 11), PRIME32_1);
    i++;
  }

  h ^= h >>> 15;
  h = Math.imul(h, PRIME32_2);
  h ^= h >>> 13;
  h = Math.imul(h, PRIME32_3);
  h ^= h >>> 16;
  return h >>> 0;
}

/** Incremental xxHash-32 for content that arrives across multiple chunks. */
export class Xxhash32Stream {
  private v1: number;
  private v2: number;
  private v3: number;
  private v4: number;
  private totalLength = 0;
  private buffered: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  private readonly seed: number;

  constructor(seed = 0) {
    this.seed = seed | 0;
    this.v1 = (seed + PRIME32_1 + PRIME32_2) | 0;
    this.v2 = (seed + PRIME32_2) | 0;
    this.v3 = seed | 0;
    this.v4 = (seed - PRIME32_1) | 0;
  }

  update(chunk: Uint8Array): void {
    if (chunk.length === 0) return;
    this.totalLength += chunk.length;

    let data = chunk;
    if (this.buffered.length > 0) {
      const merged = new Uint8Array(this.buffered.length + chunk.length);
      merged.set(this.buffered, 0);
      merged.set(chunk, this.buffered.length);
      data = merged;
    }

    let i = 0;
    while (i + 16 <= data.length) {
      this.v1 = round(this.v1, readU32LE(data, i));
      this.v2 = round(this.v2, readU32LE(data, i + 4));
      this.v3 = round(this.v3, readU32LE(data, i + 8));
      this.v4 = round(this.v4, readU32LE(data, i + 12));
      i += 16;
    }
    this.buffered = data.subarray(i);
  }

  digest(): number {
    let h: number;
    if (this.totalLength >= 16) {
      h = (rotl(this.v1, 1) + rotl(this.v2, 7) + rotl(this.v3, 12) + rotl(this.v4, 18)) | 0;
    } else {
      h = (this.seed + PRIME32_5) | 0;
    }
    h = (h + this.totalLength) | 0;

    const rest = this.buffered;
    let i = 0;
    while (i + 4 <= rest.length) {
      h = (h + Math.imul(readU32LE(rest, i), PRIME32_3)) | 0;
      h = Math.imul(rotl(h, 17), PRIME32_4);
      i += 4;
    }
    while (i < rest.length) {
      h = (h + Math.imul(rest[i]!, PRIME32_5)) | 0;
      h = Math.imul(rotl(h, 11), PRIME32_1);
      i++;
    }

    h ^= h >>> 15;
    h = Math.imul(h, PRIME32_2);
    h ^= h >>> 13;
    h = Math.imul(h, PRIME32_3);
    h ^= h >>> 16;
    return h >>> 0;
  }
}
