/**
 * Buffer I/O utilities for Native format encoding/decoding.
 */

import {
  TEXT_ENCODER,
  TEXT_DECODER,
  type DecodeOptions,
  type TypedArray,
} from "../shared.ts";

export class BufferUnderflowError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BufferUnderflowError";
  }
}

export type TypedArrayConstructor<T extends TypedArray> = {
  new (length: number): T;
  new (buffer: ArrayBuffer, byteOffset: number, length: number): T;
  BYTES_PER_ELEMENT: number;
};

// --- Standalone optimized I/O functions ---

export function varIntSize(value: number | bigint): number {
  let v = BigInt(value);
  let size = 1;
  while (v >= 0x80n) {
    size++;
    v >>= 7n;
  }
  return size;
}

export function writeVarInt(
  buffer: Uint8Array,
  offset: number,
  value: number | bigint,
): number {
  let v = BigInt(value);
  let pos = offset;
  while (v >= 0x80n) {
    buffer[pos++] = Number((v & 0x7fn) | 0x80n);
    v >>= 7n;
  }
  buffer[pos++] = Number(v);
  return pos - offset;
}

export function readVarInt(
  buffer: Uint8Array,
  cursor: { offset: number },
): number {
  let result = 0,
    shift = 0;
  while (true) {
    if (cursor.offset >= buffer.length)
      throw new BufferUnderflowError("Buffer underflow reading varint");
    const byte = buffer[cursor.offset++];
    result |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return result;
}

export function readVarInt64(
  buffer: Uint8Array,
  cursor: { offset: number },
): bigint {
  let result = 0n,
    shift = 0n;
  while (true) {
    if (cursor.offset >= buffer.length)
      throw new BufferUnderflowError("Buffer underflow reading varint64");
    const byte = BigInt(buffer[cursor.offset++]);
    result |= (byte & 0x7fn) << shift;
    if ((byte & 0x80n) === 0n) break;
    shift += 7n;
  }
  return result;
}

export class BufferWriter {
  private buffer: Uint8Array;
  private offset = 0;
  private view: DataView;

  constructor(initialSize = 65536) {
    // 64KB default
    this.buffer = new Uint8Array(initialSize);
    this.view = new DataView(this.buffer.buffer);
  }

  private ensure(bytes: number) {
    const needed = this.offset + bytes;
    if (needed <= this.buffer.length) return;
    let newSize = Math.max(this.buffer.length * 2, needed);
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
    this.view = new DataView(this.buffer.buffer);
  }

  write(chunk: Uint8Array) {
    this.ensure(chunk.length);
    this.buffer.set(chunk, this.offset);
    this.offset += chunk.length;
  }

  writeU8(v: number) {
    this.ensure(1);
    this.buffer[this.offset++] = v;
  }

  writeU32LE(v: number) {
    this.ensure(4);
    this.view.setUint32(this.offset, v, true);
    this.offset += 4;
  }

  writeU64LE(v: bigint) {
    this.ensure(8);
    this.view.setBigUint64(this.offset, v, true);
    this.offset += 8;
  }

  writeI32LE(v: number) {
    this.ensure(4);
    this.view.setInt32(this.offset, v, true);
    this.offset += 4;
  }

  writeVarint(value: number | bigint) {
    this.ensure(10);
    this.offset += writeVarInt(this.buffer, this.offset, value);
  }

  writeString(val: string) {
    // Worst case: 3 bytes per char (UTF-8) + 5 bytes for length varint
    const maxLen = val.length * 3;
    this.ensure(maxLen + 5);

    // Reserved space for 1-byte varint (common case)
    const { written } = TEXT_ENCODER.encodeInto(
      val,
      this.buffer.subarray(this.offset + 1, this.offset + 1 + maxLen),
    );

    if (written < 128) {
      this.buffer[this.offset] = written;
      this.offset += 1 + written;
    } else {
      // Multi-byte varint: shift the encoded string
      const vSize = varIntSize(written);
      this.buffer.copyWithin(
        this.offset + vSize,
        this.offset + 1,
        this.offset + 1 + written,
      );
      writeVarInt(this.buffer, this.offset, written);
      this.offset += vSize + written;
    }
  }

  reset() {
    this.offset = 0;
  }

  finish(): Uint8Array {
    return this.buffer.subarray(0, this.offset);
  }
}

export class BufferReader {
  buffer: Uint8Array;
  offset: number;
  view: DataView;
  options?: DecodeOptions;

  constructor(buffer: Uint8Array, offset = 0, options?: DecodeOptions) {
    this.buffer = buffer;
    this.offset = offset;
    this.view = new DataView(
      buffer.buffer,
      buffer.byteOffset,
      buffer.byteLength,
    );
    this.options = options;
  }

  readVarint(): number {
    return readVarInt(this.buffer, this);
  }

  readVarInt64(): bigint {
    return readVarInt64(this.buffer, this);
  }

  readString(): string {
    const len = this.readVarint();
    this.ensureAvailable(len);
    const str = TEXT_DECODER.decode(
      this.buffer.subarray(this.offset, this.offset + len),
    );
    this.offset += len;
    return str;
  }

  // Zero-copy if aligned, copy otherwise
  readTypedArray<T extends TypedArray>(
    Ctor: TypedArrayConstructor<T>,
    count: number,
  ): T {
    const elementSize = Ctor.BYTES_PER_ELEMENT;
    const byteLength = count * elementSize;
    this.ensureAvailable(byteLength);
    const currentOffset = this.buffer.byteOffset + this.offset;

    let res: T;
    if (currentOffset % elementSize === 0) {
      res = new Ctor(this.buffer.buffer as ArrayBuffer, currentOffset, count);
    } else {
      const copy = new Uint8Array(
        this.buffer.subarray(this.offset, this.offset + byteLength),
      );
      res = new Ctor(copy.buffer as ArrayBuffer, 0, count);
    }
    this.offset += byteLength;
    return res;
  }

  readBytes(length: number): Uint8Array {
    this.ensureAvailable(length);
    const res = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return res;
  }

  ensureAvailable(bytes: number): void {
    if (this.offset + bytes > this.buffer.length) {
      throw new BufferUnderflowError(
        `Need ${bytes} bytes at offset ${this.offset}, only ${this.buffer.length - this.offset} available`,
      );
    }
  }

  readU8(): number {
    this.ensureAvailable(1);
    return this.buffer[this.offset++];
  }

  readU32LE(): number {
    this.ensureAvailable(4);
    const val = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return val;
  }

  readU64LE(): bigint {
    this.ensureAvailable(8);
    const val = this.view.getBigUint64(this.offset, true);
    this.offset += 8;
    return val;
  }

  readI32LE(): number {
    this.ensureAvailable(4);
    const val = this.view.getInt32(this.offset, true);
    this.offset += 4;
    return val;
  }

  readI64LE(): bigint {
    this.ensureAvailable(8);
    const val = this.view.getBigInt64(this.offset, true);
    this.offset += 8;
    return val;
  }
}
