/**
 * Buffer I/O utilities for Native format encoding/decoding.
 */

import { TEXT_ENCODER, TEXT_DECODER, type DecodeOptions } from "../shared.ts";

type TypedArray = Int8Array | Uint8Array | Int16Array | Uint16Array | Int32Array | Uint32Array | BigInt64Array | BigUint64Array | Float32Array | Float64Array;

export type TypedArrayConstructor<T extends TypedArray> = {
  new(length: number): T;
  new(buffer: ArrayBuffer, byteOffset: number, length: number): T;
  BYTES_PER_ELEMENT: number;
};

export class BufferWriter {
  private buffer: Uint8Array;
  private offset = 0;

  constructor(initialSize = 1024 * 1024) {
    this.buffer = new Uint8Array(initialSize);
  }

  private ensure(bytes: number) {
    const needed = this.offset + bytes;
    if (needed <= this.buffer.length) return;
    let newSize = this.buffer.length * 2;
    while (newSize < needed) newSize *= 2;
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
  }

  write(chunk: Uint8Array) {
    this.ensure(chunk.length);
    this.buffer.set(chunk, this.offset);
    this.offset += chunk.length;
  }

  writeVarint(value: number) {
    this.ensure(10); // Max varint size
    while (value >= 0x80) {
      this.buffer[this.offset++] = (value & 0x7f) | 0x80;
      value >>>= 7;
    }
    this.buffer[this.offset++] = value;
  }

  writeString(val: string) {
    // Worst case: 3 bytes per char (UTF-8) + 5 bytes for length varint
    const maxLen = val.length * 3;
    this.ensure(maxLen + 5);

    // Reserve 1 byte for length, encode string directly
    const lenOffset = this.offset++;
    const { written } = TEXT_ENCODER.encodeInto(
      val,
      this.buffer.subarray(this.offset, this.offset + maxLen)
    );

    if (written < 128) {
      // Common case: length fits in 1 byte
      this.buffer[lenOffset] = written;
      this.offset += written;
    } else {
      // Rare case: need multi-byte varint for length
      // Calculate varint size and shift string bytes
      let len = written, varintSize = 1;
      while (len >= 0x80) { varintSize++; len >>>= 7; }

      // Shift string bytes to make room for longer varint
      this.buffer.copyWithin(
        lenOffset + varintSize,
        lenOffset + 1,
        this.offset + written
      );

      // Write varint at lenOffset
      len = written;
      let pos = lenOffset;
      while (len >= 0x80) {
        this.buffer[pos++] = (len & 0x7f) | 0x80;
        len >>>= 7;
      }
      this.buffer[pos] = len;
      this.offset = lenOffset + varintSize + written;
    }
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
    this.view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
    this.options = options;
  }

  readVarint(): number {
    let result = 0, shift = 0;
    while (true) {
      if (this.offset >= this.buffer.length) {
        throw new Error(`Unexpected end of buffer reading varint at offset ${this.offset}`);
      }
      const byte = this.buffer[this.offset++];
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) break;
      shift += 7;
    }
    return result;
  }

  readString(): string {
    const len = this.readVarint();
    if (this.offset + len > this.buffer.length) {
      throw new Error(`Unexpected end of buffer reading string of length ${len} at offset ${this.offset}`);
    }
    const str = TEXT_DECODER.decode(this.buffer.subarray(this.offset, this.offset + len));
    this.offset += len;
    return str;
  }

  // Zero-copy if aligned, copy otherwise
  readTypedArray<T extends TypedArray>(Ctor: TypedArrayConstructor<T>, count: number): T {
    const elementSize = Ctor.BYTES_PER_ELEMENT;
    const byteLength = count * elementSize;
    if (this.offset + byteLength > this.buffer.length) {
      throw new Error(`Unexpected end of buffer reading ${count} elements of size ${elementSize} at offset ${this.offset}`);
    }
    const currentOffset = this.buffer.byteOffset + this.offset;

    let res: T;
    if (currentOffset % elementSize === 0) {
      res = new Ctor(this.buffer.buffer as ArrayBuffer, currentOffset, count);
    } else {
      const copy = new Uint8Array(this.buffer.subarray(this.offset, this.offset + byteLength));
      res = new Ctor(copy.buffer as ArrayBuffer, 0, count);
    }
    this.offset += byteLength;
    return res;
  }

  readBytes(length: number): Uint8Array {
    if (this.offset + length > this.buffer.length) {
      throw new Error(`Unexpected end of buffer reading ${length} bytes at offset ${this.offset}`);
    }
    const res = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return res;
  }
}
