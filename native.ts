/**
 * Native format encoder/decoder for ClickHouse.
 *
 * Native is ClickHouse's columnar format - more efficient than RowBinary
 * because data doesn't need row-to-column conversion on the server.
 *
 * Note: Only Dynamic/JSON V3 format is supported at present. For ClickHouse 25.6+, enable
 * `output_format_native_use_flattened_dynamic_and_json_serialization` setting.
 *
 * A note on client_version and impact on Native format:
  The HTTP interface sets client_protocol_version to 0 by default if not explicitly specified.

  Sparse encoding in Native format is enabled when client_revision is:
  - client_revision >= DBMS_MIN_REVISION_WITH_SPARSE_SERIALIZATION (54465)

  An HTTP client using the default client_protocol_version=0 will not receive sparse encoded
  columns—they'll always be decompressed in the native format response.

  To receive sparse encoded data via HTTP, the client must explicitly set:
  ?client_protocol_version=54465 or higher.

  General notes on data layout:
    - Columnar format: data is stored column-by-column, not row-by-row.
    - Each column has its own data block. First comes the column metadata (type, name, etc), then the data for that column.
      - types are presented as type strings as you would write in DDL or when casting. The interpretation of the data depends on the type string. Some types store their underlying data in the same way, IE: dates and datetimes are both stored as Int32, so parsing the type string is necessary to know how to properly interpret the data.
      - nested types (Array, Tuple, etc.) store their data within the parent column's data block. In flattened Dynamic/JSON, each variant type's data is stored contiguously within the Dynamic's data section.

I need to make sure I have a solid understand of the data layout for each type. Needs to be clearly documented above each codec class.
 */

import {
  type ColumnDef,
  type DecodeResult,
  type DecodeOptions,
  TEXT_ENCODER,
  TEXT_DECODER,
  ClickHouseDateTime64,
  parseTypeList,
  parseTupleElements,
  ipv6ToBytes,
  bytesToIpv6,
} from "./native_utils.ts";

import { createCodec as createRowBinaryCodec, RowBinaryEncoder } from "./rowbinary.ts";

export { type ColumnDef, type DecodeResult, type DecodeOptions, ClickHouseDateTime64 };

export interface ColumnarResult {
  columns: ColumnDef[];
  columnData: (unknown[] | TypedArray)[];  // columnData[colIndex][rowIndex]
  rowCount: number;
}

export type StreamDecodeNativeResult = ColumnarResult;

const MS_PER_DAY = 86400000;
const MS_PER_SECOND = 1000;

class BufferWriter {
  private buffer: Uint8Array;
  private offset = 0;

  // Initial size 1MB, will grow as needed by doubling in size
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

  /**
   * Write a variable length integer using LEB128 encoding.
   * Used for length-prefixed strings and other varint fields.
   * variable length integer (LEB128) - write 7 bits per byte, MSB indicates more bytes follow
   */
  writeVarint(value: number) {
    this.ensure(10); // Max varint size
    while (value >= 0x80) {
      this.buffer[this.offset++] = (value & 0x7f) | 0x80;
      value >>>= 7;
    }
    this.buffer[this.offset++] = value;
  }

  /**
   * Write a length-prefixed UTF-8 string (ClickHouse String type).
   * Format: [varint length] [UTF-8 bytes]
   *
   * Optimized for the common case where encoded length < 128 bytes (single-byte varint).
   * Strategy: speculatively reserve 1 byte for length, encode string, then fix up.
   * For strings >= 128 bytes, shifts data with copyWithin to make room for multi-byte varint.
   * Benchmarked faster than alternatives (encode-first, temp buffer) across string sizes.
   */
  writeString(val: string) {
    const maxLen = val.length * 3; // worst case: 3 bytes per char (UTF-8)
    this.ensure(maxLen + 5);       // + 5 for max varint size

    // Speculatively reserve 1 byte for length, encode directly after it
    const lenOffset = this.offset++;
    const { written } = TEXT_ENCODER.encodeInto(
      val,
      this.buffer.subarray(this.offset, this.offset + maxLen)
    );

    if (written < 128) {
      // Fast path: length fits in 1 byte
      this.buffer[lenOffset] = written;
      this.offset += written;
    } else {
      // Slow path: need multi-byte varint, shift string data to make room
      let len = written, varintSize = 1;
      while (len >= 0x80) { varintSize++; len >>>= 7; }

      this.buffer.copyWithin(
        lenOffset + varintSize,  // dest: after full varint
        lenOffset + 1,           // src: after our 1-byte reservation
        this.offset + written    // end: end of encoded string
      );

      // Write the multi-byte varint
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

class BufferReader {
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

  // variable-length integer (LEB128) - read until MSB is 0
  readVarint(): number {
    let result = 0, shift = 0;
    while (true) {
      const byte = this.buffer[this.offset++];
      result |= (byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) break;
      shift += 7;
    }
    return result;
  }

  readString(): string {
    const len = this.readVarint();
    const str = TEXT_DECODER.decode(this.buffer.subarray(this.offset, this.offset + len));
    this.offset += len;
    return str;
  }

  // Zero-copy if aligned, copy otherwise TODO: what do you mean by "aligned"
  readTypedArray<T extends TypedArray>(Ctor: TypedArrayConstructor<T>, count: number): T {
    const elementSize = Ctor.BYTES_PER_ELEMENT;
    const byteLength = count * elementSize;
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
    const res = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return res;
  }
}

interface Codec {
  encode(values: unknown[] | TypedArray): Uint8Array;
  decode(reader: BufferReader, rows: number): unknown[] | TypedArray;
  // nested types need to handle prefix writing/reading - IE: metadata about the column data block that follows
  // which will impact/influence how the column data is encoded/decoded.
  writePrefix?(writer: BufferWriter, values: unknown[] | TypedArray): void;
  readPrefix?(reader: BufferReader): void;
}

type TypedArray = Int8Array | Uint8Array | Int16Array | Uint16Array | Int32Array | Uint32Array | BigInt64Array | BigUint64Array | Float32Array | Float64Array;
type TypedArrayConstructor<T extends TypedArray> = {
  new(length: number): T;
  new(buffer: ArrayBuffer, byteOffset?: number, length?: number): T;
  BYTES_PER_ELEMENT: number;
};

class NumericCodec<T extends TypedArray> implements Codec {
  private Ctor: TypedArrayConstructor<T>;
  private converter?: (v: unknown) => number | bigint;
  constructor(Ctor: TypedArrayConstructor<T>, converter?: (v: unknown) => number | bigint) {
    this.Ctor = Ctor;
    this.converter = converter;
  }

  encode(values: unknown[]): Uint8Array {
    // Fast path: input is already correct TypedArray
    if (values instanceof this.Ctor) {
      return new Uint8Array(values.buffer, values.byteOffset, values.byteLength);
    }

    const arr = new this.Ctor(values.length);

    if (this.converter) {
      for (let i = 0; i < values.length; i++) arr[i] = this.converter(values[i]) as any;
      return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
    }

    // TypedArray assignment normalizes NaN to canonical form
    for (let i = 0; i < values.length; i++) arr[i] = values[i] as any;
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): T {
    return reader.readTypedArray(this.Ctor, rows);
  }
}

class StringCodec implements Codec {
  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    // thought/question: lets say values is already an array of UInt8Array, we could
    // optimize for that case maybe?
    for (const v of values) writer.writeString(String(v));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const res = new Array(rows).fill('');
    for (let i = 0; i < rows; i++) res[i] = reader.readString();
    return res;
  }
}

class UUIDCodec implements Codec {
  encode(values: unknown[]): Uint8Array {
    const buf = new Uint8Array(values.length * 16);

    for (let i = 0; i < values.length; i++) {
      const u = values[i] as string;
      const clean = u.replace(/-/g, '');
      const bytes = new Uint8Array(16);
      for (let j = 0; j < 16; j++) bytes[j] = parseInt(clean.substring(j * 2, j * 2 + 2), 16);

      // CH stores as: [low_64_reversed] [high_64_reversed]
      const off = i * 16;
      for (let j = 0; j < 8; j++) buf[off + j] = bytes[7 - j];
      for (let j = 0; j < 8; j++) buf[off + 8 + j] = bytes[15 - j];
    }
    return buf;
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const res = new Array(rows).fill('00000000-0000-0000-0000-000000000000'); // zero value of UUID
    for (let i = 0; i < rows; i++) {
      const b = reader.buffer.subarray(reader.offset, reader.offset + 16);
      reader.offset += 16;

      const bytes = new Uint8Array(16);
      for (let j = 0; j < 8; j++) bytes[7 - j] = b[j];
      for (let j = 0; j < 8; j++) bytes[15 - j] = b[8 + j];

      const hex = Array.from(bytes).map(x => x.toString(16).padStart(2, '0')).join('');
      res[i] = `${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}`;
    }
    return res;
  }
}

class FixedStringCodec implements Codec {
  len: number;
  constructor(len: number) {
    this.len = len;
  }

  encode(values: unknown[]): Uint8Array {
    const buf = new Uint8Array(values.length * this.len);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v instanceof Uint8Array) buf.set(v.subarray(0, this.len), i * this.len);
      else {
        const bytes = TEXT_ENCODER.encode(String(v));
        buf.set(bytes.subarray(0, this.len), i * this.len);
      }
    }
    return buf;
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const res = new Array(rows).fill(null);
    for (let i = 0; i < rows; i++) {
      res[i] = reader.buffer.slice(reader.offset, reader.offset + this.len);
      reader.offset += this.len;
    }
    return res;
  }
}

class ScalarCodec implements Codec {
  private codec: ReturnType<typeof createRowBinaryCodec>;

  constructor(type: string) {
    this.codec = createRowBinaryCodec(type);
  }

  encode(values: unknown[]): Uint8Array {
    const encoder = new RowBinaryEncoder();
    for (const v of values) {
      this.codec.encode(encoder, v);
    }
    return encoder.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const res = new Array(rows).fill(null);
    const view = reader.view;
    const data = reader.buffer;
    const cursor = { offset: reader.offset };
    for (let i = 0; i < rows; i++) {
      res[i] = this.codec.decode(view, data, cursor);
    }
    reader.offset = cursor.offset;
    return res;
  }
}

class DateTime64Codec implements Codec {
  private precision: number;
  constructor(precision: number) {
    this.precision = precision;
  }

  encode(values: unknown[]): Uint8Array {
    const arr = new BigInt64Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v instanceof ClickHouseDateTime64) {
        arr[i] = v.ticks;
      } else if (typeof v === 'bigint') {
        arr[i] = v;
      } else if (v instanceof Date) {
        const ms = BigInt(v.getTime());
        const scale = 10n ** BigInt(Math.abs(this.precision - 3));
        arr[i] = this.precision >= 3 ? ms * scale : ms / scale;
      } else {
        arr[i] = BigInt(v as number);
      }
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const arr = reader.readTypedArray(BigInt64Array, rows);
    const res = new Array(rows).fill(null);
    for (let i = 0; i < rows; i++) {
      res[i] = new ClickHouseDateTime64(arr[i], this.precision);
    }
    return res;
  }
}

// handles Date, Date32, DateTime (ms since epoch / multiplier)
class EpochCodec<T extends Uint16Array | Int32Array | Uint32Array> implements Codec {
  private Ctor: TypedArrayConstructor<T>;
  private multiplier: number;

  constructor(Ctor: TypedArrayConstructor<T>, multiplier: number) {
    this.Ctor = Ctor;
    this.multiplier = multiplier;
  }

  encode(values: unknown[]): Uint8Array {
    const arr = new this.Ctor(values.length);
    for (let i = 0; i < values.length; i++) {
      // new Date() over every value even if that value is already a date?
      arr[i] = Math.floor(new Date(values[i] as any).getTime() / this.multiplier) as any;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const arr = reader.readTypedArray(this.Ctor, rows);
    const res = new Array(rows).fill(null);
    for (let i = 0; i < rows; i++) {
      res[i] = new Date((arr[i] as number) * this.multiplier);
    }
    return res;
  }
}

class IPv4Codec implements Codec {
  encode(values: unknown[]): Uint8Array {
    const arr = new Uint32Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (typeof v === 'string') {
        const parts = v.split('.').map(Number);
        arr[i] = (parts[0] | (parts[1] << 8) | (parts[2] << 16) | (parts[3] << 24)) >>> 0;
      } else {
        arr[i] = v as number;
      }
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): string[] {
    const arr = reader.readTypedArray(Uint32Array, rows);
    const res = new Array(rows).fill('');
    for (let i = 0; i < rows; i++) {
      const v = arr[i];
      res[i] = `${v & 0xFF}.${(v >> 8) & 0xFF}.${(v >> 16) & 0xFF}.${(v >> 24) & 0xFF}`;
    }
    return res;
  }
}

class IPv6Codec implements Codec {
  encode(values: unknown[]): Uint8Array {
    const result = new Uint8Array(values.length * 16);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (typeof v === 'string') {
        const bytes = ipv6ToBytes(v);
        result.set(bytes, i * 16);
      } else if (v instanceof Uint8Array) {
        result.set(v.subarray(0, 16), i * 16);
      }
    }
    return result;
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const res = new Array(rows).fill('::'); // zero value of IPv6
    for (let i = 0; i < rows; i++) {
      const bytes = reader.readBytes(16);
      res[i] = bytesToIpv6(bytes);
    }
    return res;
  }
}

// When used as a column in Map/Tuple, inner codec's prefix needs to be handled
class ArrayCodec implements Codec {
  private inner: Codec;
  private innerCtor: TypedArrayConstructor<TypedArray> | null = null;

  constructor(inner: Codec) {
    this.inner = inner;
    // Fast path for numeric types without converters (direct TypedArray assignment)
    // Excludes: UInt64/Int64 (converter), Bool (converter)
    if (inner instanceof NumericCodec) {
      const ctor = (inner as any).Ctor;
      const hasConverter = !!(inner as any).converter;
      if (!hasConverter) {
        this.innerCtor = ctor;
      }
    }
  }

  writePrefix(writer: BufferWriter, values: unknown[][]) {
    // Flatten to get inner values for prefix
    let totalCount = 0;
    for (const arr of values) totalCount += (arr as unknown[]).length;
    const flat = new Array(totalCount).fill(null);
    let idx = 0;
    for (const arr of values) {
      for (const item of arr as unknown[]) flat[idx++] = item;
    }
    this.inner.writePrefix?.(writer, flat);
  }

  readPrefix(reader: BufferReader) {
    this.inner.readPrefix?.(reader);
  }

  encode(values: unknown[][]): Uint8Array {
    const writer = new BufferWriter();
    const offsets = new BigUint64Array(values.length);

    // First pass: count total elements and compute offsets
    let totalCount = 0;
    for (let i = 0; i < values.length; i++) {
      totalCount += (values[i] as unknown[]).length;
      offsets[i] = BigInt(totalCount);
    }

    writer.write(new Uint8Array(offsets.buffer));

    // Fast path: flatten directly into TypedArray for numeric inner types
    if (this.innerCtor) {
      const flat = new this.innerCtor(totalCount);
      let idx = 0;
      for (let i = 0; i < values.length; i++) {
        const arr = values[i];
        if (arr instanceof this.innerCtor) {
          // Bulk copy TypedArray
          flat.set(arr as any, idx);
          idx += arr.length;
        } else {
          // Element-by-element for regular arrays
          for (let j = 0; j < (arr as unknown[]).length; j++) {
            flat[idx++] = (arr as unknown[])[j] as any;
          }
        }
      }
      writer.write(new Uint8Array(flat.buffer, flat.byteOffset, flat.byteLength));
    } else {
      // Slow path: flatten to regular array, delegate to inner codec
      const flat = new Array(totalCount).fill(null);
      let idx = 0;
      for (let i = 0; i < values.length; i++) {
        const arr = values[i] as unknown[];
        for (let j = 0; j < arr.length; j++) {
          flat[idx++] = arr[j];
        }
      }
      writer.write(this.inner.encode(flat));
    }

    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const totalCount = rows > 0 ? Number(offsets[rows - 1]) : 0;
    const flat = this.inner.decode(reader, totalCount);

    const res = new Array(rows).fill(null); // PACKED, avoid shared ref for arrays
    let start = 0;
    for (let i = 0; i < rows; i++) {
      const end = Number(offsets[i]);
      res[i] = flat.slice(start, end);
      start = end;
    }
    return res;
  }
}

// Delegates prefix handling to inner codec
class NullableCodec implements Codec {
  private inner: Codec;

  constructor(inner: Codec) {
    this.inner = inner;
  }

  writePrefix(writer: BufferWriter, values: unknown[]) {
    // Extract non-null values for inner prefix
    const nonNull = values.filter(v => v !== null);
    this.inner.writePrefix?.(writer, nonNull.length > 0 ? nonNull : values);
  }

  readPrefix(reader: BufferReader) {
    this.inner.readPrefix?.(reader);
  }

  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    const flags = new Uint8Array(values.length);
    const cleanValues = new Array(values.length).fill(null);

    for (let i = 0; i < values.length; i++) {
      if (values[i] === null) {
        flags[i] = 1;
        cleanValues[i] = getZeroValue(this.inner);
      } else {
        cleanValues[i] = values[i];
      }
    }

    writer.write(flags);
    writer.write(this.inner.encode(cleanValues));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const rowIsNullFlags = reader.readTypedArray(Uint8Array, rows);
    const values = this.inner.decode(reader, rows);
    // Must convert to regular array since TypedArray can't store null
    const result = new Array(rows).fill(null);
    for (let i = 0; i < rows; i++) {
      result[i] = rowIsNullFlags[i] === 1 ? null : values[i];
    }
    return result;
  }
}

// LowCardinality stores a dictionary of unique values and indices into that dictionary.
// When wrapping Nullable(T), the dictionary stores T values (not Nullable(T)) and index 0
// is reserved for NULL. This avoids storing null flags per dictionary entry - nullness is
// encoded in the index itself.
class LowCardinalityCodec implements Codec {
  private inner: Codec;
  private dictCodec: Codec; // Codec to use for dictionary (may differ from inner for Nullable)

  constructor(inner: Codec) {
    this.inner = inner;
    // For Nullable inner types, dictionary stores unwrapped type (nulls use index 0)
    this.dictCodec = inner instanceof NullableCodec ? (inner as any).inner : inner;
  }

  writePrefix(writer: BufferWriter) {
    writer.write(new Uint8Array(new BigUint64Array([1n]).buffer));
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8;
  }

  encode(values: unknown[]): Uint8Array {
    // Empty values require no output (matches ClickHouse behavior)
    if (values.length === 0) return new Uint8Array(0);

    const writer = new BufferWriter();
    const isNullable = this.inner instanceof NullableCodec;

    const dict = new Map<unknown, number>();
    const dictValues: unknown[] = [];
    const indices: number[] = [];

    // For Nullable types, index 0 is reserved for null
    if (isNullable) {
      dict.set(null, 0);
      dictValues.push(getZeroValue(this.dictCodec)); // Placeholder for null
    }

    for (const v of values) {
      if (isNullable && v === null) {
        indices.push(0); // Null uses index 0
      } else {
        const k = getDictKey(v);
        if (!dict.has(k)) {
          dict.set(k, dictValues.length);
          dictValues.push(v);
        }
        indices.push(dict.get(k)!);
      }
    }

    // Flags (UInt64):
    // - Bits 0-1: indexType - size of index integers (0=UInt8, 1=UInt16, 2=UInt32, 3=UInt64)
    // - Bit 9: HAS_ADDITIONAL_KEYS_BIT - if set this means dictionary is inline with data (vs shared global dict)
    //   global shared dictionaries are not used for the Native wire format, so this bit is always set - meaning
    //   the dictionary is included inline with the data.
    const HAS_ADDITIONAL_KEYS_BIT = 1n << 9n; // is there no cleaner way to declare a binary literal in JS/TS?
    let indexType = 0n;
    let IndexArray: any = Uint8Array;
    if (dictValues.length > 255) { indexType = 1n; IndexArray = Uint16Array; }
    if (dictValues.length > 65535) { indexType = 2n; IndexArray = Uint32Array; }

    writer.write(new Uint8Array(new BigUint64Array([HAS_ADDITIONAL_KEYS_BIT | indexType]).buffer));

    writer.write(new Uint8Array(new BigUint64Array([BigInt(dictValues.length)]).buffer));
    writer.write(this.dictCodec.encode(dictValues));
    writer.write(new Uint8Array(new BigUint64Array([BigInt(values.length)]).buffer));
    writer.write(new Uint8Array(new IndexArray(indices).buffer));

    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    if (rows === 0) return [];

    const flags = reader.view.getBigUint64(reader.offset, true);
    reader.offset += 8;

    const typeInfo = Number(flags & 0xFFn);

    // Nullable is determined by inner TYPE, not by flags
    const isNullable = this.inner instanceof NullableCodec;

    const dictSize = Number(reader.view.getBigUint64(reader.offset, true));
    reader.offset += 8;

    // Dictionary always uses dictCodec (unwrapped type for Nullable)
    const dict = this.dictCodec.decode(reader, dictSize);

    const count = Number(reader.view.getBigUint64(reader.offset, true));
    reader.offset += 8;

    let indices: TypedArray;
    if (typeInfo === 0) indices = reader.readTypedArray(Uint8Array, count);
    else if (typeInfo === 1) indices = reader.readTypedArray(Uint16Array, count);
    else if (typeInfo === 2) indices = reader.readTypedArray(Uint32Array, count);
    else indices = reader.readTypedArray(BigUint64Array, count);

    const res = new Array(count).fill(null);
    for (let i = 0; i < count; i++) {
      const idx = Number(indices[i]);
      // Index 0 is null ONLY when inner type is Nullable
      res[i] = isNullable && idx === 0 ? null : dict[idx];
    }
    return res;
  }
}

// Map is serialized as Array(Tuple(K, V))
// Prefixes are written at top level, not inside the data.
// Uses Array<[K, V]> representation to preserve duplicate keys.
class MapCodec implements Codec {
  private keyCodec: Codec;
  private valCodec: Codec;

  constructor(keyCodec: Codec, valCodec: Codec) {
    this.keyCodec = keyCodec;
    this.valCodec = valCodec;
  }

  // Convert Map, Array<[K,V]>, or object to array of [key, value] tuples
  private static toEntries(value: unknown): [unknown, unknown][] {
    if (value instanceof Map) return [...value.entries()];
    if (Array.isArray(value)) return value as [unknown, unknown][];
    return Object.entries(value as Record<string, unknown>);
  }

  // Map prefix = key prefix + value prefix (like Tuple(K, V))
  writePrefix(writer: BufferWriter, values: unknown[]) {
    // Flatten all map entries to get key/value arrays for prefix
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    for (const m of values) {
      for (const [k, v] of MapCodec.toEntries(m)) {
        keys.push(k);
        vals.push(v);
      }
    }
    this.keyCodec.writePrefix?.(writer, keys);
    this.valCodec.writePrefix?.(writer, vals);
  }

  readPrefix(reader: BufferReader) {
    this.keyCodec.readPrefix?.(reader);
    this.valCodec.readPrefix?.(reader);
  }

  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    const offsets = new BigUint64Array(values.length);
    let offset = 0n;

    for (let i = 0; i < values.length; i++) {
      const entries = MapCodec.toEntries(values[i]);
      offset += BigInt(entries.length);
      offsets[i] = offset;
      for (const [k, v] of entries) {
        keys.push(k);
        vals.push(v);
      }
    }

    // Structure: [offsets] [keys] [values]
    // Prefixes were already written via writePrefix at top level
    writer.write(new Uint8Array(offsets.buffer));
    writer.write(this.keyCodec.encode(keys));
    writer.write(this.valCodec.encode(vals));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const total = rows > 0 ? Number(offsets[rows - 1]) : 0;

    // Prefixes were already read via readPrefix at top level
    const keys = this.keyCodec.decode(reader, total);
    const vals = this.valCodec.decode(reader, total);

    const res = new Array(rows).fill(null); // PACKED, avoid shared ref for Map/Array
    let start = 0;
    for (let i = 0; i < rows; i++) {
      const end = Number(offsets[i]);
      if (reader.options?.mapAsArray) {
        const entries: [unknown, unknown][] = [];
        for (let j = start; j < end; j++) {
          entries.push([keys[j], vals[j]]);
        }
        res[i] = entries;
      } else {
        const map = new Map();
        for (let j = start; j < end; j++) {
          map.set(keys[j], vals[j]);
        }
        res[i] = map;
      }
      start = end;
    }
    return res;
  }
}

// Tuple stores each element as a column, so they need their own prefixes
class TupleCodec implements Codec {
  private elements: { name: string | null, codec: Codec }[];
  private isNamed: boolean;

  constructor(elements: { name: string | null, codec: Codec }[], isNamed: boolean) {
    this.elements = elements;
    this.isNamed = isNamed;
  }

  writePrefix(writer: BufferWriter, values: unknown[]) {
    const n = values.length;
    for (let i = 0; i < this.elements.length; i++) {
      const colValues = new Array(n).fill(null);
      const name = this.elements[i].name;
      if (this.isNamed) {
        for (let j = 0; j < n; j++) colValues[j] = (values[j] as any)[name!];
      } else {
        for (let j = 0; j < n; j++) colValues[j] = (values[j] as any)[i];
      }
      this.elements[i].codec.writePrefix?.(writer, colValues);
    }
  }

  readPrefix(reader: BufferReader) {
    for (const e of this.elements) {
      e.codec.readPrefix?.(reader);
    }
  }

  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    const n = values.length;
    for (let i = 0; i < this.elements.length; i++) {
      const colValues = new Array(n).fill(null);
      const name = this.elements[i].name;
      if (this.isNamed) {
        for (let j = 0; j < n; j++) colValues[j] = (values[j] as any)[name!];
      } else {
        for (let j = 0; j < n; j++) colValues[j] = (values[j] as any)[i];
      }
      writer.write(this.elements[i].codec.encode(colValues));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const cols = this.elements.map(e => e.codec.decode(reader, rows));
    const res = new Array(rows).fill(null);
    for (let i = 0; i < rows; i++) {
      if (this.isNamed) {
        const obj: any = {};
        for (let j = 0; j < this.elements.length; j++) obj[this.elements[j].name!] = cols[j][i];
        res[i] = obj;
      } else {
        res[i] = cols.map(c => c[i]);
      }
    }
    return res;
  }
}

// Variant Codec
// Note: COMPACT mode (mode=1) exists for storage optimization but is not sent to HTTP clients.
// ClickHouse always sends BASIC mode (mode=0) over HTTP. COMPACT mode is only used internally
// for MergeTree storage. See: https://github.com/ClickHouse/ClickHouse/pull/62774
class VariantCodec implements Codec {
  private types: Codec[];
  constructor(types: Codec[]) {
    this.types = types;
  }

  writePrefix(writer: BufferWriter) {
    // UInt64 LE mode flag: 0=BASIC (row-by-row), 1=COMPACT (granule-based, storage only)
    const BASIC_MODE = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]);
    writer.write(BASIC_MODE);
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8; // Skip encoding mode flag - always BASIC (0) for HTTP clients
  }

  // Variant encoding:
  // 1. Write discriminators array (UInt8 per row): type index 0..N-1, or 0xFF for null
  // 2. Group values by discriminator
  // 3. Write each type's data block containing only rows of that type
  // Input values are [discriminator, value] tuples, or null.
  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    const discriminators: number[] = [];
    const groups = new Map<number, unknown[]>();

    for (const v of values) {
      if (v === null) { discriminators.push(0xFF); continue; }
      const [disc, val] = v as [number, unknown];
      discriminators.push(disc);
      if (!groups.has(disc)) groups.set(disc, []);
      groups.get(disc)!.push(val);
    }

    writer.write(new Uint8Array(discriminators));
    for (let i = 0; i < this.types.length; i++) {
      if (groups.has(i)) writer.write(this.types[i].encode(groups.get(i)!));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const discriminators = reader.readTypedArray(Uint8Array, rows);
    const groups = new Map<number, number>();
    for (const d of discriminators) if (d !== 0xFF) groups.set(d, (groups.get(d) || 0) + 1);

    const decoded = new Map<number, unknown[] | TypedArray>();
    for (let i = 0; i < this.types.length; i++) {
      if (groups.has(i)) decoded.set(i, this.types[i].decode(reader, groups.get(i)!));
    }

    const res = new Array(rows).fill(null);
    const counters = new Map<number, number>();
    for (let i = 0; i < rows; i++) {
      const d = discriminators[i];
      if (d === 0xFF) res[i] = null;
      else {
        const idx = counters.get(d) || 0;
        res[i] = [d, decoded.get(d)![idx]];
        counters.set(d, idx + 1);
      }
    }
    return res;
  }
}

// implements V3 FLATTENED format only
class DynamicCodec implements Codec {
  private types: string[] = [];
  private codecs: Codec[] = [];

  writePrefix(writer: BufferWriter, values: unknown[]) {
    const typeSet = new Set<string>();
    for (const v of values) if (v !== null) typeSet.add(this.guessType(v));
    this.types = [...typeSet].sort();
    this.codecs = this.types.map(t => getCodec(t));

    writer.write(new Uint8Array(new BigUint64Array([3n]).buffer));
    writer.writeVarint(this.types.length);
    for (const t of this.types) writer.writeString(t);

    for (let i = 0; i < this.types.length; i++) {
      const typeName = this.types[i];
      const colValues = values.filter(v => v !== null && this.guessType(v) === typeName);
      this.codecs[i].writePrefix?.(writer, colValues);
    }
  }

  readPrefix(reader: BufferReader) {
    const version = reader.view.getBigUint64(reader.offset, true);
    reader.offset += 8;
    if (version !== 3n) throw new Error(`Dynamic: only V3 supported, got V${version}`);

    const count = reader.readVarint();
    this.types = [];
    for (let i = 0; i < count; i++) this.types.push(reader.readString());
    this.codecs = this.types.map(t => getCodec(t));

    for (const c of this.codecs) c.readPrefix?.(reader);
  }

  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    const typeMap = new Map(this.types.map((t, i) => [t, i]));
    const nullDisc = this.types.length;
    const discriminators: number[] = [];
    const groups = new Map<number, unknown[]>();

    for (const v of values) {
      if (v === null) { discriminators.push(nullDisc); continue; }
      const type = this.guessType(v);
      const idx = typeMap.get(type)!;
      discriminators.push(idx);
      if (!groups.has(idx)) groups.set(idx, []);
      groups.get(idx)!.push(v);
    }

    const discLimit = this.types.length + 1;
    if (discLimit <= 256) writer.write(new Uint8Array(discriminators));
    else if (discLimit <= 65536) writer.write(new Uint8Array(new Uint16Array(discriminators).buffer));
    else writer.write(new Uint8Array(new Uint32Array(discriminators).buffer));

    for (let i = 0; i < this.types.length; i++) {
      if (groups.has(i)) writer.write(this.codecs[i].encode(groups.get(i)!));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const nullDisc = this.types.length;
    const discLimit = nullDisc + 1;

    let discriminators: TypedArray;
    if (discLimit <= 256) discriminators = reader.readTypedArray(Uint8Array, rows);
    else if (discLimit <= 65536) discriminators = reader.readTypedArray(Uint16Array, rows);
    else discriminators = reader.readTypedArray(Uint32Array, rows);

    const groups = new Map<number, number>();
    for (const d of discriminators) if (d !== nullDisc) groups.set(d, (groups.get(d) || 0) + 1);

    const decoded = new Map<number, unknown[] | TypedArray>();
    for (let i = 0; i < this.types.length; i++) {
      if (groups.has(i)) decoded.set(i, this.codecs[i].decode(reader, groups.get(i)!));
    }

    const res = new Array(rows).fill(null);
    const counters = new Map<number, number>();
    for (let i = 0; i < rows; i++) {
      const d = discriminators[i];
      if (d === nullDisc) res[i] = null;
      else {
        const idx = counters.get(d) || 0;
        res[i] = decoded.get(d)![idx];
        counters.set(d, idx + 1);
      }
    }
    return res;
  }

  guessType(value: unknown): string {
    if (value === null) return "String";
    if (typeof value === "string") return "String";
    if (typeof value === "number") return Number.isInteger(value) ? "Int64" : "Float64";
    if (typeof value === "bigint") return "Int64";
    if (typeof value === "boolean") return "Bool";
    if (value instanceof Date) return "DateTime64(3)";
    if (Array.isArray(value)) return value.length ? `Array(${this.guessType(value[0])})` : "Array(String)";
    if (typeof value === "object") return "Map(String,String)";
    return "String";
  }
}

// 10. JSON Codec (V3 FLATTENED only)
class JsonCodec implements Codec {
  private paths: string[] = [];
  private pathCodecs: Map<string, DynamicCodec> = new Map();

  writePrefix(writer: BufferWriter, values: unknown[]) {
    const pathSet = new Set<string>();
    for (const v of values) {
      if (v && typeof v === 'object') Object.keys(v).forEach(k => pathSet.add(k));
    }
    this.paths = [...pathSet].sort();

    writer.write(new Uint8Array(new BigUint64Array([3n]).buffer));
    writer.writeVarint(this.paths.length);
    for (const p of this.paths) writer.writeString(p);

    for (const path of this.paths) {
      const codec = new DynamicCodec();
      const colValues = values.map(v => v ? (v as any)[path] ?? null : null);
      codec.writePrefix(writer, colValues);
      this.pathCodecs.set(path, codec);
    }
  }

  readPrefix(reader: BufferReader) {
    const ver = reader.view.getBigUint64(reader.offset, true);
    reader.offset += 8;
    if (ver !== 3n) throw new Error(`JSON: only V3 supported, got V${ver}`);

    const count = reader.readVarint();
    this.paths = [];
    for (let i = 0; i < count; i++) this.paths.push(reader.readString());

    for (const path of this.paths) {
      const codec = new DynamicCodec();
      codec.readPrefix(reader);
      this.pathCodecs.set(path, codec);
    }
  }

  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    for (const path of this.paths) {
      const colValues = values.map(v => v ? (v as any)[path] ?? null : null);
      writer.write(this.pathCodecs.get(path)!.encode(colValues));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const pathCols = new Map<string, unknown[]>();
    for (const path of this.paths) {
      pathCols.set(path, this.pathCodecs.get(path)!.decode(reader, rows));
    }

    const res = new Array(rows).fill(null);
    for (let i = 0; i < rows; i++) {
      const obj: any = {};
      for (const path of this.paths) {
        const val = pathCols.get(path)![i];
        if (val !== null) obj[path] = val;
      }
      res[i] = obj;
    }
    return res;
  }
}

const CODEC_CACHE = new Map<string, Codec>();

function getCodec(type: string): Codec {
  if (CODEC_CACHE.has(type)) return CODEC_CACHE.get(type)!;
  const codec = createCodec(type);
  CODEC_CACHE.set(type, codec);
  return codec;
}

function createCodec(type: string): Codec {
  if (type.startsWith("Nullable")) return new NullableCodec(getCodec(extractTypeArgs(type)));
  if (type.startsWith("Array")) return new ArrayCodec(getCodec(extractTypeArgs(type)));
  if (type.startsWith("LowCardinality")) return new LowCardinalityCodec(getCodec(extractTypeArgs(type)));
  if (type.startsWith("Map")) {
    const [k, v] = parseTypeList(extractTypeArgs(type));
    return new MapCodec(getCodec(k), getCodec(v));
  }
  if (type.startsWith("Tuple")) {
    const args = parseTupleElements(extractTypeArgs(type));
    const isNamed = args[0].name !== null;
    return new TupleCodec(args.map(a => ({ name: a.name, codec: getCodec(a.type) })), isNamed);
  }
  // Nested is syntactic sugar for Array(Tuple(...))
  // e.g., Nested(id UInt64, val String) -> Array(Tuple(UInt64, String))
  if (type.startsWith("Nested")) {
    const args = parseTupleElements(extractTypeArgs(type));
    const tupleCodec = new TupleCodec(args.map(a => ({ name: a.name, codec: getCodec(a.type) })), true);
    return new ArrayCodec(tupleCodec);
  }
  if (type.startsWith("Variant")) return new VariantCodec(parseTypeList(extractTypeArgs(type)).map(getCodec));
  if (type === "Dynamic") return new DynamicCodec();
  if (type === "JSON" || type.startsWith("JSON")) return new JsonCodec();

  if (type.startsWith("FixedString")) return new FixedStringCodec(parseInt(extractTypeArgs(type)));

  if (type.startsWith("DateTime64")) {
    const precisionMatch = type.match(/DateTime64\((\d+)/);
    const precision = precisionMatch ? parseInt(precisionMatch[1], 10) : 3;
    return new DateTime64Codec(precision);
  }

  // Geo Types
  if (type === "Point") return new TupleCodec([{ name: null, codec: new NumericCodec(Float64Array) }, { name: null, codec: new NumericCodec(Float64Array) }], false);
  if (type === "Ring") return new ArrayCodec(getCodec("Point"));
  if (type === "Polygon") return new ArrayCodec(getCodec("Ring"));
  if (type === "MultiPolygon") return new ArrayCodec(getCodec("Polygon"));

  switch (type) {
    case "UInt8": return new NumericCodec(Uint8Array);
    case "Int8": return new NumericCodec(Int8Array);
    case "UInt16": return new NumericCodec(Uint16Array);
    case "Int16": return new NumericCodec(Int16Array);
    case "UInt32": return new NumericCodec(Uint32Array);
    case "Int32": return new NumericCodec(Int32Array);
    case "UInt64": return new NumericCodec(BigUint64Array, (v: unknown) => BigInt(v as any));
    case "Int64": return new NumericCodec(BigInt64Array, (v: unknown) => BigInt(v as any));
    case "Float32": return new NumericCodec(Float32Array);
    case "Float64": return new NumericCodec(Float64Array);
    case "Bool": return new NumericCodec(Uint8Array, (v) => v ? 1 : 0);
    case "Date": return new EpochCodec(Uint16Array, MS_PER_DAY);
    case "Date32": return new EpochCodec(Int32Array, MS_PER_DAY);
    case "DateTime": return new EpochCodec(Uint32Array, MS_PER_SECOND);
    case "String": return new StringCodec();
    case "UUID": return new UUIDCodec();
    case "IPv4": return new IPv4Codec();
    case "IPv6": return new IPv6Codec();
  }

  if (type.startsWith("Enum")) return type.startsWith("Enum8") ? new NumericCodec(Int8Array) : new NumericCodec(Int16Array);

  // Fallback to RowBinary codec for unsupported types (Int128, Decimal, etc.)
  return new ScalarCodec(type);
}

// Extracts the content between the outermost parentheses: "Array(Int32)" → "Int32"
function extractTypeArgs(type: string): string {
  return type.substring(type.indexOf("(") + 1, type.lastIndexOf(")"));
}

// Returns a zero/empty value for a codec type. Used by NullableCodec and LowCardinalityCodec
// to fill placeholder slots. Covers scalar and container types; complex types (Variant, Dynamic,
// JSON) have their own null handling and wouldn't appear as inner types here.
function getZeroValue(codec: Codec): any {
  if (codec instanceof NumericCodec) return 0;
  if (codec instanceof StringCodec) return "";
  if (codec instanceof FixedStringCodec) return new Uint8Array(codec.len);
  if (codec instanceof UUIDCodec) return "00000000-0000-0000-0000-000000000000";
  if (codec instanceof EpochCodec) return new Date(0);
  if (codec instanceof DateTime64Codec) return new Date(0);
  if (codec instanceof IPv4Codec) return "0.0.0.0";
  if (codec instanceof IPv6Codec) return "0:0:0:0:0:0:0:0";
  if (codec instanceof ArrayCodec) return [];
  if (codec instanceof TupleCodec) return [];
  if (codec instanceof MapCodec) return new Map();
  return 0;
}

// Converts a value to a Map-compatible key for LowCardinality dictionary deduplication.
// Primitives work directly; objects need conversion since Map uses reference equality.
function getDictKey(v: unknown): unknown {
  if (v === null || typeof v !== 'object') return v;
  if (v instanceof Date) return v.getTime();
  if (v instanceof Uint8Array) {
    // FixedString - convert to string for deduplication
    let s = '';
    for (let i = 0; i < v.length; i++) s += String.fromCharCode(v[i]);
    return s;
  }
  return JSON.stringify(v); // fallback for unexpected object types
}

interface BlockResult {
  columns: ColumnDef[];
  columnData: (unknown[] | TypedArray)[];  // columnData[colIndex][rowIndex]
  rowCount: number;
  bytesConsumed: number;
  isEndMarker: boolean;
}

/**
 * Decode a single Native format block from a buffer.
 * Returns the decoded data and the number of bytes consumed.
 * Use this for streaming scenarios where you need to track buffer position.
 */
function decodeNativeBlock(
  data: Uint8Array,
  offset: number,
  options?: DecodeOptions,
): BlockResult {
  const reader = new BufferReader(data, offset, options);
  const startOffset = reader.offset;

  const numCols = reader.readVarint();
  const numRows = reader.readVarint();

  // Empty block signals end of data
  if (numCols === 0 && numRows === 0) {
    return {
      columns: [],
      columnData: [],
      rowCount: 0,
      bytesConsumed: reader.offset - startOffset,
      isEndMarker: true,
    };
  }

  const columns: ColumnDef[] = [];
  const columnData: (unknown[] | TypedArray)[] = [];

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < numCols; i++) {
    const name = reader.readString();
    const type = reader.readString();
    columns.push({ name, type });

    const codec = getCodec(type);
    codec.readPrefix?.(reader);
    columnData.push(codec.decode(reader, numRows));
  }

  return {
    columns,
    columnData,
    rowCount: numRows,
    bytesConsumed: reader.offset - startOffset,
    isEndMarker: false,
  };
}

/**
 * Encode row-oriented data to Native format.
 * Input: rows[rowIndex][colIndex]
 */
export function encodeNative(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const numRows = rows.length;
  const numCols = columns.length;

  const cols = new Array(numCols);
  for (let c = 0; c < numCols; c++) cols[c] = new Array(numRows);

  for (let r = 0; r < numRows; r++) {
    const row = rows[r];
    for (let c = 0; c < numCols; c++) cols[c][r] = row[c];
  }

  return encodeNativeColumnar(columns, cols, numRows);
}

/**
 * Encode columnar data to Native format (no transpose needed).
 * Input: columnData[colIndex][rowIndex]
 */
export function encodeNativeColumnar(
  columns: ColumnDef[],
  columnData: (unknown[] | TypedArray)[],
  rowCount?: number,
): Uint8Array {
  const writer = new BufferWriter();
  const numRows = rowCount ?? (columnData[0]?.length ?? 0);

  writer.writeVarint(columns.length);
  writer.writeVarint(numRows);

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < columns.length; i++) {
    const codec = getCodec(columns[i].type);
    writer.writeString(columns[i].name);
    writer.writeString(columns[i].type);
    codec.writePrefix?.(writer, columnData[i]);
    writer.write(codec.encode(columnData[i]));
  }

  return writer.finish();
}

export async function decodeNative(
  data: Uint8Array,
  options?: DecodeOptions,
): Promise<ColumnarResult> {
  let columns: ColumnDef[] = [];
  const allColumnData: unknown[][] = [];
  let totalRows = 0;

  // Wrap data in single-chunk async iterable and use streamDecodeNative
  async function* singleChunk() {
    yield data;
  }

  for await (const block of streamDecodeNative(singleChunk(), options)) {
    if (columns.length === 0) {
      columns = block.columns;
      for (let i = 0; i < columns.length; i++) {
        allColumnData.push([]);
      }
    }
    for (let i = 0; i < block.columnData.length; i++) {
      const col = block.columnData[i];
      const target = allColumnData[i];
      for (let j = 0; j < col.length; j++) {
        target.push(col[j]);
      }
    }
    totalRows += block.rowCount;
  }

  return { columns, columnData: allColumnData, rowCount: totalRows };
}

export async function* streamEncodeNative(
  columns: ColumnDef[],
  rows: Iterable<unknown[]> | AsyncIterable<unknown[]>,
  options: { blockSize?: number } = {},
): AsyncGenerator<Uint8Array> {
  const blockSize = options.blockSize ?? 65536;
  let batch: unknown[][] = [];

  for await (const row of rows as AsyncIterable<unknown[]>) {
    batch.push(row);
    if (batch.length >= blockSize) {
      yield encodeNative(columns, batch);
      batch = [];
    }
  }
  if (batch.length > 0) yield encodeNative(columns, batch);
}

/**
 * Lazily iterate rows as objects with column names as keys.
 * Allocates one object per row on demand.
 */
export function* asRows(result: ColumnarResult): Generator<Record<string, unknown>> {
  const { columns, columnData, rowCount } = result;
  const numCols = columns.length;
  for (let i = 0; i < rowCount; i++) {
    const row: Record<string, unknown> = {};
    for (let j = 0; j < numCols; j++) {
      row[columns[j].name] = columnData[j][i];
    }
    yield row;
  }
}

/**
 * Lazily iterate rows as positional arrays.
 * Allocates one array per row on demand.
 */
export function* asArrayRows(result: ColumnarResult): Generator<unknown[]> {
  const { columnData, rowCount } = result;
  const numCols = columnData.length;
  for (let i = 0; i < rowCount; i++) {
    const row = new Array(numCols).fill(null);
    for (let j = 0; j < numCols; j++) {
      row[j] = columnData[j][i];
    }
    yield row;
  }
}

export async function* streamDecodeNative(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions,
): AsyncGenerator<ColumnarResult> {
  const pendingChunks: Uint8Array[] = [];
  let columns: ColumnDef[] = [];
  let totalBytesReceived = 0;
  let blocksDecoded = 0;

  for await (const chunk of chunks) {
    pendingChunks.push(chunk);
    totalBytesReceived += chunk.length;

    // Try to decode as many complete blocks as possible
    while (pendingChunks.length > 0) {
      const buffer = flattenChunks(pendingChunks);
      try {
        const block = decodeNativeBlock(buffer, 0, options);

        if (block.isEndMarker) {
          consumeBytes(pendingChunks, block.bytesConsumed);
          // Don't break - continue processing any remaining data after end marker
          continue;
        }

        // Set columns from first block
        if (columns.length === 0) {
          columns = block.columns;
        }

        consumeBytes(pendingChunks, block.bytesConsumed);
        blocksDecoded++;
        yield { columns, columnData: block.columnData, rowCount: block.rowCount };
      } catch {
        // Not enough data for a complete block, wait for more chunks
        break;
      }
    }
  }

  // Handle any remaining data
  if (pendingChunks.length > 0) {
    const buffer = flattenChunks(pendingChunks);
    let offset = 0;
    while (offset < buffer.length) {
      try {
        const block = decodeNativeBlock(buffer, offset, options);
        if (block.isEndMarker) {
          offset += block.bytesConsumed;
          continue;  // Continue processing after end marker
        }

        if (columns.length === 0) columns = block.columns;
        offset += block.bytesConsumed;
        blocksDecoded++;
        yield { columns, columnData: block.columnData, rowCount: block.rowCount };
      } catch {
        break;
      }
    }
  }
}

function flattenChunks(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 0) return new Uint8Array(0);
  if (chunks.length === 1) return chunks[0];
  const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

function consumeBytes(chunks: Uint8Array[], bytes: number): number {
  let consumed = 0;
  while (bytes > 0 && chunks.length > 0) {
    if (chunks[0].length <= bytes) {
      bytes -= chunks[0].length;
      consumed += chunks[0].length;
      chunks.shift();
    } else {
      chunks[0] = chunks[0].subarray(bytes);
      consumed += bytes;
      bytes = 0;
    }
  }
  return consumed;
}

/**
 * Stream rows as objects from decoded Native blocks.
 *
 * @example
 * for await (const row of streamNativeRows(streamDecodeNative(query(...)))) {
 *   console.log(row.id, row.name);
 * }
 */
export async function* streamNativeRows(
  blocks: AsyncIterable<ColumnarResult>,
): AsyncGenerator<Record<string, unknown>> {
  for await (const block of blocks) {
    yield* asRows(block);
  }
}

/**
 * Stream encode columnar blocks to Native format.
 * Each yielded ColumnarResult produces one Native block (no re-batching).
 *
 * @example
 * // Round-trip: decode -> transform -> re-encode
 * insert("INSERT INTO t FORMAT Native",
 *   streamEncodeNativeColumnar(streamDecodeNative(query(...))),
 *   session, config);
 */
export async function* streamEncodeNativeColumnar(
  blocks: AsyncIterable<ColumnarResult>,
): AsyncGenerator<Uint8Array> {
  for await (const block of blocks) {
    yield encodeNativeColumnar(block.columns, block.columnData, block.rowCount);
  }
}
