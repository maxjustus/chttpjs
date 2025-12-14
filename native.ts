/**
 * Native format encoder/decoder for ClickHouse.
 *
 * Native is ClickHouse's columnar format - more efficient than RowBinary
 * because data doesn't need row-to-column conversion on the server.
 *
 * This implementation uses:
 * - Column-oriented architecture (TypedArrays) for performance
 * - Zero-copy decoding where possible
 * - Registry-based codec factory
 * - Full support for Complex Types (Dynamic V3, JSON V3, Variant, Tuple, Geo, IP, UUID)
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
  ?client_protocol_version=54465 or higher
 */

import {
  type ColumnDef,
  type DecodeResult,
  type DecodeOptions,
  TEXT_ENCODER,
  TEXT_DECODER,
  Float32NaN,
  Float64NaN,
  ClickHouseDateTime64,
  parseTypeList,
  parseTupleElements,
  ipv6ToBytes,
  bytesToIpv6,
} from "./native_utils.ts";

import { createCodec as createRowBinaryCodec, RowBinaryEncoder } from "./rowbinary.ts";

export { type ColumnDef, type DecodeResult, type DecodeOptions, ClickHouseDateTime64, Float32NaN, Float64NaN };

// Date/time constants
const MS_PER_DAY = 86400000;
const MS_PER_SECOND = 1000;

// ============================================================================
// Buffer Utilities
// ============================================================================

class BufferWriter {
  private chunks: Uint8Array[] = [];
  private totalSize = 0;

  write(chunk: Uint8Array) {
    this.chunks.push(chunk);
    this.totalSize += chunk.length;
  }

  writeVarint(value: number) {
    const arr: number[] = [];
    while (value >= 0x80) {
      arr.push((value & 0x7f) | 0x80);
      value >>>= 7;
    }
    arr.push(value);
    this.write(new Uint8Array(arr));
  }

  writeString(val: string) {
    const bytes = TEXT_ENCODER.encode(val);
    this.writeVarint(bytes.length);
    this.write(bytes);
  }

  finish(): Uint8Array {
    if (this.chunks.length === 1) return this.chunks[0];
    const result = new Uint8Array(this.totalSize);
    let offset = 0;
    for (const chunk of this.chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    return result;
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

  // Zero-copy if aligned, copy otherwise
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

// ============================================================================
// Codec System
// ============================================================================

interface Codec {
  encode(values: unknown[]): Uint8Array;
  decode(reader: BufferReader, rows: number): unknown[];
  writePrefix?(writer: BufferWriter, values: unknown[]): void;
  readPrefix?(reader: BufferReader): void;
}

type TypedArray = Int8Array | Uint8Array | Int16Array | Uint16Array | Int32Array | Uint32Array | BigInt64Array | BigUint64Array | Float32Array | Float64Array;
type TypedArrayConstructor<T extends TypedArray> = {
  new(length: number): T;
  new(buffer: ArrayBuffer, byteOffset?: number, length?: number): T;
  BYTES_PER_ELEMENT: number;
};

// 1. Numeric Codecs
class NumericCodec<T extends TypedArray> implements Codec {
  private Ctor: TypedArrayConstructor<T>;
  private converter?: (v: unknown) => number | bigint;
  constructor(Ctor: TypedArrayConstructor<T>, converter?: (v: unknown) => number | bigint) {
    this.Ctor = Ctor;
    this.converter = converter;
  }

  encode(values: unknown[]): Uint8Array {
    const arr = new this.Ctor(values.length);
    const result = new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);

    if (this.converter) {
      for (let i = 0; i < values.length; i++) arr[i] = this.converter(values[i]) as any;
    } else {
      for (let i = 0; i < values.length; i++) {
        const v = values[i];
        // Handle Float32NaN/Float64NaN wrappers - copy raw bytes to preserve NaN bit patterns
        if (v instanceof Float32NaN) {
          result.set(v.bytes, i * 4);
        } else if (v instanceof Float64NaN) {
          result.set(v.bytes, i * 8);
        } else {
          arr[i] = v as any;
        }
      }
    }
    return result;
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const arr = reader.readTypedArray(this.Ctor, rows);

    // For floats, wrap NaN values in special classes to preserve bit patterns
    const ctor = this.Ctor as unknown;
    if (ctor === Float32Array || ctor === Float64Array) {
      return this.decodeFloatWithNaN(arr as Float32Array | Float64Array, reader);
    }

    return [...arr];
  }

  private decodeFloatWithNaN(arr: Float32Array | Float64Array, reader: BufferReader): unknown[] {
    const bytesPerElement = arr.BYTES_PER_ELEMENT;
    const NaNClass = bytesPerElement === 4 ? Float32NaN : Float64NaN;
    const rows = arr.length;
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const val = arr[i];
      if (Number.isNaN(val)) {
        const offset = reader.offset - rows * bytesPerElement + i * bytesPerElement;
        res[i] = new NaNClass(reader.buffer.slice(offset, offset + bytesPerElement));
      } else {
        res[i] = val;
      }
    }
    return res;
  }
}

// 2. String Codec
class StringCodec implements Codec {
  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    for (const v of values) writer.writeString(String(v));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) res[i] = reader.readString();
    return res;
  }
}

// 2b. UUID Codec (ClickHouse specific endianness swap)
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
    const res = new Array(rows);
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

// 3. FixedString Codec
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
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) {
      res[i] = reader.buffer.slice(reader.offset, reader.offset + this.len);
      reader.offset += this.len;
    }
    return res;
  }
}

// 3b. Scalar Codec - wraps rowbinary codec for types not yet optimized
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
    const res = new Array(rows);
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

// 3c. DateTime64 Codec
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
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) {
      res[i] = new ClickHouseDateTime64(arr[i], this.precision);
    }
    return res;
  }
}

// 3d. Epoch Codec - handles Date, Date32, DateTime (ms since epoch / multiplier)
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
      arr[i] = Math.floor(new Date(values[i] as any).getTime() / this.multiplier) as any;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const arr = reader.readTypedArray(this.Ctor, rows);
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) {
      res[i] = new Date((arr[i] as number) * this.multiplier);
    }
    return res;
  }
}

// 3g. IPv4 Codec - encode/decode string IP addresses
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

  decode(reader: BufferReader, rows: number): unknown[] {
    const arr = reader.readTypedArray(Uint32Array, rows);
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const v = arr[i];
      res[i] = `${v & 0xFF}.${(v >> 8) & 0xFF}.${(v >> 16) & 0xFF}.${(v >> 24) & 0xFF}`;
    }
    return res;
  }
}

// 3h. IPv6 Codec - encode/decode string IPv6 addresses
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
    const res = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const bytes = reader.readBytes(16);
      res[i] = bytesToIpv6(bytes);
    }
    return res;
  }
}

// ipv6ToBytes, bytesToIpv6 imported from native_utils.ts

// 4. Array Codec
// When used as a column in Map/Tuple, inner codec's prefix needs to be handled
class ArrayCodec implements Codec {
  private inner: Codec;

  constructor(inner: Codec) {
    this.inner = inner;
  }

  writePrefix(writer: BufferWriter, values: unknown[][]) {
    // Flatten to get inner values for prefix
    const flat: unknown[] = [];
    for (const arr of values) {
      for (const item of arr as unknown[]) flat.push(item);
    }
    this.inner.writePrefix?.(writer, flat);
  }

  readPrefix(reader: BufferReader) {
    this.inner.readPrefix?.(reader);
  }

  encode(values: unknown[][]): Uint8Array {
    const writer = new BufferWriter();
    const flat: unknown[] = [];
    const offsets = new BigUint64Array(values.length);
    let currentOffset = 0n;

    for (let i = 0; i < values.length; i++) {
      const arr = values[i] as unknown[];
      currentOffset += BigInt(arr.length);
      offsets[i] = currentOffset;
      for (const item of arr) flat.push(item);
    }

    writer.write(new Uint8Array(offsets.buffer));
    writer.write(this.inner.encode(flat));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const totalCount = rows > 0 ? Number(offsets[rows - 1]) : 0;
    const flat = this.inner.decode(reader, totalCount);

    const res = new Array(rows);
    let start = 0;
    for (let i = 0; i < rows; i++) {
      const end = Number(offsets[i]);
      res[i] = flat.slice(start, end);
      start = end;
    }
    return res;
  }
}

// 5. Nullable Codec
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
    const cleanValues = new Array(values.length);

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
    const flags = reader.readTypedArray(Uint8Array, rows);
    const values = this.inner.decode(reader, rows);
    return values.map((v, i) => flags[i] === 1 ? null : v);
  }
}

// 5b. LowCardinality Codec
// When inner is Nullable and has_null_in_dict flag is set, dictionary stores unwrapped type
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

    const dict = new Map<string, number>();
    const dictValues: unknown[] = [];
    const indices: number[] = [];

    // For Nullable types, index 0 is reserved for null
    if (isNullable) {
      dict.set("null", 0);
      dictValues.push(getZeroValue(this.dictCodec)); // Placeholder for null
    }

    for (const v of values) {
      if (isNullable && v === null) {
        indices.push(0); // Null uses index 0
      } else {
        const k = JSON.stringify(v);
        if (!dict.has(k)) {
          dict.set(k, dictValues.length);
          dictValues.push(v);
        }
        indices.push(dict.get(k)!);
      }
    }

    // Always set HAS_ADDITIONAL_KEYS_BIT (bit 9) for insert data
    const HAS_ADDITIONAL_KEYS_BIT = 1n << 9n;
    let typeInfo = 0n;
    let T: any = Uint8Array;
    if (dictValues.length > 255) { typeInfo = 1n; T = Uint16Array; }
    if (dictValues.length > 65535) { typeInfo = 2n; T = Uint32Array; }

    writer.write(new Uint8Array(new BigUint64Array([HAS_ADDITIONAL_KEYS_BIT | typeInfo]).buffer));
    writer.write(new Uint8Array(new BigUint64Array([BigInt(dictValues.length)]).buffer));
    writer.write(this.dictCodec.encode(dictValues));
    writer.write(new Uint8Array(new BigUint64Array([BigInt(values.length)]).buffer));
    writer.write(new Uint8Array(new T(indices).buffer));

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

    const res = new Array(count);
    for (let i = 0; i < count; i++) {
      const idx = Number(indices[i]);
      // Index 0 is null ONLY when inner type is Nullable
      res[i] = isNullable && idx === 0 ? null : dict[idx];
    }
    return res;
  }
}

// 6. Map Codec
// Map is serialized as Array(Tuple(K, V)) for prefix purposes.
// Prefixes are written at top level, not inside the data.
// Uses Array<[K, V]> representation to preserve duplicate keys (which generateRandom can create).
class MapCodec implements Codec {
  private key: Codec;
  private val: Codec;

  constructor(key: Codec, val: Codec) {
    this.key = key;
    this.val = val;
  }

  // Map prefix = key prefix + value prefix (like Tuple(K, V))
  writePrefix(writer: BufferWriter, values: unknown[]) {
    // Flatten all map entries to get key/value arrays for prefix
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    for (const m of values) {
      for (const [k, v] of toEntries(m)) {
        keys.push(k);
        vals.push(v);
      }
    }
    this.key.writePrefix?.(writer, keys);
    this.val.writePrefix?.(writer, vals);
  }

  readPrefix(reader: BufferReader) {
    this.key.readPrefix?.(reader);
    this.val.readPrefix?.(reader);
  }

  encode(values: unknown[]): Uint8Array {
    const writer = new BufferWriter();
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    const offsets = new BigUint64Array(values.length);
    let offset = 0n;

    for (let i = 0; i < values.length; i++) {
      const entries = toEntries(values[i]);
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
    writer.write(this.key.encode(keys));
    writer.write(this.val.encode(vals));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const total = rows > 0 ? Number(offsets[rows - 1]) : 0;

    // Prefixes were already read via readPrefix at top level
    const keys = this.key.decode(reader, total);
    const vals = this.val.decode(reader, total);

    const res = new Array(rows);
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

// 7. Tuple Codec
// Tuple stores each element as a column, so they need their own prefixes
class TupleCodec implements Codec {
  private elements: { name: string | null, codec: Codec }[];
  private isNamed: boolean;

  constructor(elements: { name: string | null, codec: Codec }[], isNamed: boolean) {
    this.elements = elements;
    this.isNamed = isNamed;
  }

  writePrefix(writer: BufferWriter, values: unknown[]) {
    for (let i = 0; i < this.elements.length; i++) {
      const colValues = values.map(v => this.isNamed ? (v as any)[this.elements[i].name!] : (v as any)[i]);
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
    for (let i = 0; i < this.elements.length; i++) {
      const colValues = values.map(v => this.isNamed ? (v as any)[this.elements[i].name!] : (v as any)[i]);
      writer.write(this.elements[i].codec.encode(colValues));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): unknown[] {
    const cols = this.elements.map(e => e.codec.decode(reader, rows));
    const res = new Array(rows);
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

// 8. Variant Codec
class VariantCodec implements Codec {
  private types: Codec[];
  constructor(types: Codec[]) {
    this.types = types;
  }

  writePrefix(writer: BufferWriter) {
    writer.write(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]));
    // Nested codecs don't have separate prefixes
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8;
    // Nested codecs don't have separate prefixes
  }

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

    const decoded = new Map<number, unknown[]>();
    for (let i = 0; i < this.types.length; i++) {
      if (groups.has(i)) decoded.set(i, this.types[i].decode(reader, groups.get(i)!));
    }

    const res = new Array(rows);
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

// 9. Dynamic Codec (V3 only)
class DynamicCodec implements Codec {
  private types: string[] = [];
  private codecs: Codec[] = [];

  writePrefix(writer: BufferWriter, values: unknown[]) {
    const typeSet = new Set<string>();
    for (const v of values) if (v !== null) typeSet.add(guessType(v));
    this.types = [...typeSet].sort();
    this.codecs = this.types.map(t => getCodec(t));

    writer.write(new Uint8Array(new BigUint64Array([3n]).buffer));
    writer.writeVarint(this.types.length);
    for (const t of this.types) writer.writeString(t);

    for (let i = 0; i < this.types.length; i++) {
      const typeName = this.types[i];
      const colValues = values.filter(v => v !== null && guessType(v) === typeName);
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
      const type = guessType(v);
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

    const decoded = new Map<number, unknown[]>();
    for (let i = 0; i < this.types.length; i++) {
      if (groups.has(i)) decoded.set(i, this.codecs[i].decode(reader, groups.get(i)!));
    }

    const res = new Array(rows);
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
}

// 10. JSON Codec (V3 only)
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

    const res = new Array(rows);
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

// ============================================================================
// Factory & Utils
// ============================================================================

const CODEC_CACHE = new Map<string, Codec>();

function getCodec(type: string): Codec {
  if (CODEC_CACHE.has(type)) return CODEC_CACHE.get(type)!;
  const codec = createCodec(type);
  CODEC_CACHE.set(type, codec);
  return codec;
}

function createCodec(type: string): Codec {
  if (type.startsWith("Nullable")) return new NullableCodec(getCodec(unwrap(type)));
  if (type.startsWith("Array")) return new ArrayCodec(getCodec(unwrap(type)));
  if (type.startsWith("LowCardinality")) return new LowCardinalityCodec(getCodec(unwrap(type)));
  if (type.startsWith("Map")) {
    const [k, v] = parseArgs(unwrap(type));
    return new MapCodec(getCodec(k), getCodec(v));
  }
  if (type.startsWith("Tuple")) {
    const args = parseTupleElements(unwrap(type));
    const isNamed = args[0].name !== null;
    return new TupleCodec(args.map(a => ({ name: a.name, codec: getCodec(a.type) })), isNamed);
  }
  // Nested is syntactic sugar for Array(Tuple(...))
  // e.g., Nested(id UInt64, val String) -> Array(Tuple(UInt64, String))
  if (type.startsWith("Nested")) {
    const args = parseTupleElements(unwrap(type));
    const tupleCodec = new TupleCodec(args.map(a => ({ name: a.name, codec: getCodec(a.type) })), true);
    return new ArrayCodec(tupleCodec);
  }
  if (type.startsWith("Variant")) return new VariantCodec(parseArgs(unwrap(type)).map(getCodec));
  if (type === "Dynamic") return new DynamicCodec();
  if (type === "JSON" || type.startsWith("JSON")) return new JsonCodec();

  if (type.startsWith("FixedString")) return new FixedStringCodec(parseInt(unwrap(type)));

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

function unwrap(type: string): string {
  return type.substring(type.indexOf("(") + 1, type.lastIndexOf(")"));
}

// parseArgs aliased to parseTypeList, parseTupleElements imported from native_utils.ts
const parseArgs = parseTypeList;

function guessType(value: unknown): string {
  if (value === null) return "String";
  if (typeof value === "string") return "String";
  if (typeof value === "number") return Number.isInteger(value) ? "Int64" : "Float64";
  if (typeof value === "bigint") return "Int64";
  if (typeof value === "boolean") return "Bool";
  if (value instanceof Date) return "DateTime64(3)";
  if (Array.isArray(value)) return value.length ? `Array(${guessType(value[0])})` : "Array(String)";
  if (typeof value === "object") return "Map(String,String)";
  return "String";
}

function toEntries(value: unknown): [unknown, unknown][] {
  if (value instanceof Map) return [...value.entries()];
  if (Array.isArray(value)) return value as [unknown, unknown][];
  return Object.entries(value as Record<string, unknown>);
}

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

// ============================================================================
// Main API
// ============================================================================

interface BlockResult {
  columns: ColumnDef[];
  rows: unknown[][];
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
      rows: [],
      bytesConsumed: reader.offset - startOffset,
      isEndMarker: true,
    };
  }

  const columns: ColumnDef[] = [];
  const colData: unknown[][] = [];

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < numCols; i++) {
    const name = reader.readString();
    const type = reader.readString();
    columns.push({ name, type });

    const codec = getCodec(type);
    codec.readPrefix?.(reader);
    colData.push(codec.decode(reader, numRows));
  }

  // Convert columnar data to rows
  const rows: unknown[][] = new Array(numRows);
  for (let i = 0; i < numRows; i++) {
    const row = new Array(numCols);
    for (let j = 0; j < numCols; j++) row[j] = colData[j][i];
    rows[i] = row;
  }

  return {
    columns,
    rows,
    bytesConsumed: reader.offset - startOffset,
    isEndMarker: false,
  };
}

export function encodeNative(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const writer = new BufferWriter();
  const numRows = rows.length;

  writer.writeVarint(columns.length);
  writer.writeVarint(numRows);

  // Extract column data once
  const cols = new Array(columns.length);
  for (let i = 0; i < columns.length; i++) {
    cols[i] = new Array(numRows);
    for (let j = 0; j < numRows; j++) cols[i][j] = rows[j][i];
  }

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < columns.length; i++) {
    const codec = getCodec(columns[i].type);
    writer.writeString(columns[i].name);
    writer.writeString(columns[i].type);
    codec.writePrefix?.(writer, cols[i]);
    writer.write(codec.encode(cols[i]));
  }

  return writer.finish();
}

export function decodeNative(
  data: Uint8Array,
  options?: DecodeOptions,
): DecodeResult {
  let columns: ColumnDef[] = [];
  const allRows: unknown[][] = [];
  let offset = 0;

  // Native format can contain multiple blocks - read all of them
  while (offset < data.length) {
    const block = decodeNativeBlock(data, offset, options);
    offset += block.bytesConsumed;

    if (block.isEndMarker) break;

    // Only set columns from first block
    if (columns.length === 0) {
      columns = block.columns;
    }

    allRows.push(...block.rows);
  }

  return { columns, rows: allRows };
}

// Stream wrappers
export interface StreamDecodeNativeResult {
  columns: ColumnDef[];
  rows: unknown[][];
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

export async function* streamDecodeNative(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions,
): AsyncGenerator<StreamDecodeNativeResult> {
  let buffer = new Uint8Array(0);
  let columns: ColumnDef[] = [];

  for await (const chunk of chunks) {
    // Append new chunk to buffer
    const newBuffer = new Uint8Array(buffer.length + chunk.length);
    newBuffer.set(buffer);
    newBuffer.set(chunk, buffer.length);
    buffer = newBuffer;

    // Try to decode as many complete blocks as possible
    let offset = 0;
    while (offset < buffer.length) {
      try {
        const block = decodeNativeBlock(buffer, offset, options);

        if (block.isEndMarker) {
          offset += block.bytesConsumed;
          break;
        }

        // Set columns from first block
        if (columns.length === 0) {
          columns = block.columns;
        }

        offset += block.bytesConsumed;
        yield { columns, rows: block.rows };
      } catch {
        // Not enough data for a complete block, wait for more chunks
        break;
      }
    }

    // Keep unconsumed bytes for next iteration, release consumed memory
    if (offset > 0) {
      if (offset === buffer.length) {
        buffer = new Uint8Array(0);
      } else {
        buffer = buffer.slice(offset);
      }
    }
  }

  // Handle any remaining data
  if (buffer.length > 0) {
    let offset = 0;
    while (offset < buffer.length) {
      try {
        const block = decodeNativeBlock(buffer, offset, options);
        if (block.isEndMarker) break;

        if (columns.length === 0) columns = block.columns;
        offset += block.bytesConsumed;
        yield { columns, rows: block.rows };
      } catch {
        break;
      }
    }
  }
}
