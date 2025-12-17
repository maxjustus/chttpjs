/**
 * Codec classes for Native format encoding/decoding.
 * Each codec handles a specific ClickHouse type.
 */

import {
  type TypedArray,
  TEXT_ENCODER,
  ClickHouseDateTime64,
  parseTypeList,
  parseTupleElements,
  ipv6ToBytes,
  bytesToIpv6,
  readBigInt128,
  writeBigInt128,
  readBigInt256,
  writeBigInt256,
  decimalByteSize,
  extractDecimalScale,
  formatScaledBigInt,
  parseDecimalToScaledBigInt,
} from "../shared.ts";

import { BufferWriter, BufferReader, type TypedArrayConstructor } from "./io.ts";
import {
  type Column,
  type DiscriminatorArray,
  DataColumn,
  TupleColumn,
  MapColumn,
  VariantColumn,
  DynamicColumn,
  JsonColumn,
  NullableColumn,
  ArrayColumn,
  VARIANT_NULL_DISCRIMINATOR,
  countAndIndexDiscriminators,
} from "./columns.ts";

const MS_PER_DAY = 86400000;

// LowCardinality encoding flags
const LC_FLAG_ADDITIONAL_KEYS = 1n << 9n;
const LC_INDEX_U8 = 0n;
const LC_INDEX_U16 = 1n;
const LC_INDEX_U32 = 2n;

/**
 * Decode groups from reader based on discriminator counts.
 */
function decodeGroups(
  reader: BufferReader,
  codecs: Codec[],
  counts: Map<number, number>
): Map<number, Column> {
  const groups = new Map<number, Column>();
  for (let i = 0; i < codecs.length; i++) {
    if (counts.has(i)) groups.set(i, codecs[i].decode(reader, counts.get(i)!));
  }
  return groups;
}

const MS_PER_SECOND = 1000;

export interface Codec {
  encode(col: Column): Uint8Array;
  decode(reader: BufferReader, rows: number): Column;
  fromValues(values: unknown[]): Column;
  zeroValue(): unknown;
  // Estimate bytes needed for this column type with given row count
  estimateSize(rows: number): number;
  // Nested types need to handle prefix writing/reading
  writePrefix?(writer: BufferWriter, col: Column): void;
  readPrefix?(reader: BufferReader): void;
}

class NumericCodec<T extends TypedArray> implements Codec {
  private Ctor: TypedArrayConstructor<T>;
  private converter?: (v: unknown) => number | bigint;
  constructor(Ctor: TypedArrayConstructor<T>, converter?: (v: unknown) => number | bigint) {
    this.Ctor = Ctor;
    this.converter = converter;
  }

  encode(col: Column): Uint8Array {
    const dc = col as DataColumn<T>;
    return new Uint8Array(dc.data.buffer, dc.data.byteOffset, dc.data.byteLength);
  }

  decode(reader: BufferReader, rows: number): DataColumn<T> {
    return new DataColumn(reader.readTypedArray(this.Ctor, rows));
  }

  fromValues(values: unknown[]): DataColumn<T> {
    const arr = new this.Ctor(values.length);
    if (this.converter) {
      for (let i = 0; i < values.length; i++) arr[i] = this.converter(values[i]) as any;
    } else {
      for (let i = 0; i < values.length; i++) arr[i] = values[i] as any;
    }
    return new DataColumn(arr);
  }

  zeroValue() { return 0; }
  estimateSize(rows: number) { return rows * this.Ctor.BYTES_PER_ELEMENT; }
}

class StringCodec implements Codec {
  encode(col: Column): Uint8Array {
    const writer = new BufferWriter();
    const values = col.toArray();
    for (let i = 0; i < values.length; i++) {
      writer.writeString(String(values[i]));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): DataColumn<string[]> {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) values[i] = reader.readString();
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<string[]> {
    return new DataColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return ""; }
  // Variable length - assume average 32 bytes per string + 1 byte length prefix
  estimateSize(rows: number) { return rows * 33; }
}

class UUIDCodec implements Codec {
  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const buf = new Uint8Array(values.length * 16);

    for (let i = 0; i < values.length; i++) {
      const u = String(values[i]);
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

  decode(reader: BufferReader, rows: number): DataColumn<string[]> {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const b = reader.buffer.subarray(reader.offset, reader.offset + 16);
      reader.offset += 16;

      const bytes = new Uint8Array(16);
      for (let j = 0; j < 8; j++) bytes[7 - j] = b[j];
      for (let j = 0; j < 8; j++) bytes[15 - j] = b[8 + j];

      const hex = Array.from(bytes).map(x => x.toString(16).padStart(2, '0')).join('');
      values[i] = `${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}`;
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<string[]> {
    return new DataColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return "00000000-0000-0000-0000-000000000000"; }
  estimateSize(rows: number) { return rows * 16; }
}

class FixedStringCodec implements Codec {
  len: number;
  constructor(len: number) {
    this.len = len;
  }

  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const buf = new Uint8Array(values.length * this.len);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v instanceof Uint8Array) {
        buf.set(v.subarray(0, this.len), i * this.len);
      } else {
        const bytes = TEXT_ENCODER.encode(String(v));
        buf.set(bytes.subarray(0, this.len), i * this.len);
      }
    }
    return buf;
  }

  decode(reader: BufferReader, rows: number): DataColumn<Uint8Array[]> {
    const values: Uint8Array[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = reader.buffer.slice(reader.offset, reader.offset + this.len);
      reader.offset += this.len;
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<Uint8Array[]> {
    const result: Uint8Array[] = new Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v instanceof Uint8Array) {
        result[i] = v;
      } else if (typeof v === "string") {
        const buf = new Uint8Array(this.len);
        const encoded = TEXT_ENCODER.encode(v);
        buf.set(encoded.subarray(0, this.len));
        result[i] = buf;
      } else {
        result[i] = new Uint8Array(this.len);
      }
    }
    return new DataColumn(result);
  }

  zeroValue() { return new Uint8Array(this.len); }
  estimateSize(rows: number) { return rows * this.len; }
}

class BigIntCodec implements Codec {
  private byteSize: 16 | 32;
  private signed: boolean;

  constructor(byteSize: 16 | 32, signed: boolean) {
    this.byteSize = byteSize;
    this.signed = signed;
  }

  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const buf = new Uint8Array(values.length * this.byteSize);
    const view = new DataView(buf.buffer);
    const writer = this.byteSize === 16 ? writeBigInt128 : writeBigInt256;
    for (let i = 0; i < values.length; i++) {
      writer(view, i * this.byteSize, BigInt(values[i] as any), this.signed);
    }
    return buf;
  }

  decode(reader: BufferReader, rows: number): DataColumn<bigint[]> {
    const values: bigint[] = new Array(rows);
    const readFn = this.byteSize === 16 ? readBigInt128 : readBigInt256;
    for (let i = 0; i < rows; i++) {
      values[i] = readFn(reader.view, reader.offset, this.signed);
      reader.offset += this.byteSize;
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<bigint[]> {
    return new DataColumn(values.map(v => BigInt(v as any)));
  }

  zeroValue() { return 0n; }
  estimateSize(rows: number) { return rows * this.byteSize; }
}

class DecimalCodec implements Codec {
  private byteSize: 4 | 8 | 16 | 32;
  private scale: number;

  constructor(type: string) {
    this.byteSize = decimalByteSize(type);
    this.scale = extractDecimalScale(type);
  }

  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const buf = new Uint8Array(values.length * this.byteSize);
    const view = new DataView(buf.buffer);

    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      let scaled: bigint;
      if (typeof v === 'bigint') {
        scaled = v;
      } else if (typeof v === 'string') {
        scaled = parseDecimalToScaledBigInt(v, this.scale);
      } else {
        scaled = parseDecimalToScaledBigInt(String(v), this.scale);
      }

      const off = i * this.byteSize;
      if (this.byteSize === 4) {
        view.setInt32(off, Number(scaled), true);
      } else if (this.byteSize === 8) {
        view.setBigInt64(off, scaled, true);
      } else if (this.byteSize === 16) {
        writeBigInt128(view, off, scaled, true);
      } else {
        writeBigInt256(view, off, scaled, true);
      }
    }
    return buf;
  }

  decode(reader: BufferReader, rows: number): DataColumn<string[]> {
    const values: string[] = new Array(rows);

    for (let i = 0; i < rows; i++) {
      let scaled: bigint;
      if (this.byteSize === 4) {
        scaled = BigInt(reader.view.getInt32(reader.offset, true));
      } else if (this.byteSize === 8) {
        scaled = reader.view.getBigInt64(reader.offset, true);
      } else if (this.byteSize === 16) {
        scaled = readBigInt128(reader.view, reader.offset, true);
      } else {
        scaled = readBigInt256(reader.view, reader.offset, true);
      }
      reader.offset += this.byteSize;
      values[i] = formatScaledBigInt(scaled, this.scale);
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<string[]> {
    return new DataColumn(values.map(v => {
      if (typeof v === 'string') return v;
      if (typeof v === 'bigint') return formatScaledBigInt(v, this.scale);
      return String(v);
    }));
  }

  zeroValue() { return formatScaledBigInt(0n, this.scale); }
  estimateSize(rows: number) { return rows * this.byteSize; }
}

class DateTime64Codec implements Codec {
  private precision: number;
  constructor(precision: number) {
    this.precision = precision;
  }

  encode(col: Column): Uint8Array {
    const values = col.toArray();
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

  decode(reader: BufferReader, rows: number): DataColumn<ClickHouseDateTime64[]> {
    const arr = reader.readTypedArray(BigInt64Array, rows);
    const values: ClickHouseDateTime64[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = new ClickHouseDateTime64(arr[i], this.precision);
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<ClickHouseDateTime64[]> {
    const result: ClickHouseDateTime64[] = new Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i] as any;
      if (v instanceof ClickHouseDateTime64) {
        result[i] = v;
      } else if (v instanceof Date) {
        const ms = BigInt(v.getTime());
        const scale = 10n ** BigInt(Math.abs(this.precision - 3));
        const ticks = this.precision >= 3 ? ms * scale : ms / scale;
        result[i] = new ClickHouseDateTime64(ticks, this.precision);
      } else if (typeof v === "bigint") {
        result[i] = new ClickHouseDateTime64(v, this.precision);
      } else {
        result[i] = new ClickHouseDateTime64(0n, this.precision);
      }
    }
    return new DataColumn(result);
  }

  zeroValue() { return new Date(0); }
  estimateSize(rows: number) { return rows * 8; }
}

// handles Date, Date32, DateTime (ms since epoch / multiplier)
class EpochCodec<T extends Uint16Array | Int32Array | Uint32Array> implements Codec {
  private Ctor: TypedArrayConstructor<T>;
  private multiplier: number;

  constructor(Ctor: TypedArrayConstructor<T>, multiplier: number) {
    this.Ctor = Ctor;
    this.multiplier = multiplier;
  }

  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const arr = new this.Ctor(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      arr[i] = Math.floor(new Date(v as any).getTime() / this.multiplier) as any;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): DataColumn<Date[]> {
    const arr = reader.readTypedArray(this.Ctor, rows);
    const values: Date[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = new Date((arr[i] as number) * this.multiplier);
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<Date[]> {
    const result: Date[] = new Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v instanceof Date) {
        result[i] = v;
      } else if (typeof v === "number") {
        result[i] = new Date(v);
      } else {
        result[i] = new Date(0);
      }
    }
    return new DataColumn(result);
  }

  zeroValue() { return new Date(0); }
  estimateSize(rows: number) { return rows * this.Ctor.BYTES_PER_ELEMENT; }
}

class IPv4Codec implements Codec {
  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const arr = new Uint32Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = String(values[i]);
      const parts = v.split('.').map(Number);
      arr[i] = (parts[0] | (parts[1] << 8) | (parts[2] << 16) | (parts[3] << 24)) >>> 0;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): DataColumn<string[]> {
    const arr = reader.readTypedArray(Uint32Array, rows);
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const v = arr[i];
      values[i] = `${v & 0xFF}.${(v >> 8) & 0xFF}.${(v >> 16) & 0xFF}.${(v >> 24) & 0xFF}`;
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<string[]> {
    return new DataColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return "0.0.0.0"; }
  estimateSize(rows: number) { return rows * 4; }
}

class IPv6Codec implements Codec {
  encode(col: Column): Uint8Array {
    const values = col.toArray();
    const result = new Uint8Array(values.length * 16);
    for (let i = 0; i < values.length; i++) {
      const v = String(values[i]);
      const bytes = ipv6ToBytes(v);
      result.set(bytes, i * 16);
    }
    return result;
  }

  decode(reader: BufferReader, rows: number): DataColumn<string[]> {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const bytes = reader.readBytes(16);
      values[i] = bytesToIpv6(bytes);
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): DataColumn<string[]> {
    return new DataColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return "::"; }
  estimateSize(rows: number) { return rows * 16; }
}

// When used as a column in Map/Tuple, inner codec's prefix needs to be handled
class ArrayCodec implements Codec {
  private inner: Codec;

  constructor(inner: Codec) {
    this.inner = inner;
  }

  writePrefix(writer: BufferWriter, col: Column) {
    const arr = col as ArrayColumn;
    this.inner.writePrefix?.(writer, arr.inner);
  }

  readPrefix(reader: BufferReader) {
    this.inner.readPrefix?.(reader);
  }

  encode(col: Column): Uint8Array {
    const arr = col as ArrayColumn;
    const writer = new BufferWriter();

    // Write offsets
    writer.write(new Uint8Array(arr.offsets.buffer, arr.offsets.byteOffset, arr.offsets.byteLength));

    // Write inner data
    writer.write(this.inner.encode(arr.inner));

    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): ArrayColumn {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const totalCount = rows > 0 ? Number(offsets[rows - 1]) : 0;
    const inner = this.inner.decode(reader, totalCount);
    return new ArrayColumn(offsets, inner);
  }

  fromValues(values: unknown[]): ArrayColumn {
    const offsets = new BigUint64Array(values.length);
    const allInner: unknown[] = [];
    let offset = 0n;
    for (let i = 0; i < values.length; i++) {
      const arr = values[i] as unknown[];
      for (const v of arr) allInner.push(v);
      offset += BigInt(arr.length);
      offsets[i] = offset;
    }
    return new ArrayColumn(offsets, this.inner.fromValues(allInner));
  }

  zeroValue() { return []; }
  // 8 bytes per offset + assume average 5 elements per row
  estimateSize(rows: number) { return rows * 8 + this.inner.estimateSize(rows * 5); }
}

// Delegates prefix handling to inner codec
class NullableCodec implements Codec {
  private inner: Codec;

  constructor(inner: Codec) {
    this.inner = inner;
  }

  /** Expose inner codec for LowCardinality wrapping */
  getInnerCodec(): Codec {
    return this.inner;
  }

  writePrefix(writer: BufferWriter, col: Column) {
    const nc = col as NullableColumn;
    this.inner.writePrefix?.(writer, nc.inner);
  }

  readPrefix(reader: BufferReader) {
    this.inner.readPrefix?.(reader);
  }

  encode(col: Column): Uint8Array {
    const nc = col as NullableColumn;
    const writer = new BufferWriter();
    writer.write(nc.nullFlags);
    writer.write(this.inner.encode(nc.inner));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): NullableColumn {
    const nullFlags = reader.readTypedArray(Uint8Array, rows);
    const inner = this.inner.decode(reader, rows);
    return new NullableColumn(nullFlags, inner);
  }

  fromValues(values: unknown[]): NullableColumn {
    const nullFlags = new Uint8Array(values.length);
    const innerValues: unknown[] = new Array(values.length);
    const zeroVal = this.inner.zeroValue();
    for (let i = 0; i < values.length; i++) {
      if (values[i] === null || values[i] === undefined) {
        nullFlags[i] = 1;
        innerValues[i] = zeroVal;
      } else {
        innerValues[i] = values[i];
      }
    }
    return new NullableColumn(nullFlags, this.inner.fromValues(innerValues));
  }

  zeroValue() { return null; }
  // null flags (1 byte each) + inner data
  estimateSize(rows: number) { return rows + this.inner.estimateSize(rows); }
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
    this.dictCodec = inner instanceof NullableCodec ? inner.getInnerCodec() : inner;
  }

  writePrefix(writer: BufferWriter) {
    writer.write(new Uint8Array(new BigUint64Array([1n]).buffer));
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8;
  }

  encode(col: Column): Uint8Array {
    // LowCardinality encode builds dictionary from column values
    // This is row-oriented by nature - we need to scan values to find uniques
    if (col.length === 0) return new Uint8Array(0);

    const writer = new BufferWriter();
    const isNullable = this.inner instanceof NullableCodec;

    const dict = new Map<unknown, number>();
    const dictValues: unknown[] = [];
    const indices: number[] = [];

    // For Nullable types, index 0 is reserved for null
    if (isNullable) {
      dict.set(null, 0);
      dictValues.push(null); // Placeholder for null
    }

    for (let i = 0; i < col.length; i++) {
      const v = col.get(i);
      if (isNullable && v === null) {
        indices.push(0);
      } else {
        const k = this.getDictKey(v);
        if (!dict.has(k)) {
          dict.set(k, dictValues.length);
          dictValues.push(v);
        }
        indices.push(dict.get(k)!);
      }
    }

    let indexType = LC_INDEX_U8;
    let IndexArray: any = Uint8Array;
    if (dictValues.length > 255) { indexType = LC_INDEX_U16; IndexArray = Uint16Array; }
    if (dictValues.length > 65535) { indexType = LC_INDEX_U32; IndexArray = Uint32Array; }

    writer.write(new Uint8Array(new BigUint64Array([LC_FLAG_ADDITIONAL_KEYS | indexType]).buffer));

    // Build dictionary column from unique values
    writer.write(new Uint8Array(new BigUint64Array([BigInt(dictValues.length)]).buffer));
    writer.write(this.dictCodec.encode(this.dictCodec.fromValues(dictValues)));
    writer.write(new Uint8Array(new BigUint64Array([BigInt(col.length)]).buffer));
    writer.write(new Uint8Array(new IndexArray(indices).buffer));

    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): Column {
    if (rows === 0) return new DataColumn([]);

    const flags = reader.view.getBigUint64(reader.offset, true);
    reader.offset += 8;

    const typeInfo = Number(flags & 0xFFn);
    const isNullable = this.inner instanceof NullableCodec;

    const dictSize = Number(reader.view.getBigUint64(reader.offset, true));
    reader.offset += 8;

    const dict = this.dictCodec.decode(reader, dictSize);

    const count = Number(reader.view.getBigUint64(reader.offset, true));
    reader.offset += 8;

    let indices: TypedArray;
    if (typeInfo === 0) indices = reader.readTypedArray(Uint8Array, count);
    else if (typeInfo === 1) indices = reader.readTypedArray(Uint16Array, count);
    else if (typeInfo === 2) indices = reader.readTypedArray(Uint32Array, count);
    else indices = reader.readTypedArray(BigUint64Array, count);

    // Expand dictionary to full column
    const values: unknown[] = new Array(count);
    for (let i = 0; i < count; i++) {
      const idx = Number(indices[i]);
      values[i] = isNullable && idx === 0 ? null : dict.get(idx);
    }
    return new DataColumn(values);
  }

  fromValues(values: unknown[]): Column {
    // LowCardinality is just storage optimization - pass through to inner
    return this.inner.fromValues(values);
  }

  zeroValue() { return this.inner.zeroValue(); }

  // key for low cardinality dictionary map
  getDictKey(v: unknown): unknown {
    if (v === null || typeof v !== 'object') return v;
    if (v instanceof Date) return v.getTime();
    if (v instanceof Uint8Array) {
      // FixedString - use hex encoding for stable key generation
      let s = '\0B:'; // prefix to distinguish from regular strings
      for (let i = 0; i < v.length; i++) {
        const byte = v[i];
        s += (byte >> 4).toString(16) + (byte & 0xf).toString(16);
      }
      return s;
    }
    // Stable stringification with sorted keys for objects
    if (typeof v === 'object') {
      const keys = Object.keys(v as object).sort();
      return '\0O:' + keys.map(k => `${k}:${this.getDictKey((v as any)[k])}`).join(',');
    }
    return v;
  }

  // Dictionary + indices (assume u16 indices, max 65536 unique values)
  estimateSize(rows: number) {
    const dictSize = Math.min(rows, 65536);
    return 8 + 8 + this.dictCodec.estimateSize(dictSize) + 8 + rows * 2;
  }
}

// Map is serialized as Array(Tuple(K, V))
// Prefixes are written at top level, not inside the data.
class MapCodec implements Codec {
  private keyCodec: Codec;
  private valCodec: Codec;

  constructor(keyCodec: Codec, valCodec: Codec) {
    this.keyCodec = keyCodec;
    this.valCodec = valCodec;
  }

  writePrefix(writer: BufferWriter, col: Column) {
    const map = col as MapColumn;
    this.keyCodec.writePrefix?.(writer, map.keys);
    this.valCodec.writePrefix?.(writer, map.values);
  }

  readPrefix(reader: BufferReader) {
    this.keyCodec.readPrefix?.(reader);
    this.valCodec.readPrefix?.(reader);
  }

  encode(col: Column): Uint8Array {
    const map = col as MapColumn;
    const writer = new BufferWriter();
    writer.write(new Uint8Array(map.offsets.buffer, map.offsets.byteOffset, map.offsets.byteLength));
    writer.write(this.keyCodec.encode(map.keys));
    writer.write(this.valCodec.encode(map.values));
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): MapColumn {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const total = rows > 0 ? Number(offsets[rows - 1]) : 0;
    const keys = this.keyCodec.decode(reader, total);
    const vals = this.valCodec.decode(reader, total);
    return new MapColumn(offsets, keys, vals, reader.options?.mapAsArray ?? false);
  }

  fromValues(values: unknown[]): MapColumn {
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    const offsets = new BigUint64Array(values.length);
    let offset = 0n;
    for (let i = 0; i < values.length; i++) {
      const m = values[i];
      if (m instanceof Map) {
        for (const [k, v] of m) { keys.push(k); vals.push(v); }
        offset += BigInt(m.size);
      } else if (Array.isArray(m)) {
        for (const pair of m) {
          if (Array.isArray(pair) && pair.length === 2) { keys.push(pair[0]); vals.push(pair[1]); }
        }
        offset += BigInt(m.length);
      } else if (typeof m === "object" && m !== null) {
        const entries = Object.entries(m);
        for (const [k, v] of entries) { keys.push(k); vals.push(v); }
        offset += BigInt(entries.length);
      }
      offsets[i] = offset;
    }
    return new MapColumn(offsets, this.keyCodec.fromValues(keys), this.valCodec.fromValues(vals));
  }

  zeroValue() { return new Map(); }
  // 8 bytes per offset + assume average 3 entries per row
  estimateSize(rows: number) {
    const avgEntries = rows * 3;
    return rows * 8 + this.keyCodec.estimateSize(avgEntries) + this.valCodec.estimateSize(avgEntries);
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

  writePrefix(writer: BufferWriter, col: Column) {
    const tuple = col as TupleColumn;
    for (let i = 0; i < this.elements.length; i++) {
      this.elements[i].codec.writePrefix?.(writer, tuple.columns[i]);
    }
  }

  readPrefix(reader: BufferReader) {
    for (const e of this.elements) {
      e.codec.readPrefix?.(reader);
    }
  }

  encode(col: Column): Uint8Array {
    const tuple = col as TupleColumn;
    const writer = new BufferWriter();
    for (let i = 0; i < this.elements.length; i++) {
      writer.write(this.elements[i].codec.encode(tuple.columns[i]));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): TupleColumn {
    const cols = this.elements.map(e => e.codec.decode(reader, rows));
    return new TupleColumn(
      this.elements.map(e => ({ name: e.name })),
      cols,
      this.isNamed
    );
  }

  fromValues(values: unknown[]): TupleColumn {
    const columns: Column[] = [];
    for (let ei = 0; ei < this.elements.length; ei++) {
      const elem = this.elements[ei];
      const elemValues: unknown[] = new Array(values.length);
      for (let i = 0; i < values.length; i++) {
        const tuple = values[i] as any;
        elemValues[i] = this.isNamed ? tuple[elem.name!] : tuple[ei];
      }
      columns.push(elem.codec.fromValues(elemValues));
    }
    return new TupleColumn(
      this.elements.map(e => ({ name: e.name })),
      columns,
      this.isNamed
    );
  }

  zeroValue() { return []; }
  // Sum of all element sizes
  estimateSize(rows: number) {
    return this.elements.reduce((sum, e) => sum + e.codec.estimateSize(rows), 0);
  }
}

// 8. Variant Codec
// Note: COMPACT mode (mode=1) exists for storage optimization but is not sent to HTTP clients.
// ClickHouse always sends BASIC mode (mode=0) over HTTP. COMPACT mode is only used internally
// for MergeTree storage. See: https://github.com/ClickHouse/ClickHouse/pull/62774
class VariantCodec implements Codec {
  private typeStrings: string[];
  private codecs: Codec[];
  constructor(typeStrings: string[], codecs: Codec[]) {
    this.typeStrings = typeStrings;
    this.codecs = codecs;
  }

  writePrefix(writer: BufferWriter) {
    // UInt64 LE mode flag: 0=BASIC (row-by-row), 1=COMPACT (granule-based, storage only)
    const BASIC_MODE = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 0]);
    writer.write(BASIC_MODE);
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8; // Skip encoding mode flag - always BASIC (0) for HTTP clients
  }

  encode(col: Column): Uint8Array {
    const variant = col as VariantColumn;
    const writer = new BufferWriter();
    writer.write(variant.discriminators);
    for (let i = 0; i < this.codecs.length; i++) {
      const group = variant.groups.get(i);
      if (group) writer.write(this.codecs[i].encode(group));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): VariantColumn {
    const discriminators = reader.readTypedArray(Uint8Array, rows);
    const { counts, indices } = countAndIndexDiscriminators(discriminators, VARIANT_NULL_DISCRIMINATOR);
    const groups = decodeGroups(reader, this.codecs, counts);
    return new VariantColumn(discriminators, groups, indices);
  }

  fromValues(values: unknown[]): VariantColumn {
    const discriminators = new Uint8Array(values.length);
    const variantValues: unknown[][] = this.codecs.map(() => []);

    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v === null) {
        discriminators[i] = VARIANT_NULL_DISCRIMINATOR;
      } else if (Array.isArray(v) && v.length === 2 && typeof v[0] === "number") {
        const disc = v[0] as number;
        if (disc < 0 || disc >= this.codecs.length) {
          throw new Error(`Invalid Variant discriminator ${disc}, expected 0-${this.codecs.length - 1}`);
        }
        discriminators[i] = disc;
        variantValues[disc].push(v[1]);
      } else {
        const variantIdx = this.findVariantIndex(v, this.typeStrings);
        discriminators[i] = variantIdx;
        variantValues[variantIdx].push(v);
      }
    }

    const groups = new Map<number, Column>();
    for (let vi = 0; vi < this.codecs.length; vi++) {
      if (variantValues[vi].length > 0) {
        groups.set(vi, this.codecs[vi].fromValues(variantValues[vi]));
      }
    }

    return new VariantColumn(discriminators, groups);
  }

  zeroValue() { return null; }
  // Discriminators + variant data (assume even distribution)
  estimateSize(rows: number) {
    const perVariant = Math.ceil(rows / this.codecs.length);
    return rows + this.codecs.reduce((sum, c) => sum + c.estimateSize(perVariant), 0);
  }

  findVariantIndex(value: unknown, types: string[]): number {
    // Simple heuristic to match value to variant type
    for (let i = 0; i < types.length; i++) {
      const t = types[i];
      if (t === "String" && typeof value === "string") return i;
      if ((t === "Int64" || t === "UInt64") && typeof value === "bigint") return i;
      if ((t.startsWith("Int") || t.startsWith("UInt") || t.startsWith("Float")) && typeof value === "number") return i;
      if (t === "Bool" && typeof value === "boolean") return i;
      if ((t === "Date" || t === "DateTime" || t.startsWith("DateTime64")) && value instanceof Date) return i;
      if (t.startsWith("Array") && Array.isArray(value)) return i;
      if (t.startsWith("Map") && (value instanceof Map || (typeof value === "object" && value !== null))) return i;
    }
    return 0; // default to first type
  }
}

// 9. Dynamic Codec (V3 FLATTENED only)
class DynamicCodec implements Codec {
  private types: string[] = [];
  private codecs: Codec[] = [];

  writePrefix(writer: BufferWriter, col: Column) {
    const dyn = col as DynamicColumn;
    this.types = dyn.types;
    this.codecs = this.types.map(t => getCodec(t));

    writer.write(new Uint8Array(new BigUint64Array([3n]).buffer));
    writer.writeVarint(this.types.length);
    for (const t of this.types) writer.writeString(t);

    for (let i = 0; i < this.types.length; i++) {
      const group = dyn.groups.get(i);
      if (group) this.codecs[i].writePrefix?.(writer, group);
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

  encode(col: Column): Uint8Array {
    const dyn = col as DynamicColumn;
    const writer = new BufferWriter();

    // Write discriminators as-is (already the right type)
    writer.write(new Uint8Array(dyn.discriminators.buffer, dyn.discriminators.byteOffset, dyn.discriminators.byteLength));

    for (let i = 0; i < this.codecs.length; i++) {
      const group = dyn.groups.get(i);
      if (group) writer.write(this.codecs[i].encode(group));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): DynamicColumn {
    const nullDisc = this.types.length;
    const discLimit = nullDisc + 1;

    let discriminators: DiscriminatorArray;
    if (discLimit <= 256) discriminators = reader.readTypedArray(Uint8Array, rows);
    else if (discLimit <= 65536) discriminators = reader.readTypedArray(Uint16Array, rows);
    else discriminators = reader.readTypedArray(Uint32Array, rows);

    const { counts, indices } = countAndIndexDiscriminators(discriminators, nullDisc);
    const groups = decodeGroups(reader, this.codecs, counts);
    return new DynamicColumn(this.types, discriminators, groups, indices);
  }

  fromValues(values: unknown[]): DynamicColumn {
    // Collect unique types
    const typeMap = new Map<string, unknown[]>();
    const typeOrder: string[] = [];
    for (const v of values) {
      if (v !== null) {
        const vType = this.guessType(v);
        if (!typeMap.has(vType)) {
          typeMap.set(vType, []);
          typeOrder.push(vType);
        }
        typeMap.get(vType)!.push(v);
      }
    }

    const nullDisc = typeOrder.length;
    const discriminators = new Uint8Array(values.length);
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      discriminators[i] = v === null ? nullDisc : typeOrder.indexOf(this.guessType(v));
    }

    const groups = new Map<number, Column>();
    for (let ti = 0; ti < typeOrder.length; ti++) {
      const codec = getCodec(typeOrder[ti]);
      groups.set(ti, codec.fromValues(typeMap.get(typeOrder[ti])!));
    }

    return new DynamicColumn(typeOrder, discriminators, groups);
  }

  zeroValue() { return null; }
  // Discriminators + type data (assume most values are strings)
  estimateSize(rows: number) {
    // Dynamic can have variable discriminator size but usually 1-2 bytes + data
    return rows * 2 + this.codecs.reduce((sum, c) => sum + c.estimateSize(Math.ceil(rows / 3)), 0);
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

  writePrefix(writer: BufferWriter, col: Column) {
    const json = col as JsonColumn;
    this.paths = json.paths;
    writer.write(new Uint8Array(new BigUint64Array([3n]).buffer));
    writer.writeVarint(this.paths.length);
    for (const p of this.paths) writer.writeString(p);

    for (const path of this.paths) {
      const codec = new DynamicCodec();
      const pathCol = json.pathColumns.get(path)!;
      codec.writePrefix(writer, pathCol);
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

  encode(col: Column): Uint8Array {
    const json = col as JsonColumn;
    const writer = new BufferWriter();
    for (const path of this.paths) {
      const pathCol = json.pathColumns.get(path)!;
      writer.write(this.pathCodecs.get(path)!.encode(pathCol));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): JsonColumn {
    const pathColumns = new Map<string, DynamicColumn>();
    for (const path of this.paths) {
      pathColumns.set(path, this.pathCodecs.get(path)!.decode(reader, rows));
    }

    return new JsonColumn(this.paths, pathColumns, rows);
  }

  fromValues(values: unknown[]): JsonColumn {
    // Collect all unique paths across all objects
    const pathSet = new Set<string>();
    for (const v of values) {
      if (v && typeof v === "object" && !Array.isArray(v)) {
        for (const key of Object.keys(v as object)) {
          pathSet.add(key);
        }
      }
    }
    const paths = Array.from(pathSet).sort();

    // For each path, create a DynamicColumn
    const pathColumns = new Map<string, DynamicColumn>();
    const dynCodec = new DynamicCodec();
    for (const path of paths) {
      const pathValues: unknown[] = new Array(values.length);
      for (let i = 0; i < values.length; i++) {
        const obj = values[i] as Record<string, unknown> | null;
        pathValues[i] = obj && typeof obj === "object" ? obj[path] ?? null : null;
      }
      pathColumns.set(path, dynCodec.fromValues(pathValues));
    }

    return new JsonColumn(paths, pathColumns, values.length);
  }

  zeroValue() { return {}; }
  // JSON columns have per-path Dynamic columns; estimate is sum of path estimates
  // Since we don't know paths until readPrefix, use Dynamic's estimate per expected path
  estimateSize(rows: number) { return rows * 32; } // Conservative: ~32 bytes per row
}

// Codec cache for type string -> codec instance
const CODEC_CACHE = new Map<string, Codec>();

export function getCodec(type: string): Codec {
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
  if (type.startsWith("Variant")) {
    const innerTypes = parseTypeList(extractTypeArgs(type));
    return new VariantCodec(innerTypes, innerTypes.map(getCodec));
  }
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
    case "Int128": return new BigIntCodec(16, true);
    case "UInt128": return new BigIntCodec(16, false);
    case "Int256": return new BigIntCodec(32, true);
    case "UInt256": return new BigIntCodec(32, false);
  }

  if (type.startsWith("Enum")) return type.startsWith("Enum8") ? new NumericCodec(Int8Array) : new NumericCodec(Int16Array);

  // Decimal types
  if (type.startsWith("Decimal")) return new DecimalCodec(type);

  throw new Error(`Unknown type: ${type}`);
}

// Extracts the content between the outermost parentheses: "Array(Int32)" → "Int32"
function extractTypeArgs(type: string): string {
  return type.substring(type.indexOf("(") + 1, type.lastIndexOf(")"));
}
