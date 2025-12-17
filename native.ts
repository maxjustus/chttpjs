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
  columnData: Column[];  // columnData[colIndex]
  rowCount: number;
}

export type StreamDecodeNativeResult = ColumnarResult;

// ============================================================================
// Column Classes
// ============================================================================

type TypedArray = Int8Array | Uint8Array | Int16Array | Uint16Array | Int32Array | Uint32Array | BigInt64Array | BigUint64Array | Float32Array | Float64Array;

/**
 * Base interface for all column types.
 */
export interface BaseColumn {
  readonly length: number;
  get(i: number): unknown;
  slice(start: number, end: number): BaseColumn;
  materialize(): unknown[] | TypedArray;
}

/**
 * Recursively materialize a value to plain JS.
 * Called by column.materialize() implementations.
 */
function materializeValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;

  // If it's a column (has materialize method), materialize it
  if (value && typeof (value as any).materialize === 'function') {
    return (value as BaseColumn).materialize();
  }

  // Map - preserve as Map with materialized entries
  if (value instanceof Map) {
    const result = new Map();
    for (const [k, v] of value) {
      result.set(materializeValue(k), materializeValue(v));
    }
    return result;
  }

  // Array - materialize each element
  if (Array.isArray(value)) {
    return value.map(materializeValue);
  }

  // Object (like tuple result) - materialize values
  if (typeof value === 'object' && !(value instanceof Date) && !ArrayBuffer.isView(value)) {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = materializeValue(v);
    }
    return result;
  }

  // Primitives, Date, TypedArray - pass through
  return value;
}

/**
 * Wraps a TypedArray as a column with the BaseColumn interface.
 */
export class TypedColumn<T extends TypedArray> implements BaseColumn {
  readonly data: T;

  constructor(data: T) {
    this.data = data;
  }

  get length() { return this.data.length; }

  get(i: number): number | bigint {
    return this.data[i];
  }

  slice(start: number, end: number): TypedColumn<T> {
    return new TypedColumn(this.data.slice(start, end) as T);
  }

  materialize(): T {
    return this.data;
  }
}

/**
 * Columnar tuple - stores each element as a separate column.
 */
export class TupleColumn implements BaseColumn {
  readonly elements: { name: string | null }[];
  readonly columns: Column[];
  readonly isNamed: boolean;

  constructor(
    elements: { name: string | null }[],
    columns: Column[],
    isNamed: boolean
  ) {
    this.elements = elements;
    this.columns = columns;
    this.isNamed = isNamed;
  }

  get length(): number {
    return this.columns[0].length;
  }

  get(i: number): unknown {
    if (this.isNamed) {
      const obj: Record<string, unknown> = {};
      for (let j = 0; j < this.elements.length; j++) {
        obj[this.elements[j].name!] = this.columns[j].get(i);
      }
      return obj;
    }
    return this.columns.map(c => c.get(i));
  }

  slice(start: number, end: number): TupleColumn {
    return new TupleColumn(
      this.elements,
      this.columns.map(c => c.slice(start, end)),
      this.isNamed
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Columnar map - stores keys and values as separate columns with offsets.
 */
export class MapColumn implements BaseColumn {
  readonly offsets: BigUint64Array;
  readonly keys: Column;
  readonly values: Column;
  private mapAsArray: boolean;

  constructor(
    offsets: BigUint64Array,
    keys: Column,
    values: Column,
    mapAsArray = false
  ) {
    this.offsets = offsets;
    this.keys = keys;
    this.values = values;
    this.mapAsArray = mapAsArray;
  }

  get length(): number {
    return this.offsets.length;
  }

  get(i: number): Map<unknown, unknown> | [unknown, unknown][] {
    const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
    const end = Number(this.offsets[i]);

    if (this.mapAsArray) {
      const entries: [unknown, unknown][] = [];
      for (let j = start; j < end; j++) {
        entries.push([this.keys.get(j), this.values.get(j)]);
      }
      return entries;
    }

    const map = new Map<unknown, unknown>();
    for (let j = start; j < end; j++) {
      map.set(this.keys.get(j), this.values.get(j));
    }
    return map;
  }

  slice(start: number, end: number): MapColumn {
    const startOffset = start === 0 ? 0 : Number(this.offsets[start - 1]);
    const endOffset = Number(this.offsets[end - 1]);

    // Adjust offsets for the slice
    const newOffsets = new BigUint64Array(end - start);
    for (let i = 0; i < newOffsets.length; i++) {
      newOffsets[i] = this.offsets[start + i] - BigInt(startOffset);
    }

    return new MapColumn(
      newOffsets,
      this.keys.slice(startOffset, endOffset),
      this.values.slice(startOffset, endOffset),
      this.mapAsArray
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Columnar variant - stores discriminators and grouped values by type.
 */
export class VariantColumn implements BaseColumn {
  readonly discriminators: Uint8Array;
  readonly groups: Map<number, Column>;
  private groupIndices: Uint32Array;

  constructor(
    discriminators: Uint8Array,
    groups: Map<number, Column>
  ) {
    this.discriminators = discriminators;
    this.groups = groups;

    // Precompute group indices for O(1) access
    this.groupIndices = new Uint32Array(discriminators.length);
    const counters = new Map<number, number>();
    for (let i = 0; i < discriminators.length; i++) {
      const d = discriminators[i];
      if (d !== 0xFF) {
        this.groupIndices[i] = counters.get(d) || 0;
        counters.set(d, (counters.get(d) || 0) + 1);
      }
    }
  }

  get length(): number {
    return this.discriminators.length;
  }

  get(i: number): [number, unknown] | null {
    const d = this.discriminators[i];
    if (d === 0xFF) return null;
    const groupIdx = this.groupIndices[i];
    return [d, this.groups.get(d)!.get(groupIdx)];
  }

  slice(start: number, end: number): VariantColumn {
    const newDiscs = this.discriminators.slice(start, end);
    // Rebuild groups for the slice - use SimpleColumn for collected values
    const groupValues = new Map<number, unknown[]>();
    for (let i = start; i < end; i++) {
      const d = this.discriminators[i];
      if (d !== 0xFF) {
        if (!groupValues.has(d)) groupValues.set(d, []);
        groupValues.get(d)!.push(this.groups.get(d)!.get(this.groupIndices[i]));
      }
    }
    const newGroups = new Map<number, Column>();
    for (const [d, values] of groupValues) {
      newGroups.set(d, new SimpleColumn(values));
    }
    return new VariantColumn(newDiscs, newGroups);
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      const v = this.get(i);
      result[i] = v ? [v[0], materializeValue(v[1])] : null;
    }
    return result;
  }
}

/**
 * Columnar dynamic - similar to variant but discriminator size varies.
 */
export class DynamicColumn implements BaseColumn {
  readonly types: string[];
  readonly discriminators: Uint8Array | Uint16Array | Uint32Array;
  readonly groups: Map<number, Column>;
  private groupIndices: Uint32Array;
  private nullDisc: number;

  constructor(
    types: string[],
    discriminators: Uint8Array | Uint16Array | Uint32Array,
    groups: Map<number, Column>
  ) {
    this.types = types;
    this.discriminators = discriminators;
    this.groups = groups;
    this.nullDisc = types.length;

    // Precompute group indices
    this.groupIndices = new Uint32Array(discriminators.length);
    const counters = new Map<number, number>();
    for (let i = 0; i < discriminators.length; i++) {
      const d = discriminators[i];
      if (d !== this.nullDisc) {
        this.groupIndices[i] = counters.get(d) || 0;
        counters.set(d, (counters.get(d) || 0) + 1);
      }
    }
  }

  get length(): number {
    return this.discriminators.length;
  }

  get(i: number): unknown {
    const d = this.discriminators[i];
    if (d === this.nullDisc) return null;
    const groupIdx = this.groupIndices[i];
    return this.groups.get(d)!.get(groupIdx);
  }

  slice(start: number, end: number): DynamicColumn {
    type DiscriminatorArray = Uint8Array | Uint16Array | Uint32Array;
    const DiscCtor = this.discriminators.constructor as { new(len: number): DiscriminatorArray };
    const newDiscs = new DiscCtor(end - start);
    for (let i = 0; i < end - start; i++) {
      newDiscs[i] = this.discriminators[start + i];
    }
    // Rebuild groups for the slice
    const groupValues = new Map<number, unknown[]>();
    for (let i = start; i < end; i++) {
      const d = this.discriminators[i];
      if (d !== this.nullDisc) {
        if (!groupValues.has(d)) groupValues.set(d, []);
        groupValues.get(d)!.push(this.groups.get(d)!.get(this.groupIndices[i]));
      }
    }
    const newGroups = new Map<number, Column>();
    for (const [d, values] of groupValues) {
      newGroups.set(d, new SimpleColumn(values));
    }
    return new DynamicColumn(this.types, newDiscs, newGroups);
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Columnar JSON - stores paths with dynamic columns for each.
 */
export class JsonColumn implements BaseColumn {
  readonly paths: string[];
  readonly pathColumns: Map<string, DynamicColumn>;
  private _length: number;

  constructor(paths: string[], pathColumns: Map<string, DynamicColumn>, length: number) {
    this.paths = paths;
    this.pathColumns = pathColumns;
    this._length = length;
  }

  get length(): number {
    return this._length;
  }

  get(i: number): Record<string, unknown> {
    const obj: Record<string, unknown> = {};
    for (const path of this.paths) {
      const val = this.pathColumns.get(path)!.get(i);
      if (val !== null) obj[path] = val;
    }
    return obj;
  }

  slice(start: number, end: number): JsonColumn {
    const newPathColumns = new Map<string, DynamicColumn>();
    for (const [path, col] of this.pathColumns) {
      newPathColumns.set(path, col.slice(start, end));
    }
    return new JsonColumn(this.paths, newPathColumns, end - start);
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Generic simple column - wraps an array of values.
 * Used for String, Bytes, Date, DateTime64, Scalar (Decimal, Int128, etc.).
 */
export class SimpleColumn<T> implements BaseColumn {
  readonly values: T[];

  constructor(values: T[]) {
    this.values = values;
  }

  get length() { return this.values.length; }

  get(i: number): T {
    return this.values[i];
  }

  slice(start: number, end: number): SimpleColumn<T> {
    return new SimpleColumn(this.values.slice(start, end));
  }

  materialize(): T[] {
    return this.values.slice();
  }
}

// Type aliases for backwards compatibility and readability
export type StringColumn = SimpleColumn<string>;
export type BytesColumn = SimpleColumn<Uint8Array>;
export type DateColumn = SimpleColumn<Date>;
export type DateTime64Column = SimpleColumn<ClickHouseDateTime64>;
export type ScalarColumn = SimpleColumn<unknown>;

/**
 * Nullable column for non-float types.
 * Wraps an inner column with null flags.
 * Inner column has same length as nullFlags (includes placeholder values at null positions).
 */
export class NullableColumn implements BaseColumn {
  readonly nullFlags: Uint8Array;
  readonly inner: Column;

  constructor(nullFlags: Uint8Array, inner: Column) {
    this.nullFlags = nullFlags;
    this.inner = inner;
  }

  get length() { return this.nullFlags.length; }

  get(i: number): unknown {
    if (this.nullFlags[i]) return null;
    return this.inner.get(i);
  }

  slice(start: number, end: number): NullableColumn {
    return new NullableColumn(
      this.nullFlags.slice(start, end),
      this.inner.slice(start, end)
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      const v = this.get(i);
      result[i] = v === null ? null : materializeValue(v);
    }
    return result;
  }
}

/**
 * Array column - stores offsets and inner column.
 */
export class ArrayColumn implements BaseColumn {
  readonly offsets: BigUint64Array;
  readonly inner: Column;

  constructor(offsets: BigUint64Array, inner: Column) {
    this.offsets = offsets;
    this.inner = inner;
  }

  get length() { return this.offsets.length; }

  get(i: number): Column {
    const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
    const end = Number(this.offsets[i]);
    return this.inner.slice(start, end);
  }

  slice(start: number, end: number): ArrayColumn {
    const startOffset = start === 0 ? 0 : Number(this.offsets[start - 1]);
    const endOffset = Number(this.offsets[end - 1]);

    const newOffsets = new BigUint64Array(end - start);
    for (let i = 0; i < newOffsets.length; i++) {
      newOffsets[i] = this.offsets[start + i] - BigInt(startOffset);
    }

    return new ArrayColumn(newOffsets, this.inner.slice(startOffset, endOffset));
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

// TODO: why bother with this alias?
export type Column = BaseColumn;

const MS_PER_DAY = 86400000;
const MS_PER_SECOND = 1000;

class BufferWriter {
  private buffer: Uint8Array;
  private offset = 0;

  constructor(initialSize = 256) {
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

interface Codec {
  encode(col: Column): Uint8Array;
  decode(reader: BufferReader, rows: number): Column;
  fromValues(values: unknown[]): Column;
  zeroValue(): unknown;
  // Nested types need to handle prefix writing/reading
  writePrefix?(writer: BufferWriter, col: Column): void;
  readPrefix?(reader: BufferReader): void;
}

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

  encode(col: Column): Uint8Array {
    // Fast path: input is TypedColumn with correct underlying type
    if (col instanceof TypedColumn && col.data instanceof this.Ctor) {
      return new Uint8Array(col.data.buffer, col.data.byteOffset, col.data.byteLength);
    }
    // Slow path: convert from any Column type
    const len = col.length;
    const arr = new this.Ctor(len);
    if (this.converter) {
      for (let i = 0; i < len; i++) arr[i] = this.converter(col.get(i)) as any;
    } else {
      for (let i = 0; i < len; i++) arr[i] = col.get(i) as any;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): TypedColumn<T> {
    return new TypedColumn(reader.readTypedArray(this.Ctor, rows));
  }

  fromValues(values: unknown[]): TypedColumn<T> {
    const arr = new this.Ctor(values.length);
    if (this.converter) {
      for (let i = 0; i < values.length; i++) arr[i] = this.converter(values[i]) as any;
    } else {
      for (let i = 0; i < values.length; i++) arr[i] = values[i] as any;
    }
    return new TypedColumn(arr);
  }

  zeroValue() { return 0; }
}

class StringCodec implements Codec {
  encode(col: Column): Uint8Array {
    const writer = new BufferWriter();
    for (let i = 0; i < col.length; i++) {
      writer.writeString(String(col.get(i)));
    }
    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): StringColumn {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) values[i] = reader.readString();
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): StringColumn {
    return new SimpleColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return ""; }
}

class UUIDCodec implements Codec {
  encode(col: Column): Uint8Array {
    const buf = new Uint8Array(col.length * 16);

    for (let i = 0; i < col.length; i++) {
      const u = String(col.get(i));
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

  decode(reader: BufferReader, rows: number): StringColumn {
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
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): StringColumn {
    return new SimpleColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return "00000000-0000-0000-0000-000000000000"; }
}

class FixedStringCodec implements Codec {
  len: number;
  constructor(len: number) {
    this.len = len;
  }

  encode(col: Column): Uint8Array {
    const buf = new Uint8Array(col.length * this.len);
    for (let i = 0; i < col.length; i++) {
      const v = col.get(i);
      if (v instanceof Uint8Array) {
        buf.set(v.subarray(0, this.len), i * this.len);
      } else {
        const bytes = TEXT_ENCODER.encode(String(v));
        buf.set(bytes.subarray(0, this.len), i * this.len);
      }
    }
    return buf;
  }

  decode(reader: BufferReader, rows: number): BytesColumn {
    const values: Uint8Array[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = reader.buffer.slice(reader.offset, reader.offset + this.len);
      reader.offset += this.len;
    }
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): BytesColumn {
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
    return new SimpleColumn(result);
  }

  zeroValue() { return new Uint8Array(this.len); }
}

class ScalarCodec implements Codec {
  private codec: ReturnType<typeof createRowBinaryCodec>;

  constructor(type: string) {
    this.codec = createRowBinaryCodec(type);
  }

  encode(col: Column): Uint8Array {
    const values = col as ScalarColumn;
    const encoder = new RowBinaryEncoder();
    for (let i = 0; i < values.length; i++) {
      this.codec.encode(encoder, values.get(i));
    }
    return encoder.finish();
  }

  decode(reader: BufferReader, rows: number): ScalarColumn {
    const values: unknown[] = new Array(rows);
    const view = reader.view;
    const data = reader.buffer;
    const cursor = { offset: reader.offset };
    for (let i = 0; i < rows; i++) {
      values[i] = this.codec.decode(view, data, cursor);
    }
    reader.offset = cursor.offset;
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): ScalarColumn {
    return new SimpleColumn(values);
  }

  zeroValue() { return 0; }
}

class DateTime64Codec implements Codec {
  private precision: number;
  constructor(precision: number) {
    this.precision = precision;
  }

  encode(col: Column): Uint8Array {
    const arr = new BigInt64Array(col.length);
    for (let i = 0; i < col.length; i++) {
      const v = col.get(i);
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

  decode(reader: BufferReader, rows: number): DateTime64Column {
    const arr = reader.readTypedArray(BigInt64Array, rows);
    const values: ClickHouseDateTime64[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = new ClickHouseDateTime64(arr[i], this.precision);
    }
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): DateTime64Column {
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
    return new SimpleColumn(result);
  }

  zeroValue() { return new Date(0); }
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
    const arr = new this.Ctor(col.length);
    for (let i = 0; i < col.length; i++) {
      const v = col.get(i);
      arr[i] = Math.floor(new Date(v as any).getTime() / this.multiplier) as any;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): DateColumn {
    const arr = reader.readTypedArray(this.Ctor, rows);
    const values: Date[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = new Date((arr[i] as number) * this.multiplier);
    }
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): DateColumn {
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
    return new SimpleColumn(result);
  }

  zeroValue() { return new Date(0); }
}

class IPv4Codec implements Codec {
  encode(col: Column): Uint8Array {
    const arr = new Uint32Array(col.length);
    for (let i = 0; i < col.length; i++) {
      const v = String(col.get(i));
      const parts = v.split('.').map(Number);
      arr[i] = (parts[0] | (parts[1] << 8) | (parts[2] << 16) | (parts[3] << 24)) >>> 0;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decode(reader: BufferReader, rows: number): StringColumn {
    const arr = reader.readTypedArray(Uint32Array, rows);
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const v = arr[i];
      values[i] = `${v & 0xFF}.${(v >> 8) & 0xFF}.${(v >> 16) & 0xFF}.${(v >> 24) & 0xFF}`;
    }
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): StringColumn {
    return new SimpleColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return "0.0.0.0"; }
}

class IPv6Codec implements Codec {
  encode(col: Column): Uint8Array {
    const result = new Uint8Array(col.length * 16);
    for (let i = 0; i < col.length; i++) {
      const v = String(col.get(i));
      const bytes = ipv6ToBytes(v);
      result.set(bytes, i * 16);
    }
    return result;
  }

  decode(reader: BufferReader, rows: number): StringColumn {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const bytes = reader.readBytes(16);
      values[i] = bytesToIpv6(bytes);
    }
    return new SimpleColumn(values);
  }

  fromValues(values: unknown[]): StringColumn {
    return new SimpleColumn(values.map(v => String(v ?? "")));
  }

  zeroValue() { return "::"; }
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
}

// Delegates prefix handling to inner codec
class NullableCodec implements Codec {
  private inner: Codec;

  constructor(inner: Codec) {
    this.inner = inner;
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

    const HAS_ADDITIONAL_KEYS_BIT = 1n << 9n;
    let indexType = 0n;
    let IndexArray: any = Uint8Array;
    if (dictValues.length > 255) { indexType = 1n; IndexArray = Uint16Array; }
    if (dictValues.length > 65535) { indexType = 2n; IndexArray = Uint32Array; }

    writer.write(new Uint8Array(new BigUint64Array([HAS_ADDITIONAL_KEYS_BIT | indexType]).buffer));

    // Build dictionary column from unique values
    writer.write(new Uint8Array(new BigUint64Array([BigInt(dictValues.length)]).buffer));
    writer.write(this.dictCodec.encode(new SimpleColumn(dictValues)));
    writer.write(new Uint8Array(new BigUint64Array([BigInt(col.length)]).buffer));
    writer.write(new Uint8Array(new IndexArray(indices).buffer));

    return writer.finish();
  }

  decode(reader: BufferReader, rows: number): Column {
    if (rows === 0) return new SimpleColumn([]);

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
    return new SimpleColumn(values);
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
      // FixedString - convert to string for deduplication
      let s = '';
      for (let i = 0; i < v.length; i++) s += String.fromCharCode(v[i]);
      return s;
    }
    return JSON.stringify(v); // fallback for unexpected object types
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
    const groupCounts = new Map<number, number>();
    for (const d of discriminators) if (d !== 0xFF) groupCounts.set(d, (groupCounts.get(d) || 0) + 1);

    const groups = new Map<number, Column>();
    for (let i = 0; i < this.codecs.length; i++) {
      if (groupCounts.has(i)) groups.set(i, this.codecs[i].decode(reader, groupCounts.get(i)!));
    }

    return new VariantColumn(discriminators, groups);
  }

  fromValues(values: unknown[]): VariantColumn {
    const discriminators = new Uint8Array(values.length);
    const variantValues: unknown[][] = this.codecs.map(() => []);

    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v === null) {
        discriminators[i] = 255;
      } else if (Array.isArray(v) && v.length === 2 && typeof v[0] === "number") {
        const disc = v[0] as number;
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

    let discriminators: Uint8Array | Uint16Array | Uint32Array;
    if (discLimit <= 256) discriminators = reader.readTypedArray(Uint8Array, rows);
    else if (discLimit <= 65536) discriminators = reader.readTypedArray(Uint16Array, rows);
    else discriminators = reader.readTypedArray(Uint32Array, rows);

    const groupCounts = new Map<number, number>();
    for (const d of discriminators) if (d !== nullDisc) groupCounts.set(d, (groupCounts.get(d) || 0) + 1);

    const groups = new Map<number, Column>();
    for (let i = 0; i < this.types.length; i++) {
      if (groupCounts.has(i)) groups.set(i, this.codecs[i].decode(reader, groupCounts.get(i)!));
    }

    return new DynamicColumn(this.types, discriminators, groups);
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
  }

  if (type.startsWith("Enum")) return type.startsWith("Enum8") ? new NumericCodec(Int8Array) : new NumericCodec(Int16Array);

  // Fallback to RowBinary codec for unsupported types (Int128, Decimal, etc.)
  return new ScalarCodec(type);
}

// Extracts the content between the outermost parentheses: "Array(Int32)" → "Int32"
function extractTypeArgs(type: string): string {
  return type.substring(type.indexOf("(") + 1, type.lastIndexOf(")"));
}

interface BlockResult {
  columns: ColumnDef[];
  columnData: Column[];
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
  const columnData: Column[] = [];

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

  // Transpose rows to columns
  const cols = new Array(columns.length).fill(null);
  for (let i = 0; i < columns.length; i++) {
    cols[i] = new Array(numRows).fill(null);
    for (let j = 0; j < numRows; j++) cols[i][j] = rows[j][i];
  }

  return encodeNativeColumnar(columns, cols, numRows);
}

/**
 * Encode columnar data to Native format (no transpose needed).
 * Input: columnData[colIndex][rowIndex] or Column objects
 */
export function encodeNativeColumnar(
  columns: ColumnDef[],
  columnData: (unknown[] | Column)[],
  rowCount?: number,
): Uint8Array {
  const writer = new BufferWriter();
  const numRows = rowCount ?? (columnData[0]?.length ?? 0);

  writer.writeVarint(columns.length);
  writer.writeVarint(numRows);

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < columns.length; i++) {
    const codec = getCodec(columns[i].type);
    const data = columnData[i];

    // Convert raw data to Column if needed (duck type check for BaseColumn)
    const col: Column = (data && typeof (data as any).get === 'function')
      ? data as Column
      : codec.fromValues(data as unknown[]);

    writer.writeString(columns[i].name);
    writer.writeString(columns[i].type);
    codec.writePrefix?.(writer, col);
    writer.write(codec.encode(col));
  }

  return writer.finish();
}

export async function decodeNative(
  data: Uint8Array,
  options?: DecodeOptions,
): Promise<ColumnarResult> {
  const blocks: ColumnarResult[] = [];

  // Wrap data in single-chunk async iterable and use streamDecodeNative
  async function* singleChunk() {
    yield data;
  }

  for await (const block of streamDecodeNative(singleChunk(), options)) {
    blocks.push(block);
  }

  // Fast path: single block, return directly (preserves columnar types)
  if (blocks.length === 0) {
    return { columns: [], columnData: [], rowCount: 0 };
  }
  if (blocks.length === 1) {
    return blocks[0];
  }

  // Multi-block: merge by materializing values
  // Note: This loses byte fidelity for NaN - use streaming for exact round-trip
  const columns = blocks[0].columns;
  const numCols = columns.length;
  const allColumnData: unknown[][] = [];
  for (let i = 0; i < numCols; i++) {
    allColumnData.push([]);
  }

  let totalRows = 0;
  for (const block of blocks) {
    for (let i = 0; i < numCols; i++) {
      const col = block.columnData[i];
      const target = allColumnData[i];
      for (let j = 0; j < col.length; j++) {
        target.push(col.get(j));
      }
    }
    totalRows += block.rowCount;
  }

  // Wrap merged arrays in SimpleColumn
  return {
    columns,
    columnData: allColumnData.map(arr => new SimpleColumn(arr)),
    rowCount: totalRows,
  };
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
 * Lazily iterate rows as objects with column names as keys.
 * Allocates one object per row on demand.
 * Note: May normalize NaN values when accessing float columns.
 */
export function* asRows(result: ColumnarResult): Generator<Record<string, unknown>> {
  const { columns, columnData, rowCount } = result;
  const numCols = columns.length;
  for (let i = 0; i < rowCount; i++) {
    const row: Record<string, unknown> = {};
    for (let j = 0; j < numCols; j++) {
      row[columns[j].name] = columnData[j].get(i);
    }
    yield row;
  }
}

/**
 * Convert columnar result to array rows.
 * Useful for re-encoding or comparison with original row arrays.
 * Note: May normalize NaN values when accessing float columns.
 */
export function toArrayRows(result: ColumnarResult): unknown[][] {
  const { columnData, rowCount } = result;
  const materialized = columnData.map(col => col.materialize());
  const rows: unknown[][] = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    rows[i] = materialized.map(m => m[i]);
  }
  return rows;
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
