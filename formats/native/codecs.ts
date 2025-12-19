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

import {
  BufferWriter,
  BufferReader,
  type TypedArrayConstructor,
} from "./io.ts";
import {
  type DeserializerState,
  type SerializationNode,
  DENSE_LEAF,
} from "./index.ts";
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
  countAndIndexDiscriminators,
} from "./columns.ts";

import {
  SerializationKind,
  LowCardinality as LC,
  Dynamic,
  JSON,
  Variant,
  Sparse,
  UUID as UUIDConst,
  IPv6 as IPv6Const,
  Time,
} from "./constants.ts";

export function defaultDeserializerState(): DeserializerState {
  return {
    serNode: DENSE_LEAF,
    sparseRuntime: new Map(),
  };
}

// Alias for brevity
const { MS_PER_DAY, MS_PER_SECOND } = Time;

/**
 * Decode groups from reader based on discriminator counts.
 */
function decodeGroups(
  reader: BufferReader,
  codecs: Codec[],
  counts: Map<number, number>,
  state: DeserializerState,
): Map<number, Column> {
  const groups = new Map<number, Column>();
  for (let i = 0; i < codecs.length; i++) {
    if (counts.has(i)) {
      const childState = {
        ...state,
        serNode: state.serNode.children[i] ?? DENSE_LEAF,
      };
      groups.set(i, codecs[i].decode(reader, counts.get(i)!, childState));
    }
  }
  return groups;
}

export interface ColumnBuilder {
  append(value: unknown): ColumnBuilder;
  finish(): Column;
}

/**
 * A column builder that accumulates values and produces a Column at finish().
 * Chainable append returns self.
 */
export class ColumnBuilderImpl implements ColumnBuilder {
  private values: unknown[] = [];
  private codec: Codec;

  constructor(type: string) {
    this.codec = getCodec(type);
  }

  /** Append a value. Returns self for chaining. */
  append(value: unknown): this {
    this.values.push(value);
    return this;
  }

  /** Finalize and return immutable Column. */
  finish(): Column {
    return this.codec.fromValues(this.values);
  }
}

/**
 * Create a column builder for the given ClickHouse type.
 *
 * @param type - ClickHouse type string (e.g., "UInt32", "Array(String)")
 */
export function makeBuilder(type: string): ColumnBuilder {
  return new ColumnBuilderImpl(type);
}

export interface Codec {
  /** ClickHouse type string this codec handles */
  readonly type: string;
  encode(col: Column, sizeHint?: number): Uint8Array;
  decode(reader: BufferReader, rows: number, state: DeserializerState): Column;
  fromValues(values: unknown[]): Column;
  builder(size: number): ColumnBuilder;
  zeroValue(): unknown;
  // Estimate bytes needed for this column type with given row count
  estimateSize(rows: number): number;
  // Nested types need to handle prefix writing/reading
  writePrefix?(writer: BufferWriter, col: Column): void;
  readPrefix?(reader: BufferReader): void;
  // Dense decoding (without sparse check) - used by readSparse
  decodeDense?(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): Column;
  // Read sparse/dense serialization kind bytes for this type tree.
  // Each nested type reads its kind byte (0=dense, 1=sparse) from the wire.
  readKinds(reader: BufferReader): SerializationNode;
}

/**
 * Base class for codecs that support sparse serialization.
 * Centralizes the sparse check pattern - subclasses implement decodeDense().
 */
export abstract class BaseCodec implements Codec {
  abstract readonly type: string;
  abstract encode(col: Column, sizeHint?: number): Uint8Array;
  abstract fromValues(values: unknown[]): Column;
  abstract builder(size: number): ColumnBuilder;
  abstract zeroValue(): unknown;
  abstract estimateSize(rows: number): number;
  abstract decodeDense(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): Column;

  decode(reader: BufferReader, rows: number, state: DeserializerState): Column {
    if (state.serNode.kind === SerializationKind.Sparse) {
      return readSparse(this, reader, rows, state);
    }
    return this.decodeDense(reader, rows, state);
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    return { kind, children: [] };
  }
}

function readSparse(
  codec: Codec,
  reader: BufferReader,
  rows: number,
  state: DeserializerState,
): Column {
  const node = state.serNode;
  const [initialTrailing, hasValueAfter] = state.sparseRuntime.get(node) || [
    0,
    false,
  ];

  let trailingDefaults = initialTrailing;
  let hasValueAfterDefaults = hasValueAfter;

  const indices: number[] = [];
  let totalRows = trailingDefaults;
  let tmpOffset = 0; // We don't support partial read requests yet, so tmpOffset is always 0
  let skippedValuesRows = 0;
  let first = true;

  if (hasValueAfterDefaults) {
    if (trailingDefaults >= tmpOffset) {
      indices.push(trailingDefaults - tmpOffset);
      tmpOffset = 0;
      first = false;
    } else {
      skippedValuesRows += 1;
      tmpOffset -= trailingDefaults + 1;
    }
    trailingDefaults = 0;
    totalRows += 1;
  }

  // Read offset stream: VarInts encode gaps between non-default values
  // Each VarInt = defaults before next non-default. END flag marks last entry.
  while (true) {
    let v = BigInt(reader.readVarInt64());
    const end = (v & Sparse.END_OF_GRANULE_FLAG) !== 0n;
    if (end) {
      v &= ~Sparse.END_OF_GRANULE_FLAG;
    }

    let groupSize = Number(v);
    let nextTotalRows = totalRows + groupSize;

    // Check if we've exceeded the requested rows
    if (nextTotalRows >= rows) {
      trailingDefaults = nextTotalRows - rows;
      hasValueAfterDefaults = !end;
      break;
    }

    // END flag with remaining defaults
    if (end) {
      hasValueAfterDefaults = false;
      trailingDefaults = groupSize;
      break;
    }

    // This VarInt represents a non-default value at position (startOfGroup + groupSize)
    const startOfGroup =
      !first && indices.length > 0 ? indices[indices.length - 1] + 1 : 0;
    if (groupSize >= tmpOffset) {
      indices.push(startOfGroup + groupSize - tmpOffset);
      tmpOffset = 0;
      first = false;
    } else {
      skippedValuesRows += 1;
      tmpOffset -= groupSize + 1;
    }

    trailingDefaults = 0;
    totalRows = nextTotalRows + 1;
  }

  state.sparseRuntime.set(node, [trailingDefaults, hasValueAfterDefaults]);

  const zero = codec.zeroValue();

  // Use decodeDense if available, otherwise fall back to decode with fresh state
  const decodeFn = (r: BufferReader, n: number) =>
    codec.decodeDense
      ? codec.decodeDense(r, n, defaultDeserializerState())
      : codec.decode(r, n, defaultDeserializerState());

  if (skippedValuesRows > 0) {
    decodeFn(reader, skippedValuesRows);
  }

  if (indices.length === 0) {
    return codec.fromValues(new Array(rows).fill(zero));
  }

  const values = decodeFn(reader, indices.length);

  // Materialize to dense column
  const resultValues = new Array(rows);
  for (let i = 0; i < rows; i++) resultValues[i] = zero;

  for (let i = 0; i < indices.length; i++) {
    const idx = indices[i];
    if (idx < rows) {
      resultValues[idx] = values.get(i);
    }
  }

  return codec.fromValues(resultValues);
}

class NumericCodec<T extends TypedArray> extends BaseCodec {
  readonly type: string;
  private Ctor: TypedArrayConstructor<T>;
  private converter?: (v: unknown) => number | bigint;
  constructor(
    type: string,
    Ctor: TypedArrayConstructor<T>,
    converter?: (v: unknown) => number | bigint,
  ) {
    super();
    this.type = type;
    this.Ctor = Ctor;
    this.converter = converter;
  }

  encode(col: Column): Uint8Array {
    // Fast path: DataColumn wrapping a TypedArray - zero-copy
    if (
      col instanceof DataColumn &&
      ArrayBuffer.isView(col.data) &&
      !(col.data instanceof DataView)
    ) {
      const data = col.data as TypedArray;
      return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    // Fallback: virtual columns (Nullable, Variant, etc.) - materialize via get()
    const len = col.length;
    const arr = new this.Ctor(len);
    for (let i = 0; i < len; i++) arr[i] = col.get(i) as any;
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    return new DataColumn(this.type, reader.readTypedArray(this.Ctor, rows));
  }

  fromValues(values: unknown[]): DataColumn<T> {
    const arr = new this.Ctor(values.length);
    if (this.converter) {
      for (let i = 0; i < values.length; i++)
        arr[i] = this.converter(values[i]) as any;
    } else {
      for (let i = 0; i < values.length; i++) arr[i] = values[i] as any;
    }
    return new DataColumn(this.type, arr);
  }

  builder(size: number): ColumnBuilder {
    const arr = new this.Ctor(size);
    const type = this.type;
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        arr[offset++] = (this.converter ? this.converter(v) : v) as any;
        return builder;
      },
      finish: () => new DataColumn(type, arr),
    };
    return builder;
  }

  zeroValue() {
    return 0;
  }
  estimateSize(rows: number) {
    return rows * this.Ctor.BYTES_PER_ELEMENT;
  }
}

function SimpleArrayBuilder(
  type: string,
  size: number,
  transform?: (v: unknown) => unknown,
): ColumnBuilder {
  const arr = new Array(size);
  let offset = 0;
  const builder: ColumnBuilder = {
    append: (v: unknown) => {
      arr[offset++] = transform ? transform(v) : v;
      return builder;
    },
    finish: () => new DataColumn(type, arr),
  };
  return builder;
}

/** Default fromValues implementation using builder - reduces duplication. */
function fromValuesViaBuilder(codec: Codec, values: unknown[]): Column {
  const b = codec.builder(values.length);
  for (let i = 0; i < values.length; i++) b.append(values[i]);
  return b.finish();
}

class StringCodec extends BaseCodec {
  readonly type = "String";

  encode(col: Column, sizeHint?: number): Uint8Array {
    const len = col.length;
    const writer = new BufferWriter(sizeHint ?? this.estimateSize(len));
    for (let i = 0; i < len; i++) {
      writer.writeString(String(col.get(i)));
    }
    return writer.finish();
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) values[i] = reader.readString();
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    return SimpleArrayBuilder(this.type, size, (v) => String(v ?? ""));
  }

  zeroValue() {
    return "";
  }
  estimateSize(rows: number) {
    return rows * 33;
  }
}

class UUIDCodec extends BaseCodec {
  readonly type = "UUID";

  encode(col: Column): Uint8Array {
    const len = col.length;
    const buf = new Uint8Array(len * UUIDConst.BYTE_SIZE);

    for (let i = 0; i < len; i++) {
      const u = String(col.get(i));
      const clean = u.replace(/-/g, "");
      const bytes = new Uint8Array(16);
      for (let j = 0; j < 16; j++)
        bytes[j] = parseInt(clean.substring(j * 2, j * 2 + 2), 16);

      // CH stores as: [low_64_reversed] [high_64_reversed]
      const off = i * 16;
      for (let j = 0; j < 8; j++) buf[off + j] = bytes[7 - j];
      for (let j = 0; j < 8; j++) buf[off + 8 + j] = bytes[15 - j];
    }
    return buf;
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    reader.ensureAvailable(rows * UUIDConst.BYTE_SIZE);
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const b = reader.buffer.subarray(reader.offset, reader.offset + 16);
      reader.offset += 16;

      const bytes = new Uint8Array(16);
      for (let j = 0; j < 8; j++) bytes[7 - j] = b[j];
      for (let j = 0; j < 8; j++) bytes[15 - j] = b[8 + j];

      const hex = Array.from(bytes)
        .map((x) => x.toString(16).padStart(2, "0"))
        .join("");
      values[i] =
        `${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}`;
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    return SimpleArrayBuilder(this.type, size, (v) => String(v ?? ""));
  }

  zeroValue() {
    return "00000000-0000-0000-0000-000000000000";
  }
  estimateSize(rows: number) {
    return rows * UUIDConst.BYTE_SIZE;
  }
}

class FixedStringCodec extends BaseCodec {
  readonly type: string;
  readonly len: number;
  constructor(len: number) {
    super();
    this.len = len;
    this.type = `FixedString(${len})`;
  }

  encode(col: Column): Uint8Array {
    const count = col.length;
    const buf = new Uint8Array(count * this.len);
    for (let i = 0; i < count; i++) {
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

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    reader.ensureAvailable(rows * this.len);
    const values: Uint8Array[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = reader.buffer.slice(reader.offset, reader.offset + this.len);
      reader.offset += this.len;
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    const len = this.len;
    const type = this.type;
    const result: Uint8Array[] = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        if (v instanceof Uint8Array) {
          result[offset++] = v;
        } else if (typeof v === "string") {
          const buf = new Uint8Array(len);
          const encoded = TEXT_ENCODER.encode(v);
          buf.set(encoded.subarray(0, len));
          result[offset++] = buf;
        } else {
          result[offset++] = new Uint8Array(len);
        }
        return builder;
      },
      finish: () => new DataColumn(type, result),
    };
    return builder;
  }

  zeroValue() {
    return new Uint8Array(this.len);
  }
  estimateSize(rows: number) {
    return rows * this.len;
  }
}

class BigIntCodec extends BaseCodec {
  readonly type: string;
  private byteSize: 16 | 32;
  private signed: boolean;

  constructor(type: string, byteSize: 16 | 32, signed: boolean) {
    super();
    this.type = type;
    this.byteSize = byteSize;
    this.signed = signed;
  }

  encode(col: Column): Uint8Array {
    const len = col.length;
    const buf = new Uint8Array(len * this.byteSize);
    const view = new DataView(buf.buffer);
    const writer = this.byteSize === 16 ? writeBigInt128 : writeBigInt256;
    for (let i = 0; i < len; i++) {
      writer(view, i * this.byteSize, BigInt(col.get(i) as any), this.signed);
    }
    return buf;
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    reader.ensureAvailable(rows * this.byteSize);
    const values: bigint[] = new Array(rows);
    const readFn = this.byteSize === 16 ? readBigInt128 : readBigInt256;
    for (let i = 0; i < rows; i++) {
      values[i] = readFn(reader.view, reader.offset, this.signed);
      reader.offset += this.byteSize;
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    return SimpleArrayBuilder(this.type, size, (v) => BigInt(v as any));
  }

  zeroValue() {
    return 0n;
  }
  estimateSize(rows: number) {
    return rows * this.byteSize;
  }
}

class DecimalCodec extends BaseCodec {
  readonly type: string;
  private byteSize: 4 | 8 | 16 | 32;
  private scale: number;

  constructor(type: string) {
    super();
    this.type = type;
    this.byteSize = decimalByteSize(type);
    this.scale = extractDecimalScale(type);
  }

  encode(col: Column): Uint8Array {
    const len = col.length;
    const buf = new Uint8Array(len * this.byteSize);
    const view = new DataView(buf.buffer);

    for (let i = 0; i < len; i++) {
      const v = col.get(i);
      let scaled: bigint;
      if (typeof v === "bigint") {
        scaled = v;
      } else if (typeof v === "string") {
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

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    reader.ensureAvailable(rows * this.byteSize);
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
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const scale = this.scale;
    return SimpleArrayBuilder(type, size, (v) => {
      if (typeof v === "string") return v;
      if (typeof v === "bigint") return formatScaledBigInt(v, scale);
      return String(v);
    });
  }

  zeroValue() {
    return formatScaledBigInt(0n, this.scale);
  }
  estimateSize(rows: number) {
    return rows * this.byteSize;
  }
}

class DateTime64Codec extends BaseCodec {
  readonly type: string;
  private precision: number;
  constructor(type: string, precision: number) {
    super();
    this.type = type;
    this.precision = precision;
  }

  encode(col: Column): Uint8Array {
    const len = col.length;
    const arr = new BigInt64Array(len);
    for (let i = 0; i < len; i++) {
      const v = col.get(i);
      if (v instanceof ClickHouseDateTime64) {
        arr[i] = v.ticks;
      } else if (typeof v === "bigint") {
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

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    const arr = reader.readTypedArray(BigInt64Array, rows);
    const values: ClickHouseDateTime64[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = new ClickHouseDateTime64(arr[i], this.precision);
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const precision = this.precision;
    const result: ClickHouseDateTime64[] = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: any) => {
        if (v instanceof ClickHouseDateTime64) {
          result[offset++] = v;
        } else if (v instanceof Date) {
          const ms = BigInt(v.getTime());
          const scale = 10n ** BigInt(Math.abs(precision - 3));
          const ticks = precision >= 3 ? ms * scale : ms / scale;
          result[offset++] = new ClickHouseDateTime64(ticks, precision);
        } else if (typeof v === "bigint") {
          result[offset++] = new ClickHouseDateTime64(v, precision);
        } else {
          result[offset++] = new ClickHouseDateTime64(0n, precision);
        }
        return builder;
      },
      finish: () => new DataColumn(type, result),
    };
    return builder;
  }

  zeroValue() {
    return new Date(0);
  }
  estimateSize(rows: number) {
    return rows * 8;
  }
}

// handles Date, Date32, DateTime (ms since epoch / multiplier)
class EpochCodec<
  T extends Uint16Array | Int32Array | Uint32Array,
> extends BaseCodec {
  readonly type: string;
  private Ctor: TypedArrayConstructor<T>;
  private multiplier: number;

  constructor(
    type: string,
    Ctor: TypedArrayConstructor<T>,
    multiplier: number,
  ) {
    super();
    this.type = type;
    this.Ctor = Ctor;
    this.multiplier = multiplier;
  }

  encode(col: Column): Uint8Array {
    const len = col.length;
    const arr = new this.Ctor(len);
    for (let i = 0; i < len; i++) {
      const v = col.get(i);
      arr[i] = Math.floor(
        new Date(v as any).getTime() / this.multiplier,
      ) as any;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    const arr = reader.readTypedArray(this.Ctor, rows);
    const values: Date[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      values[i] = new Date((arr[i] as number) * this.multiplier);
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const result: Date[] = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: any) => {
        if (v instanceof Date) {
          result[offset++] = v;
        } else if (typeof v === "number") {
          result[offset++] = new Date(v);
        } else {
          result[offset++] = new Date(0);
        }
        return builder;
      },
      finish: () => new DataColumn(type, result),
    };
    return builder;
  }

  zeroValue() {
    return new Date(0);
  }
  estimateSize(rows: number) {
    return rows * this.Ctor.BYTES_PER_ELEMENT;
  }
}

class IPv4Codec extends BaseCodec {
  readonly type = "IPv4";

  encode(col: Column): Uint8Array {
    const len = col.length;
    const arr = new Uint32Array(len);
    for (let i = 0; i < len; i++) {
      const v = String(col.get(i));
      const parts = v.split(".").map(Number);
      arr[i] =
        (parts[0] | (parts[1] << 8) | (parts[2] << 16) | (parts[3] << 24)) >>>
        0;
    }
    return new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength);
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    const arr = reader.readTypedArray(Uint32Array, rows);
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const v = arr[i];
      values[i] =
        `${v & 0xff}.${(v >> 8) & 0xff}.${(v >> 16) & 0xff}.${(v >> 24) & 0xff}`;
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    return SimpleArrayBuilder(this.type, size, (v) => String(v ?? ""));
  }

  zeroValue() {
    return "0.0.0.0";
  }
  estimateSize(rows: number) {
    return rows * 4;
  }
}

class IPv6Codec extends BaseCodec {
  readonly type = "IPv6";

  encode(col: Column): Uint8Array {
    const len = col.length;
    const result = new Uint8Array(len * IPv6Const.BYTE_SIZE);
    for (let i = 0; i < len; i++) {
      const v = String(col.get(i));
      const bytes = ipv6ToBytes(v);
      result.set(bytes, i * 16);
    }
    return result;
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    const values: string[] = new Array(rows);
    for (let i = 0; i < rows; i++) {
      const bytes = reader.readBytes(IPv6Const.BYTE_SIZE);
      values[i] = bytesToIpv6(bytes);
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    return fromValuesViaBuilder(this, values);
  }

  builder(size: number): ColumnBuilder {
    return SimpleArrayBuilder(this.type, size, (v) => String(v ?? ""));
  }

  zeroValue() {
    return "::";
  }
  estimateSize(rows: number) {
    return rows * IPv6Const.BYTE_SIZE;
  }
}

// When used as a column in Map/Tuple, inner codec's prefix needs to be handled
class ArrayCodec extends BaseCodec {
  readonly type: string;
  private inner: Codec;

  constructor(type: string, inner: Codec) {
    super();
    this.type = type;
    this.inner = inner;
  }

  writePrefix(writer: BufferWriter, col: Column) {
    const arr = col as ArrayColumn;
    this.inner.writePrefix?.(writer, arr.inner);
  }

  readPrefix(reader: BufferReader) {
    this.inner.readPrefix?.(reader);
  }

  encode(col: Column, sizeHint?: number): Uint8Array {
    const arr = col as ArrayColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);

    // Write offsets
    writer.write(
      new Uint8Array(
        arr.offsets.buffer,
        arr.offsets.byteOffset,
        arr.offsets.byteLength,
      ),
    );

    // Write inner data with estimated size
    const innerHint = this.inner.estimateSize(arr.inner.length);
    writer.write(this.inner.encode(arr.inner, innerHint));

    return writer.finish();
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): Column {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const totalCount = rows > 0 ? Number(offsets[rows - 1]) : 0;
    const childState = {
      ...state,
      serNode: state.serNode.children[0] ?? DENSE_LEAF,
    };
    const inner = this.inner.decode(reader, totalCount, childState);
    return new ArrayColumn(this.type, offsets, inner);
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
    return new ArrayColumn(this.type, offsets, this.inner.fromValues(allInner));
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const offsets = new BigUint64Array(size);
    const allInner: unknown[] = [];
    let offset = 0n;
    let rowIdx = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        const arr = v as unknown[];
        for (const item of arr) allInner.push(item);
        offset += BigInt(arr.length);
        offsets[rowIdx++] = offset;
        return builder;
      },
      finish: () =>
        new ArrayColumn(type, offsets, this.inner.fromValues(allInner)),
    };
    return builder;
  }

  zeroValue() {
    return [];
  }
  // 8 bytes per offset + assume average 5 elements per row
  estimateSize(rows: number) {
    return rows * 8 + this.inner.estimateSize(rows * 5);
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    return { kind, children: [this.inner.readKinds(reader)] };
  }
}

// Delegates prefix handling to inner codec
class NullableCodec extends BaseCodec {
  readonly type: string;
  private inner: Codec;

  constructor(type: string, inner: Codec) {
    super();
    this.type = type;
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

  encode(col: Column, sizeHint?: number): Uint8Array {
    const nc = col as NullableColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);
    writer.write(nc.nullFlags);
    const innerHint = this.inner.estimateSize(nc.inner.length);
    writer.write(this.inner.encode(nc.inner, innerHint));
    return writer.finish();
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): Column {
    const nullFlags = reader.readTypedArray(Uint8Array, rows);
    const childState = {
      ...state,
      serNode: state.serNode.children[0] ?? DENSE_LEAF,
    };
    const inner = this.inner.decode(reader, rows, childState);
    return new NullableColumn(this.type, nullFlags, inner);
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
    return new NullableColumn(
      this.type,
      nullFlags,
      this.inner.fromValues(innerValues),
    );
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const nullFlags = new Uint8Array(size);
    const innerValues: unknown[] = new Array(size);
    const zeroVal = this.inner.zeroValue();
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        if (v === null || v === undefined) {
          nullFlags[offset] = 1;
          innerValues[offset] = zeroVal;
        } else {
          innerValues[offset] = v;
        }
        offset++;
        return builder;
      },
      finish: () =>
        new NullableColumn(type, nullFlags, this.inner.fromValues(innerValues)),
    };
    return builder;
  }

  zeroValue() {
    return null;
  }
  // null flags (1 byte each) + inner data
  estimateSize(rows: number) {
    return rows + this.inner.estimateSize(rows);
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    return { kind, children: [this.inner.readKinds(reader)] };
  }
}

// LowCardinality stores a dictionary of unique values and indices into that dictionary.
// When wrapping Nullable(T), the dictionary stores T values (not Nullable(T)) and index 0
// is reserved for NULL. This avoids storing null flags per dictionary entry - nullness is
// encoded in the index itself.
class LowCardinalityCodec extends BaseCodec {
  readonly type: string;
  private inner: Codec;
  private dictCodec: Codec; // Codec to use for dictionary (may differ from inner for Nullable)

  constructor(type: string, inner: Codec) {
    super();
    this.type = type;
    this.inner = inner;
    // For Nullable inner types, dictionary stores unwrapped type (nulls use index 0)
    this.dictCodec =
      inner instanceof NullableCodec ? inner.getInnerCodec() : inner;
  }

  writePrefix(writer: BufferWriter) {
    writer.writeU64LE(LC.VERSION);
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8;
  }

  encode(col: Column, sizeHint?: number): Uint8Array {
    // LowCardinality encode builds dictionary from column values
    // This is row-oriented by nature - we need to scan values to find uniques
    const len = col.length;
    if (len === 0) return new Uint8Array(0);

    const hint = sizeHint ?? this.estimateSize(len);
    const writer = new BufferWriter(hint);
    const isNullable = this.inner instanceof NullableCodec;

    const dict = new Map<unknown, number>();
    const dictValues: unknown[] = [];
    const indices: number[] = [];

    // For Nullable types, index 0 is reserved for null
    if (isNullable) {
      dict.set(null, 0);
      dictValues.push(null); // Placeholder for null
    }

    for (let i = 0; i < len; i++) {
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

    let indexType: bigint = LC.INDEX_U8;
    let IndexArray: any = Uint8Array;
    if (dictValues.length > LC.INDEX_U8_MAX) {
      indexType = LC.INDEX_U16;
      IndexArray = Uint16Array;
    }
    if (dictValues.length > LC.INDEX_U16_MAX) {
      indexType = LC.INDEX_U32;
      IndexArray = Uint32Array;
    }

    // Flag + IndexType in lower 8 bits
    writer.writeU64LE(LC.FLAG_ADDITIONAL_KEYS | indexType);

    // Build dictionary column from unique values
    writer.writeU64LE(BigInt(dictValues.length));
    const dictHint = this.dictCodec.estimateSize(dictValues.length);
    writer.write(this.dictCodec.encode(this.dictCodec.fromValues(dictValues), dictHint));
    writer.writeU64LE(BigInt(col.length));
    writer.write(new Uint8Array(new IndexArray(indices).buffer));

    return writer.finish();
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    _state: DeserializerState,
  ): Column {
    if (rows === 0) return new DataColumn(this.type, []);

    const flags = reader.readU64LE();
    const indexType = Number(flags & LC.INDEX_TYPE_MASK);
    const isNullable = this.inner instanceof NullableCodec;

    const dictSize = Number(reader.readU64LE());

    // Dictionary values are never sparse
    const dict = this.dictCodec.decode(
      reader,
      dictSize,
      defaultDeserializerState(),
    );

    const count = Number(reader.readU64LE());

    let indices: TypedArray;
    if (indexType === Number(LC.INDEX_U8))
      indices = reader.readTypedArray(Uint8Array, count);
    else if (indexType === Number(LC.INDEX_U16))
      indices = reader.readTypedArray(Uint16Array, count);
    else if (indexType === Number(LC.INDEX_U32))
      indices = reader.readTypedArray(Uint32Array, count);
    else indices = reader.readTypedArray(BigUint64Array, count);

    // Expand dictionary to full column
    const values: unknown[] = new Array(count);
    for (let i = 0; i < count; i++) {
      const idx = Number(indices[i]);
      values[i] = isNullable && idx === 0 ? null : dict.get(idx);
    }
    return new DataColumn(this.type, values);
  }

  fromValues(values: unknown[]): Column {
    // LowCardinality is just storage optimization - pass through to inner
    return this.inner.fromValues(values);
  }

  builder(size: number): ColumnBuilder {
    const values = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        values[offset++] = v;
        return builder;
      },
      finish: () => this.inner.fromValues(values),
    };
    return builder;
  }

  zeroValue() {
    return this.inner.zeroValue();
  }

  // key for low cardinality dictionary map
  getDictKey(v: unknown): unknown {
    if (v === null || typeof v !== "object") return v;
    if (v instanceof Date) return v.getTime();
    if (v instanceof Uint8Array) {
      // FixedString - use hex encoding for stable key generation
      let s = "\0B:"; // prefix to distinguish from regular strings
      for (let i = 0; i < v.length; i++) {
        const byte = v[i];
        s += (byte >> 4).toString(16) + (byte & 0xf).toString(16);
      }
      return s;
    }
    // Stable stringification with sorted keys for objects
    if (typeof v === "object") {
      const keys = Object.keys(v as object).sort();
      return (
        "\0O:" +
        keys.map((k) => `${k}:${this.getDictKey((v as any)[k])}`).join(",")
      );
    }
    return v;
  }

  // Dictionary + indices (assume u16 indices, max 65536 unique values)
  estimateSize(rows: number) {
    const dictSize = Math.min(rows, 65536);
    return 8 + 8 + this.dictCodec.estimateSize(dictSize) + 8 + rows * 2;
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    return { kind, children: [this.inner.readKinds(reader)] };
  }
}

// Map is serialized as Array(Tuple(K, V))
// Prefixes are written at top level, not inside the data.
class MapCodec extends BaseCodec {
  readonly type: string;
  private keyCodec: Codec;
  private valCodec: Codec;

  constructor(type: string, keyCodec: Codec, valCodec: Codec) {
    super();
    this.type = type;
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

  encode(col: Column, sizeHint?: number): Uint8Array {
    const map = col as MapColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);
    writer.write(
      new Uint8Array(
        map.offsets.buffer,
        map.offsets.byteOffset,
        map.offsets.byteLength,
      ),
    );
    const keyHint = this.keyCodec.estimateSize(map.keys.length);
    const valHint = this.valCodec.estimateSize(map.values.length);
    writer.write(this.keyCodec.encode(map.keys, keyHint));
    writer.write(this.valCodec.encode(map.values, valHint));
    return writer.finish();
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): Column {
    const offsets = reader.readTypedArray(BigUint64Array, rows);
    const total = rows > 0 ? Number(offsets[rows - 1]) : 0;
    const keyState = {
      ...state,
      serNode: state.serNode.children[0] ?? DENSE_LEAF,
    };
    const valState = {
      ...state,
      serNode: state.serNode.children[1] ?? DENSE_LEAF,
    };
    const keys = this.keyCodec.decode(reader, total, keyState);
    const vals = this.valCodec.decode(reader, total, valState);
    return new MapColumn(
      this.type,
      offsets,
      keys,
      vals,
      reader.options?.mapAsArray ?? false,
    );
  }

  fromValues(values: unknown[]): MapColumn {
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    const offsets = new BigUint64Array(values.length);
    let offset = 0n;
    for (let i = 0; i < values.length; i++) {
      const m = values[i];
      if (m instanceof Map) {
        for (const [k, v] of m) {
          keys.push(k);
          vals.push(v);
        }
        offset += BigInt(m.size);
      } else if (Array.isArray(m)) {
        for (const pair of m) {
          if (Array.isArray(pair) && pair.length === 2) {
            keys.push(pair[0]);
            vals.push(pair[1]);
          }
        }
        offset += BigInt(m.length);
      } else if (typeof m === "object" && m !== null) {
        const entries = Object.entries(m);
        for (const [k, v] of entries) {
          keys.push(k);
          vals.push(v);
        }
        offset += BigInt(entries.length);
      }
      offsets[i] = offset;
    }
    return new MapColumn(
      this.type,
      offsets,
      this.keyCodec.fromValues(keys),
      this.valCodec.fromValues(vals),
    );
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const keys: unknown[] = [];
    const vals: unknown[] = [];
    const offsets = new BigUint64Array(size);
    let offset = 0n;
    let rowIdx = 0;
    const builder: ColumnBuilder = {
      append: (m: any) => {
        if (m instanceof Map) {
          for (const [k, v] of m) {
            keys.push(k);
            vals.push(v);
          }
          offset += BigInt(m.size);
        } else if (Array.isArray(m)) {
          for (const pair of m) {
            if (Array.isArray(pair) && pair.length === 2) {
              keys.push(pair[0]);
              vals.push(pair[1]);
            }
          }
          offset += BigInt(m.length);
        } else if (typeof m === "object" && m !== null) {
          const entries = Object.entries(m);
          for (const [k, v] of entries) {
            keys.push(k);
            vals.push(v);
          }
          offset += BigInt(entries.length);
        }
        offsets[rowIdx++] = offset;
        return builder;
      },
      finish: () =>
        new MapColumn(
          type,
          offsets,
          this.keyCodec.fromValues(keys),
          this.valCodec.fromValues(vals),
        ),
    };
    return builder;
  }

  zeroValue() {
    return new Map();
  }
  // 8 bytes per offset + assume average 3 entries per row
  estimateSize(rows: number) {
    const avgEntries = rows * 3;
    return (
      rows * 8 +
      this.keyCodec.estimateSize(avgEntries) +
      this.valCodec.estimateSize(avgEntries)
    );
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    return {
      kind,
      children: [
        this.keyCodec.readKinds(reader),
        this.valCodec.readKinds(reader),
      ],
    };
  }
}

class TupleCodec extends BaseCodec {
  readonly type: string;
  private elements: { name: string | null; codec: Codec }[];
  private isNamed: boolean;

  constructor(
    type: string,
    elements: { name: string | null; codec: Codec }[],
    isNamed: boolean,
  ) {
    super();
    this.type = type;
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

  encode(col: Column, sizeHint?: number): Uint8Array {
    const tuple = col as TupleColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);
    for (let i = 0; i < this.elements.length; i++) {
      const elemHint = this.elements[i].codec.estimateSize(
        tuple.columns[i].length,
      );
      writer.write(this.elements[i].codec.encode(tuple.columns[i], elemHint));
    }
    return writer.finish();
  }

  decodeDense(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): Column {
    const cols = this.elements.map((e, i) => {
      const childState = {
        ...state,
        serNode: state.serNode.children[i] ?? DENSE_LEAF,
      };
      return e.codec.decode(reader, rows, childState);
    });
    return new TupleColumn(
      this.type,
      this.elements.map((e) => ({ name: e.name })),
      cols,
      this.isNamed,
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
      this.type,
      this.elements.map((e) => ({ name: e.name })),
      columns,
      this.isNamed,
    );
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const builders = this.elements.map((e) => e.codec.builder(size));
    const builder: ColumnBuilder = {
      append: (tuple: any) => {
        for (let i = 0; i < this.elements.length; i++) {
          builders[i].append(
            this.isNamed ? tuple[this.elements[i].name!] : tuple[i],
          );
        }
        return builder;
      },
      finish: () =>
        new TupleColumn(
          type,
          this.elements.map((e) => ({ name: e.name })),
          builders.map((b) => b.finish()),
          this.isNamed,
        ),
    };
    return builder;
  }

  zeroValue() {
    return [];
  }
  // Sum of all element sizes
  estimateSize(rows: number) {
    return this.elements.reduce(
      (sum, e) => sum + e.codec.estimateSize(rows),
      0,
    );
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    const children: SerializationNode[] = [];
    for (const el of this.elements) {
      children.push(el.codec.readKinds(reader));
    }
    return { kind, children };
  }
}

class VariantCodec implements Codec {
  readonly type: string;
  private typeStrings: string[];
  private codecs: Codec[];
  constructor(type: string, typeStrings: string[], codecs: Codec[]) {
    this.type = type;
    this.typeStrings = typeStrings;
    this.codecs = codecs;
  }

  writePrefix(writer: BufferWriter) {
    writer.writeU64LE(Variant.MODE_BASIC);
  }

  readPrefix(reader: BufferReader) {
    reader.offset += 8; // Skip encoding mode flag
  }

  encode(col: Column, sizeHint?: number): Uint8Array {
    const variant = col as VariantColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);
    writer.write(variant.discriminators);
    for (let i = 0; i < this.codecs.length; i++) {
      const group = variant.groups.get(i);
      if (group) {
        const groupHint = this.codecs[i].estimateSize(group.length);
        writer.write(this.codecs[i].encode(group, groupHint));
      }
    }
    return writer.finish();
  }

  decode(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): VariantColumn {
    const discriminators = reader.readTypedArray(Uint8Array, rows);
    const { counts, indices } = countAndIndexDiscriminators(
      discriminators,
      Variant.NULL_DISCRIMINATOR,
    );
    const groups = decodeGroups(reader, this.codecs, counts, state);
    return new VariantColumn(this.type, discriminators, groups, indices);
  }

  fromValues(values: unknown[]): VariantColumn {
    const discriminators = new Uint8Array(values.length);
    const variantValues: unknown[][] = this.codecs.map(() => []);

    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v === null) {
        discriminators[i] = Variant.NULL_DISCRIMINATOR;
      } else if (
        Array.isArray(v) &&
        v.length === 2 &&
        typeof v[0] === "number"
      ) {
        const disc = v[0] as number;
        if (disc < 0 || disc >= this.codecs.length) {
          throw new Error(
            `Invalid Variant discriminator ${disc}, expected 0-${this.codecs.length - 1}`,
          );
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

    return new VariantColumn(this.type, discriminators, groups);
  }

  builder(size: number): ColumnBuilder {
    const type = this.type;
    const values = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        values[offset++] = v;
        return builder;
      },
      finish: () =>
        new VariantColumn(type, ...this.buildVariantFromValues(values)),
    };
    return builder;
  }

  private buildVariantFromValues(
    values: unknown[],
  ): [Uint8Array, Map<number, Column>] {
    const discriminators = new Uint8Array(values.length);
    const variantValues: unknown[][] = this.codecs.map(() => []);

    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (v === null) {
        discriminators[i] = Variant.NULL_DISCRIMINATOR;
      } else if (
        Array.isArray(v) &&
        v.length === 2 &&
        typeof v[0] === "number"
      ) {
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

    return [discriminators, groups];
  }

  zeroValue() {
    return null;
  }
  // Discriminators + variant data (assume even distribution)
  estimateSize(rows: number) {
    const perVariant = Math.ceil(rows / this.codecs.length);
    return (
      rows + this.codecs.reduce((sum, c) => sum + c.estimateSize(perVariant), 0)
    );
  }

  findVariantIndex(value: unknown, types: string[]): number {
    // Simple heuristic to match value to variant type
    for (let i = 0; i < types.length; i++) {
      const t = types[i];
      if (t === "String" && typeof value === "string") return i;
      if ((t === "Int64" || t === "UInt64") && typeof value === "bigint")
        return i;
      if (
        (t.startsWith("Int") ||
          t.startsWith("UInt") ||
          t.startsWith("Float")) &&
        typeof value === "number"
      )
        return i;
      if (t === "Bool" && typeof value === "boolean") return i;
      if (
        (t === "Date" || t === "DateTime" || t.startsWith("DateTime64")) &&
        value instanceof Date
      )
        return i;
      if (t.startsWith("Array") && Array.isArray(value)) return i;
      if (
        t.startsWith("Map") &&
        (value instanceof Map || (typeof value === "object" && value !== null))
      )
        return i;
    }
    return 0; // default to first type
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    const children: SerializationNode[] = [];
    for (const codec of this.codecs) {
      children.push(codec.readKinds(reader));
    }
    return { kind, children };
  }
}

class DynamicCodec implements Codec {
  readonly type = "Dynamic";
  private types: string[] = [];
  private codecs: Codec[] = [];

  writePrefix(writer: BufferWriter, col: Column) {
    const dyn = col as DynamicColumn;
    this.types = dyn.types;
    this.codecs = this.types.map((t) => getCodec(t));

    writer.writeU64LE(Dynamic.VERSION_V3);
    writer.writeVarint(this.types.length);
    for (const t of this.types) writer.writeString(t);

    for (let i = 0; i < this.types.length; i++) {
      const group = dyn.groups.get(i);
      if (group) this.codecs[i].writePrefix?.(writer, group);
    }
  }

  readPrefix(reader: BufferReader) {
    const version = reader.readU64LE();
    if (version !== Dynamic.VERSION_V3)
      throw new Error(`Dynamic: only V3 supported, got V${version}`);

    const count = reader.readVarint();
    this.types = [];
    for (let i = 0; i < count; i++) this.types.push(reader.readString());
    this.codecs = this.types.map((t) => getCodec(t));

    for (const c of this.codecs) c.readPrefix?.(reader);
  }

  encode(col: Column, sizeHint?: number): Uint8Array {
    const dyn = col as DynamicColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);

    // Write discriminators as-is (already the right type)
    writer.write(
      new Uint8Array(
        dyn.discriminators.buffer,
        dyn.discriminators.byteOffset,
        dyn.discriminators.byteLength,
      ),
    );

    for (let i = 0; i < this.codecs.length; i++) {
      const group = dyn.groups.get(i);
      if (group) {
        const groupHint = this.codecs[i].estimateSize(group.length);
        writer.write(this.codecs[i].encode(group, groupHint));
      }
    }
    return writer.finish();
  }

  decode(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): DynamicColumn {
    const nullDisc = this.types.length;
    const discLimit = nullDisc + 1;

    let discriminators: DiscriminatorArray;
    if (discLimit <= 256)
      discriminators = reader.readTypedArray(Uint8Array, rows);
    else if (discLimit <= 65536)
      discriminators = reader.readTypedArray(Uint16Array, rows);
    else discriminators = reader.readTypedArray(Uint32Array, rows);

    const { counts, indices } = countAndIndexDiscriminators(
      discriminators,
      nullDisc,
    );
    const groups = decodeGroups(reader, this.codecs, counts, state);
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
      discriminators[i] =
        v === null ? nullDisc : typeOrder.indexOf(this.guessType(v));
    }

    const groups = new Map<number, Column>();
    for (let ti = 0; ti < typeOrder.length; ti++) {
      const codec = getCodec(typeOrder[ti]);
      groups.set(ti, codec.fromValues(typeMap.get(typeOrder[ti])!));
    }

    return new DynamicColumn(typeOrder, discriminators, groups);
  }

  builder(size: number): ColumnBuilder {
    const values = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        values[offset++] = v;
        return builder;
      },
      finish: () => this.fromValues(values),
    };
    return builder;
  }

  zeroValue() {
    return null;
  }
  // Discriminators + type data (assume most values are strings)
  estimateSize(rows: number) {
    // Dynamic can have variable discriminator size but usually 1-2 bytes + data
    return (
      rows * 2 +
      this.codecs.reduce(
        (sum, c) => sum + c.estimateSize(Math.ceil(rows / 3)),
        0,
      )
    );
  }

  guessType(value: unknown): string {
    if (value === null) return "String";
    if (typeof value === "string") return "String";
    if (typeof value === "number")
      return Number.isInteger(value) ? "Int64" : "Float64";
    if (typeof value === "bigint") return "Int64";
    if (typeof value === "boolean") return "Bool";
    if (value instanceof Date) return "DateTime64(3)";
    if (Array.isArray(value))
      return value.length
        ? `Array(${this.guessType(value[0])})`
        : "Array(String)";
    if (typeof value === "object") return "Map(String,String)";
    return "String";
  }

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    const children: SerializationNode[] = [];
    for (const codec of this.codecs) {
      children.push(codec.readKinds(reader));
    }
    return { kind, children };
  }
}

class JsonCodec implements Codec {
  readonly type = "JSON";
  private paths: string[] = [];
  private pathCodecs: Map<string, DynamicCodec> = new Map();

  writePrefix(writer: BufferWriter, col: Column) {
    const json = col as JsonColumn;
    this.paths = json.paths;
    writer.writeU64LE(JSON.VERSION_V3);
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
    const ver = reader.readU64LE();
    if (ver !== JSON.VERSION_V3)
      throw new Error(`JSON: only V3 supported, got V${ver}`);

    const count = reader.readVarint();
    this.paths = [];
    for (let i = 0; i < count; i++) this.paths.push(reader.readString());

    for (const path of this.paths) {
      const codec = new DynamicCodec();
      codec.readPrefix(reader);
      this.pathCodecs.set(path, codec);
    }
  }

  encode(col: Column, sizeHint?: number): Uint8Array {
    const json = col as JsonColumn;
    const hint = sizeHint ?? this.estimateSize(col.length);
    const writer = new BufferWriter(hint);
    for (const path of this.paths) {
      const pathCol = json.pathColumns.get(path)!;
      const pathCodec = this.pathCodecs.get(path)!;
      const pathHint = pathCodec.estimateSize(pathCol.length);
      writer.write(pathCodec.encode(pathCol, pathHint));
    }
    return writer.finish();
  }

  decode(
    reader: BufferReader,
    rows: number,
    state: DeserializerState,
  ): JsonColumn {
    const pathColumns = new Map<string, DynamicColumn>();
    for (let i = 0; i < this.paths.length; i++) {
      const p = this.paths[i];
      // JSON paths are encoded as Dynamic columns. We use the path index as the path component.
      const childState = {
        ...state,
        serNode: state.serNode.children[i] ?? DENSE_LEAF,
      };
      pathColumns.set(
        p,
        this.pathCodecs.get(p)!.decode(reader, rows, childState),
      );
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
        pathValues[i] =
          obj && typeof obj === "object" ? (obj[path] ?? null) : null;
      }
      pathColumns.set(path, dynCodec.fromValues(pathValues));
    }

    return new JsonColumn(paths, pathColumns, values.length);
  }

  builder(size: number): ColumnBuilder {
    const values = new Array(size);
    let offset = 0;
    const builder: ColumnBuilder = {
      append: (v: unknown) => {
        values[offset++] = v;
        return builder;
      },
      finish: () => this.fromValues(values),
    };
    return builder;
  }

  zeroValue() {
    return {};
  }
  // JSON columns have per-path Dynamic columns; estimate is sum of path estimates
  // Since we don't know paths until readPrefix, use Dynamic's estimate per expected path
  estimateSize(rows: number) {
    return rows * 32;
  } // Conservative: ~32 bytes per row

  readKinds(reader: BufferReader): SerializationNode {
    const kind = reader.readU8();
    const children: SerializationNode[] = [];
    for (const pathCodec of this.pathCodecs.values()) {
      children.push(pathCodec.readKinds(reader));
    }
    return { kind, children };
  }
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
  if (type.startsWith("Nullable"))
    return new NullableCodec(type, getCodec(extractTypeArgs(type)));
  if (type.startsWith("Array"))
    return new ArrayCodec(type, getCodec(extractTypeArgs(type)));
  if (type.startsWith("LowCardinality"))
    return new LowCardinalityCodec(type, getCodec(extractTypeArgs(type)));
  if (type.startsWith("Map")) {
    const [k, v] = parseTypeList(extractTypeArgs(type));
    return new MapCodec(type, getCodec(k), getCodec(v));
  }
  if (type.startsWith("Tuple")) {
    const args = parseTupleElements(extractTypeArgs(type));
    const isNamed = args[0].name !== null;
    return new TupleCodec(
      type,
      args.map((a) => ({ name: a.name, codec: getCodec(a.type) })),
      isNamed,
    );
  }
  // Nested is syntactic sugar for Array(Tuple(...))
  // e.g., Nested(id UInt64, val String) -> Array(Tuple(UInt64, String))
  if (type.startsWith("Nested")) {
    const args = parseTupleElements(extractTypeArgs(type));
    const tupleType = `Tuple(${args.map((a) => `${a.name} ${a.type}`).join(", ")})`;
    const tupleCodec = new TupleCodec(
      tupleType,
      args.map((a) => ({ name: a.name, codec: getCodec(a.type) })),
      true,
    );
    return new ArrayCodec(type, tupleCodec);
  }
  if (type.startsWith("Variant")) {
    const innerTypes = parseTypeList(extractTypeArgs(type));
    return new VariantCodec(type, innerTypes, innerTypes.map(getCodec));
  }
  if (type === "Dynamic") return new DynamicCodec();
  if (type === "JSON" || type.startsWith("JSON")) return new JsonCodec();

  if (type.startsWith("FixedString"))
    return new FixedStringCodec(parseInt(extractTypeArgs(type)));

  if (type.startsWith("DateTime64")) {
    const precisionMatch = type.match(/DateTime64\((\d+)/);
    const precision = precisionMatch ? parseInt(precisionMatch[1], 10) : 3;
    return new DateTime64Codec(type, precision);
  }

  // Geo Types
  if (type === "Point")
    return new TupleCodec(
      type,
      [
        { name: null, codec: getCodec("Float64") },
        { name: null, codec: getCodec("Float64") },
      ],
      false,
    );
  if (type === "Ring") return new ArrayCodec(type, getCodec("Point"));
  if (type === "Polygon") return new ArrayCodec(type, getCodec("Ring"));
  if (type === "MultiPolygon") return new ArrayCodec(type, getCodec("Polygon"));

  switch (type) {
    case "UInt8":
      return new NumericCodec(type, Uint8Array);
    case "Int8":
      return new NumericCodec(type, Int8Array);
    case "UInt16":
      return new NumericCodec(type, Uint16Array);
    case "Int16":
      return new NumericCodec(type, Int16Array);
    case "UInt32":
      return new NumericCodec(type, Uint32Array);
    case "Int32":
      return new NumericCodec(type, Int32Array);
    case "UInt64":
      return new NumericCodec(type, BigUint64Array, (v: unknown) =>
        BigInt(v as any),
      );
    case "Int64":
      return new NumericCodec(type, BigInt64Array, (v: unknown) =>
        BigInt(v as any),
      );
    case "Float32":
      return new NumericCodec(type, Float32Array);
    case "Float64":
      return new NumericCodec(type, Float64Array);
    case "Bool":
      return new NumericCodec(type, Uint8Array, (v) => (v ? 1 : 0));
    case "Date":
      return new EpochCodec(type, Uint16Array, MS_PER_DAY);
    case "Date32":
      return new EpochCodec(type, Int32Array, MS_PER_DAY);
    case "DateTime":
      return new EpochCodec(type, Uint32Array, MS_PER_SECOND);
    case "String":
      return new StringCodec();
    case "UUID":
      return new UUIDCodec();
    case "IPv4":
      return new IPv4Codec();
    case "IPv6":
      return new IPv6Codec();
    case "Int128":
      return new BigIntCodec(type, 16, true);
    case "UInt128":
      return new BigIntCodec(type, 16, false);
    case "Int256":
      return new BigIntCodec(type, 32, true);
    case "UInt256":
      return new BigIntCodec(type, 32, false);
  }

  if (type.startsWith("Enum"))
    return type.startsWith("Enum8")
      ? new NumericCodec(type, Int8Array)
      : new NumericCodec(type, Int16Array);

  // Decimal types
  if (type.startsWith("Decimal")) return new DecimalCodec(type);

  throw new Error(`Unknown type: ${type}`);
}

// Extracts the content between the outermost parentheses: "Array(Int32)" → "Int32"
function extractTypeArgs(type: string): string {
  return type.substring(type.indexOf("(") + 1, type.lastIndexOf(")"));
}