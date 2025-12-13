/**
 * RowBinary encoder/decoder for ClickHouse
 * Refactored to use pre-compiled Codecs for performance.
 */

// ============================================================================
// Types & Constants
// ============================================================================

export type ScalarType =
  | "Int8"
  | "Int16"
  | "Int32"
  | "Int64"
  | "UInt8"
  | "UInt16"
  | "UInt32"
  | "UInt64"
  | "Float32"
  | "Float64"
  | "String"
  | "Bool"
  | "Date"
  | "DateTime"
  | "UUID"
  | "IPv4"
  | "IPv6";

export type ColumnType = string;

export interface ColumnDef {
  name: string;
  type: ColumnType;
}

export interface DecodeResult {
  columns: ColumnDef[];
  rows: unknown[][];
}

export interface DecodeOptions {
  /** Decode Map types as Array<[K, V]> instead of Map<K, V> to preserve duplicate keys */
  mapAsArray?: boolean;
}

export interface Cursor {
  offset: number;
  options?: DecodeOptions;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// 128-bit constants
const INT128_MAX = (1n << 127n) - 1n;
const INT128_MIN = -(1n << 127n);

// TypedArray mapping for fast paths
const TYPED_ARRAYS: Record<
  string,
  {
    new(
      buffer: ArrayBuffer,
      byteOffset: number,
      length: number,
    ): ArrayBufferView;
    BYTES_PER_ELEMENT: number;
  }
> = {
  Int8: Int8Array,
  UInt8: Uint8Array,
  Int16: Int16Array,
  UInt16: Uint16Array,
  Int32: Int32Array,
  UInt32: Uint32Array,
  Int64: BigInt64Array,
  UInt64: BigUint64Array,
  Float32: Float32Array,
  Float64: Float64Array,
};

// NaN wrapper classes to preserve IEEE 754 bit patterns during round-trips.
//
// Problem: JavaScript's DataView.setFloat32/setFloat64 canonicalize all NaN values to a single
// "quiet NaN" representation (0x7FC00000 for float32). IEEE 754 defines many valid NaN bit
// patterns - signaling NaNs have bit 22 clear, quiet NaNs have it set. ClickHouse's
// generateRandom() produces signaling NaNs, which get silently converted:
//
//   Signaling NaN: 0xFF8C0839 (bit 22 = 0)
//   After JS:      0xFFCC0839 (bit 22 = 1) ← canonicalized to quiet NaN
//
// Solution: Detect NaN on decode and store raw bytes. On encode, copy bytes directly instead
// of using setFloat32/setFloat64. The wrapper provides NaN semantics via valueOf() so
// arithmetic and comparisons work as expected.
export class Float32NaN {
  readonly bytes: Uint8Array;
  constructor(bytes: Uint8Array) { this.bytes = bytes; }
  valueOf(): number { return NaN; }
  toString(): string { return "NaN"; }
  toJSON(): null { return null; }
  [Symbol.toPrimitive](): number { return NaN; }
}

export class Float64NaN {
  readonly bytes: Uint8Array;
  constructor(bytes: Uint8Array) { this.bytes = bytes; }
  valueOf(): number { return NaN; }
  toString(): string { return "NaN"; }
  toJSON(): null { return null; }
  [Symbol.toPrimitive](): number { return NaN; }
}

// ============================================================================
// Encoder Class
// ============================================================================

export class RowBinaryEncoder {
  buffer: Uint8Array;
  view: DataView;
  offset = 0;

  constructor(initialSize = 64 * 1024) {
    this.buffer = new Uint8Array(initialSize);
    this.view = new DataView(this.buffer.buffer);
  }

  ensure(needed: number): void {
    if (this.offset + needed <= this.buffer.length) return;
    const newSize = Math.max(this.buffer.length * 2, this.offset + needed);
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
    this.view = new DataView(this.buffer.buffer);
  }

  finish(): Uint8Array {
    return this.buffer.subarray(0, this.offset);
  }

  // Primitives - manual byte writing for speed on small ints
  u8(val: number) {
    this.ensure(1);
    this.buffer[this.offset++] = val;
  }

  // JavaScript automatically converts signed integers (val) to their unsigned 8-bit
  // representation when assigned to this.buffer (a Uint8Array) so we can use the same method.
  i8(val: number) {
    this.u8(val);
  }

  u16(val: number) {
    this.ensure(2);
    this.buffer[this.offset++] = val;
    this.buffer[this.offset++] = val >> 8;
  }

  i16(val: number) {
    this.u16(val);
  }

  u32(val: number) {
    this.ensure(4);
    this.buffer[this.offset++] = val;
    this.buffer[this.offset++] = val >> 8;
    this.buffer[this.offset++] = val >> 16;
    this.buffer[this.offset++] = val >>> 24;
  }

  i32(val: number) {
    this.u32(val);
  }

  u64(val: bigint) {
    this.ensure(8);
    this.view.setBigUint64(this.offset, val, true);
    this.offset += 8;
  }

  i64(val: bigint) {
    this.ensure(8);
    this.view.setBigInt64(this.offset, val, true);
    this.offset += 8;
  }

  f32(val: number) {
    this.ensure(4);
    this.view.setFloat32(this.offset, val, true);
    this.offset += 4;
  }

  f64(val: number) {
    this.ensure(8);
    this.view.setFloat64(this.offset, val, true);
    this.offset += 8;
  }

  // LEB128 for uint32 values (max 5 bytes)
  leb128(value: number) {
    this.ensure(5); // ensure at least 5 bytes of capacity for worst case

    this.offset += this.leb128At(value, this.offset);
  }

  // returns number of bytes written
  private leb128At(value: number, bytePosition: number): number {
    if (value < 128) {
      this.buffer[bytePosition] = value;
      return 1;
    }

    const start = bytePosition;

    do {
      const more = value > 0x7f ? 0x80 : 0; // continuation bit signalling that there are more bytes to write after this one
      this.buffer[bytePosition++] = (value & 0x7f) | more;
      value >>>= 7;
    } while (value !== 0);

    return bytePosition - start;
  }

  string(value: string | Uint8Array) {
    if (value instanceof Uint8Array) {
      this.bytes(value);
      return;
    }

    // Estimate size: max 3 bytes per char + 5 bytes length
    const maxLen = value.length * 3 + 5;
    this.ensure(maxLen);

    // Write placeholder for length
    const lenOffset = this.offset;
    this.offset += 1;

    const { written } = textEncoder.encodeInto(
      value,
      this.buffer.subarray(this.offset),
    );

    if (written! <= 127) {
      this.buffer[lenOffset] = written!;
      this.offset += written!;
    } else {
      // Need multi-byte LEB128 - shift data
      const lenBytes = leb128Size(written!);
      if (lenBytes > 1) {
        this.buffer.copyWithin(
          lenOffset + lenBytes,
          lenOffset + 1,
          this.offset + written!,
        );
      }

      const bytesWrittenForLen = this.leb128At(written!, lenOffset);

      this.offset = lenOffset + bytesWrittenForLen + written!;
    }
  }

  // Writes length-prefixed bytes
  bytes(value: Uint8Array) {
    this.leb128(value.length);
    this.raw(value);
  }

  // Writes raw bytes without length prefix
  raw(value: Uint8Array) {
    this.ensure(value.length);
    this.buffer.set(value, this.offset);
    this.offset += value.length;
  }
}

function leb128Size(value: number): number {
  // Math.clz - count leading zeros in binary representation. Optimizes to single instruction on modern CPUs.
  const bits = 32 - Math.clz32(value | 1);
  return Math.ceil(bits / 7);
}

// ============================================================================
// StreamingReader - Buffer manager for streaming decode
// ============================================================================

/**
 * StreamingReader accumulates chunks from an AsyncIterable and provides
 * buffer slices for sync decode with retry-on-underflow semantics.
 */
class StreamingReader {
  private buffer: Uint8Array;
  private bufferLen = 0;  // valid bytes in buffer
  private offset = 0;     // current read position
  private source: AsyncIterator<Uint8Array>;
  private done = false;
  options?: DecodeOptions;

  constructor(chunks: AsyncIterable<Uint8Array>, initialSize = 64 * 1024) {
    this.buffer = new Uint8Array(initialSize);
    this.source = chunks[Symbol.asyncIterator]();
  }

  /** Get slice of valid data from current position for decode */
  getSlice(): Uint8Array {
    return this.buffer.subarray(this.offset, this.bufferLen);
  }

  /** Get DataView for the valid slice */
  getView(): DataView {
    const slice = this.getSlice();
    return new DataView(slice.buffer, slice.byteOffset, slice.byteLength);
  }

  /** Advance position after successful decode */
  advance(n: number): void {
    this.offset += n;
  }

  /** Check if more data might be available */
  hasMore(): boolean {
    return this.offset < this.bufferLen || !this.done;
  }

  /** Available bytes in buffer */
  available(): number {
    return this.bufferLen - this.offset;
  }

  /** Pull more data from source. Returns false if EOF. */
  async pullMore(): Promise<boolean> {
    if (this.done) return false;
    const { done, value } = await this.source.next();
    if (done) {
      this.done = true;
      return false;
    }
    this.appendChunk(value);
    return true;
  }

  /** Ensure at least n bytes available, pulling if needed */
  async ensure(n: number): Promise<boolean> {
    while (this.available() < n && !this.done) {
      await this.pullMore();
    }
    return this.available() >= n;
  }

  private appendChunk(chunk: Uint8Array): void {
    const needed = this.available() + chunk.length;

    if (needed > this.buffer.length - this.offset) {
      // Allocate new buffer (don't reuse - old buffer may have TypedArray views)
      const newSize = Math.max(this.buffer.length * 2, needed);
      const newBuffer = new Uint8Array(newSize);
      newBuffer.set(this.buffer.subarray(this.offset, this.bufferLen));
      this.buffer = newBuffer;
      this.bufferLen = this.available();
      this.offset = 0;
    }

    this.buffer.set(chunk, this.bufferLen);
    this.bufferLen += chunk.length;
  }

}

// ============================================================================
// Codec Interface & Implementation
// ============================================================================

interface Codec {
  encode(enc: RowBinaryEncoder, value: unknown): void;
  decode(view: DataView, buffer: Uint8Array, cursor: Cursor): unknown;
}

const cache = new Map<string, Codec>();

export function createCodec(type: string): Codec {
  if (cache.has(type)) return cache.get(type)!;

  const codec = createCodecImpl(type);
  // Cache simple types only to avoid memory leaks with unique complex types if that ever happens,
  // though for column types it's usually bounded.
  // For now, caching everything as unique schema count is low.
  cache.set(type, codec);
  return codec;
}

function createCodecImpl(type: string): Codec {
  // 1. Simple Scalar Codecs
  if (SCALAR_CODECS[type]) return SCALAR_CODECS[type];

  // 2. Complex Types
  if (type.startsWith('Nullable(')) return new NullableCodec(type.slice(9, -1))
  if (type.startsWith('LowCardinality(')) return createCodec(type.slice(15, -1))
  if (type.startsWith('Array(')) return new ArrayCodec(type.slice(6, -1))
  if (type.startsWith('Nested(')) return new ArrayCodec(`Tuple(${type.slice(7, -1)})`)
  if (type.startsWith('Map(')) return new MapCodec(type.slice(4, -1))
  if (type.startsWith("Tuple(")) return new TupleCodec(type.slice(6, -1));
  if (type.startsWith("FixedString("))
    return new FixedStringCodec(parseInt(type.slice(12, -1), 10));
  if (type.startsWith("DateTime64")) return new DateTime64Codec(type);
  if (type.startsWith("Decimal")) return new DecimalCodec(type);
  if (type.startsWith("Enum"))
    return type.startsWith("Enum8") ? SCALAR_CODECS.Int8 : SCALAR_CODECS.Int16;

  if (type === "Nothing") return new NothingCodec();
  if (type === "Date32") return new Date32Codec();
  if (type.startsWith("JSON") || type === "Object('json')")
    return new JsonCodec();
  if (type === "Dynamic") return new DynamicCodec();
  if (type.startsWith("Variant(")) return new VariantCodec(type.slice(8, -1));

  throw new Error(`Unknown or unsupported type: ${type}`);
}

// --- Scalar Codecs ---

const SCALAR_CODECS: Record<string, Codec> = {
  UInt8: {
    encode: (e, v) => e.u8(v as number),
    decode: (v, _, c) => v.getUint8(c.offset++),
  },
  Int8: {
    encode: (e, v) => e.i8(v as number),
    decode: (v, _, c) => v.getInt8(c.offset++),
  },
  UInt16: {
    encode: (e, v) => e.u16(v as number),
    decode: (v, _, c) => {
      const r = v.getUint16(c.offset, true);
      c.offset += 2;
      return r;
    },
  },
  Int16: {
    encode: (e, v) => e.i16(v as number),
    decode: (v, _, c) => {
      const r = v.getInt16(c.offset, true);
      c.offset += 2;
      return r;
    },
  },
  UInt32: {
    encode: (e, v) => e.u32(v as number),
    decode: (v, _, c) => {
      const r = v.getUint32(c.offset, true);
      c.offset += 4;
      return r;
    },
  },
  Int32: {
    encode: (e, v) => e.i32(v as number),
    decode: (v, _, c) => {
      const r = v.getInt32(c.offset, true);
      c.offset += 4;
      return r;
    },
  },
  UInt64: {
    encode: (e, v) => e.u64(BigInt(v as any)),
    decode: (v, _, c) => {
      const r = v.getBigUint64(c.offset, true);
      c.offset += 8;
      return r;
    },
  },
  Int64: {
    encode: (e, v) => e.i64(BigInt(v as any)),
    decode: (v, _, c) => {
      const r = v.getBigInt64(c.offset, true);
      c.offset += 8;
      return r;
    },
  },
  Float32: {
    encode: (e, v) => {
      e.ensure(4);
      if (v instanceof Float32NaN) {
        e.buffer.set(v.bytes, e.offset);
      } else {
        e.view.setFloat32(e.offset, v as number, true);
      }
      e.offset += 4;
    },
    decode: (_, b, c) => {
      checkBounds(b, c, 4);
      const bytes = b.subarray(c.offset, c.offset + 4);
      const view = new DataView(b.buffer, b.byteOffset + c.offset, 4);
      const val = view.getFloat32(0, true);
      c.offset += 4;
      if (Number.isNaN(val)) {
        return new Float32NaN(bytes.slice());
      }
      return val;
    },
  },
  Float64: {
    encode: (e, v) => {
      e.ensure(8);
      if (v instanceof Float64NaN) {
        e.buffer.set(v.bytes, e.offset);
      } else {
        e.view.setFloat64(e.offset, v as number, true);
      }
      e.offset += 8;
    },
    decode: (_, b, c) => {
      checkBounds(b, c, 8);
      const bytes = b.subarray(c.offset, c.offset + 8);
      const view = new DataView(b.buffer, b.byteOffset + c.offset, 8);
      const val = view.getFloat64(0, true);
      c.offset += 8;
      if (Number.isNaN(val)) {
        return new Float64NaN(bytes.slice());
      }
      return val;
    },
  },
  Bool: {
    encode: (e, v) => e.u8(v ? 1 : 0),
    decode: (v, _, c) => v.getUint8(c.offset++) !== 0,
  },
  String: {
    encode: (e, v) => e.string(v as string | Uint8Array),
    decode: (_, b, c) => readString(b, c),
  },
  Date: {
    encode: (e, v) => e.u16(Math.floor((v instanceof Date ? v : new Date(v as string)).getTime() / 86400000)),
    decode: (v, _, c) => { const r = new Date(v.getUint16(c.offset, true) * 86400000); c.offset += 2; return r }
  },
  DateTime: {
    encode: (e, v) => e.u32(Math.floor((v instanceof Date ? v : new Date(v as string)).getTime() / 1000)),
    decode: (v, _, c) => { const r = new Date(v.getUint32(c.offset, true) * 1000); c.offset += 4; return r }
  },
  UUID: {
    encode: (e, v) => {
      e.ensure(16);
      const hex = (v as string).replace(/-/g, "");
      for (let i = 0; i < 8; i++) {
        e.buffer[e.offset + 7 - i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
        e.buffer[e.offset + 15 - i] = parseInt(
          hex.slice(16 + i * 2, 16 + i * 2 + 2),
          16,
        );
      }
      e.offset += 16;
    },
    decode: (_, b, c) => {
      checkBounds(b, c, 16);
      const bytes = b.subarray(c.offset, c.offset + 16);
      const high = Array.from(bytes.subarray(0, 8)).reverse();
      const low = Array.from(bytes.subarray(8, 16)).reverse();
      const hex = [...high, ...low]
        .map((x) => x.toString(16).padStart(2, "0"))
        .join("");
      c.offset += 16;
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    },
  },
  IPv4: {
    encode: (e, v) => {
      const parts = (v as string).split(".").map(Number);
      e.u32(
        ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>>
        0,
      );
    },
    decode: (v, _, c) => {
      const num = v.getUint32(c.offset, true);
      c.offset += 4;
      return `${(num >> 24) & 0xff}.${(num >> 16) & 0xff}.${(num >> 8) & 0xff}.${num & 0xff}`;
    },
  },
  IPv6: {
    encode: (e, v) => {
      e.ensure(16);
      const parts = expandIPv6(v as string);
      for (const part of parts) {
        const val = parseInt(part || "0", 16);
        e.buffer[e.offset++] = (val >> 8) & 0xff;
        e.buffer[e.offset++] = val & 0xff;
      }
    },
    decode: (_, b, c) => {
      checkBounds(b, c, 16);
      const parts: string[] = [];
      for (let i = 0; i < 16; i += 2) {
        parts.push(((b[c.offset + i] << 8) | b[c.offset + i + 1]).toString(16));
      }
      c.offset += 16;
      return parts.join(":");
    },
  },
  Int128: {
    encode: (e, v) => write128(e, BigInt(v as any), true),
    decode: (v, _, c) => read128(v, c, true),
  },
  UInt128: {
    encode: (e, v) => write128(e, BigInt(v as any), false),
    decode: (v, _, c) => read128(v, c, false),
  },
  Int256: {
    encode: (e, v) => write256(e, BigInt(v as any), true),
    decode: (v, _, c) => read256(v, c, true),
  },
  UInt256: {
    encode: (e, v) => write256(e, BigInt(v as any), false),
    decode: (v, _, c) => read256(v, c, false),
  },
};

// --- Complex Codecs ---

class NothingCodec implements Codec {
  encode(_e: RowBinaryEncoder, _v: unknown) { }
  decode(_v: DataView, _b: Uint8Array, _c: Cursor) {
    return null;
  }
}

class NullableCodec implements Codec {
  private inner: Codec;
  constructor(innerType: string) {
    this.inner = createCodec(innerType);
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    if (v === null) {
      e.u8(1);
    } else {
      e.u8(0);
      this.inner.encode(e, v);
    }
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    if (v.getUint8(c.offset++) === 1) return null;
    return this.inner.decode(v, b, c);
  }
}

class ArrayCodec implements Codec {
  private inner: Codec;
  private isString: boolean;
  private TypedArrayCtor: any;

  constructor(innerType: string) {
    this.inner = createCodec(innerType);
    this.isString = innerType === "String";
    this.TypedArrayCtor = TYPED_ARRAYS[innerType];
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    // Fast path: TypedArray input with matching type
    if (this.TypedArrayCtor && ArrayBuffer.isView(v)) {
      const view = v as ArrayBufferView;
      e.leb128((view as any).length);
      e.raw(new Uint8Array(view.buffer, view.byteOffset, view.byteLength));
      return;
    }

    const arr = v as unknown[];
    e.leb128(arr.length);
    if (this.isString) {
      for (let i = 0; i < arr.length; i++)
        e.string(arr[i] as string | Uint8Array);
    } else {
      for (let i = 0; i < arr.length; i++) this.inner.encode(e, arr[i]);
    }
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    const len = readLEB128(b, c);

    // Fast path: TypedArray view (no copy)
    // Safe in streaming because we don't compact - old buffers stay alive via views
    if (this.TypedArrayCtor) {
      const byteLen = len * this.TypedArrayCtor.BYTES_PER_ELEMENT
      checkBounds(b, c, byteLen);
      const absoluteOffset = b.byteOffset + c.offset
      if (absoluteOffset % this.TypedArrayCtor.BYTES_PER_ELEMENT === 0) {
        const res = new this.TypedArrayCtor(b.buffer, absoluteOffset, len)
        c.offset += byteLen
        return res
      }
      // Unaligned: must copy to aligned buffer
      const copy = new Uint8Array(byteLen)
      copy.set(b.subarray(c.offset, c.offset + byteLen))
      c.offset += byteLen
      return new this.TypedArrayCtor(copy.buffer)
    }

    const result = new Array(len)
    for (let i = 0; i < len; i++) result[i] = this.inner.decode(v, b, c);
    return result;
  }
}

class TupleCodec implements Codec {
  private elements: { name: string | null; codec: Codec }[];
  private isNamed: boolean;

  constructor(typeBody: string) {
    const parsed = parseTupleElements(typeBody);
    this.elements = parsed.map((p) => ({
      name: p.name,
      codec: createCodec(p.type),
    }));
    this.isNamed = this.elements.length > 0 && this.elements[0].name !== null;
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    if (this.isNamed) {
      const obj = v as Record<string, unknown>;
      for (const el of this.elements) el.codec.encode(e, obj[el.name!]);
    } else {
      const arr = v as unknown[];
      for (let i = 0; i < this.elements.length; i++)
        this.elements[i].codec.encode(e, arr[i]);
    }
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    if (this.isNamed) {
      const obj: Record<string, unknown> = {};
      for (const el of this.elements) obj[el.name!] = el.codec.decode(v, b, c);
      return obj;
    } else {
      const arr = new Array(this.elements.length);
      for (let i = 0; i < this.elements.length; i++)
        arr[i] = this.elements[i].codec.decode(v, b, c);
      return arr;
    }
  }
}

class MapCodec implements Codec {
  private key: Codec;
  private value: Codec;

  constructor(typeBody: string) {
    const [k, v] = parseTypeList(typeBody);
    this.key = createCodec(k);
    this.value = createCodec(v);
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    // Accept Map, Object, or Array<[K, V]> for flexibility
    let entries: [unknown, unknown][];
    if (v instanceof Map) {
      entries = [...v.entries()];
    } else if (Array.isArray(v)) {
      entries = v as [unknown, unknown][];
    } else {
      entries = Object.entries(v as Record<string, unknown>);
    }
    e.leb128(entries.length);
    for (const [k, val] of entries) {
      this.key.encode(e, k);
      this.value.encode(e, val);
    }
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    const len = readLEB128(b, c);
    if (c.options?.mapAsArray) {
      const result: [unknown, unknown][] = []
      for (let i = 0; i < len; i++) {
        const key = this.key.decode(v, b, c)
        const val = this.value.decode(v, b, c)
        result.push([key, val])
      }
      return result
    }
    const result = new Map()
    for (let i = 0; i < len; i++) {
      const key = this.key.decode(v, b, c)
      const val = this.value.decode(v, b, c)
      result.set(key, val)
    }
    return result
  }
}

class FixedStringCodec implements Codec {
  private n: number;
  constructor(n: number) {
    this.n = n;
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    e.ensure(this.n);
    const bytes = v instanceof Uint8Array ? v : textEncoder.encode(v as string);
    e.buffer.fill(0, e.offset, e.offset + this.n);
    e.buffer.set(bytes.subarray(0, this.n), e.offset);
    e.offset += this.n;
  }

  decode(_: DataView, b: Uint8Array, c: Cursor) {
    checkBounds(b, c, this.n);
    const bytes = new Uint8Array(this.n)
    bytes.set(b.subarray(c.offset, c.offset + this.n))
    c.offset += this.n
    return bytes
  }
}

class Date32Codec implements Codec {
  encode(e: RowBinaryEncoder, v: unknown) {
    e.i32(Math.floor((v instanceof Date ? v : new Date(v as string)).getTime() / 86400000))
  }
  decode(v: DataView, _: Uint8Array, c: Cursor) {
    const days = v.getInt32(c.offset, true)
    c.offset += 4
    return new Date(days * 86400000)
  }
}

export class ClickHouseDateTime64 {
  public ticks: bigint
  public precision: number
  private pow: bigint

  constructor(ticks: bigint, precision: number) {
    this.ticks = ticks
    this.precision = precision
    this.pow = 10n ** BigInt(Math.abs(precision - 3))
  }

  /**
   * Convert to native Date object.
   * Throws if value overflows JS Date range or precision is lost (sub-millisecond components).
   */
  toDate(): Date {
    const ms = this.precision >= 3 ? this.ticks / this.pow : this.ticks * this.pow
    // Check for overflow (JS Date range: ±8.64e15 ms)
    if (ms > 8640000000000000n || ms < -8640000000000000n) {
      throw new RangeError(`DateTime64 value ${ms}ms overflows JS Date range (±8.64e15ms). Use toClosestDate() to clamp.`)
    }
    // Check for precision loss
    if (this.precision > 3 && this.ticks % this.pow !== 0n) {
      throw new Error(`Precision loss: DateTime64(${this.precision}) value ${this.ticks} cannot be represented as Date without losing precision. Use toClosestDate() or access .ticks directly.`)
    }
    return new Date(Number(ms))
  }

  /**
   * Convert to native Date object, truncating sub-millisecond precision and clamping to JS Date range.
   */
  toClosestDate(): Date {
    let ms = this.precision >= 3 ? this.ticks / this.pow : this.ticks * this.pow
    // Clamp to JS Date range
    if (ms > 8640000000000000n) ms = 8640000000000000n
    if (ms < -8640000000000000n) ms = -8640000000000000n
    return new Date(Number(ms))
  }

  toJSON(): string {
    return this.toClosestDate().toJSON()
  }

  toString(): string {
    return this.toClosestDate().toString()
  }
}

class DateTime64Codec implements Codec {
  private precision: number
  private pow: bigint

  constructor(type: string) {
    const match = type.match(/DateTime64\((\d+)/)
    this.precision = match ? parseInt(match[1], 10) : 3
    this.pow = 10n ** BigInt(Math.abs(this.precision - 3))
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    if (v instanceof ClickHouseDateTime64) {
      // If precisions match, use ticks directly. If not, we might need to rescale?
      // For simplicity, we assume user passes correct precision or we just write ticks.
      // ClickHouse usually expects the ticks to match the column precision.
      // If we want to be safe, we could rescale, but that requires knowing source precision.
      // We'll trust the user/ticks for now, or use toClosestDate logic?
      // Ideally we just write ticks.
      e.i64(v.ticks)
      return
    }
    if (typeof v === 'bigint') {
      e.i64(v)
      return
    }
    const ms = BigInt((v instanceof Date ? v : new Date(v as string)).getTime())
    const ticks = this.precision >= 3 ? ms * this.pow : ms / this.pow
    e.i64(ticks)
  }

  decode(v: DataView, _: Uint8Array, c: Cursor) {
    const ticks = v.getBigInt64(c.offset, true)
    c.offset += 8
    return new ClickHouseDateTime64(ticks, this.precision)
  }
}

class DecimalCodec implements Codec {
  private byteSize: 4 | 8 | 16 | 32;
  private scale: number;

  constructor(type: string) {
    this.scale = extractDecimalScale(type);
    this.byteSize = decimalByteSize(type);
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    const strVal = typeof v === "string" ? v : String(v);
    const scaled = parseDecimalToScaledBigInt(strVal, this.scale);
    e.ensure(this.byteSize);
    writeScaledInt(e.view, e.offset, scaled, this.byteSize);
    e.offset += this.byteSize;
  }

  decode(v: DataView, _b: Uint8Array, c: Cursor) {
    const val = readScaledInt(v, c.offset, this.byteSize);
    c.offset += this.byteSize;
    return formatScaledBigInt(val, this.scale);
  }
}

class VariantCodec implements Codec {
  private types: Codec[];
  constructor(body: string) {
    this.types = parseTypeList(body).map(createCodec);
  }

  encode(e: RowBinaryEncoder, v: unknown) {
    if (v === null) {
      e.u8(0xff);
      return;
    }
    const val = v as { type: number; value: unknown };
    e.u8(val.type);
    this.types[val.type].encode(e, val.value);
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    const idx = v.getUint8(c.offset++);
    if (idx === 0xff) return null;
    const val = this.types[idx].decode(v, b, c);
    return { type: idx, value: val };
  }
}

class JsonCodec implements Codec {
  encode(e: RowBinaryEncoder, v: unknown) {
    const obj = v as Record<string, unknown>;
    const paths = Object.keys(obj);
    e.leb128(paths.length);
    for (const path of paths) {
      e.string(path);
      const val = obj[path];
      if (val === null) {
        e.u8(0); // Nothing
      } else {
        const type = inferType(val);
        e.raw(encodeTypeBinary(type));
        createCodec(type).encode(e, val);
      }
    }
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    const numPaths = readLEB128(b, c);
    const result: Record<string, unknown> = {};
    for (let i = 0; i < numPaths; i++) {
      const path = readString(b, c);
      const type = decodeTypeBinary(b, c);
      if (type === "Nothing") {
        result[path] = null;
      } else {
        result[path] = createCodec(type).decode(v, b, c);
      }
    }
    return result;
  }
}

class DynamicCodec implements Codec {
  encode(e: RowBinaryEncoder, v: unknown) {
    if (v === null) {
      e.u8(0);
      return;
    }
    let type: string;
    let val: unknown;
    if (isExplicitDynamic(v)) {
      type = v.type;
      val = v.value;
    } else {
      type = inferType(v);
      val = v;
    }
    e.raw(encodeTypeBinary(type));
    createCodec(type).encode(e, val);
  }

  decode(v: DataView, b: Uint8Array, c: Cursor) {
    const type = decodeTypeBinary(b, c);
    if (type === "Nothing") return null;
    const val = createCodec(type).decode(v, b, c);
    return { type, value: val };
  }
}

// ============================================================================
// Public API
// ============================================================================

export function encodeRowBinaryWithNames(
  columns: ColumnDef[],
  rows: unknown[][],
): Uint8Array {
  const encoder = new RowBinaryEncoder();

  // 1. Column count
  encoder.leb128(columns.length);

  // 2. Names
  for (const col of columns) encoder.string(col.name);

  // 3. Pre-compile codecs
  const codecs = columns.map((c) => createCodec(c.type));

  // 4. Encode rows
  for (const row of rows) {
    for (let i = 0; i < columns.length; i++) {
      codecs[i].encode(encoder, row[i]);
    }
  }

  return encoder.finish();
}

export function decodeRowBinaryWithNames(
  data: Uint8Array,
  types: ColumnType[],
): DecodeResult {
  const cursor = { offset: 0 };
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  // 1. Count
  const colCount = readLEB128(data, cursor);

  if (colCount !== types.length) {
    throw new Error(
      `Column count mismatch: data has ${colCount}, provided ${types.length}`,
    );
  }

  // 2. Names
  const columns: ColumnDef[] = [];
  for (let i = 0; i < colCount; i++) {
    const name = readString(data, cursor);
    columns.push({ name, type: types[i] });
  }

  // 3. Codecs
  const codecs = types.map((t) => createCodec(t));

  // 4. Rows
  const rows: unknown[][] = [];
  while (cursor.offset < data.length) {
    const row = new Array(colCount);
    for (let i = 0; i < colCount; i++) {
      row[i] = codecs[i].decode(view, data, cursor);
    }
    rows.push(row);
  }

  return { columns, rows };
}

export function decodeRowBinaryWithNamesAndTypes(
  data: Uint8Array,
  options?: DecodeOptions,
): DecodeResult {
  const cursor: Cursor = { offset: 0, options };
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  const colCount = readLEB128(data, cursor);

  const names: string[] = [];
  for (let i = 0; i < colCount; i++) names.push(readString(data, cursor));

  const types: string[] = [];
  for (let i = 0; i < colCount; i++) types.push(readString(data, cursor));

  const columns = names.map((name, i) => ({ name, type: types[i] }));
  const codecs = types.map(createCodec);

  const rows: unknown[][] = [];
  while (cursor.offset < data.length) {
    const row = new Array(colCount);
    for (let i = 0; i < colCount; i++) {
      row[i] = codecs[i].decode(view, data, cursor);
    }
    rows.push(row);
  }

  return { columns, rows };
}

// ============================================================================
// Streaming API
// ============================================================================

export interface StreamDecodeOptions extends DecodeOptions {
  /** Yield rows in batches of this size (default: 1) */
  batchSize?: number;
}

export interface StreamDecodeResult {
  columns: ColumnDef[];
  rows: unknown[][];  // batch of rows
}

/** Read a string from streaming reader with retry-on-underflow */
async function readStreamString(reader: StreamingReader, options: DecodeOptions | undefined, errorMsg: string): Promise<string> {
  while (true) {
    const slice = reader.getSlice();
    const cursor: Cursor = { offset: 0, options };
    try {
      const str = readString(slice, cursor);
      reader.advance(cursor.offset);
      return str;
    } catch (e) {
      if (e instanceof RangeError) {
        if (!(await reader.pullMore())) throw new Error(errorMsg);
        continue;
      }
      throw e;
    }
  }
}

/** Core row decode loop - shared by both streaming decode functions */
async function* streamDecodeRows(
  reader: StreamingReader,
  columns: ColumnDef[],
  codecs: Codec[],
  batchSize: number,
  options: DecodeOptions | undefined,
): AsyncGenerator<StreamDecodeResult> {
  const colCount = columns.length;
  let batch: unknown[][] = [];

  while (reader.hasMore()) {
    const slice = reader.getSlice();
    if (slice.length === 0) {
      if (!(await reader.pullMore())) break;
      continue;
    }

    const view = new DataView(slice.buffer, slice.byteOffset, slice.byteLength);
    const cursor: Cursor = { offset: 0, options };

    try {
      const row = new Array(colCount);
      for (let i = 0; i < colCount; i++) {
        row[i] = codecs[i].decode(view, slice, cursor);
      }
      reader.advance(cursor.offset);
      batch.push(row);
      if (batch.length >= batchSize) {
        yield { columns, rows: batch };
        batch = [];
      }
    } catch (e) {
      if (e instanceof RangeError) {
        if (!(await reader.pullMore())) throw new Error('Unexpected EOF mid-row');
        continue;
      }
      throw e;
    }
  }

  if (batch.length > 0) {
    yield { columns, rows: batch };
  }
}

/**
 * Streaming decode of RowBinaryWithNamesAndTypes format.
 * Yields rows in batches as they're parsed from the chunk stream.
 */
export async function* streamDecodeRowBinaryWithNamesAndTypes(
  chunks: AsyncIterable<Uint8Array>,
  options?: StreamDecodeOptions,
): AsyncGenerator<StreamDecodeResult> {
  const reader = new StreamingReader(chunks);
  reader.options = options;
  const batchSize = options?.batchSize ?? 1;

  if (!(await reader.ensure(1))) return;

  // Parse header with retry
  while (true) {
    const slice = reader.getSlice();
    const cursor: Cursor = { offset: 0, options };
    try {
      const colCount = readLEB128(slice, cursor);
      reader.advance(cursor.offset);

      // Read names and types
      const names: string[] = [];
      const types: string[] = [];
      for (let i = 0; i < colCount; i++) {
        names.push(await readStreamString(reader, options, 'Unexpected EOF reading column names'));
      }
      for (let i = 0; i < colCount; i++) {
        types.push(await readStreamString(reader, options, 'Unexpected EOF reading column types'));
      }

      const columns = names.map((name, i) => ({ name, type: types[i] }));
      const codecs = types.map(createCodec);

      yield* streamDecodeRows(reader, columns, codecs, batchSize, options);
      return;
    } catch (e) {
      if (e instanceof RangeError) {
        if (!(await reader.pullMore())) throw new Error('Unexpected EOF reading header');
        continue;
      }
      throw e;
    }
  }
}

/**
 * Convenience wrapper: collect all rows from streaming decode into DecodeResult.
 */
export async function streamDecodeRowBinaryWithNamesAndTypesAll(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions,
): Promise<DecodeResult> {
  let columns: ColumnDef[] = [];
  const rows: unknown[][] = [];

  for await (const { columns: cols, rows: batch } of streamDecodeRowBinaryWithNamesAndTypes(chunks, { ...options, batchSize: 10000 })) {
    columns = cols;
    rows.push(...batch);
  }

  return { columns, rows };
}

/**
 * Streaming decode of RowBinaryWithNames format.
 * Types must be provided externally.
 */
export async function* streamDecodeRowBinaryWithNames(
  chunks: AsyncIterable<Uint8Array>,
  types: ColumnType[],
  options?: StreamDecodeOptions,
): AsyncGenerator<StreamDecodeResult> {
  const reader = new StreamingReader(chunks);
  reader.options = options;
  const batchSize = options?.batchSize ?? 1;

  if (!(await reader.ensure(1))) return;

  while (true) {
    const slice = reader.getSlice();
    const cursor: Cursor = { offset: 0, options };
    try {
      const colCount = readLEB128(slice, cursor);
      reader.advance(cursor.offset);

      if (colCount !== types.length) {
        throw new Error(`Column count mismatch: data has ${colCount}, provided ${types.length}`);
      }

      // Read names
      const columns: ColumnDef[] = [];
      for (let i = 0; i < colCount; i++) {
        const name = await readStreamString(reader, options, 'Unexpected EOF reading column names');
        columns.push({ name, type: types[i] });
      }

      const codecs = types.map(createCodec);

      yield* streamDecodeRows(reader, columns, codecs, batchSize, options);
      return;
    } catch (e) {
      if (e instanceof RangeError) {
        if (!(await reader.pullMore())) throw new Error('Unexpected EOF reading header');
        continue;
      }
      throw e;
    }
  }
}

export interface StreamingEncodeOptions {
  /** Target chunk size in bytes (default: 64KB) */
  chunkSize?: number;
  /** Include column names header (default: true) */
  includeHeader?: boolean;
}

/**
 * Streaming encode rows as RowBinaryWithNames format.
 * Yields Uint8Array chunks as the buffer fills up.
 */
export async function* streamEncodeRowBinaryWithNames(
  columns: ColumnDef[],
  rows: AsyncIterable<unknown[]> | Iterable<unknown[]>,
  options?: StreamingEncodeOptions,
): AsyncGenerator<Uint8Array> {
  const chunkSize = options?.chunkSize ?? 64 * 1024;
  const threshold = chunkSize - 4096; // Leave room for a row

  const encoder = new RowBinaryEncoder(chunkSize);
  const codecs = columns.map((c) => createCodec(c.type));

  // Encode header if requested
  if (options?.includeHeader !== false) {
    encoder.leb128(columns.length);
    for (const col of columns) {
      encoder.string(col.name);
    }
  }

  // Fast path for sync iterables - no await per row
  if (Symbol.iterator in rows && !(Symbol.asyncIterator in rows)) {
    for (const row of rows as Iterable<unknown[]>) {
      for (let i = 0; i < columns.length; i++) {
        codecs[i].encode(encoder, row[i]);
      }
      if (encoder.offset >= threshold) {
        yield encoder.buffer.slice(0, encoder.offset);
        encoder.offset = 0;
      }
    }
  } else {
    // Async path
    for await (const row of rows as AsyncIterable<unknown[]>) {
      for (let i = 0; i < columns.length; i++) {
        codecs[i].encode(encoder, row[i]);
      }
      if (encoder.offset >= threshold) {
        yield encoder.buffer.slice(0, encoder.offset);
        encoder.offset = 0;
      }
    }
  }

  // Yield any remaining data
  if (encoder.offset > 0) {
    yield encoder.finish();
  }
}

// ============================================================================
// Internal Helpers
// ============================================================================

// --- IO Helpers ---

/** Throws RangeError if not enough bytes available - used for streaming retry */
function checkBounds(b: Uint8Array, c: Cursor, n: number): void {
  if (c.offset + n > b.length) throw new RangeError('Buffer underflow');
}

function readLEB128(data: Uint8Array, c: Cursor): number {
  let value = 0;
  let shift = 0;
  while (true) {
    if (c.offset >= data.length) throw new RangeError('Buffer underflow');
    const byte = data[c.offset++];
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return value;
}

function utf8DecodeSmall(data: Uint8Array, start: number, end: number): string {
  let result = "";
  let i = start;
  while (i < end) {
    const byte = data[i++];
    if (byte < 0x80) {
      result += String.fromCharCode(byte);
    } else if (byte < 0xe0) {
      result += String.fromCharCode(((byte & 0x1f) << 6) | (data[i++] & 0x3f));
    } else if (byte < 0xf0) {
      result += String.fromCharCode(
        ((byte & 0x0f) << 12) | ((data[i++] & 0x3f) << 6) | (data[i++] & 0x3f),
      );
    } else {
      const cp =
        ((byte & 0x07) << 18) |
        ((data[i++] & 0x3f) << 12) |
        ((data[i++] & 0x3f) << 6) |
        (data[i++] & 0x3f);
      result += String.fromCharCode(
        0xd800 + ((cp - 0x10000) >> 10),
        0xdc00 + ((cp - 0x10000) & 0x3ff),
      );
    }
  }
  return result;
}

function readString(data: Uint8Array, c: Cursor): string {
  const len = readLEB128(data, c);
  checkBounds(data, c, len);
  const end = c.offset + len;
  // Optimization for short strings
  const str =
    len < 12
      ? utf8DecodeSmall(data, c.offset, end)
      : textDecoder.decode(data.subarray(c.offset, end));
  c.offset = end;
  return str;
}

// Core 128/256-bit helpers (used by both Int128/256 codecs and Decimal codecs)
function writeBigInt128(v: DataView, o: number, val: bigint, signed: boolean): void {
  const low = val & 0xffffffffffffffffn;
  const high = val >> 64n;
  v.setBigUint64(o, low, true);
  if (signed) v.setBigInt64(o + 8, high, true);
  else v.setBigUint64(o + 8, high, true);
}

function readBigInt128(v: DataView, o: number, signed: boolean): bigint {
  const low = v.getBigUint64(o, true);
  const high = signed ? v.getBigInt64(o + 8, true) : v.getBigUint64(o + 8, true);
  return (high << 64n) | low;
}

function writeBigInt256(v: DataView, o: number, val: bigint, signed: boolean): void {
  for (let i = 0; i < 3; i++) {
    v.setBigUint64(o + i * 8, val & 0xffffffffffffffffn, true);
    val >>= 64n;
  }
  if (signed) v.setBigInt64(o + 24, val, true);
  else v.setBigUint64(o + 24, val, true);
}

function readBigInt256(v: DataView, o: number, signed: boolean): bigint {
  let val = signed ? v.getBigInt64(o + 24, true) : v.getBigUint64(o + 24, true);
  for (let i = 2; i >= 0; i--) {
    val = (val << 64n) | v.getBigUint64(o + i * 8, true);
  }
  return val;
}

function write128(e: RowBinaryEncoder, value: bigint, signed: boolean): void {
  e.ensure(16);
  writeBigInt128(e.view, e.offset, value, signed);
  e.offset += 16;
}

function read128(view: DataView, c: Cursor, signed: boolean): bigint {
  const val = readBigInt128(view, c.offset, signed);
  c.offset += 16;
  return val;
}

function write256(e: RowBinaryEncoder, value: bigint, signed: boolean): void {
  e.ensure(32);
  writeBigInt256(e.view, e.offset, value, signed);
  e.offset += 32;
}

function read256(view: DataView, c: Cursor, signed: boolean): bigint {
  const val = readBigInt256(view, c.offset, signed);
  c.offset += 32;
  return val;
}

// --- Decimal Helpers ---

function decimalByteSize(type: string): 4 | 8 | 16 | 32 {
  if (type.startsWith("Decimal32")) return 4;
  if (type.startsWith("Decimal64")) return 8;
  if (type.startsWith("Decimal128")) return 16;
  if (type.startsWith("Decimal256")) return 32;
  const match = type.match(/Decimal\((\d+),/);
  if (match) {
    const p = parseInt(match[1], 10);
    if (p <= 9) return 4;
    if (p <= 18) return 8;
    if (p <= 38) return 16;
    return 32;
  }
  return 16;
}

function extractDecimalScale(type: string): number {
  const match = type.match(/Decimal\d*\((?:\d+,\s*)?(\d+)\)/);
  return match ? parseInt(match[1], 10) : 0;
}

function writeScaledInt(v: DataView, o: number, val: bigint, size: number): void {
  switch (size) {
    case 4: v.setInt32(o, Number(val), true); break;
    case 8: v.setBigInt64(o, val, true); break;
    case 16: writeBigInt128(v, o, val, true); break;
    case 32: writeBigInt256(v, o, val, true); break;
  }
}

function readScaledInt(v: DataView, o: number, size: number): bigint {
  switch (size) {
    case 4: return BigInt(v.getInt32(o, true));
    case 8: return v.getBigInt64(o, true);
    case 16: return readBigInt128(v, o, true);
    case 32: return readBigInt256(v, o, true);
  }
  return 0n;
}

function parseDecimalToScaledBigInt(str: string, scale: number): bigint {
  const neg = str.startsWith("-");
  if (neg) str = str.slice(1);
  const dot = str.indexOf(".");
  let intP: string, fracP: string;
  if (dot === -1) {
    intP = str;
    fracP = "";
  } else {
    intP = str.slice(0, dot);
    fracP = str.slice(dot + 1);
  }

  if (fracP.length < scale) fracP = fracP.padEnd(scale, "0");
  else if (fracP.length > scale) fracP = fracP.slice(0, scale);

  const val = BigInt(intP + fracP);
  return neg ? -val : val;
}

function formatScaledBigInt(val: bigint, scale: number): string {
  const neg = val < 0n;
  if (neg) val = -val;
  let str = val.toString();
  if (scale === 0) return neg ? "-" + str : str;
  while (str.length <= scale) str = "0" + str;
  const intP = str.slice(0, -scale);
  const fracP = str.slice(-scale);
  const r = intP + "." + fracP;
  return neg ? "-" + r : r;
}

// --- Parse Helpers ---

function parseTypeList(inner: string): string[] {
  const types: string[] = [];
  let depth = 0;
  let current = "";
  for (const char of inner) {
    if (char === "(") depth++;
    if (char === ")") depth--;
    if (char === "," && depth === 0) {
      types.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }
  if (current.trim()) types.push(current.trim());
  return types;
}

function parseTupleElements(
  inner: string,
): { name: string | null; type: string }[] {
  const parts = parseTypeList(inner);
  return parts.map((part) => {
    // Attempt to match "name Type"
    // Heuristic: check if first word is a known type prefix. If so, it's unnamed.
    // If not, it might be a name.
    const match = part.match(/^([a-z_][a-z0-9_]*)\s+(.+)$/i);
    if (match) {
      const name = match[1];
      const type = match[2];
      const typeKeywords = [
        "Int",
        "UInt",
        "Float",
        "String",
        "Bool",
        "Date",
        "DateTime",
        "Nullable",
        "Array",
        "Tuple",
        "Map",
        "Enum",
        "UUID",
        "IPv",
        "Decimal",
        "FixedString",
        "Variant",
        "JSON",
        "Object",
      ];
      if (!typeKeywords.some((kw) => name.startsWith(kw))) {
        return { name, type };
      }
    }
    return { name: null, type: part };
  });
}

function expandIPv6(str: string): string[] {
  let parts = str.split(":");
  const emptyIdx = parts.indexOf("");
  if (emptyIdx !== -1) {
    const before = parts.slice(0, emptyIdx).filter((p) => p);
    const after = parts.slice(emptyIdx + 1).filter((p) => p);
    const missing = 8 - before.length - after.length;
    parts = [...before, ...Array(missing).fill("0"), ...after];
  }
  return parts;
}

function isExplicitDynamic(v: unknown): v is { type: string; value: unknown } {
  return (
    typeof v === "object" &&
    v !== null &&
    "type" in v &&
    "value" in v &&
    Object.keys(v).length === 2
  );
}

function inferType(value: unknown): string {
  if (value === null) return "Nothing";
  if (typeof value === "boolean") return "Bool";
  if (typeof value === "string") return "String";
  if (typeof value === "bigint") {
    if (value >= INT128_MIN && value <= INT128_MAX) return "Int128";
    return "Int256";
  }
  if (typeof value === "number") {
    if (Number.isInteger(value)) return "Int64";
    return "Float64";
  }
  if (value instanceof Date) return "DateTime64(3)";
  if (value instanceof ClickHouseDateTime64) return `DateTime64(${value.precision})`;
  if (Array.isArray(value)) {
    if (value.length === 0) return "Array(Nothing)";
    return `Array(${inferType(value[0])})`;
  }
  throw new Error(`Cannot infer Dynamic type for: ${typeof value}`);
}

// ============================================================================
// Type Signature Binary Encoding (for Dynamic/Variant/JSON headers)
// ============================================================================

const TYPE_CODES: Record<string, number> = {
  Nothing: 0x00,
  UInt8: 0x01,
  UInt16: 0x02,
  UInt32: 0x03,
  UInt64: 0x04,
  UInt128: 0x05,
  UInt256: 0x06,
  Int8: 0x07,
  Int16: 0x08,
  Int32: 0x09,
  Int64: 0x0a,
  Int128: 0x0b,
  Int256: 0x0c,
  Float32: 0x0d,
  Float64: 0x0e,
  Date: 0x0f,
  Date32: 0x10,
  DateTime: 0x11,
  DateTime64: 0x13,
  String: 0x15,
  FixedString: 0x16,
  Enum8: 0x17,
  Enum16: 0x18,
  Decimal32: 0x19,
  Decimal64: 0x1a,
  Decimal128: 0x1b,
  Decimal256: 0x1c,
  UUID: 0x1d,
  Array: 0x1e,
  Tuple: 0x1f,
  Nullable: 0x23,
  Map: 0x27,
  IPv4: 0x28,
  IPv6: 0x29,
  Variant: 0x2a,
  Dynamic: 0x2b,
  Bool: 0x2d,
};

const REVERSE_TYPE_CODES: Record<number, string> = {};
for (const [k, v] of Object.entries(TYPE_CODES)) REVERSE_TYPE_CODES[v] = k;
REVERSE_TYPE_CODES[0x20] = "Tuple"; // named tuple alias

function encodeTypeBinary(type: string): Uint8Array {
  const enc = new RowBinaryEncoder();
  encodeTypeTo(type, enc);
  return enc.finish();
}

function encodeTypeTo(type: string, e: RowBinaryEncoder) {
  if (TYPE_CODES[type] !== undefined) {
    e.u8(TYPE_CODES[type]);
    return;
  }

  if (type.startsWith("Nullable(")) {
    e.u8(0x23);
    encodeTypeTo(type.slice(9, -1), e);
    return;
  }
  if (type.startsWith("Array(")) {
    e.u8(0x1e);
    encodeTypeTo(type.slice(6, -1), e);
    return;
  }
  if (type.startsWith("Map(")) {
    e.u8(0x27);
    const [k, v] = parseTypeList(type.slice(4, -1));
    encodeTypeTo(k, e);
    encodeTypeTo(v, e);
    return;
  }

  if (type.startsWith("Tuple(")) {
    const elems = parseTupleElements(type.slice(6, -1));
    const isNamed = elems.length > 0 && elems[0].name !== null;
    e.u8(isNamed ? 0x20 : 0x1f);
    e.leb128(elems.length);
    for (const el of elems) {
      if (isNamed) {
        e.string(el.name!);
      }
      encodeTypeTo(el.type, e);
    }
    return;
  }

  if (type.startsWith("Variant(")) {
    e.u8(0x2a);
    const types = parseTypeList(type.slice(8, -1));
    e.leb128(types.length);
    for (const t of types) encodeTypeTo(t, e);
    return;
  }

  if (type.startsWith("DateTime64")) {
    const match = type.match(/DateTime64\((\d+)(?:,\s*'([^']+)')?\)/);
    if (match) {
      const p = parseInt(match[1], 10);
      const tz = match[2];
      if (tz) {
        e.u8(0x14);
        e.u8(p);
        e.string(tz);
      } else {
        e.u8(0x13);
        e.u8(p);
      }
      return;
    }
  }

  if (type.startsWith("FixedString(")) {
    e.u8(0x16);
    e.leb128(parseInt(type.slice(12, -1), 10));
    return;
  }

  // Decimals
  for (const [prefix, code] of [
    ["Decimal32", 0x19],
    ["Decimal64", 0x1a],
    ["Decimal128", 0x1b],
    ["Decimal256", 0x1c],
  ] as const) {
    if (type.startsWith(prefix + "(")) {
      e.u8(code);
      const inner = type.slice(prefix.length + 1, -1);
      const nums = inner.split(",").map((s) => parseInt(s.trim(), 10));
      if (nums.length === 1) {
        const defaultP =
          prefix === "Decimal32"
            ? 9
            : prefix === "Decimal64"
              ? 18
              : prefix === "Decimal128"
                ? 38
                : 76;
        e.u8(defaultP);
        e.u8(nums[0]);
      } else {
        e.u8(nums[0]);
        e.u8(nums[1]);
      }
      return;
    }
  }

  throw new Error(`Binary type encoding not fully implemented for: ${type}`);
}

function decodeTypeBinary(data: Uint8Array, c: Cursor): string {
  const code = data[c.offset++];
  const simple = REVERSE_TYPE_CODES[code];
  if (
    simple &&
    ![
      "Array",
      "Tuple",
      "Nullable",
      "Map",
      "Variant",
      "DateTime64",
      "FixedString",
      "Decimal32",
      "Decimal64",
      "Decimal128",
      "Decimal256",
    ].includes(simple)
  )
    return simple;

  if (code === 0x23) {
    return `Nullable(${decodeTypeBinary(data, c)})`;
  }
  if (code === 0x1e) {
    return `Array(${decodeTypeBinary(data, c)})`;
  }
  if (code === 0x27) {
    const k = decodeTypeBinary(data, c);
    const v = decodeTypeBinary(data, c);
    return `Map(${k}, ${v})`;
  }

  if (code === 0x1f) {
    // Tuple unnamed
    const count = readLEB128(data, c);
    const types: string[] = [];
    for (let i = 0; i < count; i++) types.push(decodeTypeBinary(data, c));
    return `Tuple(${types.join(", ")})`;
  }

  if (code === 0x20) {
    // Tuple named
    const count = readLEB128(data, c);
    const parts: string[] = [];
    for (let i = 0; i < count; i++) {
      const name = readString(data, c);
      const type = decodeTypeBinary(data, c);
      parts.push(`${name} ${type}`);
    }
    return `Tuple(${parts.join(", ")})`;
  }

  if (code === 0x2a) {
    // Variant
    const count = readLEB128(data, c);
    const types: string[] = [];
    for (let i = 0; i < count; i++) types.push(decodeTypeBinary(data, c));
    return `Variant(${types.join(", ")})`;
  }

  if (code === 0x13) {
    const p = data[c.offset++];
    return `DateTime64(${p})`;
  }
  if (code === 0x14) {
    const p = data[c.offset++];
    const tz = readString(data, c);
    return `DateTime64(${p}, '${tz}')`;
  }

  if (code === 0x16) {
    const n = readLEB128(data, c);
    return `FixedString(${n})`;
  }

  if (code >= 0x19 && code <= 0x1c) {
    // Decimals
    const p = data[c.offset++];
    const s = data[c.offset++];
    const name =
      code === 0x19
        ? "Decimal32"
        : code === 0x1a
          ? "Decimal64"
          : code === 0x1b
            ? "Decimal128"
            : "Decimal256";
    return `${name}(${p}, ${s})`;
  }

  throw new Error(
    `Binary type decoding not fully implemented for code: ${code}`,
  );
}
