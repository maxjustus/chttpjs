import {
  type ColumnDef,
  type DecodeResult,
  type DecodeOptions,
  type Cursor,
  TEXT_ENCODER as textEncoder,
  HEX_LUT,
  BYTE_TO_HEX,
  TYPED_ARRAYS,
  Float32NaN,
  Float64NaN,
  ClickHouseDateTime64,
  readVarint as readLEB128FromUtils,
  leb128Size,
  readString,
  checkBounds,
  writeBigInt128,
  readBigInt128,
  writeBigInt256,
  readBigInt256,
  parseTypeList,
  parseTupleElements,
  decimalByteSize,
  extractDecimalScale,
  parseDecimalToScaledBigInt,
  formatScaledBigInt,
  expandIPv6,
  inferType,
} from "./shared.ts";

export {
  type ColumnDef,
  type DecodeResult,
  type DecodeOptions,
  type Cursor,
  Float32NaN,
  Float64NaN,
  ClickHouseDateTime64,
};

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

interface Codec {
  encode(enc: RowBinaryEncoder, value: unknown): void;
  decode(view: DataView, buffer: Uint8Array, cursor: Cursor): unknown;
}

const cache = new Map<string, Codec>();

export function createCodec(type: string): Codec {
  if (cache.has(type)) return cache.get(type)!;

  const codec = createCodecImpl(type);
  cache.set(type, codec);
  return codec;
}

function createCodecImpl(type: string): Codec {
  if (SCALAR_CODECS[type]) return SCALAR_CODECS[type];

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

  // Geo types - aliases for underlying container types
  if (type === "Point") return new TupleCodec("Float64, Float64");
  if (type === "Ring") return new ArrayCodec("Point");
  if (type === "Polygon") return new ArrayCodec("Ring");
  if (type === "MultiPolygon") return new ArrayCodec("Polygon");

  throw new Error(`Unknown or unsupported type: ${type}`);
}

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
  // UUID: Uses HEX_LUT/BYTE_TO_HEX lookup tables for ~11x encode, ~60x decode speedup
  // vs parseInt/toString. ClickHouse stores UUID as two LE 64-bit halves, bytes reversed.
  UUID: {
    encode: (e, v) => {
      e.ensure(16);
      const str = v as string;
      if (str.length !== 36) {
        throw new Error(`Invalid UUID length ${str.length}, expected 36 (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)`);
      }
      const ptr = e.offset;
      // First 8 bytes (reversed) - hyphens at positions 8, 13
      for (let i = 0; i < 8; i++) {
        let hexOffset = i * 2;
        if (i >= 4) hexOffset++; // skip hyphen at 8
        if (i >= 6) hexOffset++; // skip hyphen at 13
        const h1 = HEX_LUT[str.charCodeAt(hexOffset)];
        const h2 = HEX_LUT[str.charCodeAt(hexOffset + 1)];
        e.buffer[ptr + 7 - i] = (h1 << 4) | h2;
      }
      // Second 8 bytes (reversed) - hyphen at position 23
      for (let i = 0; i < 8; i++) {
        let hexOffset = 19 + i * 2;
        if (i >= 2) hexOffset++; // skip hyphen at 23
        const h1 = HEX_LUT[str.charCodeAt(hexOffset)];
        const h2 = HEX_LUT[str.charCodeAt(hexOffset + 1)];
        e.buffer[ptr + 15 - i] = (h1 << 4) | h2;
      }
      e.offset += 16;
    },
    decode: (_, b, c) => {
      checkBounds(b, c, 16);
      const o = c.offset;
      // Read bytes in reverse order for each half
      const b0 = b[o + 7], b1 = b[o + 6], b2 = b[o + 5], b3 = b[o + 4];
      const b4 = b[o + 3], b5 = b[o + 2], b6 = b[o + 1], b7 = b[o];
      const b8 = b[o + 15], b9 = b[o + 14], b10 = b[o + 13], b11 = b[o + 12];
      const b12 = b[o + 11], b13 = b[o + 10], b14 = b[o + 9], b15 = b[o + 8];
      c.offset += 16;
      return (
        BYTE_TO_HEX[b0] + BYTE_TO_HEX[b1] + BYTE_TO_HEX[b2] + BYTE_TO_HEX[b3] + "-" +
        BYTE_TO_HEX[b4] + BYTE_TO_HEX[b5] + "-" +
        BYTE_TO_HEX[b6] + BYTE_TO_HEX[b7] + "-" +
        BYTE_TO_HEX[b8] + BYTE_TO_HEX[b9] + "-" +
        BYTE_TO_HEX[b10] + BYTE_TO_HEX[b11] + BYTE_TO_HEX[b12] + BYTE_TO_HEX[b13] +
        BYTE_TO_HEX[b14] + BYTE_TO_HEX[b15]
      );
    },
  },
  // IPv4: Manual char parsing avoids split().map(Number) allocation (~1.25x faster)
  IPv4: {
    encode: (e, v) => {
      const s = v as string;
      if (s.length < 7 || s.length > 15) { // "0.0.0.0" to "255.255.255.255"
        throw new Error(`Invalid IPv4 address length ${s.length}`);
      }
      let val = 0, pos = 0;
      for (let i = 0; i < 4; i++) {
        let num = 0, ch = s.charCodeAt(pos);
        while (pos < s.length && ch >= 48 && ch <= 57) { num = num * 10 + (ch - 48); ch = s.charCodeAt(++pos); }
        if (num > 255) throw new Error(`Invalid IPv4 octet value ${num}`);
        pos++; // skip dot
        val = (val << 8) | num;
      }
      e.u32(val >>> 0);
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

export function encodeRowBinary(
  columns: ColumnDef[],
  rows: unknown[][],
): Uint8Array {
  const encoder = new RowBinaryEncoder();
  encoder.leb128(columns.length);
  for (const col of columns) encoder.string(col.name);
  for (const col of columns) encoder.string(col.type);

  const codecs = columns.map((c) => createCodec(c.type));
  for (const row of rows) {
    for (let i = 0; i < columns.length; i++) {
      codecs[i].encode(encoder, row[i]);
    }
  }

  return encoder.finish();
}

export function decodeRowBinary(
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

/** Core row decode loop - shared by both streaming decode functions.
 * Yields all complete rows from each chunk - natural batching based on decompression output. */
async function* streamDecodeRows(
  reader: StreamingReader,
  columns: ColumnDef[],
  codecs: Codec[],
  options: DecodeOptions | undefined,
): AsyncGenerator<StreamDecodeResult> {
  const colCount = columns.length;

  while (reader.hasMore()) {
    const slice = reader.getSlice();
    if (slice.length === 0) {
      if (!(await reader.pullMore())) break;
      continue;
    }

    const view = new DataView(slice.buffer, slice.byteOffset, slice.byteLength);
    const cursor: Cursor = { offset: 0, options };
    const batch: unknown[][] = [];
    let lastRowEnd = 0;

    try {
      // Decode all complete rows from current buffer
      while (cursor.offset < slice.length) {
        const row = new Array(colCount);
        for (let i = 0; i < colCount; i++) {
          row[i] = codecs[i].decode(view, slice, cursor);
        }
        batch.push(row);
        lastRowEnd = cursor.offset;
      }
      // Advance past all decoded rows and yield batch
      reader.advance(lastRowEnd);
      if (batch.length > 0) {
        yield { columns, rows: batch };
      }
    } catch (e) {
      if (e instanceof RangeError) {
        // Advance past completed rows, yield them, pull more for incomplete row
        if (lastRowEnd > 0) {
          reader.advance(lastRowEnd);
          if (batch.length > 0) {
            yield { columns, rows: batch };
          }
        }
        if (!(await reader.pullMore())) throw new Error('Unexpected EOF mid-row');
        continue;
      }
      throw e;
    }
  }
}

/**
 * Streaming decode of RowBinaryWithNamesAndTypes format.
 * Yields batches of rows as they arrive from each chunk.
 */
export async function* streamDecodeRowBinary(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions,
): AsyncGenerator<StreamDecodeResult> {
  const reader = new StreamingReader(chunks);
  reader.options = options;

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

      yield* streamDecodeRows(reader, columns, codecs, options);
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
 * Streaming encode rows as RowBinaryWithNamesAndTypes format.
 * Yields Uint8Array chunks as the buffer fills up.
 */
export async function* streamEncodeRowBinary(
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
    for (const col of columns) {
      encoder.string(col.type);
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

const readLEB128 = readLEB128FromUtils;

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

function isExplicitDynamic(v: unknown): v is { type: string; value: unknown } {
  return (
    typeof v === "object" &&
    v !== null &&
    "type" in v &&
    "value" in v &&
    Object.keys(v).length === 2
  );
}

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
