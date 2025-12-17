/**
 * Shared utilities for Native and RowBinary format codecs.
 */

// ============================================================================
// Types
// ============================================================================

export type TypedArray =
  | Int8Array
  | Uint8Array
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | BigInt64Array
  | BigUint64Array
  | Float32Array
  | Float64Array;

export interface ColumnDef {
  name: string;
  type: string;
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

// ============================================================================
// Constants
// ============================================================================

export const TEXT_ENCODER = new TextEncoder();
export const TEXT_DECODER = new TextDecoder();

// 128-bit constants
export const INT128_MAX = (1n << 127n) - 1n;
export const INT128_MIN = -(1n << 127n);

// Hex lookup tables for UUID encode/decode (~11x/~60x speedup vs parseInt/toString)
export const HEX_LUT = new Uint8Array(256); // char code -> nibble value (255 = invalid)
export const BYTE_TO_HEX: string[] = [];    // byte -> "00"-"ff"
for (let i = 0; i < 256; i++) {
  HEX_LUT[i] = 255;
  BYTE_TO_HEX[i] = i.toString(16).padStart(2, "0");
}
for (let i = 0; i < 10; i++) HEX_LUT[48 + i] = i;      // '0'-'9'
for (let i = 0; i < 6; i++) {
  HEX_LUT[65 + i] = 10 + i; // 'A'-'F'
  HEX_LUT[97 + i] = 10 + i; // 'a'-'f'
}

// TypedArray mapping for fast paths
export const TYPED_ARRAYS: Record<
  string,
  {
    new(buffer: ArrayBuffer, byteOffset: number, length: number): ArrayBufferView;
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

// ============================================================================
// NaN Wrapper Classes
// ============================================================================

/**
 * NaN wrapper classes to preserve IEEE 754 bit patterns during round-trips.
 *
 * Problem: JavaScript's DataView.setFloat32/setFloat64 canonicalize all NaN values to a single
 * "quiet NaN" representation (0x7FC00000 for float32). IEEE 754 defines many valid NaN bit
 * patterns - signaling NaNs have bit 22 clear, quiet NaNs have it set. ClickHouse's
 * generateRandom() produces signaling NaNs, which get silently converted:
 *
 *   Signaling NaN: 0xFF8C0839 (bit 22 = 0)
 *   After JS:      0xFFCC0839 (bit 22 = 1) ← canonicalized to quiet NaN
 *
 * Solution: Detect NaN on decode and store raw bytes. On encode, copy bytes directly instead
 * of using setFloat32/setFloat64. The wrapper provides NaN semantics via valueOf() so
 * arithmetic and comparisons work as expected.
 */
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
// DateTime64 Wrapper
// ============================================================================

export class ClickHouseDateTime64 {
  public ticks: bigint;
  public precision: number;
  private pow: bigint;

  constructor(ticks: bigint, precision: number) {
    this.ticks = ticks;
    this.precision = precision;
    this.pow = 10n ** BigInt(Math.abs(precision - 3));
  }

  /**
   * Convert to native Date object.
   * Throws if value overflows JS Date range or precision is lost (sub-millisecond components).
   */
  toDate(): Date {
    const ms = this.precision >= 3 ? this.ticks / this.pow : this.ticks * this.pow;
    // Check for overflow (JS Date range: ±8.64e15 ms)
    if (ms > 8640000000000000n || ms < -8640000000000000n) {
      throw new RangeError(`DateTime64 value ${ms}ms overflows JS Date range (±8.64e15ms). Use toClosestDate() to clamp.`);
    }
    // Check for precision loss
    if (this.precision > 3 && this.ticks % this.pow !== 0n) {
      throw new Error(`Precision loss: DateTime64(${this.precision}) value ${this.ticks} cannot be represented as Date without losing precision. Use toClosestDate() or access .ticks directly.`);
    }
    return new Date(Number(ms));
  }

  /**
   * Convert to native Date object, truncating sub-millisecond precision and clamping to JS Date range.
   */
  toClosestDate(): Date {
    let ms = this.precision >= 3 ? this.ticks / this.pow : this.ticks * this.pow;
    // Clamp to JS Date range
    if (ms > 8640000000000000n) ms = 8640000000000000n;
    if (ms < -8640000000000000n) ms = -8640000000000000n;
    return new Date(Number(ms));
  }

  toJSON(): string {
    return this.toClosestDate().toJSON();
  }

  toString(): string {
    return this.toClosestDate().toString();
  }
}

// ============================================================================
// Varint Encoding/Decoding
// ============================================================================

export function readVarint(data: Uint8Array, cursor: { offset: number }): number {
  let value = 0;
  let shift = 0;
  while (true) {
    if (cursor.offset >= data.length) throw new RangeError('Buffer underflow');
    const byte = data[cursor.offset++];
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return value;
}

export function writeVarint(value: number): Uint8Array {
  const arr: number[] = [];
  while (value >= 0x80) {
    arr.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  arr.push(value);
  return new Uint8Array(arr);
}

export function leb128Size(value: number): number {
  const bits = 32 - Math.clz32(value | 1);
  return Math.ceil(bits / 7);
}

// ============================================================================
// String Encoding/Decoding
// ============================================================================

export function utf8DecodeSmall(data: Uint8Array, start: number, end: number): string {
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

export function readString(data: Uint8Array, cursor: { offset: number }): string {
  const len = readVarint(data, cursor);
  checkBounds(data, cursor, len);
  const end = cursor.offset + len;
  const str = len < 12
    ? utf8DecodeSmall(data, cursor.offset, end)
    : TEXT_DECODER.decode(data.subarray(cursor.offset, end));
  cursor.offset = end;
  return str;
}

export function checkBounds(data: Uint8Array, cursor: { offset: number }, n: number): void {
  if (cursor.offset + n > data.length) throw new RangeError('Buffer underflow');
}

// ============================================================================
// BigInt 128/256-bit Helpers
// ============================================================================

export function writeBigInt128(v: DataView, o: number, val: bigint, signed: boolean): void {
  const low = val & 0xffffffffffffffffn;
  const high = val >> 64n;
  v.setBigUint64(o, low, true);
  if (signed) v.setBigInt64(o + 8, high, true);
  else v.setBigUint64(o + 8, high, true);
}

export function readBigInt128(v: DataView, o: number, signed: boolean): bigint {
  const low = v.getBigUint64(o, true);
  const high = signed ? v.getBigInt64(o + 8, true) : v.getBigUint64(o + 8, true);
  return (high << 64n) | low;
}

export function writeBigInt256(v: DataView, o: number, val: bigint, signed: boolean): void {
  for (let i = 0; i < 3; i++) {
    v.setBigUint64(o + i * 8, val & 0xffffffffffffffffn, true);
    val >>= 64n;
  }
  if (signed) v.setBigInt64(o + 24, val, true);
  else v.setBigUint64(o + 24, val, true);
}

export function readBigInt256(v: DataView, o: number, signed: boolean): bigint {
  let val = signed ? v.getBigInt64(o + 24, true) : v.getBigUint64(o + 24, true);
  for (let i = 2; i >= 0; i--) {
    val = (val << 64n) | v.getBigUint64(o + i * 8, true);
  }
  return val;
}

// ============================================================================
// Type Parsing Helpers
// ============================================================================

export function parseTypeList(inner: string): string[] {
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

export function parseTupleElements(inner: string): { name: string | null; type: string }[] {
  const parts = parseTypeList(inner);
  return parts.map((part) => {
    const match = part.match(/^([a-z_][a-z0-9_]*)\s+(.+)$/i);
    if (match) {
      const name = match[1];
      const type = match[2];
      const typeKeywords = [
        "Int", "UInt", "Float", "String", "Bool", "Date", "DateTime",
        "Nullable", "Array", "Tuple", "Map", "Enum", "UUID", "IPv",
        "Decimal", "FixedString", "Variant", "JSON", "Object", "LowCardinality",
        "Nested", "Nothing", "Dynamic", "Point", "Ring", "Polygon", "MultiPolygon",
      ];
      if (!typeKeywords.some((kw) => name.startsWith(kw))) {
        return { name, type };
      }
    }
    return { name: null, type: part };
  });
}

// ============================================================================
// Decimal Helpers
// ============================================================================

export function decimalByteSize(type: string): 4 | 8 | 16 | 32 {
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

export function extractDecimalScale(type: string): number {
  const match = type.match(/Decimal\d*\((?:\d+,\s*)?(\d+)\)/);
  return match ? parseInt(match[1], 10) : 0;
}

export function parseDecimalToScaledBigInt(str: string, scale: number): bigint {
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

export function formatScaledBigInt(val: bigint, scale: number): string {
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

// ============================================================================
// IPv6 Helpers
// ============================================================================

export function expandIPv6(str: string): string[] {
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

export function ipv6ToBytes(ip: string): Uint8Array {
  const bytes = new Uint8Array(16);
  let parts = ip.split('::');
  let left: string[] = [];
  let right: string[] = [];

  if (parts.length === 2) {
    left = parts[0] ? parts[0].split(':') : [];
    right = parts[1] ? parts[1].split(':') : [];
    const missing = 8 - left.length - right.length;
    const middle = new Array(missing).fill('0');
    parts = [...left, ...middle, ...right];
  } else {
    parts = ip.split(':');
  }

  for (let i = 0; i < 8; i++) {
    const val = parseInt(parts[i] || '0', 16);
    bytes[i * 2] = (val >> 8) & 0xFF;
    bytes[i * 2 + 1] = val & 0xFF;
  }
  return bytes;
}

export function bytesToIpv6(bytes: Uint8Array): string {
  const parts: string[] = [];
  for (let i = 0; i < 8; i++) {
    const val = (bytes[i * 2] << 8) | bytes[i * 2 + 1];
    parts.push(val.toString(16));
  }
  return parts.join(':');
}

// ============================================================================
// Type Inference
// ============================================================================

export function inferType(value: unknown): string {
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
  throw new Error(`Cannot infer type for: ${typeof value}`);
}
