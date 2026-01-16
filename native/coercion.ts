/**
 * Type coercion helpers for Native format encoding.
 * Validates and converts JavaScript values to ClickHouse-compatible types.
 * Throws TypeError/RangeError for values that cannot be safely coerced.
 */

import { ClickHouseDateTime64, type TypedArray } from "./types.ts";

// --- Core coercion functions ---

/**
 * Coerce value to number. Handles boolean, bigint, null, and string.
 * Note: TypedArrays wrap/truncate on overflow; validate ranges to avoid silent corruption.
 */
export function toNumber(v: unknown): number {
  if (typeof v === "number") return v;
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "bigint") return Number(v);
  if (v == null) return 0;
  if (typeof v === "string") {
    const trimmed = v.trim();
    if (trimmed.length === 0) {
      throw new TypeError(`Cannot coerce string "${v}" to number`);
    }
    const n = Number(trimmed);
    if (Number.isNaN(n)) {
      throw new TypeError(`Cannot coerce string "${v}" to number`);
    }
    return n;
  }
  const n = +(v as any);
  if (Number.isNaN(n)) {
    throw new TypeError(`Cannot coerce ${typeof v} "${v}" to number`);
  }
  return n;
}

/**
 * Coerce value to bigint. Handles boolean, number, null.
 */
export function toBigInt(v: unknown): bigint {
  if (typeof v === "bigint") return v;
  if (typeof v === "boolean") return v ? 1n : 0n;
  if (typeof v === "number") return BigInt(Math.trunc(v));
  if (v == null) return 0n;
  try {
    return BigInt(v as any);
  } catch {
    throw new TypeError(`Cannot coerce ${typeof v} "${v}" to bigint`);
  }
}

// --- Range constants ---

const INT8_MIN = -0x80;
const INT8_MAX = 0x7f;
const UINT8_MAX = 0xff;
const INT16_MIN = -0x8000;
const INT16_MAX = 0x7fff;
export const UINT16_MAX = 0xffff;
export const INT32_MIN = -0x80000000;
export const INT32_MAX = 0x7fffffff;
export const UINT32_MAX = 0xffffffff;

export const INT64_MIN = -(1n << 63n);
export const INT64_MAX = (1n << 63n) - 1n;
const UINT64_MAX = (1n << 64n) - 1n;
export const INT128_MIN = -(1n << 127n);
export const INT128_MAX = (1n << 127n) - 1n;
export const UINT128_MAX = (1n << 128n) - 1n;
export const INT256_MIN = -(1n << 255n);
export const INT256_MAX = (1n << 255n) - 1n;
export const UINT256_MAX = (1n << 256n) - 1n;

// --- Range-checked converters ---

function toIntInRange(v: unknown, typeName: string, min: number, max: number): number {
  const n = toNumber(v);
  if (!Number.isFinite(n)) {
    throw new TypeError(`Cannot coerce ${typeof v} "${v}" to ${typeName}`);
  }
  if (!Number.isInteger(n)) {
    throw new TypeError(`Cannot coerce ${typeof v} "${v}" to ${typeName} (expected integer)`);
  }
  if (n < min || n > max) {
    throw new RangeError(`${typeName} out of range: ${n} not in [${min}, ${max}]`);
  }
  return n;
}

export function toBigIntInRange(v: unknown, typeName: string, min: bigint, max: bigint): bigint {
  const b =
    typeof v === "number"
      ? (() => {
          if (!Number.isFinite(v)) {
            throw new TypeError(`Cannot coerce number "${v}" to ${typeName}`);
          }
          if (!Number.isInteger(v)) {
            throw new TypeError(`Cannot coerce number "${v}" to ${typeName} (expected integer)`);
          }
          if (!Number.isSafeInteger(v)) {
            throw new RangeError(
              `${typeName} cannot safely represent number "${v}". Use bigint or string.`,
            );
          }
          return BigInt(v);
        })()
      : toBigInt(v);

  if (b < min || b > max) {
    throw new RangeError(`${typeName} out of range: ${b} not in [${min}, ${max}]`);
  }
  return b;
}

// --- Typed integer converters ---

export const toUInt8 = (v: unknown) => toIntInRange(v, "UInt8", 0, UINT8_MAX);
export const toInt8 = (v: unknown) => toIntInRange(v, "Int8", INT8_MIN, INT8_MAX);
export const toUInt16 = (v: unknown) => toIntInRange(v, "UInt16", 0, UINT16_MAX);
export const toInt16 = (v: unknown) => toIntInRange(v, "Int16", INT16_MIN, INT16_MAX);
export const toUInt32 = (v: unknown) => toIntInRange(v, "UInt32", 0, UINT32_MAX);
export const toInt32 = (v: unknown) => toIntInRange(v, "Int32", INT32_MIN, INT32_MAX);
export const toUInt64 = (v: unknown) => toBigIntInRange(v, "UInt64", 0n, UINT64_MAX);
export const toInt64 = (v: unknown) => toBigIntInRange(v, "Int64", INT64_MIN, INT64_MAX);

// --- String coercion ---

function stringifyReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof Map) return Object.fromEntries(value);
  if (value instanceof Set) return Array.from(value);
  if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
    return Array.from(value as unknown as ArrayLike<number>);
  }
  return value;
}

export function coerceToString(v: unknown): string {
  if (v == null) return "";
  if (v instanceof Date) return v.toJSON();
  if (v instanceof ClickHouseDateTime64) return v.toJSON();
  switch (typeof v) {
    case "string":
    case "number":
    case "boolean":
    case "bigint":
      return String(v);
    case "object": {
      const s = JSON.stringify(v, stringifyReplacer);
      return typeof s === "string" ? s : "";
    }
    default:
      return String(v);
  }
}

// --- Boolean coercion ---

export function toBool(v: unknown): number {
  if (typeof v === "boolean") return v ? 1 : 0;
  if (typeof v === "number") return v !== 0 ? 1 : 0;
  if (v == null) return 0;
  if (typeof v === "string") {
    const lower = v.toLowerCase();
    if (lower === "true" || lower === "1") return 1;
    if (lower === "false" || lower === "0") return 0;
    throw new TypeError(`Cannot coerce string "${v}" to Bool`);
  }
  throw new TypeError(`Cannot coerce ${typeof v} to Bool`);
}

// --- Date/time coercion ---

export function toValidDate(v: unknown, typeName: string): Date {
  const d = new Date(v as any);
  if (Number.isNaN(d.getTime())) {
    throw new TypeError(`Cannot coerce "${v}" to ${typeName}`);
  }
  return d;
}

// --- IP address validation ---

export const IPV4_REGEX = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/;

export function toValidIPv4(v: unknown): string {
  if (v == null) return "0.0.0.0";
  const s = String(v);
  const m = IPV4_REGEX.exec(s);
  if (!m) {
    throw new TypeError(`Invalid IPv4 address: "${s}"`);
  }
  for (let i = 1; i <= 4; i++) {
    const octet = parseInt(m[i], 10);
    if (octet > 255) {
      throw new TypeError(`Invalid IPv4 address: "${s}" (octet ${octet} > 255)`);
    }
  }
  return s;
}

export function toValidIPv6(v: unknown): string {
  if (v == null) return "::";
  return String(v);
}

// --- UUID validation ---

const UUID_REGEX =
  /^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$/;

export function toValidUUID(v: unknown): string {
  if (v == null) return "00000000-0000-0000-0000-000000000000";
  const s = String(v);
  if (!UUID_REGEX.test(s)) {
    throw new TypeError(`Invalid UUID: "${s}"`);
  }
  return s;
}

// --- Decimal validation ---

export function toValidDecimal(v: unknown): string {
  if (v == null) return "0";
  if (typeof v === "bigint") return String(v);
  const s = String(v);
  // Basic validation: optional sign, digits, optional decimal point with digits
  if (!/^-?\d+(\.\d+)?$/.test(s)) {
    throw new TypeError(`Invalid Decimal: "${s}"`);
  }
  return s;
}

// --- Array detection ---

/** Check if value is an array-like (regular array or TypedArray) */
export function isArrayLike(v: unknown): v is unknown[] | TypedArray {
  return Array.isArray(v) || (ArrayBuffer.isView(v) && !(v instanceof DataView));
}
