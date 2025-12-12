/**
 * RowBinary encoder/decoder for ClickHouse
 */

// Types
export type ScalarType =
  | 'Int8' | 'Int16' | 'Int32' | 'Int64'
  | 'UInt8' | 'UInt16' | 'UInt32' | 'UInt64'
  | 'Float32' | 'Float64'
  | 'String' | 'Bool' | 'Date' | 'DateTime'

// ColumnType is string to allow arbitrary nesting:
// - Tuple(Int32, Array(String), Tuple(Float64, String))
// - Map(String, Array(Int32))
// - Array(Tuple(String, Int32))
export type ColumnType = string

export interface ColumnDef {
  name: string
  type: ColumnType
}

// LEB128 encoding (unsigned)
function leb128Size(value: number): number {
  let size = 0
  do {
    value >>>= 7
    size++
  } while (value !== 0)
  return size
}

function writeLEB128(view: DataView, offset: number, value: number): number {
  do {
    let byte = value & 0x7f
    value >>>= 7
    if (value !== 0) byte |= 0x80
    view.setUint8(offset++, byte)
  } while (value !== 0)
  return offset
}

// Parse decimal string to scaled BigInt
// e.g. "123.45" with scale 3 -> 123450n
function parseDecimalToScaledBigInt(str: string, scale: number): bigint {
  const negative = str.startsWith('-')
  if (negative) str = str.slice(1)
  const dotIdx = str.indexOf('.')
  let intPart: string
  let fracPart: string
  if (dotIdx === -1) {
    intPart = str
    fracPart = ''
  } else {
    intPart = str.slice(0, dotIdx)
    fracPart = str.slice(dotIdx + 1)
  }
  // Pad or truncate fractional part to scale digits
  if (fracPart.length < scale) {
    fracPart = fracPart.padEnd(scale, '0')
  } else if (fracPart.length > scale) {
    fracPart = fracPart.slice(0, scale)
  }
  const combined = intPart + fracPart
  const val = BigInt(combined)
  return negative ? -val : val
}

// Format scaled BigInt back to decimal string
function formatScaledBigInt(val: bigint, scale: number): string {
  const negative = val < 0n
  if (negative) val = -val
  let str = val.toString()
  if (scale === 0) return negative ? '-' + str : str
  // Pad with leading zeros if needed
  while (str.length <= scale) str = '0' + str
  const intPart = str.slice(0, -scale)
  const fracPart = str.slice(-scale)
  const result = intPart + '.' + fracPart
  return negative ? '-' + result : result
}

// Parse comma-separated type list with balanced parentheses
// e.g. "Int32, String, Tuple(Float64, String)" -> ["Int32", "String", "Tuple(Float64, String)"]
function parseTypeList(inner: string): string[] {
  const types: string[] = []
  let depth = 0
  let current = ""
  for (const char of inner) {
    if (char === '(') depth++
    if (char === ')') depth--
    if (char === ',' && depth === 0) {
      types.push(current.trim())
      current = ""
    } else {
      current += char
    }
  }
  if (current.trim()) types.push(current.trim())
  return types
}

interface TupleElement {
  name: string | null
  type: string
}

// Parse tuple elements, handling both named and unnamed forms
// Named: "a Int32, b String" -> [{name: "a", type: "Int32"}, {name: "b", type: "String"}]
// Unnamed: "Int32, String" -> [{name: null, type: "Int32"}, {name: null, type: "String"}]
function parseTupleElements(inner: string): TupleElement[] {
  const parts = parseTypeList(inner)
  return parts.map(part => {
    // Named element: identifier followed by space then type
    // Match: "name Type" where Type starts with uppercase or is a complex type
    // But NOT: "Array(String)" which starts with uppercase but has no space before paren
    const match = part.match(/^([a-z_][a-z0-9_]*)\s+(.+)$/i)
    if (match) {
      const potentialName = match[1]
      const potentialType = match[2]
      // Check if potentialName looks like a type (starts with known type prefix)
      const typeKeywords = ['Int', 'UInt', 'Float', 'String', 'Bool', 'Date', 'DateTime',
        'Nullable', 'Array', 'Tuple', 'Map', 'Enum', 'UUID', 'IPv', 'Decimal', 'FixedString',
        'Variant', 'JSON', 'Object']
      const isTypeName = typeKeywords.some(kw => potentialName.startsWith(kw))
      if (!isTypeName) {
        return { name: potentialName, type: potentialType }
      }
    }
    // Unnamed element
    return { name: null, type: part }
  })
}

// ============================================================================
// Shared Helper Functions
// ============================================================================

// Encode UUID hex string to bytes (ClickHouse stores as two reversed 8-byte parts)
function encodeUUIDBytes(hex: string, target: Uint8Array, offset: number): void {
  for (let i = 0; i < 8; i++) {
    target[offset + 7 - i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
    target[offset + 15 - i] = parseInt(hex.slice(16 + i * 2, 16 + i * 2 + 2), 16)
  }
}

// Expand IPv6 :: shorthand to full 8 parts
function expandIPv6(str: string): string[] {
  let parts = str.split(':')
  const emptyIdx = parts.indexOf('')
  if (emptyIdx !== -1) {
    const before = parts.slice(0, emptyIdx).filter(p => p)
    const after = parts.slice(emptyIdx + 1).filter(p => p)
    const missing = 8 - before.length - after.length
    parts = [...before, ...Array(missing).fill('0'), ...after]
  }
  return parts
}

// Extract scale from Decimal type string (handles both single and double arg forms)
function extractDecimalScale(type: string): number {
  // Matches: Decimal32(9), Decimal64(18, 4), Decimal128(38, 10), etc.
  const match = type.match(/Decimal\d*\((?:\d+,\s*)?(\d+)\)/)
  return match ? parseInt(match[1], 10) : 0
}

// ============================================================================
// Binary Type Encoding for Dynamic type
// See: https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding
// ============================================================================

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// Type codes for binary encoding
const TYPE_CODES: Record<string, number> = {
  'Nothing': 0x00,
  'UInt8': 0x01, 'UInt16': 0x02, 'UInt32': 0x03, 'UInt64': 0x04,
  'UInt128': 0x05, 'UInt256': 0x06,
  'Int8': 0x07, 'Int16': 0x08, 'Int32': 0x09, 'Int64': 0x0A,
  'Int128': 0x0B, 'Int256': 0x0C,
  'Float32': 0x0D, 'Float64': 0x0E,
  'Date': 0x0F, 'Date32': 0x10,
  'DateTime': 0x11, // 0x12 = DateTime with tz
  'DateTime64': 0x13, // 0x14 = DateTime64 with tz
  'String': 0x15,
  'FixedString': 0x16,
  'Enum8': 0x17, 'Enum16': 0x18,
  'Decimal32': 0x19, 'Decimal64': 0x1A, 'Decimal128': 0x1B, 'Decimal256': 0x1C,
  'UUID': 0x1D,
  'Array': 0x1E,
  'Tuple': 0x1F, // 0x20 = named tuple
  'Nullable': 0x23,
  'Map': 0x27,
  'IPv4': 0x28, 'IPv6': 0x29,
  'Variant': 0x2A,
  'Dynamic': 0x2B,
  'Bool': 0x2D,
}

// Reverse mapping for decoding
const CODE_TO_TYPE: Record<number, string> = {}
for (const [type, code] of Object.entries(TYPE_CODES)) {
  CODE_TO_TYPE[code] = type
}
// Add special codes
CODE_TO_TYPE[0x12] = 'DateTime' // with timezone
CODE_TO_TYPE[0x14] = 'DateTime64' // with timezone
CODE_TO_TYPE[0x20] = 'Tuple' // named tuple

// Encode a type string to binary format
function encodeTypeBinary(type: string): Uint8Array {
  const parts: number[] = []

  // Simple types (exact match)
  if (TYPE_CODES[type] !== undefined) {
    return new Uint8Array([TYPE_CODES[type]])
  }

  // Nullable(T)
  if (type.startsWith('Nullable(')) {
    parts.push(0x23)
    const inner = type.slice(9, -1)
    const innerEncoded = encodeTypeBinary(inner)
    parts.push(...innerEncoded)
    return new Uint8Array(parts)
  }

  // Array(T)
  if (type.startsWith('Array(')) {
    parts.push(0x1E)
    const inner = type.slice(6, -1)
    const innerEncoded = encodeTypeBinary(inner)
    parts.push(...innerEncoded)
    return new Uint8Array(parts)
  }

  // Map(K, V)
  if (type.startsWith('Map(')) {
    parts.push(0x27)
    const [keyType, valueType] = parseTypeList(type.slice(4, -1))
    parts.push(...encodeTypeBinary(keyType))
    parts.push(...encodeTypeBinary(valueType))
    return new Uint8Array(parts)
  }

  // Tuple(...) or Tuple(name Type, ...)
  if (type.startsWith('Tuple(')) {
    const elements = parseTupleElements(type.slice(6, -1))
    const isNamed = elements.length > 0 && elements[0].name !== null
    parts.push(isNamed ? 0x20 : 0x1F)
    // var_uint count
    parts.push(...encodeVarUint(elements.length))
    for (const elem of elements) {
      if (isNamed) {
        // name as var_uint length + bytes
        const nameBytes = textEncoder.encode(elem.name!)
        parts.push(...encodeVarUint(nameBytes.length))
        parts.push(...nameBytes)
      }
      parts.push(...encodeTypeBinary(elem.type))
    }
    return new Uint8Array(parts)
  }

  // Variant(T1, T2, ...)
  if (type.startsWith('Variant(')) {
    parts.push(0x2A)
    const variantTypes = parseTypeList(type.slice(8, -1))
    parts.push(...encodeVarUint(variantTypes.length))
    for (const vt of variantTypes) {
      parts.push(...encodeTypeBinary(vt))
    }
    return new Uint8Array(parts)
  }

  // DateTime64(precision) or DateTime64(precision, 'tz')
  if (type.startsWith('DateTime64')) {
    const match = type.match(/DateTime64\((\d+)(?:,\s*'([^']+)')?\)/)
    if (match) {
      const precision = parseInt(match[1], 10)
      const tz = match[2]
      if (tz) {
        parts.push(0x14)
        parts.push(precision)
        const tzBytes = textEncoder.encode(tz)
        parts.push(...encodeVarUint(tzBytes.length))
        parts.push(...tzBytes)
      } else {
        parts.push(0x13)
        parts.push(precision)
      }
      return new Uint8Array(parts)
    }
  }

  // FixedString(N)
  if (type.startsWith('FixedString(')) {
    parts.push(0x16)
    const n = parseInt(type.slice(12, -1), 10)
    parts.push(...encodeVarUint(n))
    return new Uint8Array(parts)
  }

  // Decimal32/64/128/256(P, S) or Decimal32/64/128/256(S)
  for (const [prefix, code] of [['Decimal32', 0x19], ['Decimal64', 0x1A], ['Decimal128', 0x1B], ['Decimal256', 0x1C]] as const) {
    if (type.startsWith(prefix + '(')) {
      parts.push(code)
      const inner = type.slice(prefix.length + 1, -1)
      const nums = inner.split(',').map(s => parseInt(s.trim(), 10))
      if (nums.length === 1) {
        // Just scale - precision is implicit from type
        const defaultPrecision = prefix === 'Decimal32' ? 9 : prefix === 'Decimal64' ? 18 : prefix === 'Decimal128' ? 38 : 76
        parts.push(defaultPrecision, nums[0])
      } else {
        parts.push(nums[0], nums[1])
      }
      return new Uint8Array(parts)
    }
  }

  // Generic Decimal(P, S)
  if (type.startsWith('Decimal(')) {
    const match = type.match(/Decimal\((\d+),\s*(\d+)\)/)
    if (match) {
      const precision = parseInt(match[1], 10)
      const scale = parseInt(match[2], 10)
      // Determine which Decimal type based on precision
      let code: number
      if (precision <= 9) code = 0x19
      else if (precision <= 18) code = 0x1A
      else if (precision <= 38) code = 0x1B
      else code = 0x1C
      parts.push(code, precision, scale)
      return new Uint8Array(parts)
    }
  }

  throw new Error(`Cannot encode type to binary: ${type}`)
}

// Helper to encode variable-length unsigned integer
function encodeVarUint(value: number): number[] {
  const bytes: number[] = []
  do {
    let byte = value & 0x7f
    value >>>= 7
    if (value !== 0) byte |= 0x80
    bytes.push(byte)
  } while (value !== 0)
  return bytes
}

// Decode a type from binary format, returns [type, bytesRead]
function decodeTypeBinary(data: Uint8Array, offset: number): [string, number] {
  const code = data[offset]
  offset++

  // Simple types
  const simpleType = CODE_TO_TYPE[code]
  if (simpleType && !['Array', 'Tuple', 'Nullable', 'Map', 'Variant', 'DateTime64', 'FixedString', 'Decimal32', 'Decimal64', 'Decimal128', 'Decimal256', 'DateTime'].includes(simpleType)) {
    return [simpleType, offset]
  }

  switch (code) {
    case 0x23: { // Nullable
      const [inner, newOffset] = decodeTypeBinary(data, offset)
      return [`Nullable(${inner})`, newOffset]
    }
    case 0x1E: { // Array
      const [inner, newOffset] = decodeTypeBinary(data, offset)
      return [`Array(${inner})`, newOffset]
    }
    case 0x27: { // Map
      const [keyType, keyEnd] = decodeTypeBinary(data, offset)
      const [valueType, valueEnd] = decodeTypeBinary(data, keyEnd)
      return [`Map(${keyType}, ${valueType})`, valueEnd]
    }
    case 0x1F: { // Tuple (unnamed)
      const [count, pos] = decodeVarUint(data, offset)
      offset = pos
      const types: string[] = []
      for (let i = 0; i < count; i++) {
        const [elemType, newOffset] = decodeTypeBinary(data, offset)
        types.push(elemType)
        offset = newOffset
      }
      return [`Tuple(${types.join(', ')})`, offset]
    }
    case 0x20: { // Tuple (named)
      const [count, pos] = decodeVarUint(data, offset)
      offset = pos
      const parts: string[] = []
      for (let i = 0; i < count; i++) {
        const [nameLen, namePos] = decodeVarUint(data, offset)
        const name = textDecoder.decode(data.subarray(namePos, namePos + nameLen))
        offset = namePos + nameLen
        const [elemType, newOffset] = decodeTypeBinary(data, offset)
        parts.push(`${name} ${elemType}`)
        offset = newOffset
      }
      return [`Tuple(${parts.join(', ')})`, offset]
    }
    case 0x2A: { // Variant
      const [count, pos] = decodeVarUint(data, offset)
      offset = pos
      const types: string[] = []
      for (let i = 0; i < count; i++) {
        const [varType, newOffset] = decodeTypeBinary(data, offset)
        types.push(varType)
        offset = newOffset
      }
      return [`Variant(${types.join(', ')})`, offset]
    }
    case 0x11: // DateTime (no tz)
      return ['DateTime', offset]
    case 0x12: { // DateTime with tz
      const [tzLen, tzPos] = decodeVarUint(data, offset)
      const tz = textDecoder.decode(data.subarray(tzPos, tzPos + tzLen))
      return [`DateTime('${tz}')`, tzPos + tzLen]
    }
    case 0x13: { // DateTime64 (no tz)
      const precision = data[offset]
      return [`DateTime64(${precision})`, offset + 1]
    }
    case 0x14: { // DateTime64 with tz
      const precision = data[offset]
      offset++
      const [tzLen, tzPos] = decodeVarUint(data, offset)
      const tz = textDecoder.decode(data.subarray(tzPos, tzPos + tzLen))
      return [`DateTime64(${precision}, '${tz}')`, tzPos + tzLen]
    }
    case 0x16: { // FixedString
      const [n, newOffset] = decodeVarUint(data, offset)
      return [`FixedString(${n})`, newOffset]
    }
    case 0x19: { // Decimal32
      const precision = data[offset]
      const scale = data[offset + 1]
      return [`Decimal32(${precision}, ${scale})`, offset + 2]
    }
    case 0x1A: { // Decimal64
      const precision = data[offset]
      const scale = data[offset + 1]
      return [`Decimal64(${precision}, ${scale})`, offset + 2]
    }
    case 0x1B: { // Decimal128
      const precision = data[offset]
      const scale = data[offset + 1]
      return [`Decimal128(${precision}, ${scale})`, offset + 2]
    }
    case 0x1C: { // Decimal256
      const precision = data[offset]
      const scale = data[offset + 1]
      return [`Decimal256(${precision}, ${scale})`, offset + 2]
    }
    default:
      throw new Error(`Unknown type code: 0x${code.toString(16)}`)
  }
}

// Helper to decode variable-length unsigned integer
function decodeVarUint(data: Uint8Array, offset: number): [number, number] {
  let value = 0
  let shift = 0
  while (true) {
    const byte = data[offset++]
    value |= (byte & 0x7f) << shift
    if ((byte & 0x80) === 0) break
    shift += 7
  }
  return [value, offset]
}

// ============================================================================
// Dynamic Type Inference
// ============================================================================

// Check if a value is an explicit Dynamic {type, value} object
function isExplicitDynamic(v: unknown): v is { type: string; value: unknown } {
  return typeof v === 'object' && v !== null &&
    'type' in v && 'value' in v &&
    typeof (v as { type: unknown }).type === 'string' &&
    Object.keys(v).length === 2
}

// 128-bit boundary for BigInt
const INT128_MAX = (1n << 127n) - 1n
const INT128_MIN = -(1n << 127n)

// Infer ClickHouse type from JS value
function inferType(value: unknown): string {
  if (value === null) return 'Nothing'
  if (typeof value === 'boolean') return 'Bool'
  if (typeof value === 'string') return 'String'
  if (typeof value === 'bigint') {
    // Use Int128 if within range, otherwise Int256
    if (value >= INT128_MIN && value <= INT128_MAX) return 'Int128'
    return 'Int256'
  }
  if (typeof value === 'number') {
    // Integer vs float: check if it's a whole number
    if (Number.isInteger(value)) return 'Int64'
    return 'Float64'
  }
  if (value instanceof Date) return 'DateTime64(3)'
  if (Array.isArray(value)) {
    if (value.length === 0) return 'Array(Nothing)'
    // Infer element type from first element
    const elemType = inferType(value[0])
    return `Array(${elemType})`
  }
  throw new Error(`Cannot infer Dynamic type for value: ${typeof value}`)
}

// Scalar type sizes
const SCALAR_SIZES: Record<ScalarType, number | null> = {
  Int8: 1, Int16: 2, Int32: 4, Int64: 8,
  UInt8: 1, UInt16: 2, UInt32: 4, UInt64: 8,
  Float32: 4, Float64: 8,
  Bool: 1,
  Date: 2,
  DateTime: 4,
  String: null, // variable
}

// Fast path codecs for numeric array elements
// Avoids switch statement overhead in encode/decode loops
interface NumericArrayCodec {
  size: number
  encode: (view: DataView, offset: number, value: number | bigint) => void
  decode: (view: DataView, offset: number) => number | bigint
}

const NUMERIC_ARRAY_CODECS: Record<string, NumericArrayCodec> = {
  Int32:   { size: 4, encode: (v, o, val) => v.setInt32(o, val as number, true),        decode: (v, o) => v.getInt32(o, true) },
  UInt32:  { size: 4, encode: (v, o, val) => v.setUint32(o, val as number, true),       decode: (v, o) => v.getUint32(o, true) },
  Int64:   { size: 8, encode: (v, o, val) => v.setBigInt64(o, BigInt(val), true),       decode: (v, o) => v.getBigInt64(o, true) },
  UInt64:  { size: 8, encode: (v, o, val) => v.setBigUint64(o, BigInt(val), true),      decode: (v, o) => v.getBigUint64(o, true) },
  Float32: { size: 4, encode: (v, o, val) => v.setFloat32(o, val as number, true),      decode: (v, o) => v.getFloat32(o, true) },
  Float64: { size: 8, encode: (v, o, val) => v.setFloat64(o, val as number, true),      decode: (v, o) => v.getFloat64(o, true) },
}

// TypedArray constructors for fast array decoding (returns view into buffer)
type TypedArrayConstructor =
  | typeof Int32Array | typeof Uint32Array
  | typeof BigInt64Array | typeof BigUint64Array
  | typeof Float32Array | typeof Float64Array

const TYPED_ARRAY_CTORS: Record<string, TypedArrayConstructor> = {
  Int32: Int32Array,
  UInt32: Uint32Array,
  Int64: BigInt64Array,
  UInt64: BigUint64Array,
  Float32: Float32Array,
  Float64: Float64Array,
}

// Encoder functions - write value at offset, return new offset
type Encoder = (view: DataView, offset: number, value: unknown) => number

const scalarEncoders: Record<ScalarType, Encoder> = {
  Int8: (view, offset, value) => {
    view.setInt8(offset, value as number)
    return offset + 1
  },
  Int16: (view, offset, value) => {
    view.setInt16(offset, value as number, true)
    return offset + 2
  },
  Int32: (view, offset, value) => {
    view.setInt32(offset, value as number, true)
    return offset + 4
  },
  Int64: (view, offset, value) => {
    view.setBigInt64(offset, BigInt(value as number | bigint), true)
    return offset + 8
  },
  UInt8: (view, offset, value) => {
    view.setUint8(offset, value as number)
    return offset + 1
  },
  UInt16: (view, offset, value) => {
    view.setUint16(offset, value as number, true)
    return offset + 2
  },
  UInt32: (view, offset, value) => {
    view.setUint32(offset, value as number, true)
    return offset + 4
  },
  UInt64: (view, offset, value) => {
    view.setBigUint64(offset, BigInt(value as number | bigint), true)
    return offset + 8
  },
  Float32: (view, offset, value) => {
    view.setFloat32(offset, value as number, true)
    return offset + 4
  },
  Float64: (view, offset, value) => {
    view.setFloat64(offset, value as number, true)
    return offset + 8
  },
  Bool: (view, offset, value) => {
    view.setUint8(offset, value ? 1 : 0)
    return offset + 1
  },
  Date: (view, offset, value) => {
    const date = value as Date
    const days = Math.floor(date.getTime() / 86400000)
    view.setUint16(offset, days, true)
    return offset + 2
  },
  DateTime: (view, offset, value) => {
    const date = value as Date
    const seconds = Math.floor(date.getTime() / 1000)
    view.setUint32(offset, seconds, true)
    return offset + 4
  },
  String: (view, offset, value) => {
    const bytes = value instanceof Uint8Array ? value : textEncoder.encode(value as string)
    offset = writeLEB128(view, offset, bytes.length)
    new Uint8Array(view.buffer, offset, bytes.length).set(bytes)
    return offset + bytes.length
  },
}

// Calculate size of a value for a given type
function valueSize(type: string, value: unknown): number {
  // Nullable(T)
  if (type.startsWith('Nullable(')) {
    const innerType = type.slice(9, -1)
    if (value === null) return 1
    return 1 + valueSize(innerType, value)
  }

  // Date32 - Int32 LE
  if (type === 'Date32') return 4

  // FixedString(N) - exactly N bytes
  if (type.startsWith('FixedString(')) {
    return parseInt(type.slice(12, -1), 10)
  }

  // Enum8/Enum16 - just the value size
  if (type.startsWith('Enum8')) return 1
  if (type.startsWith('Enum16')) return 2

  // UUID - 16 bytes
  if (type === 'UUID') return 16

  // IPv4 - 4 bytes (UInt32 LE)
  if (type === 'IPv4') return 4

  // IPv6 - 16 bytes (BE)
  if (type === 'IPv6') return 16

  // DateTime64 - 8 bytes (Int64 LE)
  if (type.startsWith('DateTime64')) return 8

  // Int128/UInt128 - 16 bytes
  if (type === 'Int128' || type === 'UInt128') return 16

  // Int256/UInt256 - 32 bytes
  if (type === 'Int256' || type === 'UInt256') return 32

  // Decimal types - size based on bit width in name
  if (type.startsWith('Decimal32')) return 4
  if (type.startsWith('Decimal64')) return 8
  if (type.startsWith('Decimal128')) return 16
  if (type.startsWith('Decimal256')) return 32

  // Generic Decimal(P, S) - size based on precision
  if (type.startsWith('Decimal(')) {
    const match = type.match(/Decimal\((\d+),\s*\d+\)/)
    if (match) {
      const precision = parseInt(match[1], 10)
      if (precision <= 9) return 4
      if (precision <= 18) return 8
      if (precision <= 38) return 16
      return 32
    }
  }

  // Variant(T1, T2, ...) - UInt8 discriminator + value (0xFF = NULL)
  if (type.startsWith('Variant(')) {
    if (value === null) return 1 // Just discriminator 0xFF
    const variantTypes = parseTypeList(type.slice(8, -1))
    const v = value as { type: number; value: unknown }
    return 1 + valueSize(variantTypes[v.type], v.value)
  }

  // JSON / Object('json') - native binary format:
  // <VarUInt num_paths> then for each path: <path_string><dynamic_value>
  if (type === 'JSON' || type === "Object('json')") {
    const obj = value as Record<string, unknown>
    const paths = Object.keys(obj)
    let size = leb128Size(paths.length)
    for (const path of paths) {
      // Path as string
      const pathBytes = textEncoder.encode(path)
      size += leb128Size(pathBytes.length) + pathBytes.length
      // Dynamic value (type + value)
      const val = obj[path]
      if (val === null) {
        size += 1 // Just 0x00 Nothing
      } else {
        const inferredType = inferType(val)
        const typeBytes = encodeTypeBinary(inferredType)
        size += typeBytes.length + valueSize(inferredType, val)
      }
    }
    return size
  }

  // Dynamic - self-describing type: <binary_type><value>
  if (type === 'Dynamic') {
    if (value === null) return 1 // Just 0x00 Nothing
    // Check if explicit {type, value} or infer from JS type
    let innerType: string
    let innerValue: unknown
    if (isExplicitDynamic(value)) {
      innerType = value.type
      innerValue = value.value
    } else {
      innerType = inferType(value)
      innerValue = value
    }
    const typeBytes = encodeTypeBinary(innerType)
    return typeBytes.length + valueSize(innerType, innerValue)
  }

  // Tuple(T1, T2, ...) or Tuple(name1 T1, name2 T2, ...) - sum of element sizes
  if (type.startsWith('Tuple(')) {
    const elements = parseTupleElements(type.slice(6, -1))
    const isNamed = elements.length > 0 && elements[0].name !== null
    let size = 0
    if (isNamed) {
      const obj = value as Record<string, unknown>
      for (const { name, type: elemType } of elements) {
        size += valueSize(elemType, obj[name!])
      }
    } else {
      const values = value as unknown[]
      for (let i = 0; i < elements.length; i++) {
        size += valueSize(elements[i].type, values[i])
      }
    }
    return size
  }

  // Map(K, V) - encoded as array of key-value pairs
  if (type.startsWith('Map(')) {
    const [keyType, valueType] = parseTypeList(type.slice(4, -1))
    const map = value as Map<unknown, unknown> | Record<string, unknown>
    const entries = map instanceof Map ? [...map.entries()] : Object.entries(map)
    let size = leb128Size(entries.length)
    for (const [k, v] of entries) {
      size += valueSize(keyType, k)
      size += valueSize(valueType, v)
    }
    return size
  }

  // Array(T) - leb128 count + elements (recursive)
  if (type.startsWith('Array(')) {
    const innerType = type.slice(6, -1)

    // Typed arrays - only valid for scalar numeric types
    if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
      const arr = value as ArrayBufferView
      const count = (arr as unknown as { length: number }).length
      return leb128Size(count) + arr.byteLength
    }

    // JS arrays - recurse for each element
    const arr = value as unknown[]
    let size = leb128Size(arr.length)
    for (const item of arr) {
      size += valueSize(innerType, item)
    }
    return size
  }

  // Scalar types
  const scalarType = type as ScalarType
  const fixedSize = SCALAR_SIZES[scalarType]
  if (fixedSize !== null) return fixedSize

  // String
  const bytes = value instanceof Uint8Array ? value : textEncoder.encode(value as string)
  return leb128Size(bytes.length) + bytes.length
}

// Encode a value
function encodeValue(view: DataView, offset: number, type: string, value: unknown): number {
  // Nullable(T)
  if (type.startsWith('Nullable(')) {
    const innerType = type.slice(9, -1)
    if (value === null) {
      view.setUint8(offset, 1)
      return offset + 1
    }
    view.setUint8(offset, 0)
    return encodeValue(view, offset + 1, innerType, value)
  }

  // Date32 - Int32 LE (signed days since epoch)
  if (type === 'Date32') {
    const date = value as Date
    const days = Math.floor(date.getTime() / 86400000)
    view.setInt32(offset, days, true)
    return offset + 4
  }

  // FixedString(N) - exactly N bytes, padded with \x00
  if (type.startsWith('FixedString(')) {
    const n = parseInt(type.slice(12, -1), 10)
    const bytes = value instanceof Uint8Array ? value : textEncoder.encode(value as string)
    const arr = new Uint8Array(view.buffer, offset, n)
    arr.fill(0) // pad with zeros
    arr.set(bytes.subarray(0, n)) // copy up to n bytes
    return offset + n
  }

  // Enum8 - Int8 value
  if (type.startsWith('Enum8')) {
    view.setInt8(offset, value as number)
    return offset + 1
  }

  // Enum16 - Int16 LE value
  if (type.startsWith('Enum16')) {
    view.setInt16(offset, value as number, true)
    return offset + 2
  }

  // UUID - stored as two UInt64 LE (high part first, each in LE)
  if (type === 'UUID') {
    const hex = (value as string).replace(/-/g, '')
    encodeUUIDBytes(hex, new Uint8Array(view.buffer, offset), 0)
    return offset + 16
  }

  // IPv4 - UInt32 LE from "a.b.c.d" (stored as network-order value in LE)
  if (type === 'IPv4') {
    const parts = (value as string).split('.').map(Number)
    // Network order: first octet is MSB
    const num = ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0
    view.setUint32(offset, num, true)
    return offset + 4
  }

  // IPv6 - 16 bytes BE from "xxxx:xxxx:..."
  if (type === 'IPv6') {
    const arr = new Uint8Array(view.buffer, offset, 16)
    const parts = expandIPv6(value as string)
    let byteIdx = 0
    for (const part of parts) {
      const val = parseInt(part || '0', 16)
      arr[byteIdx++] = (val >> 8) & 0xff
      arr[byteIdx++] = val & 0xff
    }
    return offset + 16
  }

  // DateTime64(precision) or DateTime64(precision, timezone) - Int64 LE ticks
  if (type.startsWith('DateTime64')) {
    // Parse precision from type string: DateTime64(3) or DateTime64(3, 'UTC')
    const match = type.match(/DateTime64\((\d+)/)
    const precision = match ? parseInt(match[1], 10) : 3
    const date = value as Date
    // Convert ms to ticks: ms * 10^(precision - 3)
    const ms = BigInt(date.getTime())
    const ticks = precision >= 3
      ? ms * (10n ** BigInt(precision - 3))
      : ms / (10n ** BigInt(3 - precision))
    view.setBigInt64(offset, ticks, true)
    return offset + 8
  }

  // Int128 - 16 bytes LE (signed)
  if (type === 'Int128') {
    const val = BigInt(value as bigint | number | string)
    const low = val & 0xffffffffffffffffn
    const high = val >> 64n
    view.setBigUint64(offset, low, true)
    view.setBigInt64(offset + 8, high, true)
    return offset + 16
  }

  // UInt128 - 16 bytes LE (unsigned)
  if (type === 'UInt128') {
    const val = BigInt(value as bigint | number | string)
    const low = val & 0xffffffffffffffffn
    const high = val >> 64n
    view.setBigUint64(offset, low, true)
    view.setBigUint64(offset + 8, high, true)
    return offset + 16
  }

  // Int256 - 32 bytes LE (signed)
  if (type === 'Int256') {
    let val = BigInt(value as bigint | number | string)
    for (let i = 0; i < 3; i++) {
      view.setBigUint64(offset + i * 8, val & 0xffffffffffffffffn, true)
      val >>= 64n
    }
    view.setBigInt64(offset + 24, val, true) // top 64 bits signed
    return offset + 32
  }

  // UInt256 - 32 bytes LE (unsigned)
  if (type === 'UInt256') {
    let val = BigInt(value as bigint | number | string)
    for (let i = 0; i < 4; i++) {
      view.setBigUint64(offset + i * 8, val & 0xffffffffffffffffn, true)
      val >>= 64n
    }
    return offset + 32
  }

  // Decimal32(P, S) - Int32 LE scaled integer
  if (type.startsWith('Decimal32')) {
    const scale = extractDecimalScale(type)
    const scaled = Math.round((value as number) * Math.pow(10, scale))
    view.setInt32(offset, scaled, true)
    return offset + 4
  }

  // Decimal64(P, S) - Int64 LE scaled integer
  if (type.startsWith('Decimal64')) {
    const scale = extractDecimalScale(type)
    const scaled = BigInt(Math.round((value as number) * Math.pow(10, scale)))
    view.setBigInt64(offset, scaled, true)
    return offset + 8
  }

  // Decimal128(S) or Decimal128(P, S) - Int128 LE scaled integer
  if (type.startsWith('Decimal128')) {
    const scale = extractDecimalScale(type)
    // Accept string for precision
    const strVal = typeof value === 'string' ? value : String(value)
    const scaled = parseDecimalToScaledBigInt(strVal, scale)
    // Write as Int128
    const low = scaled & 0xffffffffffffffffn
    const high = scaled >> 64n
    view.setBigUint64(offset, low, true)
    view.setBigInt64(offset + 8, high, true)
    return offset + 16
  }

  // Decimal256(S) or Decimal256(P, S) - Int256 LE scaled integer
  if (type.startsWith('Decimal256')) {
    const scale = extractDecimalScale(type)
    const strVal = typeof value === 'string' ? value : String(value)
    let scaled = parseDecimalToScaledBigInt(strVal, scale)
    // Write as Int256
    for (let i = 0; i < 3; i++) {
      view.setBigUint64(offset + i * 8, scaled & 0xffffffffffffffffn, true)
      scaled >>= 64n
    }
    view.setBigInt64(offset + 24, scaled, true)
    return offset + 32
  }

  // Generic Decimal(P, S) - dispatch based on precision
  if (type.startsWith('Decimal(')) {
    const match = type.match(/Decimal\((\d+),\s*(\d+)\)/)
    if (match) {
      const precision = parseInt(match[1], 10)
      const scale = parseInt(match[2], 10)
      const strVal = typeof value === 'string' ? value : String(value)
      const scaled = parseDecimalToScaledBigInt(strVal, scale)

      if (precision <= 9) {
        view.setInt32(offset, Number(scaled), true)
        return offset + 4
      } else if (precision <= 18) {
        view.setBigInt64(offset, scaled, true)
        return offset + 8
      } else if (precision <= 38) {
        const low = scaled & 0xffffffffffffffffn
        const high = scaled >> 64n
        view.setBigUint64(offset, low, true)
        view.setBigInt64(offset + 8, high, true)
        return offset + 16
      } else {
        let s = scaled
        for (let i = 0; i < 3; i++) {
          view.setBigUint64(offset + i * 8, s & 0xffffffffffffffffn, true)
          s >>= 64n
        }
        view.setBigInt64(offset + 24, s, true)
        return offset + 32
      }
    }
  }

  // Variant(T1, T2, ...) - UInt8 discriminator + value (0xFF = NULL)
  if (type.startsWith('Variant(')) {
    if (value === null) {
      view.setUint8(offset, 0xff)
      return offset + 1
    }
    const variantTypes = parseTypeList(type.slice(8, -1))
    const v = value as { type: number; value: unknown }
    view.setUint8(offset, v.type)
    return encodeValue(view, offset + 1, variantTypes[v.type], v.value)
  }

  // JSON / Object('json') - native binary format:
  // <VarUInt num_paths> then for each path: <path_string><dynamic_value>
  if (type === 'JSON' || type === "Object('json')") {
    const obj = value as Record<string, unknown>
    const paths = Object.keys(obj)
    offset = writeLEB128(view, offset, paths.length)
    for (const path of paths) {
      // Path as string
      const pathBytes = textEncoder.encode(path)
      offset = writeLEB128(view, offset, pathBytes.length)
      new Uint8Array(view.buffer, offset, pathBytes.length).set(pathBytes)
      offset += pathBytes.length
      // Dynamic value (type + value)
      const val = obj[path]
      if (val === null) {
        view.setUint8(offset, 0x00) // Nothing
        offset += 1
      } else {
        const inferredType = inferType(val)
        const typeBytes = encodeTypeBinary(inferredType)
        new Uint8Array(view.buffer, offset, typeBytes.length).set(typeBytes)
        offset += typeBytes.length
        offset = encodeValue(view, offset, inferredType, val)
      }
    }
    return offset
  }

  // Dynamic - self-describing type: <binary_type><value>
  if (type === 'Dynamic') {
    if (value === null) {
      view.setUint8(offset, 0x00) // Nothing type code
      return offset + 1
    }
    // Check if explicit {type, value} or infer from JS type
    let innerType: string
    let innerValue: unknown
    if (isExplicitDynamic(value)) {
      innerType = value.type
      innerValue = value.value
    } else {
      innerType = inferType(value)
      innerValue = value
    }
    const typeBytes = encodeTypeBinary(innerType)
    new Uint8Array(view.buffer, offset, typeBytes.length).set(typeBytes)
    offset += typeBytes.length
    return encodeValue(view, offset, innerType, innerValue)
  }

  // Tuple(T1, T2, ...) or Tuple(name1 T1, name2 T2, ...) - encode elements sequentially
  if (type.startsWith('Tuple(')) {
    const elements = parseTupleElements(type.slice(6, -1))
    const isNamed = elements.length > 0 && elements[0].name !== null
    if (isNamed) {
      const obj = value as Record<string, unknown>
      for (const { name, type: elemType } of elements) {
        offset = encodeValue(view, offset, elemType, obj[name!])
      }
    } else {
      const values = value as unknown[]
      for (let i = 0; i < elements.length; i++) {
        offset = encodeValue(view, offset, elements[i].type, values[i])
      }
    }
    return offset
  }

  // Map(K, V) - encode as array of key-value pairs
  if (type.startsWith('Map(')) {
    const [keyType, valueType] = parseTypeList(type.slice(4, -1))
    const map = value as Map<unknown, unknown> | Record<string, unknown>
    const entries = map instanceof Map ? [...map.entries()] : Object.entries(map)
    offset = writeLEB128(view, offset, entries.length)
    for (const [k, v] of entries) {
      offset = encodeValue(view, offset, keyType, k)
      offset = encodeValue(view, offset, valueType, v)
    }
    return offset
  }

  // Array(T) - leb128 count + elements (recursive)
  if (type.startsWith('Array(')) {
    const innerType = type.slice(6, -1)

    // Typed arrays - direct copy for scalars
    if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
      const arr = value as ArrayBufferView
      const count = (arr as unknown as { length: number }).length
      offset = writeLEB128(view, offset, count)
      new Uint8Array(view.buffer, offset, arr.byteLength).set(
        new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength)
      )
      return offset + arr.byteLength
    }

    // JS arrays - recurse for each element
    const arr = value as unknown[]
    offset = writeLEB128(view, offset, arr.length)
    for (const item of arr) {
      offset = encodeValue(view, offset, innerType, item)
    }
    return offset
  }

  // Scalar types - use encoder lookup
  return scalarEncoders[type as ScalarType](view, offset, value)
}

/**
 * Encode data in RowBinaryWithNames format
 */
export function encodeRowBinaryWithNames(
  columns: ColumnDef[],
  rows: unknown[][]
): Uint8Array {
  // Use streaming encoder for better performance
  const encoder = new RowBinaryEncoder()
  encoder.writeRowBinaryWithNames(columns, rows)
  return encoder.finish()
}

/**
 * Streaming RowBinary encoder - single pass with growable buffer
 */
class RowBinaryEncoder {
  private buffer: Uint8Array
  private view: DataView
  private offset = 0

  constructor(initialSize = 64 * 1024) {
    this.buffer = new Uint8Array(initialSize)
    this.view = new DataView(this.buffer.buffer)
  }

  private ensure(needed: number): void {
    if (this.offset + needed <= this.buffer.length) return
    const newSize = Math.max(this.buffer.length * 2, this.offset + needed)
    const newBuffer = new Uint8Array(newSize)
    newBuffer.set(this.buffer.subarray(0, this.offset))
    this.buffer = newBuffer
    this.view = new DataView(this.buffer.buffer)
  }

  private writeLEB128(value: number): void {
    this.ensure(5) // max 5 bytes for 32-bit
    do {
      let byte = value & 0x7f
      value >>>= 7
      if (value !== 0) byte |= 0x80
      this.buffer[this.offset++] = byte
    } while (value !== 0)
  }

  private writeString(value: string | Uint8Array): void {
    if (value instanceof Uint8Array) {
      this.writeLEB128(value.length)
      this.ensure(value.length)
      this.buffer.set(value, this.offset)
      this.offset += value.length
    } else {
      // Use encodeInto to avoid allocation
      // First estimate: 3 bytes per char max for UTF-8
      this.ensure(5 + value.length * 3)
      // Write placeholder for length, encode string, then fix length
      const lenOffset = this.offset
      this.offset += 1 // Reserve 1 byte for length (will fix if > 127)
      const { written } = textEncoder.encodeInto(value, this.buffer.subarray(this.offset))
      if (written <= 127) {
        this.buffer[lenOffset] = written
        this.offset += written
      } else {
        // Need multi-byte LEB128 - shift data and write proper length
        const lenBytes = leb128Size(written)
        if (lenBytes > 1) {
          // Move string data to make room for length
          this.buffer.copyWithin(lenOffset + lenBytes, lenOffset + 1, this.offset + written)
        }
        // Write length at original position
        let len = written
        let pos = lenOffset
        do {
          let byte = len & 0x7f
          len >>>= 7
          if (len !== 0) byte |= 0x80
          this.buffer[pos++] = byte
        } while (len !== 0)
        this.offset = lenOffset + lenBytes + written
      }
    }
  }

  writeValue(type: string, value: unknown): void {
    // Fast path for common scalar types
    switch (type) {
      case 'UInt8':
        this.ensure(1)
        this.view.setUint8(this.offset++, value as number)
        return
      case 'UInt16':
        this.ensure(2)
        this.view.setUint16(this.offset, value as number, true)
        this.offset += 2
        return
      case 'UInt32':
        this.ensure(4)
        this.view.setUint32(this.offset, value as number, true)
        this.offset += 4
        return
      case 'UInt64':
        this.ensure(8)
        this.view.setBigUint64(this.offset, BigInt(value as number | bigint), true)
        this.offset += 8
        return
      case 'Int8':
        this.ensure(1)
        this.view.setInt8(this.offset++, value as number)
        return
      case 'Int16':
        this.ensure(2)
        this.view.setInt16(this.offset, value as number, true)
        this.offset += 2
        return
      case 'Int32':
        this.ensure(4)
        this.view.setInt32(this.offset, value as number, true)
        this.offset += 4
        return
      case 'Int64':
        this.ensure(8)
        this.view.setBigInt64(this.offset, BigInt(value as number | bigint), true)
        this.offset += 8
        return
      case 'Float32':
        this.ensure(4)
        this.view.setFloat32(this.offset, value as number, true)
        this.offset += 4
        return
      case 'Float64':
        this.ensure(8)
        this.view.setFloat64(this.offset, value as number, true)
        this.offset += 8
        return
      case 'Bool':
        this.ensure(1)
        this.buffer[this.offset++] = value ? 1 : 0
        return
      case 'String':
        this.writeString(value as string | Uint8Array)
        return
      case 'Date': {
        this.ensure(2)
        const days = Math.floor((value as Date).getTime() / 86400000)
        this.view.setUint16(this.offset, days, true)
        this.offset += 2
        return
      }
      case 'DateTime': {
        this.ensure(4)
        const seconds = Math.floor((value as Date).getTime() / 1000)
        this.view.setUint32(this.offset, seconds, true)
        this.offset += 4
        return
      }
    }

    // Complex types
    this.writeComplexValue(type, value)
  }

  private writeComplexValue(type: string, value: unknown): void {
    // Nullable(T)
    if (type.startsWith('Nullable(')) {
      this.ensure(1)
      if (value === null) {
        this.buffer[this.offset++] = 1
        return
      }
      this.buffer[this.offset++] = 0
      this.writeValue(type.slice(9, -1), value)
      return
    }

    // Array(T)
    if (type.startsWith('Array(')) {
      const innerType = type.slice(6, -1)

      // Typed arrays - direct copy for scalars
      if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
        const arr = value as ArrayBufferView
        const count = (arr as unknown as { length: number }).length
        this.writeLEB128(count)
        this.ensure(arr.byteLength)
        this.buffer.set(new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength), this.offset)
        this.offset += arr.byteLength
        return
      }

      const arr = value as unknown[]
      this.writeLEB128(arr.length)

      // Fast path for String arrays
      if (innerType === 'String') {
        for (let i = 0; i < arr.length; i++) {
          this.writeString(arr[i] as string)
        }
        return
      }

      // Fast path for numeric arrays
      const codec = NUMERIC_ARRAY_CODECS[innerType]
      if (codec) {
        this.ensure(arr.length * codec.size)
        for (let i = 0; i < arr.length; i++) {
          codec.encode(this.view, this.offset, arr[i] as number | bigint)
          this.offset += codec.size
        }
        return
      }

      // Generic path for complex inner types
      for (const item of arr) {
        this.writeValue(innerType, item)
      }
      return
    }

    // Tuple - named or positional
    if (type.startsWith('Tuple(')) {
      const elements = parseTupleElements(type.slice(6, -1))
      const isNamed = elements.length > 0 && elements[0].name !== null
      if (isNamed) {
        const obj = value as Record<string, unknown>
        for (const { name, type: elemType } of elements) {
          this.writeValue(elemType, obj[name!])
        }
      } else {
        const arr = value as unknown[]
        for (let i = 0; i < elements.length; i++) {
          this.writeValue(elements[i].type, arr[i])
        }
      }
      return
    }

    // Map(K, V)
    if (type.startsWith('Map(')) {
      const [keyType, valueType] = parseTypeList(type.slice(4, -1))
      const map = value as Map<unknown, unknown> | Record<string, unknown>
      const entries = map instanceof Map ? [...map.entries()] : Object.entries(map)
      this.writeLEB128(entries.length)
      for (const [k, v] of entries) {
        this.writeValue(keyType, k)
        this.writeValue(valueType, v)
      }
      return
    }

    // FixedString(N)
    if (type.startsWith('FixedString(')) {
      const n = parseInt(type.slice(12, -1), 10)
      this.ensure(n)
      const bytes = value instanceof Uint8Array ? value : textEncoder.encode(value as string)
      this.buffer.fill(0, this.offset, this.offset + n)
      this.buffer.set(bytes.subarray(0, n), this.offset)
      this.offset += n
      return
    }

    // DateTime64(precision)
    if (type.startsWith('DateTime64')) {
      this.ensure(8)
      const match = type.match(/DateTime64\((\d+)/)
      const precision = match ? parseInt(match[1], 10) : 3
      const date = value as Date
      const ms = BigInt(date.getTime())
      const ticks = precision >= 3
        ? ms * (10n ** BigInt(precision - 3))
        : ms / (10n ** BigInt(3 - precision))
      this.view.setBigInt64(this.offset, ticks, true)
      this.offset += 8
      return
    }

    // Date32
    if (type === 'Date32') {
      this.ensure(4)
      const days = Math.floor((value as Date).getTime() / 86400000)
      this.view.setInt32(this.offset, days, true)
      this.offset += 4
      return
    }

    // UUID
    if (type === 'UUID') {
      this.ensure(16)
      const hex = (value as string).replace(/-/g, '')
      encodeUUIDBytes(hex, this.buffer, this.offset)
      this.offset += 16
      return
    }

    // IPv4
    if (type === 'IPv4') {
      this.ensure(4)
      const parts = (value as string).split('.').map(Number)
      const num = ((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) >>> 0
      this.view.setUint32(this.offset, num, true)
      this.offset += 4
      return
    }

    // IPv6
    if (type === 'IPv6') {
      this.ensure(16)
      const parts = expandIPv6(value as string)
      let byteIdx = this.offset
      for (const part of parts) {
        const val = parseInt(part || '0', 16)
        this.buffer[byteIdx++] = (val >> 8) & 0xff
        this.buffer[byteIdx++] = val & 0xff
      }
      this.offset += 16
      return
    }

    // Int128
    if (type === 'Int128') {
      this.ensure(16)
      const val = BigInt(value as bigint | number | string)
      const low = val & 0xffffffffffffffffn
      const high = val >> 64n
      this.view.setBigUint64(this.offset, low, true)
      this.view.setBigInt64(this.offset + 8, high, true)
      this.offset += 16
      return
    }

    // UInt128
    if (type === 'UInt128') {
      this.ensure(16)
      const val = BigInt(value as bigint | number | string)
      const low = val & 0xffffffffffffffffn
      const high = val >> 64n
      this.view.setBigUint64(this.offset, low, true)
      this.view.setBigUint64(this.offset + 8, high, true)
      this.offset += 16
      return
    }

    // Int256
    if (type === 'Int256') {
      this.ensure(32)
      let val = BigInt(value as bigint | number | string)
      for (let i = 0; i < 3; i++) {
        this.view.setBigUint64(this.offset + i * 8, val & 0xffffffffffffffffn, true)
        val >>= 64n
      }
      this.view.setBigInt64(this.offset + 24, val, true)
      this.offset += 32
      return
    }

    // UInt256
    if (type === 'UInt256') {
      this.ensure(32)
      let val = BigInt(value as bigint | number | string)
      for (let i = 0; i < 4; i++) {
        this.view.setBigUint64(this.offset + i * 8, val & 0xffffffffffffffffn, true)
        val >>= 64n
      }
      this.offset += 32
      return
    }

    // Enum8
    if (type.startsWith('Enum8')) {
      this.ensure(1)
      this.view.setInt8(this.offset++, value as number)
      return
    }

    // Enum16
    if (type.startsWith('Enum16')) {
      this.ensure(2)
      this.view.setInt16(this.offset, value as number, true)
      this.offset += 2
      return
    }

    // Decimal types
    if (type.startsWith('Decimal32')) {
      this.ensure(4)
      const scale = extractDecimalScale(type)
      const scaled = Math.round((value as number) * Math.pow(10, scale))
      this.view.setInt32(this.offset, scaled, true)
      this.offset += 4
      return
    }

    if (type.startsWith('Decimal64')) {
      this.ensure(8)
      const scale = extractDecimalScale(type)
      const scaled = BigInt(Math.round((value as number) * Math.pow(10, scale)))
      this.view.setBigInt64(this.offset, scaled, true)
      this.offset += 8
      return
    }

    if (type.startsWith('Decimal128')) {
      this.ensure(16)
      const scale = extractDecimalScale(type)
      const strVal = typeof value === 'string' ? value : String(value)
      const scaled = parseDecimalToScaledBigInt(strVal, scale)
      const low = scaled & 0xffffffffffffffffn
      const high = scaled >> 64n
      this.view.setBigUint64(this.offset, low, true)
      this.view.setBigInt64(this.offset + 8, high, true)
      this.offset += 16
      return
    }

    if (type.startsWith('Decimal256')) {
      this.ensure(32)
      const scale = extractDecimalScale(type)
      const strVal = typeof value === 'string' ? value : String(value)
      let scaled = parseDecimalToScaledBigInt(strVal, scale)
      for (let i = 0; i < 3; i++) {
        this.view.setBigUint64(this.offset + i * 8, scaled & 0xffffffffffffffffn, true)
        scaled >>= 64n
      }
      this.view.setBigInt64(this.offset + 24, scaled, true)
      this.offset += 32
      return
    }

    // Generic Decimal(P, S)
    if (type.startsWith('Decimal(')) {
      const match = type.match(/Decimal\((\d+),\s*(\d+)\)/)
      if (match) {
        const precision = parseInt(match[1], 10)
        const scale = parseInt(match[2], 10)
        const strVal = typeof value === 'string' ? value : String(value)
        const scaled = parseDecimalToScaledBigInt(strVal, scale)

        if (precision <= 9) {
          this.ensure(4)
          this.view.setInt32(this.offset, Number(scaled), true)
          this.offset += 4
        } else if (precision <= 18) {
          this.ensure(8)
          this.view.setBigInt64(this.offset, scaled, true)
          this.offset += 8
        } else if (precision <= 38) {
          this.ensure(16)
          const low = scaled & 0xffffffffffffffffn
          const high = scaled >> 64n
          this.view.setBigUint64(this.offset, low, true)
          this.view.setBigInt64(this.offset + 8, high, true)
          this.offset += 16
        } else {
          this.ensure(32)
          let s = scaled
          for (let i = 0; i < 3; i++) {
            this.view.setBigUint64(this.offset + i * 8, s & 0xffffffffffffffffn, true)
            s >>= 64n
          }
          this.view.setBigInt64(this.offset + 24, s, true)
          this.offset += 32
        }
        return
      }
    }

    // Variant
    if (type.startsWith('Variant(')) {
      this.ensure(1)
      if (value === null) {
        this.buffer[this.offset++] = 0xff
        return
      }
      const variantTypes = parseTypeList(type.slice(8, -1))
      const v = value as { type: number; value: unknown }
      this.buffer[this.offset++] = v.type
      this.writeValue(variantTypes[v.type], v.value)
      return
    }

    // JSON / Object('json')
    if (type === 'JSON' || type === "Object('json')") {
      const obj = value as Record<string, unknown>
      const paths = Object.keys(obj)
      this.writeLEB128(paths.length)
      for (const path of paths) {
        this.writeString(path)
        const val = obj[path]
        if (val === null) {
          this.ensure(1)
          this.buffer[this.offset++] = 0x00 // Nothing
        } else {
          const inferredType = inferType(val)
          const typeBytes = encodeTypeBinary(inferredType)
          this.ensure(typeBytes.length)
          this.buffer.set(typeBytes, this.offset)
          this.offset += typeBytes.length
          this.writeValue(inferredType, val)
        }
      }
      return
    }

    // Dynamic
    if (type === 'Dynamic') {
      if (value === null) {
        this.ensure(1)
        this.buffer[this.offset++] = 0x00 // Nothing
        return
      }
      let innerType: string
      let innerValue: unknown
      if (isExplicitDynamic(value)) {
        innerType = value.type
        innerValue = value.value
      } else {
        innerType = inferType(value)
        innerValue = value
      }
      const typeBytes = encodeTypeBinary(innerType)
      this.ensure(typeBytes.length)
      this.buffer.set(typeBytes, this.offset)
      this.offset += typeBytes.length
      this.writeValue(innerType, innerValue)
      return
    }

    throw new Error(`Unknown type: ${type}`)
  }

  writeRowBinaryWithNames(columns: ColumnDef[], rows: unknown[][]): void {
    // Column count
    this.writeLEB128(columns.length)

    // Column names
    for (const col of columns) {
      this.writeString(col.name)
    }

    // Rows
    for (const row of rows) {
      for (let i = 0; i < columns.length; i++) {
        this.writeValue(columns[i].type, row[i])
      }
    }
  }

  finish(): Uint8Array {
    return this.buffer.subarray(0, this.offset)
  }
}

// ============================================================================
// Decoding
// ============================================================================

// Read LEB128 encoded unsigned integer, returns [value, newOffset]
function readLEB128(data: Uint8Array, offset: number): [number, number] {
  let value = 0
  let shift = 0
  while (true) {
    const byte = data[offset++]
    value |= (byte & 0x7f) << shift
    if ((byte & 0x80) === 0) break
    shift += 7
  }
  return [value, offset]
}

// Pure JS UTF-8 decode for small strings (faster than TextDecoder for < 200 bytes)
function utf8DecodeSmall(data: Uint8Array, start: number, end: number): string {
  let result = ''
  let i = start
  while (i < end) {
    const byte = data[i++]
    if (byte < 0x80) {
      result += String.fromCharCode(byte)
    } else if (byte < 0xe0) {
      result += String.fromCharCode(((byte & 0x1f) << 6) | (data[i++] & 0x3f))
    } else if (byte < 0xf0) {
      result += String.fromCharCode(
        ((byte & 0x0f) << 12) | ((data[i++] & 0x3f) << 6) | (data[i++] & 0x3f)
      )
    } else {
      // 4-byte sequence (surrogate pair)
      const cp = ((byte & 0x07) << 18) | ((data[i++] & 0x3f) << 12) |
                 ((data[i++] & 0x3f) << 6) | (data[i++] & 0x3f)
      result += String.fromCharCode(
        0xd800 + ((cp - 0x10000) >> 10),
        0xdc00 + ((cp - 0x10000) & 0x3ff)
      )
    }
  }
  return result
}

const MIN_TEXT_DECODER_LENGTH = 8  // Pure JS faster below this, TextDecoder wins above

// Read length-prefixed string, returns [string, newOffset]
function readString(data: Uint8Array, offset: number): [string, number] {
  const [len, pos] = readLEB128(data, offset)
  const end = pos + len
  // Use pure JS for small strings (avoids TextDecoder bridge overhead)
  const str = len < MIN_TEXT_DECODER_LENGTH
    ? utf8DecodeSmall(data, pos, end)
    : textDecoder.decode(data.subarray(pos, end))
  return [str, end]
}

// Decoder functions - read value at offset, return [value, newOffset]
type Decoder = (view: DataView, data: Uint8Array, offset: number) => [unknown, number]

const scalarDecoders: Record<ScalarType, Decoder> = {
  Int8: (view, _, offset) => [view.getInt8(offset), offset + 1],
  Int16: (view, _, offset) => [view.getInt16(offset, true), offset + 2],
  Int32: (view, _, offset) => [view.getInt32(offset, true), offset + 4],
  Int64: (view, _, offset) => [view.getBigInt64(offset, true), offset + 8],
  UInt8: (view, _, offset) => [view.getUint8(offset), offset + 1],
  UInt16: (view, _, offset) => [view.getUint16(offset, true), offset + 2],
  UInt32: (view, _, offset) => [view.getUint32(offset, true), offset + 4],
  UInt64: (view, _, offset) => [view.getBigUint64(offset, true), offset + 8],
  Float32: (view, _, offset) => [view.getFloat32(offset, true), offset + 4],
  Float64: (view, _, offset) => [view.getFloat64(offset, true), offset + 8],
  Bool: (view, _, offset) => [view.getUint8(offset) !== 0, offset + 1],
  Date: (view, _, offset) => {
    const days = view.getUint16(offset, true)
    return [new Date(days * 86400000), offset + 2]
  },
  DateTime: (view, _, offset) => {
    const seconds = view.getUint32(offset, true)
    return [new Date(seconds * 1000), offset + 4]
  },
  String: (_, data, offset) => readString(data, offset),
}

// Decode a value given its type
function decodeValue(view: DataView, data: Uint8Array, offset: number, type: string): [unknown, number] {
  // Nullable(T)
  if (type.startsWith('Nullable(')) {
    const innerType = type.slice(9, -1)
    const isNull = data[offset] !== 0
    offset++
    if (isNull) return [null, offset]
    return decodeValue(view, data, offset, innerType)
  }

  // Date32 - Int32 LE (signed days since epoch)
  if (type === 'Date32') {
    const days = view.getInt32(offset, true)
    return [new Date(days * 86400000), offset + 4]
  }

  // FixedString(N) - exactly N bytes, strip trailing \x00
  if (type.startsWith('FixedString(')) {
    const n = parseInt(type.slice(12, -1), 10)
    const bytes = data.subarray(offset, offset + n)
    // Find first null byte to trim
    let end = n
    for (let i = 0; i < n; i++) {
      if (bytes[i] === 0) {
        end = i
        break
      }
    }
    return [textDecoder.decode(bytes.subarray(0, end)), offset + n]
  }

  // Enum8 - Int8 value
  if (type.startsWith('Enum8')) {
    return [view.getInt8(offset), offset + 1]
  }

  // Enum16 - Int16 LE value
  if (type.startsWith('Enum16')) {
    return [view.getInt16(offset, true), offset + 2]
  }

  // UUID - stored as two UInt64 LE (high part first, each in LE)
  if (type === 'UUID') {
    const bytes = data.subarray(offset, offset + 16)
    // Reverse first 8 bytes (high part), then reverse second 8 bytes (low part)
    const highPart = Array.from(bytes.subarray(0, 8)).reverse()
    const lowPart = Array.from(bytes.subarray(8, 16)).reverse()
    const hex = [...highPart, ...lowPart].map(b => b.toString(16).padStart(2, '0')).join('')
    const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
    return [uuid, offset + 16]
  }

  // IPv4 - UInt32 LE to "a.b.c.d" (stored as network-order value in LE)
  if (type === 'IPv4') {
    const num = view.getUint32(offset, true)
    // Network order: first octet is MSB
    const ip = `${(num >> 24) & 0xff}.${(num >> 16) & 0xff}.${(num >> 8) & 0xff}.${num & 0xff}`
    return [ip, offset + 4]
  }

  // IPv6 - 16 bytes BE to "xxxx:xxxx:..."
  if (type === 'IPv6') {
    const bytes = data.subarray(offset, offset + 16)
    const parts: string[] = []
    for (let i = 0; i < 16; i += 2) {
      const val = (bytes[i] << 8) | bytes[i + 1]
      parts.push(val.toString(16))
    }
    return [parts.join(':'), offset + 16]
  }

  // DateTime64(precision) - Int64 LE ticks to Date
  if (type.startsWith('DateTime64')) {
    const match = type.match(/DateTime64\((\d+)/)
    const precision = match ? parseInt(match[1], 10) : 3
    const ticks = view.getBigInt64(offset, true)
    // Convert ticks to ms: ticks / 10^(precision - 3)
    const ms = precision >= 3
      ? ticks / (10n ** BigInt(precision - 3))
      : ticks * (10n ** BigInt(3 - precision))
    return [new Date(Number(ms)), offset + 8]
  }

  // Int128 - 16 bytes LE (signed)
  if (type === 'Int128') {
    const low = view.getBigUint64(offset, true)
    const high = view.getBigInt64(offset + 8, true)
    return [(high << 64n) | low, offset + 16]
  }

  // UInt128 - 16 bytes LE (unsigned)
  if (type === 'UInt128') {
    const low = view.getBigUint64(offset, true)
    const high = view.getBigUint64(offset + 8, true)
    return [(high << 64n) | low, offset + 16]
  }

  // Int256 - 32 bytes LE (signed)
  if (type === 'Int256') {
    let val = view.getBigInt64(offset + 24, true) // top 64 bits signed
    for (let i = 2; i >= 0; i--) {
      val = (val << 64n) | view.getBigUint64(offset + i * 8, true)
    }
    return [val, offset + 32]
  }

  // UInt256 - 32 bytes LE (unsigned)
  if (type === 'UInt256') {
    let val = 0n
    for (let i = 3; i >= 0; i--) {
      val = (val << 64n) | view.getBigUint64(offset + i * 8, true)
    }
    return [val, offset + 32]
  }

  // Decimal32(P, S) - Int32 LE to string
  if (type.startsWith('Decimal32')) {
    const scale = extractDecimalScale(type)
    const val = view.getInt32(offset, true)
    return [formatScaledBigInt(BigInt(val), scale), offset + 4]
  }

  // Decimal64(P, S) - Int64 LE to string
  if (type.startsWith('Decimal64')) {
    const scale = extractDecimalScale(type)
    const val = view.getBigInt64(offset, true)
    return [formatScaledBigInt(val, scale), offset + 8]
  }

  // Decimal128(S) or Decimal128(P, S) - Int128 LE to string
  if (type.startsWith('Decimal128')) {
    const scale = extractDecimalScale(type)
    const low = view.getBigUint64(offset, true)
    const high = view.getBigInt64(offset + 8, true)
    const val = (high << 64n) | low
    return [formatScaledBigInt(val, scale), offset + 16]
  }

  // Decimal256(S) or Decimal256(P, S) - Int256 LE to string
  if (type.startsWith('Decimal256')) {
    const scale = extractDecimalScale(type)
    let val = view.getBigInt64(offset + 24, true)
    for (let i = 2; i >= 0; i--) {
      val = (val << 64n) | view.getBigUint64(offset + i * 8, true)
    }
    return [formatScaledBigInt(val, scale), offset + 32]
  }

  // Generic Decimal(P, S) - dispatch based on precision
  if (type.startsWith('Decimal(')) {
    const match = type.match(/Decimal\((\d+),\s*(\d+)\)/)
    if (match) {
      const precision = parseInt(match[1], 10)
      const scale = parseInt(match[2], 10)

      if (precision <= 9) {
        const val = BigInt(view.getInt32(offset, true))
        return [formatScaledBigInt(val, scale), offset + 4]
      } else if (precision <= 18) {
        const val = view.getBigInt64(offset, true)
        return [formatScaledBigInt(val, scale), offset + 8]
      } else if (precision <= 38) {
        const low = view.getBigUint64(offset, true)
        const high = view.getBigInt64(offset + 8, true)
        const val = (high << 64n) | low
        return [formatScaledBigInt(val, scale), offset + 16]
      } else {
        let val = view.getBigInt64(offset + 24, true)
        for (let i = 2; i >= 0; i--) {
          val = (val << 64n) | view.getBigUint64(offset + i * 8, true)
        }
        return [formatScaledBigInt(val, scale), offset + 32]
      }
    }
  }

  // Variant(T1, T2, ...) - UInt8 discriminator + value (0xFF = NULL)
  if (type.startsWith('Variant(')) {
    const discriminator = data[offset]
    offset++
    if (discriminator === 0xff) return [null, offset]
    const variantTypes = parseTypeList(type.slice(8, -1))
    const [val, newOffset] = decodeValue(view, data, offset, variantTypes[discriminator])
    return [{ type: discriminator, value: val }, newOffset]
  }

  // JSON / Object('json') - native binary format:
  // <VarUInt num_paths> then for each path: <path_string><dynamic_value>
  if (type === 'JSON' || type === "Object('json')") {
    const [numPaths, pos] = readLEB128(data, offset)
    offset = pos
    const result: Record<string, unknown> = {}
    for (let i = 0; i < numPaths; i++) {
      // Read path string
      const [path, pathEnd] = readString(data, offset)
      offset = pathEnd
      // Read dynamic value
      const [innerType, typeEnd] = decodeTypeBinary(data, offset)
      if (innerType === 'Nothing') {
        result[path] = null
        offset = typeEnd
      } else {
        const [val, valEnd] = decodeValue(view, data, typeEnd, innerType)
        result[path] = val
        offset = valEnd
      }
    }
    return [result, offset]
  }

  // Dynamic - self-describing type: <binary_type><value>
  if (type === 'Dynamic') {
    const [innerType, typeEnd] = decodeTypeBinary(data, offset)
    if (innerType === 'Nothing') {
      return [null, typeEnd]
    }
    const [val, valEnd] = decodeValue(view, data, typeEnd, innerType)
    return [{ type: innerType, value: val }, valEnd]
  }

  // Tuple(T1, T2, ...) or Tuple(name1 T1, name2 T2, ...) - decode elements sequentially
  if (type.startsWith('Tuple(')) {
    const elements = parseTupleElements(type.slice(6, -1))
    const isNamed = elements.length > 0 && elements[0].name !== null
    if (isNamed) {
      const obj: Record<string, unknown> = {}
      for (const { name, type: elemType } of elements) {
        const [val, newOffset] = decodeValue(view, data, offset, elemType)
        obj[name!] = val
        offset = newOffset
      }
      return [obj, offset]
    } else {
      const values: unknown[] = []
      for (const { type: elemType } of elements) {
        const [val, newOffset] = decodeValue(view, data, offset, elemType)
        values.push(val)
        offset = newOffset
      }
      return [values, offset]
    }
  }

  // Map(K, V) - decode as array of key-value pairs, return as object
  if (type.startsWith('Map(')) {
    const [keyType, valueType] = parseTypeList(type.slice(4, -1))
    const [count, pos] = readLEB128(data, offset)
    offset = pos
    const result: Record<string, unknown> = {}
    for (let i = 0; i < count; i++) {
      const [key, keyEnd] = decodeValue(view, data, offset, keyType)
      const [val, valEnd] = decodeValue(view, data, keyEnd, valueType)
      result[String(key)] = val
      offset = valEnd
    }
    return [result, offset]
  }

  // Array(T) - decode count then elements
  if (type.startsWith('Array(')) {
    const innerType = type.slice(6, -1)
    const [count, pos] = readLEB128(data, offset)
    offset = pos

    // Fast path for String arrays
    if (innerType === 'String') {
      const values: string[] = new Array(count)
      for (let i = 0; i < count; i++) {
        const [str, newOff] = readString(data, offset)
        values[i] = str
        offset = newOff
      }
      return [values, offset]
    }

    // Fast path for numeric arrays - return TypedArray view if aligned
    const Ctor = TYPED_ARRAY_CTORS[innerType]
    if (Ctor) {
      const codec = NUMERIC_ARRAY_CODECS[innerType]
      const byteLen = count * codec.size
      const absoluteOffset = data.byteOffset + offset
      // TypedArrays require alignment (e.g., Int32Array needs 4-byte alignment)
      if (absoluteOffset % codec.size === 0) {
        const values = new Ctor(data.buffer, absoluteOffset, count)
        return [values, offset + byteLen]
      }
      // Fallback: unaligned data, decode per-element
      const values = new Array(count)
      for (let i = 0; i < count; i++) {
        values[i] = codec.decode(view, offset)
        offset += codec.size
      }
      return [values, offset]
    }

    // Generic path for complex inner types
    const values: unknown[] = new Array(count)
    for (let i = 0; i < count; i++) {
      const [val, newOffset] = decodeValue(view, data, offset, innerType)
      values[i] = val
      offset = newOffset
    }
    return [values, offset]
  }

  // Scalar types
  const decoder = scalarDecoders[type as ScalarType]
  if (!decoder) {
    throw new Error(`Unknown type: ${type}`)
  }
  return decoder(view, data, offset)
}

export interface DecodeResult {
  columns: ColumnDef[]
  rows: unknown[][]
}

/**
 * Decode RowBinaryWithNames format data.
 * Requires column types to be provided since the format only includes names.
 */
export function decodeRowBinaryWithNames(
  data: Uint8Array,
  types: ColumnType[]
): DecodeResult {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = 0

  // Read column count
  const [colCount, pos1] = readLEB128(data, offset)
  offset = pos1

  if (colCount !== types.length) {
    throw new Error(`Column count mismatch: data has ${colCount}, provided ${types.length} types`)
  }

  // Read column names
  const columns: ColumnDef[] = []
  for (let i = 0; i < colCount; i++) {
    const [name, pos] = readString(data, offset)
    columns.push({ name, type: types[i] })
    offset = pos
  }

  // Read rows until end of data
  const rows: unknown[][] = []
  while (offset < data.length) {
    const row: unknown[] = []
    for (let i = 0; i < colCount; i++) {
      const [val, newOffset] = decodeValue(view, data, offset, types[i])
      row.push(val)
      offset = newOffset
    }
    rows.push(row)
  }

  return { columns, rows }
}

/**
 * Decode RowBinaryWithNamesAndTypes format data.
 * Self-describing format that includes column names and types.
 */
export function decodeRowBinaryWithNamesAndTypes(data: Uint8Array): DecodeResult {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
  let offset = 0

  // Read column count
  const [colCount, pos1] = readLEB128(data, offset)
  offset = pos1

  // Read column names
  const names: string[] = []
  for (let i = 0; i < colCount; i++) {
    const [name, pos] = readString(data, offset)
    names.push(name)
    offset = pos
  }

  // Read column types
  const types: string[] = []
  for (let i = 0; i < colCount; i++) {
    const [type, pos] = readString(data, offset)
    types.push(type)
    offset = pos
  }

  // Build column definitions
  const columns: ColumnDef[] = names.map((name, i) => ({ name, type: types[i] }))

  // Read rows until end of data
  const rows: unknown[][] = []
  while (offset < data.length) {
    const row: unknown[] = []
    for (let i = 0; i < colCount; i++) {
      const [val, newOffset] = decodeValue(view, data, offset, types[i])
      row.push(val)
      offset = newOffset
    }
    rows.push(row)
  }

  return { columns, rows }
}
