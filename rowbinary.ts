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

// Encoder functions - write value at offset, return new offset
type Encoder = (view: DataView, offset: number, value: unknown) => number

const textEncoder = new TextEncoder()

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

  // JSON / Object('json') - encoded as String (JSON.stringify)
  if (type === 'JSON' || type === "Object('json')") {
    const jsonStr = JSON.stringify(value)
    const bytes = textEncoder.encode(jsonStr)
    return leb128Size(bytes.length) + bytes.length
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
    const arr = new Uint8Array(view.buffer, offset, 16)
    // First 8 bytes (high part) stored in reverse order
    for (let i = 0; i < 8; i++) {
      arr[7 - i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16)
    }
    // Second 8 bytes (low part) stored in reverse order
    for (let i = 0; i < 8; i++) {
      arr[15 - i] = parseInt(hex.slice(16 + i * 2, 16 + i * 2 + 2), 16)
    }
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
    const str = value as string
    // Handle :: expansion
    let parts = str.split(':')
    const emptyIdx = parts.indexOf('')
    if (emptyIdx !== -1) {
      // Found ::, expand it
      const before = parts.slice(0, emptyIdx).filter(p => p)
      const after = parts.slice(emptyIdx + 1).filter(p => p)
      const missing = 8 - before.length - after.length
      parts = [...before, ...Array(missing).fill('0'), ...after]
    }
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
    const scaleMatch = type.match(/Decimal32\(\d+,\s*(\d+)\)/)
    const scale = scaleMatch ? parseInt(scaleMatch[1], 10) : 0
    const scaled = Math.round((value as number) * Math.pow(10, scale))
    view.setInt32(offset, scaled, true)
    return offset + 4
  }

  // Decimal64(P, S) - Int64 LE scaled integer
  if (type.startsWith('Decimal64')) {
    const scaleMatch = type.match(/Decimal64\(\d+,\s*(\d+)\)/)
    const scale = scaleMatch ? parseInt(scaleMatch[1], 10) : 0
    const scaled = BigInt(Math.round((value as number) * Math.pow(10, scale)))
    view.setBigInt64(offset, scaled, true)
    return offset + 8
  }

  // Decimal128(S) or Decimal128(P, S) - Int128 LE scaled integer
  if (type.startsWith('Decimal128')) {
    // Match either Decimal128(S) or Decimal128(P, S)
    const singleMatch = type.match(/Decimal128\((\d+)\)$/)
    const doubleMatch = type.match(/Decimal128\(\d+,\s*(\d+)\)/)
    const scale = singleMatch ? parseInt(singleMatch[1], 10) : (doubleMatch ? parseInt(doubleMatch[1], 10) : 0)
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
    // Match either Decimal256(S) or Decimal256(P, S)
    const singleMatch = type.match(/Decimal256\((\d+)\)$/)
    const doubleMatch = type.match(/Decimal256\(\d+,\s*(\d+)\)/)
    const scale = singleMatch ? parseInt(singleMatch[1], 10) : (doubleMatch ? parseInt(doubleMatch[1], 10) : 0)
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

  // JSON / Object('json') - encoded as String (JSON.stringify)
  if (type === 'JSON' || type === "Object('json')") {
    const jsonStr = JSON.stringify(value)
    const bytes = textEncoder.encode(jsonStr)
    offset = writeLEB128(view, offset, bytes.length)
    new Uint8Array(view.buffer, offset, bytes.length).set(bytes)
    return offset + bytes.length
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
  // Calculate total size
  let size = leb128Size(columns.length)

  // Header: column names
  const encodedNames: Uint8Array[] = []
  for (const col of columns) {
    const nameBytes = textEncoder.encode(col.name)
    encodedNames.push(nameBytes)
    size += leb128Size(nameBytes.length) + nameBytes.length
  }

  // Row data
  for (const row of rows) {
    for (let i = 0; i < columns.length; i++) {
      size += valueSize(columns[i].type, row[i])
    }
  }

  // Allocate and write
  const buffer = new ArrayBuffer(size)
  const view = new DataView(buffer)
  const bytes = new Uint8Array(buffer)
  let offset = 0

  // Write column count
  offset = writeLEB128(view, offset, columns.length)

  // Write column names
  for (const nameBytes of encodedNames) {
    offset = writeLEB128(view, offset, nameBytes.length)
    bytes.set(nameBytes, offset)
    offset += nameBytes.length
  }

  // Write rows
  for (const row of rows) {
    for (let i = 0; i < columns.length; i++) {
      offset = encodeValue(view, offset, columns[i].type, row[i])
    }
  }

  return bytes
}

// ============================================================================
// Decoding
// ============================================================================

const textDecoder = new TextDecoder()

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

// Read length-prefixed string, returns [string, newOffset]
function readString(data: Uint8Array, offset: number): [string, number] {
  const [len, pos] = readLEB128(data, offset)
  const str = textDecoder.decode(data.subarray(pos, pos + len))
  return [str, pos + len]
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
    const scaleMatch = type.match(/Decimal32\(\d+,\s*(\d+)\)/)
    const scale = scaleMatch ? parseInt(scaleMatch[1], 10) : 0
    const val = view.getInt32(offset, true)
    return [formatScaledBigInt(BigInt(val), scale), offset + 4]
  }

  // Decimal64(P, S) - Int64 LE to string
  if (type.startsWith('Decimal64')) {
    const scaleMatch = type.match(/Decimal64\(\d+,\s*(\d+)\)/)
    const scale = scaleMatch ? parseInt(scaleMatch[1], 10) : 0
    const val = view.getBigInt64(offset, true)
    return [formatScaledBigInt(val, scale), offset + 8]
  }

  // Decimal128(S) or Decimal128(P, S) - Int128 LE to string
  if (type.startsWith('Decimal128')) {
    const singleMatch = type.match(/Decimal128\((\d+)\)$/)
    const doubleMatch = type.match(/Decimal128\(\d+,\s*(\d+)\)/)
    const scale = singleMatch ? parseInt(singleMatch[1], 10) : (doubleMatch ? parseInt(doubleMatch[1], 10) : 0)
    const low = view.getBigUint64(offset, true)
    const high = view.getBigInt64(offset + 8, true)
    const val = (high << 64n) | low
    return [formatScaledBigInt(val, scale), offset + 16]
  }

  // Decimal256(S) or Decimal256(P, S) - Int256 LE to string
  if (type.startsWith('Decimal256')) {
    const singleMatch = type.match(/Decimal256\((\d+)\)$/)
    const doubleMatch = type.match(/Decimal256\(\d+,\s*(\d+)\)/)
    const scale = singleMatch ? parseInt(singleMatch[1], 10) : (doubleMatch ? parseInt(doubleMatch[1], 10) : 0)
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

  // JSON / Object('json') - read as String, JSON.parse
  if (type === 'JSON' || type === "Object('json')") {
    const [jsonStr, newOffset] = readString(data, offset)
    return [JSON.parse(jsonStr), newOffset]
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
    const values: unknown[] = []
    for (let i = 0; i < count; i++) {
      const [val, newOffset] = decodeValue(view, data, offset, innerType)
      values.push(val)
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
