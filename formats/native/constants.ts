/**
 * Constants for ClickHouse Native format.
 */

export const Compression = {
  CHECKSUM_SIZE: 16,
  HEADER_SIZE: 9, // method(1) + compressed_size(4) + uncompressed_size(4)
  FULL_HEADER_SIZE: 25, // checksum + header
} as const;

export const BlockInfoField = {
  End: 0,
  IsOverflows: 1,
  BucketNum: 2,
} as const;

export const SerializationKind = {
  Default: 0,
  Sparse: 1,
  Custom: 2,
} as const;

export const LowCardinality = {
  // Version written in prefix (UInt64 LE)
  VERSION: 1n,

  // Flag indicating additional keys are present in the dictionary
  FLAG_ADDITIONAL_KEYS: 1n << 9n,

  // Index type codes (stored in lower 8 bits of flags)
  INDEX_TYPE_MASK: 0xffn,
  INDEX_U8: 0n,
  INDEX_U16: 1n,
  INDEX_U32: 2n,
  INDEX_U64: 3n,

  // Index type selection thresholds (max dictionary size for each index type)
  INDEX_U8_MAX: 255,
  INDEX_U16_MAX: 65535,
} as const;

export const Dynamic = {
  VERSION_V3: 3n,
} as const;

export const JSON = {
  VERSION_V3: 3n,
} as const;

export const Variant = {
  // UInt64 LE mode flag: 0=BASIC (row-by-row), 1=COMPACT (granule-based, storage only)
  MODE_BASIC: 0n,
  MODE_COMPACT: 1n,
  
  // 0xFF (255) is reserved for NULL in Variant
  NULL_DISCRIMINATOR: 0xFF,
} as const;

export const Sparse = {
  // END flag marks last entry in sparse offset stream
  END_OF_GRANULE_FLAG: 1n << 62n,
} as const;

export const UUID = {
  BYTE_SIZE: 16,
} as const;

export const IPv6 = {
  BYTE_SIZE: 16,
} as const;

export const Decimal = {
  BYTE_SIZE_32: 4,
  BYTE_SIZE_64: 8,
  BYTE_SIZE_128: 16,
  BYTE_SIZE_256: 32,
} as const;

/** Time conversion constants for Date/DateTime encoding */
export const Time = {
  /** Milliseconds per day (24 * 60 * 60 * 1000) */
  MS_PER_DAY: 86400000,
  /** Milliseconds per second */
  MS_PER_SECOND: 1000,
} as const;
