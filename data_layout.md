# ClickHouse Native Wire Format Data Layout

TODO: update this to not reference / assume use of native.ts. IE: writePrefix/readPrefix methods.

## Compressed Blocks

ClickHouse uses a custom compression framing format for native protocol compression. This is **not** standard LZ4/ZSTD framing - it's a ClickHouse-specific wrapper around raw compressed data.

### Block Layout

```
┌──────────────────────────────────────────────────────────────────┐
│ Checksum (16 bytes)                                              │
│   CityHash128 of [Header + Compressed Data]                      │
├──────────────────────────────────────────────────────────────────┤
│ Header (9 bytes)                                                 │
│   ├── Method (1 byte): 0x02=None, 0x82=LZ4, 0x90=ZSTD            │
│   ├── Compressed Size (4 bytes, UInt32 LE): Header + Data length │
│   └── Uncompressed Size (4 bytes, UInt32 LE): Original data size │
├──────────────────────────────────────────────────────────────────┤
│ Compressed Data (variable)                                       │
│   Raw LZ4 block or ZSTD frame (no library framing prefix!)       │
└──────────────────────────────────────────────────────────────────┘
```

**Total block size** = 16 (checksum) + 9 (header) + compressed_data_length = 25 + compressed_data_length

Note: The "Compressed Size" field in the header includes the 9-byte header itself, so:
`compressed_size = 9 + compressed_data.length`

### Raw Block Format (No Library Framing)

**Critical quirk**: The compressed data is a **raw** LZ4 block or ZSTD frame with **no framing prefix**.

Many LZ4 libraries (including lz4-napi and lz4-wasm) prepend a 4-byte uncompressed size prefix to their output. ClickHouse does **not** use this prefix - it stores the uncompressed size in the header instead.

When **compressing** for ClickHouse:
- Compress the data with your library
- Strip the 4-byte prefix if your library adds one
- Place raw compressed bytes in the block

When **decompressing** from ClickHouse:
- Read the uncompressed size from the header
- Prepend a 4-byte LE size prefix if your library requires one
- Pass to decompressor

Example (LZ4 with lz4-napi):
```typescript
// Compress: strip 4-byte prefix
const rawBlock = native.compressSync(data).subarray(4);

// Decompress: prepend 4-byte prefix
const withPrefix = new Uint8Array(4 + compressed.length);
withPrefix[0] = uncompressedSize & 0xff;
withPrefix[1] = (uncompressedSize >> 8) & 0xff;
withPrefix[2] = (uncompressedSize >> 16) & 0xff;
withPrefix[3] = (uncompressedSize >> 24) & 0xff;
withPrefix.set(compressed, 4);
const decompressed = native.uncompressSync(withPrefix);
```

ZSTD libraries typically don't have this quirk - they work with raw frames directly.

### Checksum

The checksum is **CityHash128 v1.0.2** computed over the header + compressed data (bytes 16 through end of block). Version matters - different CityHash versions produce different outputs.

The hash is stored in a specific byte order:
- Bytes 0-7: Low 64 bits of hash (little-endian)
- Bytes 8-15: High 64 bits of hash (little-endian)

Note: CityHash128 returns `[high, low]` by default, so the halves must be swapped for ClickHouse's expected order.

### Method Codes

| Method | Code | Description |
|--------|------|-------------|
| None   | 0x02 | No compression (passthrough) |
| LZ4    | 0x82 | LZ4 block compression |
| ZSTD   | 0x90 | ZSTD compression |

### Multiple Blocks

A compressed stream may contain multiple consecutive blocks. To decode:

1. Read 25 bytes (checksum + header)
2. Extract compressed_size from header (bytes 17-20, UInt32 LE)
3. Block size = 16 + compressed_size
4. Read remaining block data
5. Verify checksum, decompress
6. Repeat until end of stream

## Block Structure & Field Delimitation

The Native format has **no explicit delimiters** between fields. Parsing relies strictly on sequential reading, length prefixes, and known data types.

**Parsing Order:**

1.  **Block Header**:
    * `num_columns` (Varint): The number of columns in this block.
    * `num_rows` (Varint): The number of rows for every column in this block.
    * These are only presented once so you'll need to hang on to these two values for the rest of the parsing process.

2.  **Column Iteration** (Repeat `num_columns` times):
    * **Name** (`String`): Length-prefixed string (Varint Length + Bytes).
    * **Type** (`String`): Length-prefixed string.
      * *Critical*: The parser must use this type string to determine the expected binary layout of the following Prefix and Data sections.
    * **Prefix** (Optional, Type-dependent):
      * Some types (like `LowCardinality`, `Dynamic`, `JSON`) store metadata here.
      * Recursive types (`Array`, `Tuple`) delegate this to their inner types.
      * Simple scalar types usually have no prefix.
    * **Data** (Type-dependent):
      * The codec reads exactly `num_rows` of values.
      * The size is determined implicitly (e.g., `Int64` * `num_rows`) or explicitly via length prefixes (e.g., `String`).
    * **suffix**: none for native wire format.

---

## Basic Types

### Numeric (Int/UInt/Float)
Fixed-width, little-endian.
-   **Layout**: `[value_0] [value_1] ... [value_N]`
-   **Sizes**:
  - `Int8`/`UInt8`: 1 byte
  - `Int16`/`UInt16`: 2 bytes
  - `Int32`/`UInt32`: 4 bytes
  - `Int64`/`UInt64`: 8 bytes
  - `Float32`: 4 bytes (IEEE 754)
    example:
    > 1.5 in binary = 1.1 = 1.1 × 2^0
    >
    > IEEE 754 layout (32 bits):
    > ┌──────┬──────────┬─────────────────────────┐
    > │ Sign │ Exponent │ Mantissa (fractional)   │
    > │ 1bit │ 8 bits   │ 23 bits                 │
    > └──────┴──────────┴─────────────────────────┘
    >
    > Sign:     0 (positive)
    > Exponent: 0 + 127 (bias) = 127 = 01111111
    > Mantissa: .1 (implicit 1. prefix) = 10000000000000000000000
    >
    > Full: 0 01111111 10000000000000000000000 = 0x3FC00000
    >
    > Over the wire (little-endian): [0x00, 0x00, 0xC0, 0x3F]
  - `Float64`: 8 bytes (IEEE 754)
    > Example: 1.5
    >
    > IEEE 754 layout (64 bits):
    > ┌──────┬───────────┬────────────────────────────┐
    > │ Sign │ Exponent  │ Mantissa (fractional)      │
    > │ 1bit │ 11 bits   │ 52 bits                    │
    > └──────┴───────────┴────────────────────────────┘
    >
    > Sign:     0 (positive)
    > Exponent: 0 + 1023 (bias) = 1023 = 01111111111
    > Mantissa: .1 (implicit 1. prefix) = 1000...(52 bits)
    >
    > Full: 0x3FF8000000000000
    >
    > Over the wire (little-endian): [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF8, 0x3F]

### Bool
Presented as `UInt8`.
- **Layout**: `[0/1] [0/1] ...`

### String
Sequence of length-prefixed strings.
- **Layout**: `[len_0 (Varint)] [bytes_0] [len_1 (Varint)] [bytes_1] ...`

### FixedString(N)
Fixed sequence of `N` bytes per value.
- **Layout**: `[bytes_0 (N)] [bytes_1 (N)] ...`

### UUID
ClickHouse stores UUIDs as two little-endian UInt64 values. So for each UUID it's turned into 16 bytes, then each 8-byte half is reversed.
- **Layout**: `[uuid_0 (16 bytes)] [uuid_1 (16 bytes)] ...`
example:
> UUID string: 550e8400-e29b-41d4-a716-446655440000
> 
> Strip dashes: 550e8400e29b41d4a716446655440000
> 
> Each pair of hex chars = 1 byte:
> 
> Index:   0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15
> Hex:    55  0e  84  00  e2  9b  41  d4  a7  16  44  66  55  44  00  00
>         ├─────────────────────────────┤  ├────────────────────────────┤
>         UUID string chars 0-15           UUID string chars 16-31
>         (first half: 550e8400e29b41d4)   (second half: a716446655440000)
> 
> ClickHouse reverses each 8-byte half:
> 
> Wire:   d4  41  9b  e2  00  84  0e  55  00  00  44  55  66  44  16  a7
>         ├─────────────────────────────┤  ├────────────────────────────┤
>         bytes[7] down to bytes[0]        bytes[15] down to bytes[8]
> 
> So:
> - wire[0] = bytes[7] = d4
> - wire[1] = bytes[6] = 41
> - ...
> - wire[7] = bytes[0] = 55
> - wire[8] = bytes[15] = 00
> - ...
> - wire[15] = bytes[8] = a7

### Date / Date32
Days since epoch (1970-01-01).
- **Date**: `UInt16` `[day_0 (2 bytes)] ...`
- **Date32**: `Int32` `[day_0 (4 bytes)] ...`

### DateTime
Seconds since epoch.
- **Layout**: `UInt32` `[sec_0 (4 bytes)] ...`

### DateTime64(P)
Ticks since epoch as `Int64` (little-endian) - where ticks are 10^-P seconds.
Because the value is stored as a signed integer it supports dates before epoch (1970) as well.
> | P   | Tick unit          | Scale from ms |
> |-----|--------------------|---------------|
> | 0   | seconds (10⁰)      | ms / 1000     |
> | 3   | milliseconds (10³) | ms × 1        |
> | 6   | microseconds (10⁶) | ms × 1000     |
> | 9   | nanoseconds (10⁹)  | ms × 1000000  |
>
> So DateTime64(3) stores milliseconds since epoch, DateTime64(6) stores microseconds, etc.

  Wire format: signed 64-bit little-endian integer (Int64).
-   **Layout**: `Int64` `[tick_0 (8 bytes)] ...`

### IPv4
Presented as `UInt32` (little-endian. IE: byte-reversed).
> Packed into UInt32 from each octet in the standard dotted-decimal notation.
> 192      168      0        1
> 11000000 10101000 00000000 00000001
-   **Layout**: `[ip_0 (4 bytes)] ...`

### IPv6
Presented as 16 bytes (big-endian).
- **Layout**: `[ip_0 (16 bytes)] ...`

### Enum8 / Enum16
Presented as underlying integer type.
- **Enum8**: `Int8`.
- **Enum16**: `Int16`.

### Geo Types
represented by an underlying container type:
- **Point**: `Tuple(Float64, Float64)`
- **Ring**: `Array(Point)`
- **Polygon**: `Array(Ring)`
- **MultiPolygon**: `Array(Polygon)`

---

## Container Types

### Array(T)
- **Prefix**: `T.writePrefix` (called on flattened data).
- **Data**:
  1. `offsets`: `BigUint64` array of size `num_rows`. Cumulative end-indices.
  2. `data`: Encoded flattened values of type `T`.
- **Layout**: `[offset_0 (8 bytes)] ... [offset_N] [T_encoded_data]`

### Nullable(T)
- **Prefix**: `T.writePrefix` (called on non-null values).
- **Data**:
 1.  `null_flags`: `UInt8` array of size `num_rows`. `1` = NULL, `0` = Present.
 2.  `data`: Encoded values of type `T` (nulls replaced with zero-values).
- **Layout**: `[flag_0 (1 byte)] ... [flag_N] [T_encoded_data]`

### Tuple(T1, T2, ...)
- **Prefix**: `T1.writePrefix`, `T2.writePrefix`, ...
- **Data**: Concatenation of columns.
- **Layout**: `[T1_encoded_data] [T2_encoded_data] ...`

### Nested
Syntactic sugar for `Array(Tuple(...))`.
-   **Layout**: Same as `Array(Tuple(...))`.

### Map(K, V)
Serialized similar to `Array(Tuple(K, V))` but with split key/value storage.
- **Prefix**: `K.writePrefix`, `V.writePrefix`.
- **Data**:
 1. `offsets`: `BigUint64` array of size `num_rows` for this map's entries.
 2. `keys`: Encoded column `K` (flattened). TODO: what does "flattened" mean here?
 3. `values`: Encoded column `V` (flattened).
- **Layout**: `[offsets] [K_encoded_data] [V_encoded_data]`

### LowCardinality(T)
- **Prefix**:
    -  `version`: `UInt64` (value: 1).
- **Data**:
    1. `flags`: `UInt64` (Contains `HAS_ADDITIONAL_KEYS_BIT` and dictionary type size).
      - Bit 0 (1): `HAS_ADDITIONAL_KEYS_BIT` - if set, indicates presence of additional keys beyond the main dictionary. - what does that mean?
      - Bits 1-2: `DICT_INDEX_SIZE` - size of index type used
    2. `dict_size`: `UInt64` (Number of unique values).
    3. `dictionary_data`: Encoded values of type `T` (length `dict_size`).
    4. `count`: `UInt64` (Number of rows, should match `num_rows`).
    5. `indices`: Array of indices pointing to dictionary. Type depends on `dict_size` (`UInt8`, `UInt16`, `UInt32`, or `UInt64`).
- **Layout**: `[version] [flags] [dict_size] [dictionary_data] [count] [indices...]`

---

## Complex Types

### Variant(T1, T2, ...)
- **Prefix**: `UInt64` little-endian mode flag (0=BASIC, 1=COMPACT)

#### BASIC Mode (mode=0)
- **Data**:
  1. `discriminators`: `UInt8` × `num_rows`. Type index (0 to N-1), or `0xFF` for null.
  2. `columns`: Encoded blocks for each type present. Only rows matching that type.
- **Layout**: `[discriminators] [T1_subset_data] [T2_subset_data] ...`

COMPACT Mode (mode=1) is never sent over TCP/HTTP, only used for on-disk storage so I won't cover it.

### Dynamic (V3 Flattened)
- **Prefix**:
  1. `version`: `UInt64` (value: 3).
  2. `num_types`: `Varint`.
  3. `types`: Sequence of `String` (type names).
  4. **Sub-Prefixes**: Prefix data for each type found in `types`, in order.
    * Effectively calls `read/writePrefix` for each type sequentially.
    * If a type has no prefix data, nothing is written/read. You MUST read/write this in the exact order of `types`.
- **Data**:
  1.  `discriminators`: Array of indices (`UInt8`/`UInt16`/`UInt32` depending on number of types in `types`) of length `num_rows`.
    * Value `i` (< `types.length`) maps to `types[i]`.
    * Value `types.length` indicates **NULL**.
  2.  `columns`: Concatenated data blocks for each type in `types`.
    * Block `i` contains data **only** for rows where `discriminator == i`.
    * Example: If `types[0]` is `String`, and rows 0, 2, 5 are Strings, then `columns[0]` contains exactly 3 string values packed contiguously.
-   **Layout**: `[discriminators] [Type0_Data_Block] [Type1_Data_Block] ...`
-   **Decoding Hint**:
    1. Read `discriminators` first.
    2. Count occurrences of each type index `i` to determine `rowCount_i`.
    3. Sequentially read `Type0` (size `rowCount_0`), then `Type1`, etc., into separate arrays/buffers.
    4.  **Reconstruct rows**:
        *   Initialize a read cursor `idx_k = 0` for each type `k`.
        *   Loop through `discriminators`:
            *   Let `d` be the current discriminator value.
            *   If `d == types.length`, the value is `NULL`.
            *   Else, the value is `TypeBuffer_d[idx_d]`. Increment `idx_d`.

        **Visualization Example:**
        ```
        Types: ["String", "UInt64", "LowCardinality(String)"]
        Data: [
          { type: "String", value: "hello" },
          { type: "UInt64", value: 42 },
          { type: "String", value: "world" },
          { type: "NULL", value: null },
          { type: "LowCardinality(String)", value: "lc_val" }
        ]

        [Dynamic Header]
          |-- Version: 3
          |-- Num Types: 3
          |-- Type 0: "String"
          |-- Type 1: "UInt64"
          |-- Type 2: "LowCardinality(String)"

        [Prefixes]
          |-- [Prefix for String]     -> (Empty)
          |-- [Prefix for UInt64]     -> (Empty)
          |-- [Prefix for LC(String)] -> [Ver=1] [Flags] [DictSize=1] [Dict="lc_val"] ...

        [Data]
          |-- [Discriminators] ([0, 1, 0, 3, 2])  (3 = NULL index)
          |-- [String Block]     (["hello", "world"])
          |-- [UInt64 Block]     ([42])
          |-- [LC(String) Block] ([Index_0])
        ```

### JSON (V3 Flattened)
Presented as a set of `Dynamic` columns, one for each path found in the JSON objects it contains.
So JSON in Flattened format is like a map of key paths to `Dynamic` values.
- **Prefix**:
  1. `version`: `UInt64` (value: 3).
  2. `num_paths`: `Varint`.
  3. `paths`: Sequence of `String` (key paths).
    every single path in the JSON object is treated as a completely independent Dynamic column with its own prefix and data.
  4. `sub_prefixes`: `Dynamic` prefix for each path.
- **Data**:
  - Concatenation of `Dynamic` columns for each path.
- **Layout**: `[Dynamic_Path1_Data] [Dynamic_Path2_Data] ...`
```
Structure Visualization:
  [JSON Header]
    |-- Version: 3
    |-- Num Paths: 2
    |-- Path 1: "details.name"
    |-- Path 2: "details.id"
  
  [Prefixes]
    |-- [Dynamic Prefix for "details.name"]
    |      |-- Version: 3
    |      |-- Types: ["String"]
    |      |-- ...
    |
    |-- [Dynamic Prefix for "details.id"]
           |-- Version: 3
           |-- Types: ["UInt64", "String"] (mixed types possible)
           |-- ...
  
  [Data]
    |-- [Dynamic Body for "details.name"] (Discriminators + Data)
    |-- [Dynamic Body for "details.id"]   (Discriminators + Data)
```

## Other Types (Fallback)

For types not handled by a specialized Native codec (e.g., `Decimal`, `Int128`, `Int256`), the system falls back to a `ScalarCodec` which uses **RowBinary** encoding.

This works because these are fixed-width scalar types where the Native column format is identical to the concatenation of RowBinary values.

-   **Examples**: `Decimal(P, S)`, `Int128`, `UInt128`, `Int256`, `UInt256`.
-   **Layout**: `[RowBinary_Value_0] [RowBinary_Value_1] ...`

### Sparse serialization

WIP: some AI crap that's somewhat useful that I need to clean up / validate.

✦ Sparse serialization (introduced in ClickHouse 22.9, protocol revision 54454) is a storage and wire optimization for columns
  with a high percentage of default values. Instead of storing every row, it only stores the non-default values and a sparse
  index of their positions.

  Custom Serialization Header

  In the TCP protocol, every column in a Data packet (if revision >= 54454) is preceded by a 1-byte flag
  has_custom_serialization.

  If has_custom_serialization is 1, the column uses a custom serialization "Kinds" plan. ClickHouse sends a recursive tree of
  "Kind" bytes, one for each node in the type tree (e.g., Array(Int32) sends one byte for the Array and one for the Int32).


  ┌──────┬─────────┬───────────────────────────┐
  │ Kind │ Name    │ Description               │
  ├──────┼─────────┼───────────────────────────┤
  │ 0x00 │ Default │ Standard columnar format. │
  │ 0x01 │ Sparse  │ Sparse serialization.     │
  └──────┴─────────┴───────────────────────────┘

  Sparse Layout (Kind 0x01)

  When a type node is marked as Sparse, its data layout changes significantly:

   1. Sparse Index (Offsets): A sequence of Varints representing the gaps between non-default values.
       * Each Varint v indicates that the next v rows contain the default value.
       * If the Varint has bit 62 ($1 \ll 62$) set, it is the End of Stream marker. It indicates the number of trailing default
         values until the end of the block.
       * The index reading continues until the sum of all gaps and non-default values matches the num_rows of the block.

   2. Non-Default Values: Immediately following the complete index, the non-default values are stored contiguously using the
      standard codec for that type.
       * There is exactly one value for every gap Varint that did not have the "End of Stream" bit set.

  Visualization Example
  Consider a String column with 10 rows, where only rows 1 and 5 have values ("A" and "B"), and the rest are empty strings (the
  default).

  Index Section:
   * Varint(1): 1 default row (row 0), followed by a value.
   * Varint(3): 3 default rows (rows 2, 3, 4), followed by a value.
   * Varint(0x4000000000000004): Bit 62 set + value 4. 4 trailing default rows (rows 6, 7, 8, 9). End of block.

  Data Section:
   * String("A") (for the first gap)
   * String("B") (for the second gap)

  Resulting Column:
  ["", "A", "", "", "", "B", "", "", "", ""]

  Interaction with Container Types
  Sparse serialization can be applied to any level of a container. For example, in a Nullable(UInt64), the Nullable node itself
  could be dense while the inner UInt64 node is sparse. If the Nullable node is sparse, the "default value" used for gaps is
  NULL.
