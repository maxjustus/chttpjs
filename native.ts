/**
 * Native format encoder/decoder for ClickHouse.
 *
 * This module re-exports from formats/native/ for backwards compatibility.
 * The implementation has been split into:
 *   - formats/native/io.ts      - Buffer I/O utilities
 *   - formats/native/columns.ts - Column data structures
 *   - formats/native/codecs.ts  - Type codecs
 *   - formats/native/index.ts   - Public API
 */

export {
  // Types
  type ColumnDef,
  type DecodeResult,
  type DecodeOptions,
  ClickHouseDateTime64,
  type Column,
  type BaseColumn,
  type ColumnarResult,
  type StreamDecodeNativeResult,

  // Column classes
  TypedColumn,
  SimpleColumn,
  TupleColumn,
  MapColumn,
  VariantColumn,
  DynamicColumn,
  JsonColumn,
  NullableColumn,
  ArrayColumn,
  type StringColumn,
  type BytesColumn,
  type DateColumn,
  type DateTime64Column,
  type ScalarColumn,

  // Public API functions
  encodeNative,
  encodeNativeColumnar,
  decodeNative,
  streamEncodeNative,
  streamDecodeNative,
  streamEncodeNativeColumnar,
  streamNativeRows,
  asRows,
  toArrayRows,
} from "./formats/native/index.ts";
