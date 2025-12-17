/**
 * Native format encoder/decoder for ClickHouse.
 *
 * Native is ClickHouse's columnar format - more efficient than RowBinary
 * because data doesn't need row-to-column conversion on the server.
 *
 * Note: Only Dynamic/JSON V3 format is supported at present. For ClickHouse 25.6+, enable
 * `output_format_native_use_flattened_dynamic_and_json_serialization` setting.
 */

import {
  type ColumnDef,
  type DecodeResult,
  type DecodeOptions,
  ClickHouseDateTime64,
} from "../shared.ts";

import { BufferWriter, BufferReader } from "./io.ts";
import { getCodec } from "./codecs.ts";
import {
  type Column,
  DataColumn,
} from "./columns.ts";

// Re-export types for public API
export { type ColumnDef, type DecodeResult, type DecodeOptions, ClickHouseDateTime64 };
export { type Column };
export {
  DataColumn,
  TupleColumn,
  MapColumn,
  VariantColumn,
  DynamicColumn,
  JsonColumn,
  NullableColumn,
  ArrayColumn,
} from "./columns.ts";

export interface ColumnarResult {
  columns: ColumnDef[];
  columnData: Column[];  // columnData[colIndex]
  rowCount: number;
}

// TypedArray constructors for fast path in encodeNative()
// Only simple numeric types that can be assigned directly without conversion
type SimpleTypedArrayCtor = Uint8ArrayConstructor | Int8ArrayConstructor |
  Uint16ArrayConstructor | Int16ArrayConstructor |
  Uint32ArrayConstructor | Int32ArrayConstructor |
  Float32ArrayConstructor | Float64ArrayConstructor;

const SIMPLE_TYPED_ARRAYS: Record<string, SimpleTypedArrayCtor | undefined> = {
  'UInt8': Uint8Array,
  'Int8': Int8Array,
  'UInt16': Uint16Array,
  'Int16': Int16Array,
  'UInt32': Uint32Array,
  'Int32': Int32Array,
  'Float32': Float32Array,
  'Float64': Float64Array,
  // Note: UInt64/Int64 need BigInt conversion, Bool needs 0/1 conversion
  // Date/DateTime need epoch conversion - all handled by slow path
};

export type StreamDecodeNativeResult = ColumnarResult;

interface BlockResult {
  columns: ColumnDef[];
  columnData: Column[];
  rowCount: number;
  bytesConsumed: number;
  isEndMarker: boolean;
}

interface BlockEstimate {
  estimatedSize: number;
  headerSize: number;  // bytes consumed reading header (numCols, numRows, names, types)
}

/**
 * Peek at block header to estimate total block size without full decode.
 * Returns null if not enough data for header, or the estimate.
 */
function estimateBlockSize(
  data: Uint8Array,
  offset: number,
): BlockEstimate | null {
  try {
    const reader = new BufferReader(data, offset);
    const startOffset = reader.offset;

    const numCols = reader.readVarint();
    const numRows = reader.readVarint();

    // End marker - tiny block
    if (numCols === 0 && numRows === 0) {
      return { estimatedSize: reader.offset - startOffset, headerSize: reader.offset - startOffset };
    }

    // Read column names and types to estimate data size
    let dataEstimate = 0;
    for (let i = 0; i < numCols; i++) {
      reader.readString(); // name
      const type = reader.readString();
      dataEstimate += getCodec(type).estimateSize(numRows);
    }

    const headerSize = reader.offset - startOffset;
    // Add 20% buffer for prefix data, LowCardinality dictionaries, etc.
    return { estimatedSize: headerSize + Math.ceil(dataEstimate * 1.2), headerSize };
  } catch {
    // Not enough data even for header
    return null;
  }
}

/**
 * Decode a single Native format block from a buffer.
 * Returns the decoded data and the number of bytes consumed.
 * Use this for streaming scenarios where you need to track buffer position.
 */
function decodeNativeBlock(
  data: Uint8Array,
  offset: number,
  options?: DecodeOptions,
): BlockResult {
  const reader = new BufferReader(data, offset, options);
  const startOffset = reader.offset;

  const numCols = reader.readVarint();
  const numRows = reader.readVarint();

  // Empty block signals end of data
  if (numCols === 0 && numRows === 0) {
    return {
      columns: [],
      columnData: [],
      rowCount: 0,
      bytesConsumed: reader.offset - startOffset,
      isEndMarker: true,
    };
  }

  const columns: ColumnDef[] = [];
  const columnData: Column[] = [];

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < numCols; i++) {
    const name = reader.readString();
    const type = reader.readString();
    columns.push({ name, type });

    const codec = getCodec(type);
    codec.readPrefix?.(reader);
    columnData.push(codec.decode(reader, numRows));
  }

  return {
    columns,
    columnData,
    rowCount: numRows,
    bytesConsumed: reader.offset - startOffset,
    isEndMarker: false,
  };
}

/**
 * Encode row-oriented data to Native format.
 * Input: rows[rowIndex][colIndex]
 */
export function encodeNative(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const numRows = rows.length;
  const numCols = columns.length;
  const cols: (unknown[] | Column)[] = new Array(numCols);

  // Transpose rows to columns with fast path for simple numeric types
  for (let i = 0; i < numCols; i++) {
    const TypedArrayCtor = SIMPLE_TYPED_ARRAYS[columns[i].type];
    if (TypedArrayCtor) {
      // Fast path: write directly to TypedArray (no intermediate JS array)
      const arr = new TypedArrayCtor(numRows);
      for (let j = 0; j < numRows; j++) arr[j] = rows[j][i] as number;
      cols[i] = new DataColumn(arr);
    } else {
      // Slow path: JS array for complex/converted types
      const arr = new Array(numRows);
      for (let j = 0; j < numRows; j++) arr[j] = rows[j][i];
      cols[i] = arr;
    }
  }

  return encodeNativeColumnar(columns, cols, numRows);
}

/**
 * Encode columnar data to Native format (no transpose needed).
 * Input: columnData[colIndex][rowIndex] or Column objects
 */
export function encodeNativeColumnar(
  columns: ColumnDef[],
  columnData: (unknown[] | Column)[],
  rowCount?: number,
): Uint8Array {
  const writer = new BufferWriter();
  const numRows = rowCount ?? (columnData[0]?.length ?? 0);

  writer.writeVarint(columns.length);
  writer.writeVarint(numRows);

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < columns.length; i++) {
    const codec = getCodec(columns[i].type);
    const data = columnData[i];

    // Convert raw data to Column if needed
    // Fast path: TypedArrays wrap directly in DataColumn (zero-copy)
    // Duck type check for existing Column objects
    const col: Column = (data && typeof (data as any).get === 'function')
      ? data as Column
      : ArrayBuffer.isView(data) && !(data instanceof DataView)
        ? new DataColumn(data as any)
        : codec.fromValues(data as unknown[]);

    writer.writeString(columns[i].name);
    writer.writeString(columns[i].type);
    codec.writePrefix?.(writer, col);
    writer.write(codec.encode(col));
  }

  return writer.finish();
}

export async function decodeNative(
  data: Uint8Array,
  options?: DecodeOptions,
): Promise<ColumnarResult> {
  const blocks: ColumnarResult[] = [];

  // Wrap data in single-chunk async iterable and use streamDecodeNative
  async function* singleChunk() {
    yield data;
  }

  for await (const block of streamDecodeNative(singleChunk(), options)) {
    blocks.push(block);
  }

  // Fast path: single block, return directly (preserves columnar types)
  if (blocks.length === 0) {
    return { columns: [], columnData: [], rowCount: 0 };
  }
  if (blocks.length === 1) {
    return blocks[0];
  }

  // Multi-block: merge by materializing values
  // Note: This loses byte fidelity for NaN - use streaming for exact round-trip
  const columns = blocks[0].columns;
  const numCols = columns.length;
  const allColumnData: unknown[][] = [];
  for (let i = 0; i < numCols; i++) {
    allColumnData.push([]);
  }

  let totalRows = 0;
  for (const block of blocks) {
    for (let i = 0; i < numCols; i++) {
      const col = block.columnData[i];
      const target = allColumnData[i];
      for (let j = 0; j < col.length; j++) {
        target.push(col.get(j));
      }
    }
    totalRows += block.rowCount;
  }

  // Wrap merged arrays in DataColumn
  return {
    columns,
    columnData: allColumnData.map(arr => new DataColumn(arr)),
    rowCount: totalRows,
  };
}

export async function* streamEncodeNative(
  columns: ColumnDef[],
  rows: Iterable<unknown[]> | AsyncIterable<unknown[]>,
  options: { blockSize?: number } = {},
): AsyncGenerator<Uint8Array> {
  const blockSize = options.blockSize ?? 65536;
  let batch: unknown[][] = [];

  for await (const row of rows as AsyncIterable<unknown[]>) {
    batch.push(row);
    if (batch.length >= blockSize) {
      yield encodeNative(columns, batch);
      batch = [];
    }
  }
  if (batch.length > 0) yield encodeNative(columns, batch);
}

function flattenChunks(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 0) return new Uint8Array(0);
  if (chunks.length === 1) return chunks[0];
  const totalLength = chunks.reduce((sum, c) => sum + c.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

function consumeBytes(chunks: Uint8Array[], bytes: number): number {
  let consumed = 0;
  while (bytes > 0 && chunks.length > 0) {
    if (chunks[0].length <= bytes) {
      bytes -= chunks[0].length;
      consumed += chunks[0].length;
      chunks.shift();
    } else {
      chunks[0] = chunks[0].subarray(bytes);
      consumed += bytes;
      bytes = 0;
    }
  }
  return consumed;
}

/**
 * Lazily iterate rows as objects with column names as keys.
 * Allocates one object per row on demand.
 * Note: May normalize NaN values when accessing float columns.
 */
export function* asRows(result: ColumnarResult): Generator<Record<string, unknown>> {
  const { columns, columnData, rowCount } = result;
  const numCols = columns.length;

  // Hybrid approach: direct data for DataColumn (avoids TypedArray→JS array copy),
  // toArray() for composite types (benefits from caching)
  const cols: unknown[][] = columnData.map(col =>
    col instanceof DataColumn ? col.data as unknown[] : col.toArray()
  );

  for (let i = 0; i < rowCount; i++) {
    const row: Record<string, unknown> = {};
    for (let j = 0; j < numCols; j++) {
      row[columns[j].name] = cols[j][i];
    }
    yield row;
  }
}

/**
 * Convert columnar result to array rows.
 * Useful for re-encoding or comparison with original row arrays.
 */
export function toArrayRows(result: ColumnarResult): unknown[][] {
  const { columnData, rowCount } = result;
  const numCols = columnData.length;

  // Hybrid approach: direct data for DataColumn (avoids TypedArray→JS array copy),
  // toArray() for composite types (benefits from caching)
  const cols: unknown[][] = columnData.map(col =>
    col instanceof DataColumn ? col.data as unknown[] : col.toArray()
  );

  const rows: unknown[][] = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    const row = new Array(numCols);
    for (let j = 0; j < numCols; j++) {
      row[j] = cols[j][i];
    }
    rows[i] = row;
  }
  return rows;
}

export async function* streamDecodeNative(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions & { debug?: boolean; minBufferSize?: number },
): AsyncGenerator<ColumnarResult> {
  const pendingChunks: Uint8Array[] = [];
  let columns: ColumnDef[] = [];
  let totalBytesReceived = 0;
  let blocksDecoded = 0;
  let bufferUnderruns = 0;
  let pendingBytes = 0;

  // Minimum bytes to buffer before attempting decode (reduces failed decode attempts)
  // We track average block size to predict when we have enough data
  const minBuffer = options?.minBufferSize ?? 2 * 1024 * 1024; // 2MB default
  let avgBlockSize = 0;
  let totalBlockBytes = 0;

  for await (const chunk of chunks) {
    pendingChunks.push(chunk);
    pendingBytes += chunk.length;
    totalBytesReceived += chunk.length;

    // Skip decode attempt if we don't have enough data yet
    // Use 120% of average block size as threshold (conservative to avoid wasted partial decodes)
    const threshold = blocksDecoded > 0
      ? Math.max(minBuffer / 4, avgBlockSize * 1.2)  // After first block, use avg + 20% buffer
      : minBuffer;
    if (pendingBytes < threshold) continue;

    // Try to decode as many complete blocks as possible
    while (pendingChunks.length > 0) {
      const buffer = flattenChunks(pendingChunks);

      // First, estimate if we have enough data (avoids wasted decode attempts)
      const estimate = estimateBlockSize(buffer, 0);
      if (estimate === null || buffer.length < estimate.estimatedSize) {
        // Not enough data - wait for more
        break;
      }

      try {
        const block = decodeNativeBlock(buffer, 0, options);

        if (block.isEndMarker) {
          consumeBytes(pendingChunks, block.bytesConsumed);
          pendingBytes -= block.bytesConsumed;
          // Don't break - continue processing any remaining data after end marker
          continue;
        }

        // Set columns from first block
        if (columns.length === 0) {
          columns = block.columns;
        }

        totalBlockBytes += block.bytesConsumed;
        consumeBytes(pendingChunks, block.bytesConsumed);
        pendingBytes -= block.bytesConsumed;
        blocksDecoded++;
        avgBlockSize = totalBlockBytes / blocksDecoded;
        yield { columns, columnData: block.columnData, rowCount: block.rowCount };
      } catch {
        // Estimate was wrong (e.g., variable-length data larger than expected)
        // Wait for more chunks
        bufferUnderruns++;
        break;
      }
    }
  }

  if (options?.debug) {
    console.log(`[streamDecodeNative] ${blocksDecoded} blocks, ${totalBytesReceived} bytes, ${bufferUnderruns} underruns`);
  }

  // Handle any remaining data after stream ends
  if (pendingChunks.length > 0) {
    const buffer = flattenChunks(pendingChunks);
    // After streaming loop ends, all remaining data should be in pendingChunks
    // If there's substantial data left, try to decode it
    let offset = 0;
    while (offset < buffer.length) {
      try {
        const block = decodeNativeBlock(buffer, offset, options);
        if (block.isEndMarker) {
          offset += block.bytesConsumed;
          continue;  // Continue processing after end marker
        }

        if (columns.length === 0) columns = block.columns;
        offset += block.bytesConsumed;
        blocksDecoded++;
        yield { columns, columnData: block.columnData, rowCount: block.rowCount };
      } catch (e) {
        // If we have remaining bytes and can't decode, might be trailing data or error
        // Allow for: end marker (2 bytes), partial block header, padding
        const remainingBytes = buffer.length - offset;
        if (remainingBytes > 100) {
          // Too much remaining data to be trailing junk - real decode error
          throw new Error(`Native decode error at offset ${offset}/${buffer.length} (${remainingBytes} bytes remaining, ${blocksDecoded} blocks decoded): ${e}`);
        }
        break;
      }
    }
  }
}

/**
 * Stream rows as objects from decoded Native blocks.
 *
 * @example
 * for await (const row of streamNativeRows(streamDecodeNative(query(...)))) {
 *   console.log(row.id, row.name);
 * }
 */
export async function* streamNativeRows(
  blocks: AsyncIterable<ColumnarResult>,
): AsyncGenerator<Record<string, unknown>> {
  for await (const block of blocks) {
    yield* asRows(block);
  }
}

/**
 * Stream encode columnar blocks to Native format.
 * Each yielded ColumnarResult produces one Native block (no re-batching).
 *
 * @example
 * // Round-trip: decode -> transform -> re-encode
 * insert("INSERT INTO t FORMAT Native",
 *   streamEncodeNativeColumnar(streamDecodeNative(query(...))),
 *   session, config);
 */
export async function* streamEncodeNativeColumnar(
  blocks: AsyncIterable<ColumnarResult>,
): AsyncGenerator<Uint8Array> {
  for await (const block of blocks) {
    yield encodeNativeColumnar(block.columns, block.columnData, block.rowCount);
  }
}
