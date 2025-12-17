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
import { Table, type Row } from "./table.ts";

// Re-export types for public API
export { type ColumnDef, type DecodeResult, type DecodeOptions, ClickHouseDateTime64 };
export { type Column, Table, type Row };
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

export interface Block {
  columns: ColumnDef[];
  columnData: Column[];  // columnData[colIndex]
  rowCount: number;
}

/** @deprecated Use Block instead */
export type ColumnarResult = Block;

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

export type StreamDecodeNativeResult = Block;

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
  const numRows = rowCount ?? (columnData[0]?.length ?? 0);

  // Estimate total size for pre-allocation
  let totalEstimate = 10; // header varints
  for (let i = 0; i < columns.length; i++) {
    totalEstimate += columns[i].name.length + columns[i].type.length + 10;
    totalEstimate += getCodec(columns[i].type).estimateSize(numRows);
  }
  const writer = new BufferWriter(Math.ceil(totalEstimate * 1.2));

  writer.writeVarint(columns.length);
  writer.writeVarint(numRows);

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < columns.length; i++) {
    const codec = getCodec(columns[i].type);
    const data = columnData[i];

    // Convert raw data to Column if needed
    // Fast path: existing Column objects (duck type check for 'get' and 'length')
    const col: Column = (data && typeof (data as any).get === 'function' && typeof (data as any).length === 'number')
      ? data as Column
      : ArrayBuffer.isView(data) && !(data instanceof DataView)
        ? new DataColumn(data as any)
        : codec.fromValues(data as unknown[]);

    writer.writeString(columns[i].name);
    writer.writeString(columns[i].type);
    codec.writePrefix?.(writer, col);
    const colHint = codec.estimateSize(col.length);
    writer.write(codec.encode(col, colHint));
  }

  return writer.finish();
}

export async function decodeNative(
  data: Uint8Array,
  options?: DecodeOptions,
): Promise<Block> {
  const blocks: Block[] = [];

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

  return mergeBlocks(blocks);
}

/**
 * Merge multiple blocks into a single block.
 * Note: This materializes all column data - use streaming for large datasets.
 */
export function mergeBlocks(blocks: Block[]): Block {
  if (blocks.length === 0) {
    return { columns: [], columnData: [], rowCount: 0 };
  }
  if (blocks.length === 1) {
    return blocks[0];
  }

  const columns = blocks[0].columns;
  const numCols = columns.length;
  const merged: unknown[][] = [];
  for (let i = 0; i < numCols; i++) {
    merged.push([]);
  }

  let totalRows = 0;
  for (const block of blocks) {
    for (let i = 0; i < numCols; i++) {
      const col = block.columnData[i];
      const len = col.length;
      for (let j = 0; j < len; j++) {
        merged[i].push(col.get(j));
      }
    }
    totalRows += block.rowCount;
  }

  return {
    columns,
    columnData: merged.map(arr => new DataColumn(arr)),
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

/**
 * Growable buffer for streaming decode. Replaces chunk array + flattenChunks().
 * Amortized O(n) vs O(n²) for many small chunks.
 */
class StreamBuffer {
  private buffer: Uint8Array;
  private readOffset = 0;
  private writeOffset = 0;

  constructor(initialSize = 2 * 1024 * 1024) {
    this.buffer = new Uint8Array(initialSize);
  }

  get available(): number {
    return this.writeOffset - this.readOffset;
  }

  append(chunk: Uint8Array): void {
    const needed = this.writeOffset + chunk.length;
    if (needed > this.buffer.length) {
      this.grow(needed);
    }
    this.buffer.set(chunk, this.writeOffset);
    this.writeOffset += chunk.length;
  }

  getReadView(): Uint8Array {
    return this.buffer.subarray(this.readOffset, this.writeOffset);
  }

  consume(bytes: number): void {
    this.readOffset += bytes;
    // Compact when >50% consumed
    if (this.readOffset > this.buffer.length / 2) {
      this.compact();
    }
  }

  private compact(): void {
    const remaining = this.writeOffset - this.readOffset;
    if (remaining > 0 && this.readOffset > 0) {
      this.buffer.copyWithin(0, this.readOffset, this.writeOffset);
    }
    this.writeOffset = remaining;
    this.readOffset = 0;
  }

  private grow(minCapacity: number): void {
    if (this.readOffset > 0) {
      this.compact();
      if (this.buffer.length >= minCapacity) return;
    }
    let newSize = this.buffer.length;
    while (newSize < minCapacity) {
      newSize = Math.min(newSize * 2, newSize + 64 * 1024 * 1024);
    }
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.writeOffset));
    this.buffer = newBuffer;
  }
}

/**
 * Lazily iterate rows as objects with column names as keys.
 * Allocates one object per row on demand.
 */
export function* asRows(result: Block): Generator<Record<string, unknown>> {
  const { columns, columnData, rowCount } = result;
  const numCols = columns.length;

  for (let i = 0; i < rowCount; i++) {
    const row: Record<string, unknown> = {};
    for (let j = 0; j < numCols; j++) {
      row[columns[j].name] = columnData[j].get(i);
    }
    yield row;
  }
}

/**
 * Convert columnar result to array rows.
 * Useful for re-encoding or comparison with original row arrays.
 */
export function toArrayRows(result: Block): unknown[][] {
  const { columnData, rowCount } = result;
  const numCols = columnData.length;

  const rows: unknown[][] = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    const row = new Array(numCols);
    for (let j = 0; j < numCols; j++) {
      row[j] = columnData[j].get(i);
    }
    rows[i] = row;
  }
  return rows;
}

export async function* streamDecodeNative(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions & { debug?: boolean; minBufferSize?: number },
): AsyncGenerator<Block> {
  const minBuffer = options?.minBufferSize ?? 2 * 1024 * 1024;
  const streamBuffer = new StreamBuffer(minBuffer);
  let columns: ColumnDef[] = [];
  let totalBytesReceived = 0;
  let blocksDecoded = 0;
  let bufferUnderruns = 0;

  // Track average block size to predict when we have enough data
  let avgBlockSize = 0;
  let totalBlockBytes = 0;

  for await (const chunk of chunks) {
    streamBuffer.append(chunk);
    totalBytesReceived += chunk.length;

    // Skip decode attempt if we don't have enough data yet
    const threshold = blocksDecoded > 0
      ? Math.max(minBuffer / 4, avgBlockSize * 1.2)
      : minBuffer;
    if (streamBuffer.available < threshold) continue;

    // Try to decode as many complete blocks as possible
    while (streamBuffer.available > 0) {
      const buffer = streamBuffer.getReadView();

      // First, estimate if we have enough data
      const estimate = estimateBlockSize(buffer, 0);
      if (estimate === null || buffer.length < estimate.estimatedSize) {
        break;
      }

      try {
        const block = decodeNativeBlock(buffer, 0, options);

        if (block.isEndMarker) {
          streamBuffer.consume(block.bytesConsumed);
          continue;
        }

        if (columns.length === 0) {
          columns = block.columns;
        }

        totalBlockBytes += block.bytesConsumed;
        streamBuffer.consume(block.bytesConsumed);
        blocksDecoded++;
        avgBlockSize = totalBlockBytes / blocksDecoded;
        yield { columns, columnData: block.columnData, rowCount: block.rowCount };
      } catch {
        bufferUnderruns++;
        break;
      }
    }
  }

  if (options?.debug) {
    console.log(`[streamDecodeNative] ${blocksDecoded} blocks, ${totalBytesReceived} bytes, ${bufferUnderruns} underruns`);
  }

  // Handle any remaining data after stream ends
  while (streamBuffer.available > 0) {
    const buffer = streamBuffer.getReadView();
    try {
      const block = decodeNativeBlock(buffer, 0, options);
      if (block.isEndMarker) {
        streamBuffer.consume(block.bytesConsumed);
        continue;
      }

      if (columns.length === 0) columns = block.columns;
      streamBuffer.consume(block.bytesConsumed);
      blocksDecoded++;
      yield { columns, columnData: block.columnData, rowCount: block.rowCount };
    } catch (e) {
      const remainingBytes = streamBuffer.available;
      if (remainingBytes > 100) {
        throw new Error(`Native decode error (${remainingBytes} bytes remaining, ${blocksDecoded} blocks decoded): ${e}`);
      }
      break;
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
  blocks: AsyncIterable<Block>,
): AsyncGenerator<Record<string, unknown>> {
  for await (const block of blocks) {
    yield* asRows(block);
  }
}

/**
 * Stream encode columnar blocks to Native format.
 * Each yielded Block produces one Native block (no re-batching).
 *
 * @example
 * // Round-trip: decode -> transform -> re-encode
 * insert("INSERT INTO t FORMAT Native",
 *   streamEncodeNativeColumnar(streamDecodeNative(query(...))),
 *   session, config);
 */
export async function* streamEncodeNativeColumnar(
  blocks: AsyncIterable<Block>,
): AsyncGenerator<Uint8Array> {
  for await (const block of blocks) {
    yield encodeNativeColumnar(block.columns, block.columnData, block.rowCount);
  }
}