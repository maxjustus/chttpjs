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
import { type Column } from "./columns.ts";
import {
  Table,
  TableBuilder,
  type Row,
  tableFromArrays,
  tableFromRows,
  tableFromCols,
  tableBuilder,
} from "./table.ts";

// Re-export types for public API
export { type ColumnDef, type DecodeResult, type DecodeOptions, ClickHouseDateTime64 };
export { type Column, Table, TableBuilder, type Row };
export { tableFromArrays, tableFromRows, tableFromCols, tableBuilder };
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
export { makeBuilder, type ColumnBuilder } from "./codecs.ts";

export interface Block {
  columns: ColumnDef[];
  columnData: Column[];  // columnData[colIndex]
  rowCount: number;
}

/**
 * Node in the serialization tree. Tracks kind (dense/sparse) for each position
 * in the type tree. Children correspond to nested types (Array element, Map key/value, etc.)
 */
export interface SerializationNode {
  kind: number;  // 0 = Dense, 1 = Sparse
  children: SerializationNode[];
}

/** Default node for dense serialization (no sparse encoding) */
export const DENSE_LEAF: SerializationNode = { kind: 0, children: [] };

/**
 * State maintained during a block deserialization.
 */
export interface DeserializerState {
  serNode: SerializationNode;
  /**
   * Tracks partial sparse groups across granules/blocks.
   * Map key is the SerializationNode reference, value is [trailing_defaults, has_value_after_defaults].
   */
  sparseRuntime: Map<SerializationNode, [number, boolean]>;
}

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
  options?: DecodeOptions,
): BlockEstimate | null {
  try {
    const reader = new BufferReader(data, offset, options);
    const startOffset = reader.offset;

    const clientVersion = options?.clientVersion ?? 0;
    if (clientVersion > 0) {
      while (true) {
        const fieldId = reader.readVarint();
        if (fieldId === 0) break;
        if (fieldId === 1) reader.offset += 1; // is_overflows
        else if (fieldId === 2) reader.offset += 4; // bucket_num
      }
    }

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
      const typeStr = reader.readString();
      
      if (clientVersion >= 54454) {
        const hasCustom = reader.buffer[reader.offset++] !== 0;
        if (hasCustom) {
          skipSerializationTree(reader, typeStr);
        }
      }

      dataEstimate += getCodec(typeStr).estimateSize(numRows);
    }

    const headerSize = reader.offset - startOffset;
    // Add 20% buffer for prefix data, LowCardinality dictionaries, etc.
    return { estimatedSize: headerSize + Math.ceil(dataEstimate * 1.2), headerSize };
  } catch {
    // Not enough data even for header
    return null;
  }
}

import { parseTypeList, parseTupleElements } from "../shared.ts";

/**
 * Skip serialization tree bytes without building the tree.
 * Used when we only need to advance past the kind metadata.
 */
export function skipSerializationTree(reader: BufferReader, typeStr: string): void {
  reader.offset++; // skip kind byte

  if (typeStr.startsWith("Tuple")) {
    const elements = parseTupleElements(typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")")));
    for (const el of elements) {
      skipSerializationTree(reader, el.type);
    }
  } else if (typeStr.startsWith("Array")) {
    const innerType = typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")"));
    skipSerializationTree(reader, innerType);
  } else if (typeStr.startsWith("Map")) {
    const args = parseTypeList(typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")")));
    skipSerializationTree(reader, args[0]);
    skipSerializationTree(reader, args[1]);
  } else if (typeStr.startsWith("Nullable")) {
    const innerType = typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")"));
    skipSerializationTree(reader, innerType);
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

  const clientVersion = options?.clientVersion ?? 0;
  if (clientVersion > 0) {
    while (true) {
      const fieldId = reader.readVarint();
      if (fieldId === 0) break;
      if (fieldId === 1) reader.offset += 1; // is_overflows
      else if (fieldId === 2) reader.offset += 4; // bucket_num
    }
  }

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

  // Native format: per-column [name, type, [has_custom, [kinds...]], prefix, data]
  for (let i = 0; i < numCols; i++) {
    const name = reader.readString();
    const type = reader.readString();
    columns.push({ name, type });

    const codec = getCodec(type);

    let serNode: SerializationNode = DENSE_LEAF;
    if (clientVersion >= 54454) {
      const hasCustomSerialization = reader.buffer[reader.offset++] !== 0;
      if (hasCustomSerialization) {
        serNode = codec.readKinds(reader);
      }
    }

    const state: DeserializerState = { serNode, sparseRuntime: new Map() };
    codec.readPrefix?.(reader);
    columnData.push(codec.decode(reader, numRows, state));
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
 * Encode a Table to Native format.
 */
export function encodeNative(table: Table): Uint8Array {
  const { columns, columnData, rowCount } = table;

  // Estimate total size for pre-allocation
  let totalEstimate = 10; // header varints
  for (let i = 0; i < columns.length; i++) {
    totalEstimate += columns[i].name.length + columns[i].type.length + 10;
    totalEstimate += getCodec(columns[i].type).estimateSize(rowCount);
  }
  const writer = new BufferWriter(Math.ceil(totalEstimate * 1.2));

  writer.writeVarint(columns.length);
  writer.writeVarint(rowCount);

  // Native format: per-column [name, type, prefix, data]
  for (let i = 0; i < columns.length; i++) {
    const codec = getCodec(columns[i].type);
    const col = columnData[i];

    writer.writeString(columns[i].name);
    writer.writeString(columns[i].type);
    codec.writePrefix?.(writer, col);
    const colHint = codec.estimateSize(col.length);
    writer.write(codec.encode(col, colHint));
  }

  return writer.finish();
}

/**
 * Decode Native format data into a Table.
 *
 * This function is async because:
 * - Internally reuses streamDecodeNative for code simplicity
 * - Maintains consistent API for future streaming optimizations
 * - Handles multi-block data uniformly with streaming decode
 */
export async function decodeNative(
  data: Uint8Array,
  options?: DecodeOptions,
): Promise<Table> {
  const blocks: Table[] = [];

  async function* singleChunk() {
    yield data;
  }

  for await (const block of streamDecodeNative(singleChunk(), options)) {
    blocks.push(block);
  }

  // Fast path: single block
  if (blocks.length === 0) {
    return new Table({ columns: [], columnData: [], rowCount: 0 });
  }
  if (blocks.length === 1) {
    return blocks[0];
  }

  return mergeBlocks(blocks);
}

/**
 * Merge multiple blocks into a single Table.
 * Note: This materializes all column data - use streaming for large datasets.
 */
export function mergeBlocks(blocks: (Block | Table)[]): Table {
  if (blocks.length === 0) {
    return new Table({ columns: [], columnData: [], rowCount: 0 });
  }
  if (blocks.length === 1) {
    const first = blocks[0];
    return first instanceof Table ? first : Table.from(first);
  }

  const first = blocks[0];
  const columns = first.columns;
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

  return new Table({
    columns,
    columnData: merged.map((arr, i) => getCodec(columns[i].type).fromValues(arr)),
    rowCount: totalRows,
  });
}

/**
 * Stream encode Tables to Native format.
 * Each yielded Table produces one Native block.
 */
export async function* streamEncodeNative(
  tables: AsyncIterable<Table>,
): AsyncGenerator<Uint8Array> {
  for await (const table of tables) {
    yield encodeNative(table);
  }
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
 * Helper to decode a block from a StreamBuffer with a stable slice.
 */
function decodeFromStream(streamBuffer: StreamBuffer, options?: DecodeOptions): BlockResult | null {
  const buffer = streamBuffer.getReadView();
  if (buffer.length === 0) return null;

  const estimate = estimateBlockSize(buffer, 0, options);
  if (estimate === null) return null;

  // We try to decode if we have enough data according to the estimate.
  // We use a stable copy to ensure virtual columns don't break on compaction.
  if (buffer.length >= estimate.estimatedSize) {
    const stableSlice = buffer.slice(0, estimate.estimatedSize);
    try {
      const block = decodeNativeBlock(stableSlice, 0, options);
      streamBuffer.consume(block.bytesConsumed);
      return block;
    } catch {
      // If decode failed even with enough estimated data, wait for more
      return null;
    }
  }

  // If we don't have enough data for the estimate, but the stream has potentially ended,
  // we might still want to try. But streamDecodeNative handles the final cleanup.
  return null;
}

/**
 * Lazily iterate rows as objects with column names as keys.
 * Allocates one object per row on demand.
 */
export function* asRows(result: Block | Table): Generator<Record<string, unknown>> {
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
export function toArrayRows(result: Block | Table): unknown[][] {
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
): AsyncGenerator<Table> {
  const minBuffer = options?.minBufferSize ?? 2 * 1024 * 1024;
  const streamBuffer = new StreamBuffer(minBuffer);
  let columns: ColumnDef[] = [];
  let totalBytesReceived = 0;
  let blocksDecoded = 0;

  for await (const chunk of chunks) {
    streamBuffer.append(chunk);
    totalBytesReceived += chunk.length;

    while (true) {
      const block = decodeFromStream(streamBuffer, options);
      if (!block) break;

      if (block.isEndMarker) continue;

      if (columns.length === 0) columns = block.columns;
      blocksDecoded++;
      yield Table.from({ columns, columnData: block.columnData, rowCount: block.rowCount });
    }
  }

  // Final cleanup: try to decode whatever is left without the conservative estimate
  let buffer = streamBuffer.getReadView();
  while (buffer.length > 0) {
    try {
      // Use slice() to ensure stable columns even in the final blocks
      const block = decodeNativeBlock(buffer.slice(), 0, options);
      streamBuffer.consume(block.bytesConsumed);
      buffer = streamBuffer.getReadView();

      if (block.isEndMarker) continue;
      if (columns.length === 0) columns = block.columns;
      blocksDecoded++;
      yield Table.from({ columns, columnData: block.columnData, rowCount: block.rowCount });
    } catch {
      break;
    }
  }

  if (options?.debug) {
    console.log(`[streamDecodeNative] ${blocksDecoded} blocks, ${totalBytesReceived} bytes`);
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
  blocks: AsyncIterable<Table>,
): AsyncGenerator<Record<string, unknown>> {
  for await (const block of blocks) {
    yield* asRows(block);
  }
}

