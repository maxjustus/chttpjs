/**
 * Native format encoder/decoder for ClickHouse.
 *
 * Native is ClickHouse's columnar wire format - data doesn't need row-to-column
 * conversion on the server.
 *
 * Note: Only Dynamic/JSON V3 format is supported at present. For ClickHouse 25.6+, enable
 * `output_format_native_use_flattened_dynamic_and_json_serialization` setting.
 */

import { getCodec } from "./codecs.ts";
import { type Column, DataColumn, EnumColumn } from "./columns.ts";
import { BlockInfoField } from "./constants.ts";
import { BufferReader, BufferUnderflowError, BufferWriter, StreamBuffer } from "./io.ts";
import { collectRows, rows } from "./rows.ts";
import {
  DEFAULT_DENSE_NODE,
  type DeserializerState,
  type SerializationNode,
} from "./serialization.ts";
import {
  batchFromCols,
  batchFromRows,
  type ExternalTableData,
  type MaterializeOptions,
  RecordBatch,
  type Row,
} from "./table.ts";
import { type ColumnDef, type DecodeOptions } from "./types.ts";

// Re-export types for public API
export {
  ClickHouseDateTime64,
  type ColumnDef,
  type DecodeOptions,
  type DecodeResult,
  TEXT_DECODER,
} from "./types.ts";

// Re-export table helpers / types
export { type Column, RecordBatch, type Row, type MaterializeOptions, EnumColumn };
export { batchFromRows, batchFromCols, type ExternalTableData };
export { rows, collectRows };
export { getCodec } from "./codecs.ts";
// Re-export constants needed by tcp_client
export { BlockInfoField, Compression } from "./constants.ts";
// Re-export IO utilities needed by tcp_client
export {
  BufferReader,
  BufferUnderflowError,
  BufferWriter,
  readVarInt64,
  StreamBuffer,
} from "./io.ts";

export interface Block {
  columns: ColumnDef[];
  columnData: Column[];
  rowCount: number;
  decodeTimeMs?: number;
}

interface BlockResult extends Block {
  bytesConsumed: number;
  isEndMarker: boolean;
}

/**
 * Partial decode state for resumable decoding.
 * When underflow occurs mid-block, this captures completed columns
 * so retry can resume without re-parsing them.
 */
export interface PartialBlockState {
  columns: ColumnDef[];
  columnData: Column[];
  numCols: number;
  numRows: number;
  nextColIndex: number;
  resumeOffset: number;
  startOffset: number;
}

/**
 * Thrown when block decode runs out of data mid-parse.
 * Contains partial state to enable resumable decoding.
 */
export class BlockUnderflowError extends BufferUnderflowError {
  readonly partial: PartialBlockState;

  constructor(message: string, partial: PartialBlockState) {
    super(message);
    this.name = "BlockUnderflowError";
    this.partial = partial;
  }
}

/**
 * Decode a single Native format block using an existing BufferReader.
 * Supports resumable decoding: pass partial state from previous underflow to continue.
 * Throws BlockUnderflowError with partial state if more data is needed.
 */
export function decodeNativeBlockWithReader(
  reader: BufferReader,
  options?: DecodeOptions,
  partial?: PartialBlockState,
): BlockResult {
  const clientVersion = options?.clientVersion ?? 0;
  const startOffset = partial?.startOffset ?? reader.offset;
  let numCols: number;
  let numRows: number;
  let columns: ColumnDef[];
  let columnData: Column[];
  let startColIndex: number;

  if (partial) {
    // Resume from checkpoint
    numCols = partial.numCols;
    numRows = partial.numRows;
    columns = partial.columns;
    columnData = partial.columnData;
    startColIndex = partial.nextColIndex;
    reader.offset = partial.resumeOffset;
  } else {
    // Fresh decode - parse header
    if (clientVersion > 0) {
      while (true) {
        const fieldId = reader.readVarint();
        if (fieldId === BlockInfoField.End) break;
        if (fieldId === BlockInfoField.IsOverflows)
          reader.offset += 1; // is_overflows
        else if (fieldId === BlockInfoField.BucketNum) reader.offset += 4; // bucket_num
      }
    }

    numCols = reader.readVarint();
    numRows = reader.readVarint();

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

    columns = [];
    columnData = [];
    startColIndex = 0;
  }

  // Native format: per-column [name, type, [has_custom, [kinds...]], prefix, data]
  for (let i = startColIndex; i < numCols; i++) {
    // Checkpoint: offset before parsing this column
    const colStartOffset = reader.offset;

    try {
      const name = reader.readString();
      const type = reader.readString();
      columns.push({ name, type });

      const codec = getCodec(type);

      let serNode: SerializationNode = DEFAULT_DENSE_NODE;
      if (clientVersion >= 54454) {
        const hasCustomSerialization = reader.readU8() !== 0;
        if (hasCustomSerialization) {
          serNode = codec.readKinds(reader);
        }
      }

      const state: DeserializerState = { serNode, sparseRuntime: new Map() };
      // Only read prefix and decode when there are rows - empty blocks are schema-only
      if (numRows > 0) {
        codec.readPrefix?.(reader);
        columnData.push(codec.decode(reader, numRows, state));
      } else {
        // Schema-only block: no prefix or data, create empty column
        columnData.push(new DataColumn(type, []));
      }
    } catch (err) {
      if (err instanceof BufferUnderflowError) {
        // Reset reader to column boundary and throw with partial state
        reader.offset = colStartOffset;
        throw new BlockUnderflowError(err.message, {
          columns: columns.slice(0, i),
          columnData: columnData.slice(0, i),
          numCols,
          numRows,
          nextColIndex: i,
          resumeOffset: colStartOffset,
          startOffset,
        });
      }
      throw err;
    }
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
 * Decode a single Native format block from a buffer.
 * Returns the decoded data and the number of bytes consumed.
 * Use this for streaming scenarios where you need to track buffer position.
 */
export function decodeNativeBlock(
  data: Uint8Array,
  offset: number,
  options?: DecodeOptions,
): BlockResult {
  const reader = new BufferReader(data, offset, options);
  return decodeNativeBlockWithReader(reader, options);
}

/**
 * Encode a RecordBatch to Native format.
 */
export function encodeNative(batch: RecordBatch): Uint8Array {
  const { columns, columnData, rowCount } = batch;

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
    // Only write prefix and data when there are rows (matches decode behavior)
    if (rowCount > 0) {
      codec.writePrefix?.(writer, col);
      const colHint = codec.estimateSize(col.length);
      writer.write(codec.encode(col, colHint));
    }
  }

  return writer.finish();
}

/**
 * Stream encode RecordBatches to Native format.
 * Each yielded RecordBatch produces one Native block.
 */
export async function* streamEncodeNative(
  batches: AsyncIterable<RecordBatch>,
): AsyncGenerator<Uint8Array> {
  for await (const batch of batches) {
    yield encodeNative(batch);
  }
}

interface DecodeStats {
  underruns: number;
  tooSmall: number;
}

/**
 * Helper to decode a block from a StreamBuffer with a stable slice.
 * Returns null if not enough data is available (BufferUnderflowError).
 */
function decodeFromStream(
  streamBuffer: StreamBuffer,
  options?: DecodeOptions,
  stats?: DecodeStats,
): BlockResult | null {
  const buffer = streamBuffer.view;
  if (buffer.length < 8) {
    if (stats) stats.tooSmall++;
    return null; // minimum: 2 varints for numCols/numRows
  }

  // Use a stable copy so zero-copy typed arrays survive StreamBuffer compaction
  const stableSlice = buffer.slice();
  try {
    const block = decodeNativeBlock(stableSlice, 0, options);
    streamBuffer.consume(block.bytesConsumed);
    return block;
  } catch (e) {
    if (e instanceof BufferUnderflowError) {
      if (stats) stats.underruns++;
      return null;
    }
    throw e;
  }
}

export async function* streamDecodeNative(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions & { debug?: boolean; minBufferSize?: number },
): AsyncGenerator<RecordBatch> {
  const minBuffer = options?.minBufferSize ?? 2 * 1024 * 1024;
  const streamBuffer = new StreamBuffer(minBuffer);
  let columns: ColumnDef[] = [];
  let totalBytesReceived = 0;
  let blocksDecoded = 0;
  const stats: DecodeStats = { underruns: 0, tooSmall: 0 };

  for await (const chunk of chunks) {
    streamBuffer.append(chunk);
    totalBytesReceived += chunk.length;

    while (true) {
      const block = decodeFromStream(streamBuffer, options, stats);
      if (!block) break;

      if (block.isEndMarker) continue;

      if (columns.length === 0) columns = block.columns;
      blocksDecoded++;
      yield RecordBatch.from({
        columns,
        columnData: block.columnData,
        rowCount: block.rowCount,
      });
    }
  }

  // Final cleanup: decode whatever is left
  let buffer = streamBuffer.view;
  while (buffer.length > 0) {
    try {
      // Use slice() to ensure stable columns even in the final blocks
      const block = decodeNativeBlock(buffer.slice(), 0, options);
      streamBuffer.consume(block.bytesConsumed);
      buffer = streamBuffer.view;

      if (block.isEndMarker) continue;
      if (columns.length === 0) columns = block.columns;
      blocksDecoded++;
      yield RecordBatch.from({
        columns,
        columnData: block.columnData,
        rowCount: block.rowCount,
      });
    } catch {
      break;
    }
  }

  if (options?.debug) {
    console.log(
      `[streamDecodeNative] ${blocksDecoded} blocks, ${totalBytesReceived} bytes, ${stats.underruns} underruns, ${stats.tooSmall} too-small`,
    );
  }
}

/**
 * Iterate rows from RecordBatches.
 *
 * `RecordBatch` implements the iterable protocol, so you can iterate rows
 * directly from each batch yielded by `streamDecodeNative()`.
 *
 * @example
 * for await (const batch of streamDecodeNative(query(...))) {
 *   for (const row of batch) {
 *     console.log(row.id, row.name);
 *   }
 * }
 */
