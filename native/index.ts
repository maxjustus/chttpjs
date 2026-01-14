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
import { BufferReader, BufferWriter, StreamBuffer } from "./io.ts";
import { collectRows, rows } from "./rows.ts";
import {
  DEFAULT_DENSE_NODE,
  type DeserializerState,
  type SerializationNode,
} from "./serialization.ts";
import { batchFromCols, batchFromRows, type MaterializeOptions, RecordBatch, type Row } from "./table.ts";
import { type ColumnDef, type DecodeOptions, parseTupleElements, parseTypeList } from "./types.ts";

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
export { batchFromRows, batchFromCols };
export { rows, collectRows };
export { getCodec } from "./codecs.ts";
// Re-export constants needed by tcp_client
export { BlockInfoField, Compression } from "./constants.ts";
// Re-export IO utilities needed by tcp_client
export { BufferReader, BufferUnderflowError, BufferWriter, readVarInt64, StreamBuffer } from "./io.ts";

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

interface BlockEstimate {
  estimatedSize: number;
  headerSize: number; // bytes consumed reading header (numCols, numRows, names, types)
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
        if (fieldId === BlockInfoField.End) break;
        if (fieldId === BlockInfoField.IsOverflows)
          reader.offset += 1; // is_overflows
        else if (fieldId === BlockInfoField.BucketNum) reader.offset += 4; // bucket_num
      }
    }

    const numCols = reader.readVarint();
    const numRows = reader.readVarint();

    // End marker - tiny block
    if (numCols === 0 && numRows === 0) {
      return {
        estimatedSize: reader.offset - startOffset,
        headerSize: reader.offset - startOffset,
      };
    }

    // Read column names and types to estimate data size
    let dataEstimate = 0;

    for (let i = 0; i < numCols; i++) {
      reader.readString(); // name
      const typeStr = reader.readString();

      if (clientVersion >= 54454) {
        const hasCustom = reader.readU8() !== 0;
        if (hasCustom) {
          skipSerializationTree(reader, typeStr);
        }
      }

      dataEstimate += getCodec(typeStr).estimateSize(numRows);
    }

    const headerSize = reader.offset - startOffset;
    // Add 20% buffer for prefix data, LowCardinality dictionaries, etc.
    return {
      estimatedSize: headerSize + Math.ceil(dataEstimate * 1.2),
      headerSize,
    };
  } catch {
    // Not enough data even for header
    return null;
  }
}

/**
 * Skip serialization tree bytes without building the tree.
 * Used when we only need to advance past the kind metadata.
 */
function skipSerializationTree(reader: BufferReader, typeStr: string): void {
  reader.readU8(); // kind byte

  if (typeStr.startsWith("Tuple")) {
    const elements = parseTupleElements(
      typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")")),
    );
    for (const el of elements) {
      skipSerializationTree(reader, el.type);
    }
  } else if (typeStr.startsWith("Array")) {
    const innerType = typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")"));
    skipSerializationTree(reader, innerType);
  } else if (typeStr.startsWith("Map")) {
    const args = parseTypeList(
      typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")")),
    );
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
export function decodeNativeBlock(
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
      if (fieldId === BlockInfoField.End) break;
      if (fieldId === BlockInfoField.IsOverflows)
        reader.offset += 1; // is_overflows
      else if (fieldId === BlockInfoField.BucketNum) reader.offset += 4; // bucket_num
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
    codec.writePrefix?.(writer, col);
    const colHint = codec.estimateSize(col.length);
    writer.write(codec.encode(col, colHint));
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

/**
 * Helper to decode a block from a StreamBuffer with a stable slice.
 */
function decodeFromStream(streamBuffer: StreamBuffer, options?: DecodeOptions): BlockResult | null {
  const buffer = streamBuffer.view;
  if (buffer.length === 0) return null;

  const estimate = estimateBlockSize(buffer, 0, options);
  if (estimate === null) return null;

  // Only attempt decode once we likely have enough bytes for the block.
  // Use a stable copy so zero-copy typed arrays survive StreamBuffer compaction.
  if (buffer.length >= estimate.estimatedSize) {
    const stableSlice = buffer.slice();
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

export async function* streamDecodeNative(
  chunks: AsyncIterable<Uint8Array>,
  options?: DecodeOptions & { debug?: boolean; minBufferSize?: number },
): AsyncGenerator<RecordBatch> {
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
      yield RecordBatch.from({
        columns,
        columnData: block.columnData,
        rowCount: block.rowCount,
      });
    }
  }

  // Final cleanup: try to decode whatever is left without the conservative estimate
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
    console.log(`[streamDecodeNative] ${blocksDecoded} blocks, ${totalBytesReceived} bytes`);
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
