import type { StreamingReader } from "./reader.ts";

/**
 * Metadata about a block in the TCP protocol.
 * Uses a field-based encoding where field 0 marks the end.
 */
export interface BlockInfo {
  isOverflows: boolean;
  bucketNum: number;
}

export async function readBlockInfo(reader: StreamingReader): Promise<BlockInfo> {
  let isOverflows = false;
  let bucketNum = -1;

  while (true) {
    const fieldNum = Number(await reader.readVarInt());
    if (fieldNum === 0) break;

    switch (fieldNum) {
      case 1:
        isOverflows = (await reader.readU8()) !== 0;
        break;
      case 2:
        bucketNum = await reader.readInt32LE();
        break;
      default:
        // Forward compatibility: unknown fields should be ignored or throw
        throw new Error(`Unknown BlockInfo field: ${fieldNum}`);
    }
  }

  return { isOverflows, bucketNum };
}
