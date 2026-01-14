export type { ClickHouseSettings } from "../settings.ts";
export {
  type CollectableAsyncGenerator,
  type ColumnSchema,
  type ExternalTableData,
  type InsertOptions,
  type QueryOptions,
  TcpClient,
  type TcpClientOptions,
} from "./client.ts";
export {
  type AccumulatedProgress,
  ClickHouseException,
  type LogEntry,
  type Packet,
  type ProfileInfo,
  type Progress,
  type ServerHello,
} from "./types.ts";

import type { RecordBatch } from "@maxjustus/chttp/native";
import type { Packet } from "./types.ts";

/**
 * Extract RecordBatches from Data packets.
 *
 * @example
 * for await (const batch of recordBatches(client.query(...))) {
 *   console.log(batch.rowCount);
 * }
 */
export async function* recordBatches(packets: AsyncIterable<Packet>): AsyncGenerator<RecordBatch> {
  for await (const p of packets) {
    if (p.type === "Data") yield p.batch;
  }
}
