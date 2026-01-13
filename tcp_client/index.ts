export { TcpClient, type TcpClientOptions, type ColumnSchema, type QueryOptions, type InsertOptions, type ExternalTableData, type CollectableAsyncGenerator } from "./client.ts";
export type { ClickHouseSettings } from "../settings.ts";
export {
  type Packet,
  type Progress,
  type AccumulatedProgress,
  type ProfileInfo,
  type LogEntry,
  type ServerHello,
  ClickHouseException,
} from "./types.ts";
import { type Packet } from "./types.ts";
import { type RecordBatch } from "@maxjustus/chttp/native";

/**
 * Extract RecordBatches from Data packets.
 *
 * @example
 * for await (const batch of recordBatches(client.query(...))) {
 *   console.log(batch.rowCount);
 * }
 */
export async function* recordBatches(
  packets: AsyncIterable<Packet>,
): AsyncGenerator<RecordBatch> {
  for await (const p of packets) {
    if (p.type === "Data") yield p.batch;
  }
}
