import assert from "node:assert";
import {
  RecordBatchBuilder,
  RecordBatch,
  encodeNative,
  streamDecodeNative,
  type ColumnDef,
  type DecodeOptions,
} from "../native/index.ts";
import { TcpClient } from "../tcp_client/client.ts";
import type { QueryPacket } from "../client.ts";

// Async iterable helpers
export async function consume(input: AsyncIterable<QueryPacket>): Promise<void> {
  for await (const _ of input) {}
}

export async function collect<T>(gen: AsyncIterable<T>): Promise<T[]> {
  const results: T[] = [];
  for await (const item of gen) results.push(item);
  return results;
}

export async function* toAsync<T>(iter: Iterable<T>): AsyncIterable<T> {
  for (const item of iter) yield item;
}

// Assertion helpers
export function assertArrayEqual(
  actual: ArrayLike<unknown>,
  expected: unknown[],
  message?: string
): void {
  assert.strictEqual(
    actual.length,
    expected.length,
    message ? `${message}: length mismatch` : `length mismatch: ${actual.length} vs ${expected.length}`,
  );
  for (let i = 0; i < expected.length; i++) {
    assert.strictEqual(
      actual[i],
      expected[i],
      message ? `${message}: mismatch at index ${i}` : `mismatch at index ${i}`
    );
  }
}

// Encoding helpers
export function encodeNativeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const builder = new RecordBatchBuilder(columns);
  for (const row of rows) builder.appendRow(row);
  return encodeNative(builder.finish());
}

/**
 * Convert a batch to array-of-arrays format (for test assertions).
 */
export function toArrayRows(batch: RecordBatch): unknown[][] {
  const { columnData, rowCount } = batch;
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

/**
 * Decode a single Native block from bytes. Convenience for tests.
 */
export async function decodeBatch(data: Uint8Array, options?: DecodeOptions): Promise<RecordBatch> {
  const batches: RecordBatch[] = [];
  for await (const batch of streamDecodeNative(toAsync([data]), options)) {
    batches.push(batch);
  }
  if (batches.length === 0) {
    return RecordBatch.from({ columns: [], columnData: [], rowCount: 0 });
  }
  if (batches.length === 1) {
    return batches[0];
  }
  throw new Error("decodeBatch: expected single batch, got multiple");
}

export function generateSessionId(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
}

// TCP client helpers

export type TcpConfig = {
  host: string;
  tcpPort: number;
  username: string;
  password: string;
};

export function connectTcpClient(
  config: TcpConfig,
  opts?: Omit<Parameters<typeof TcpClient.connect>[0], "host" | "port" | "user" | "password">
) {
  return TcpClient.connect({
    host: config.host,
    port: config.tcpPort,
    user: config.username,
    password: config.password,
    ...opts,
  });
}

export async function collectQueryResults(
  client: TcpClient,
  sql: string,
  options?: Parameters<TcpClient["query"]>[1]
): Promise<unknown[][]> {
  const allRows: unknown[][] = [];
  for await (const packet of client.query(sql, options)) {
    if (packet.type === "Data") {
      allRows.push(...toArrayRows(packet.batch));
    }
  }
  return allRows;
}
