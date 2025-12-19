import assert from "node:assert";
import {
  RecordBatchBuilder,
  RecordBatch,
  encodeNative,
  streamDecodeNative,
  type ColumnDef,
  type DecodeOptions,
} from "../native/index.ts";

// Async iterable helpers
export async function consume(s: AsyncIterable<unknown>): Promise<void> {
  for await (const _ of s) {}
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
