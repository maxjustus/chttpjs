import { type RecordBatch, type Row } from "./table.ts";

/**
 * Iterate rows from a stream of RecordBatches.
 *
 * Convenience async flattener so consumers don't have to manually iterate batches.
 *
 * Note: yielded rows are the per-row proxy objects produced by iterating a RecordBatch.
 * They are safe to store/collect (each iteration yields a distinct row object).
 *
 * @example
 * for await (const row of rows(streamDecodeNative(query(...)))) {
 *   console.log(row.id, row.name);
 * }
 */
export async function* rows(
  batches: AsyncIterable<RecordBatch>,
): AsyncGenerator<Row> {
  for await (const batch of batches) {
    for (const row of batch) yield row;
  }
}

/**
 * Collect all rows from a stream of RecordBatches into an array of plain objects.
 *
 * This materializes each row via `row.toObject()` so the results are safe to
 * store/serialize/display without Proxy semantics.
 *
 * @example
 * const rows = await collectRows(streamDecodeNative(query(...)));
 */
export async function collectRows(
  batches: AsyncIterable<RecordBatch>,
): Promise<Record<string, unknown>[]> {
  const result: Record<string, unknown>[] = [];
  for await (const batch of batches) {
    for (const row of batch) {
      result.push(row.toObject());
    }
  }
  return result;
}