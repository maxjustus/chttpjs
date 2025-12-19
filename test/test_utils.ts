import assert from "node:assert";
import { TableBuilder, encodeNative, type ColumnDef } from "../formats/native/index.ts";

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
  const builder = new TableBuilder(columns);
  for (const row of rows) builder.appendRow(row);
  return encodeNative(builder.finish());
}

export function generateSessionId(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`;
}
