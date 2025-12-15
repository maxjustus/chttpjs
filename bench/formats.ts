// Benchmark: Native vs RowBinary vs JSONEachRow
//
// Tests encoding/decoding performance for all formats with various data types.

import { init, encodeBlock, decodeBlock, Method } from "../compression.ts";
import {
  encodeRowBinary,
  decodeRowBinary,
  streamDecodeRowBinary,
  streamEncodeRowBinary,
  type ColumnDef,
} from "../rowbinary.ts";
import { encodeNative, decodeNative, toArrayRows } from "../native.ts";

// --- Test data generators ---

interface TestRow {
  id: number;
  name: string;
  email: string;
  active: boolean;
  score: number;
  created_at: Date;
}

function generateSimpleData(count: number): TestRow[] {
  const rows: TestRow[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      name: `user_${i}`,
      email: `user${i}@example.com`,
      active: i % 2 === 0,
      score: Math.random() * 100,
      created_at: new Date("2024-01-15T10:30:00Z"),
    });
  }
  return rows;
}

// Data with strings that need JSON escaping
function generateEscapeData(
  count: number,
): Array<{ id: number; name: string; desc: string; path: string }> {
  const rows: Array<{ id: number; name: string; desc: string; path: string }> =
    [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      name: `user "test" ${i}`,
      desc: `Line1\nLine2\tTabbed`,
      path: `C:\\Users\\test\\file${i}.txt`,
    });
  }
  return rows;
}

// Data with arrays and nullable
function generateComplexData(count: number): Array<{
  id: number;
  tags: string[];
  scores: number[];
  metadata: string | null;
}> {
  const rows: Array<{
    id: number;
    tags: string[];
    scores: number[];
    metadata: string | null;
  }> = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      tags: [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`],
      scores: Array.from({ length: 50 }, () => Math.random() * 100),
      metadata: i % 3 === 0 ? null : `meta_${i}`,
    });
  }
  return rows;
}

// Data with arrays and nullable (TypedArrays)
function generateComplexTypedData(count: number): Array<{
  id: number;
  tags: string[];
  scores: Float64Array;
  metadata: string | null;
}> {
  const rows: Array<{
    id: number;
    tags: string[];
    scores: Float64Array;
    metadata: string | null;
  }> = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      tags: [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`],
      scores: new Float64Array(Array.from({ length: 50 }, () => Math.random() * 100)),
      metadata: i % 3 === 0 ? null : `meta_${i}`,
    });
  }
  return rows;
}

// --- Benchmark infrastructure ---

function bench(
  name: string,
  fn: () => void,
  warmup = 50,
  iterations = 100,
): { name: string; ms: number } {
  // Warmup
  for (let i = 0; i < warmup; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  const elapsed = performance.now() - start;

  return { name, ms: elapsed / iterations };
}

function formatResult(
  result: { name: string; ms: number },
  rows: number,
): string {
  const rowsPerSec = rows / (result.ms / 1000);
  return `  ${result.name.padEnd(30)} ${result.ms.toFixed(3).padStart(8)}ms  ${(rowsPerSec / 1_000_000).toFixed(2).padStart(6)}M rows/sec`;
}

async function benchAsync(
  name: string,
  fn: () => Promise<void>,
  warmup = 50,
  iterations = 100,
): Promise<{ name: string; ms: number }> {
  for (let i = 0; i < warmup; i++) await fn();
  const start = performance.now();
  for (let i = 0; i < iterations; i++) await fn();
  const elapsed = performance.now() - start;
  return { name, ms: elapsed / iterations };
}

// Helper to simulate chunked streaming from a buffer
async function* chunkedStream(data: Uint8Array, chunkSize: number): AsyncIterable<Uint8Array> {
  for (let i = 0; i < data.length; i += chunkSize) {
    yield data.subarray(i, Math.min(i + chunkSize, data.length));
  }
}

async function collectChunks(gen: AsyncIterable<Uint8Array>): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  for await (const chunk of gen) chunks.push(chunk);
  const total = chunks.reduce((sum, c) => sum + c.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

// Helper to collect all rows from streaming decode
async function collectRowBinary(chunks: AsyncIterable<Uint8Array>): Promise<{ columns: ColumnDef[]; rows: unknown[][] }> {
  let columns: ColumnDef[] = [];
  const rows: unknown[][] = [];
  for await (const batch of streamDecodeRowBinary(chunks)) {
    columns = batch.columns;
    rows.push(...batch.rows);
  }
  return { columns, rows };
}

// --- JSONEachRow helpers ---

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeJsonEachRow(rows: Record<string, unknown>[]): Uint8Array {
  let json = "";
  for (const row of rows) {
    json += JSON.stringify(row) + "\n";
  }
  return encoder.encode(json);
}

function decodeJsonEachRow<T>(data: Uint8Array): T[] {
  const text = decoder.decode(data);
  const lines = text.trim().split("\n");
  return lines.map((line) => JSON.parse(line) as T);
}

// --- Run benchmarks ---

async function main() {
  await init();

  const ROWS = 10_000;
  const ITERATIONS = 50;

  console.log(
    `Benchmarking with ${ROWS} rows, ${ITERATIONS} iterations each\n`,
  );

  // === Simple data ===
  console.log(
    "=== Simple Data (6 columns: int, 2 strings, bool, float, datetime) ===\n",
  );

  const simpleData = generateSimpleData(ROWS);
  const simpleColumns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "name", type: "String" },
    { name: "email", type: "String" },
    { name: "active", type: "Bool" },
    { name: "score", type: "Float64" },
    { name: "created_at", type: "DateTime" },
  ];
  const simpleRowsArray = simpleData.map((r) => [
    r.id,
    r.name,
    r.email,
    r.active,
    r.score,
    r.created_at,
  ]);

  // Pre-encode for decode benchmarks
  const simpleJsonEncoded = encodeJsonEachRow(simpleData);
  const simpleRowBinaryEncoded = encodeRowBinary(
    simpleColumns,
    simpleRowsArray,
  );
  const simpleNativeEncoded = encodeNative(simpleColumns, simpleRowsArray);

  console.log(
    `  Encoded sizes: JSON=${simpleJsonEncoded.length}, RowBinary=${simpleRowBinaryEncoded.length} (${((simpleRowBinaryEncoded.length / simpleJsonEncoded.length) * 100).toFixed(1)}%), Native=${simpleNativeEncoded.length} (${((simpleNativeEncoded.length / simpleJsonEncoded.length) * 100).toFixed(1)}%)\n`,
  );

  // Encoding benchmarks
  console.log("Encoding:");
  const jsonEncodeSimple = bench(
    "JSONEachRow encode",
    () => {
      encodeJsonEachRow(simpleData);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonEncodeSimple, ROWS));

  const rbEncodeSimple = bench(
    "RowBinary encode",
    () => {
      encodeRowBinary(simpleColumns, simpleRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbEncodeSimple, ROWS));

  const nativeEncodeSimple = bench(
    "Native encode",
    () => {
      encodeNative(simpleColumns, simpleRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeEncodeSimple, ROWS));

  // Decoding benchmarks
  console.log("\nDecoding:");
  const jsonDecodeSimple = bench(
    "JSONEachRow decode",
    () => {
      decodeJsonEachRow(simpleJsonEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonDecodeSimple, ROWS));

  const rbDecodeSimple = bench(
    "RowBinary decode",
    () => {
      decodeRowBinary(simpleRowBinaryEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbDecodeSimple, ROWS));

  const nativeDecodeSimple = await benchAsync(
    "Native decode",
    async () => {
      await decodeNative(simpleNativeEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeDecodeSimple, ROWS));

  // Full path with compression
  const simpleJsonCompressed = encodeBlock(simpleJsonEncoded, Method.LZ4);
  const simpleRbCompressed = encodeBlock(simpleRowBinaryEncoded, Method.LZ4);
  const simpleNativeCompressed = encodeBlock(simpleNativeEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${simpleJsonCompressed.length}, RowBinary+LZ4=${simpleRbCompressed.length} (${((simpleRbCompressed.length / simpleJsonCompressed.length) * 100).toFixed(1)}%), Native+LZ4=${simpleNativeCompressed.length} (${((simpleNativeCompressed.length / simpleJsonCompressed.length) * 100).toFixed(1)}%)`,
  );

  console.log("\nFull path (encode + LZ4 compress):");
  const jsonFullSimple = bench(
    "JSONEachRow + LZ4",
    () => {
      const data = encodeJsonEachRow(simpleData);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonFullSimple, ROWS));

  const rbFullSimple = bench(
    "RowBinary + LZ4",
    () => {
      const data = encodeRowBinary(simpleColumns, simpleRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbFullSimple, ROWS));

  const nativeFullSimple = bench(
    "Native + LZ4",
    () => {
      const data = encodeNative(simpleColumns, simpleRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeFullSimple, ROWS));

  // === Escape data ===
  console.log(
    "\n=== Escape Data (strings with quotes, newlines, backslashes) ===\n",
  );

  const escapeData = generateEscapeData(ROWS);
  const escapeColumns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "name", type: "String" },
    { name: "desc", type: "String" },
    { name: "path", type: "String" },
  ];
  const escapeRowsArray = escapeData.map((r) => [r.id, r.name, r.desc, r.path]);

  const escapeJsonEncoded = encodeJsonEachRow(escapeData);
  const escapeRowBinaryEncoded = encodeRowBinary(
    escapeColumns,
    escapeRowsArray,
  );
  const escapeNativeEncoded = encodeNative(escapeColumns, escapeRowsArray);

  console.log(
    `  Encoded sizes: JSON=${escapeJsonEncoded.length}, RowBinary=${escapeRowBinaryEncoded.length} (${((escapeRowBinaryEncoded.length / escapeJsonEncoded.length) * 100).toFixed(1)}%), Native=${escapeNativeEncoded.length} (${((escapeNativeEncoded.length / escapeJsonEncoded.length) * 100).toFixed(1)}%)\n`,
  );

  console.log("Encoding:");
  const jsonEncodeEscape = bench(
    "JSONEachRow encode",
    () => {
      encodeJsonEachRow(escapeData);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonEncodeEscape, ROWS));

  const rbEncodeEscape = bench(
    "RowBinary encode",
    () => {
      encodeRowBinary(escapeColumns, escapeRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbEncodeEscape, ROWS));

  const nativeEncodeEscape = bench(
    "Native encode",
    () => {
      encodeNative(escapeColumns, escapeRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeEncodeEscape, ROWS));

  // Full path with compression
  const escapeJsonCompressed = encodeBlock(escapeJsonEncoded, Method.LZ4);
  const escapeRbCompressed = encodeBlock(escapeRowBinaryEncoded, Method.LZ4);
  const escapeNativeCompressed = encodeBlock(escapeNativeEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${escapeJsonCompressed.length}, RowBinary+LZ4=${escapeRbCompressed.length} (${((escapeRbCompressed.length / escapeJsonCompressed.length) * 100).toFixed(1)}%), Native+LZ4=${escapeNativeCompressed.length} (${((escapeNativeCompressed.length / escapeJsonCompressed.length) * 100).toFixed(1)}%)`,
  );

  console.log("\nFull path (encode + LZ4 compress):");
  const jsonFullEscape = bench(
    "JSONEachRow + LZ4",
    () => {
      const data = encodeJsonEachRow(escapeData);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonFullEscape, ROWS));

  const rbFullEscape = bench(
    "RowBinary + LZ4",
    () => {
      const data = encodeRowBinary(escapeColumns, escapeRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbFullEscape, ROWS));

  const nativeFullEscape = bench(
    "Native + LZ4",
    () => {
      const data = encodeNative(escapeColumns, escapeRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeFullEscape, ROWS));

  // === Complex data ===
  console.log("\n=== Complex Data (arrays, nullable) ===\n");

  const complexData = generateComplexData(ROWS);
  const complexColumns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "tags", type: "Array(String)" },
    { name: "scores", type: "Array(Float64)" },
    { name: "metadata", type: "Nullable(String)" },
  ];
  const complexRowsArray = complexData.map((r) => [
    r.id,
    r.tags,
    r.scores,
    r.metadata,
  ]);

  const complexJsonEncoded = encodeJsonEachRow(complexData);
  const complexRowBinaryEncoded = encodeRowBinary(
    complexColumns,
    complexRowsArray,
  );
  const complexNativeEncoded = encodeNative(complexColumns, complexRowsArray);

  console.log(
    `  Encoded sizes: JSON=${complexJsonEncoded.length}, RowBinary=${complexRowBinaryEncoded.length} (${((complexRowBinaryEncoded.length / complexJsonEncoded.length) * 100).toFixed(1)}%), Native=${complexNativeEncoded.length} (${((complexNativeEncoded.length / complexJsonEncoded.length) * 100).toFixed(1)}%)\n`,
  );

  console.log("Encoding:");
  const jsonEncodeComplex = bench(
    "JSONEachRow encode",
    () => {
      encodeJsonEachRow(complexData);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonEncodeComplex, ROWS));

  const rbEncodeComplex = bench(
    "RowBinary encode",
    () => {
      encodeRowBinary(complexColumns, complexRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbEncodeComplex, ROWS));

  const nativeEncodeComplex = bench(
    "Native encode",
    () => {
      encodeNative(complexColumns, complexRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeEncodeComplex, ROWS));

  console.log("\nDecoding:");
  const jsonDecodeComplex = bench(
    "JSONEachRow decode",
    () => {
      decodeJsonEachRow(complexJsonEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonDecodeComplex, ROWS));

  const rbDecodeComplex = bench(
    "RowBinary decode",
    () => {
      decodeRowBinary(complexRowBinaryEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbDecodeComplex, ROWS));

  const nativeDecodeComplex = await benchAsync(
    "Native decode",
    async () => {
      await decodeNative(complexNativeEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeDecodeComplex, ROWS));

  // Full path with compression
  const complexJsonCompressed = encodeBlock(complexJsonEncoded, Method.LZ4);
  const complexRbCompressed = encodeBlock(complexRowBinaryEncoded, Method.LZ4);
  const complexNativeCompressed = encodeBlock(complexNativeEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${complexJsonCompressed.length}, RowBinary+LZ4=${complexRbCompressed.length} (${((complexRbCompressed.length / complexJsonCompressed.length) * 100).toFixed(1)}%), Native+LZ4=${complexNativeCompressed.length} (${((complexNativeCompressed.length / complexJsonCompressed.length) * 100).toFixed(1)}%)`,
  );

  console.log("\nFull path (encode + LZ4 compress):");
  const jsonFullComplex = bench(
    "JSONEachRow + LZ4",
    () => {
      const data = encodeJsonEachRow(complexData);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonFullComplex, ROWS));

  const rbFullComplex = bench(
    "RowBinary + LZ4",
    () => {
      const data = encodeRowBinary(complexColumns, complexRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbFullComplex, ROWS));

  const nativeFullComplex = bench(
    "Native + LZ4",
    () => {
      const data = encodeNative(complexColumns, complexRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeFullComplex, ROWS));

  // === Complex Data (Typed) ===
  console.log("\n=== Complex Data (Typed) (arrays as TypedArrays) ===\n");

  const complexTypedData = generateComplexTypedData(ROWS);
  // Reuse complexColumns definition
  const complexTypedRowsArray = complexTypedData.map((r) => [
    r.id,
    r.tags,
    r.scores,
    r.metadata,
  ]);

  const complexTypedJsonEncoded = encodeJsonEachRow(complexTypedData);
  const complexTypedRowBinaryEncoded = encodeRowBinary(
    complexColumns,
    complexTypedRowsArray,
  );
  const complexTypedNativeEncoded = encodeNative(complexColumns, complexTypedRowsArray);

  console.log(
    `  Encoded sizes: JSON=${complexTypedJsonEncoded.length}, RowBinary=${complexTypedRowBinaryEncoded.length} (${((complexTypedRowBinaryEncoded.length / complexTypedJsonEncoded.length) * 100).toFixed(1)}%), Native=${complexTypedNativeEncoded.length} (${((complexTypedNativeEncoded.length / complexTypedJsonEncoded.length) * 100).toFixed(1)}%)\n`,
  );

  console.log("Encoding:");
  const jsonEncodeComplexTyped = bench(
    "JSONEachRow encode",
    () => {
      encodeJsonEachRow(complexTypedData);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonEncodeComplexTyped, ROWS));

  const rbEncodeComplexTyped = bench(
    "RowBinary encode",
    () => {
      encodeRowBinary(complexColumns, complexTypedRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbEncodeComplexTyped, ROWS));

  const nativeEncodeComplexTyped = bench(
    "Native encode",
    () => {
      encodeNative(complexColumns, complexTypedRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeEncodeComplexTyped, ROWS));

  console.log("\nDecoding:");
  const jsonDecodeComplexTyped = bench(
    "JSONEachRow decode",
    () => {
      decodeJsonEachRow(complexTypedJsonEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonDecodeComplexTyped, ROWS));

  const rbDecodeComplexTyped = bench(
    "RowBinary decode",
    () => {
      decodeRowBinary(complexTypedRowBinaryEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbDecodeComplexTyped, ROWS));

  const nativeDecodeComplexTyped = await benchAsync(
    "Native decode",
    async () => {
      await decodeNative(complexTypedNativeEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeDecodeComplexTyped, ROWS));

  // Full path with compression
  const complexTypedJsonCompressed = encodeBlock(complexTypedJsonEncoded, Method.LZ4);
  const complexTypedRbCompressed = encodeBlock(complexTypedRowBinaryEncoded, Method.LZ4);
  const complexTypedNativeCompressed = encodeBlock(complexTypedNativeEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${complexTypedJsonCompressed.length}, RowBinary+LZ4=${complexTypedRbCompressed.length} (${((complexTypedRbCompressed.length / complexTypedJsonCompressed.length) * 100).toFixed(1)}%), Native+LZ4=${complexTypedNativeCompressed.length} (${((complexTypedNativeCompressed.length / complexTypedJsonCompressed.length) * 100).toFixed(1)}%)`,
  );

  console.log("\nFull path (encode + LZ4 compress):");
  const jsonFullComplexTyped = bench(
    "JSONEachRow + LZ4",
    () => {
      const data = encodeJsonEachRow(complexTypedData);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(jsonFullComplexTyped, ROWS));

  const rbFullComplexTyped = bench(
    "RowBinary + LZ4",
    () => {
      const data = encodeRowBinary(complexColumns, complexTypedRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbFullComplexTyped, ROWS));

  const nativeFullComplexTyped = bench(
    "Native + LZ4",
    () => {
      const data = encodeNative(complexColumns, complexTypedRowsArray);
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeFullComplexTyped, ROWS));

  // === Summary ===
  console.log("\n=== Summary (speedup vs JSON) ===\n");

  const fmtSpeed = (json: number, rb: number, native: number) =>
    `RB ${(json / rb).toFixed(2)}x, Native ${(json / native).toFixed(2)}x`;
  const fmtSize = (json: number, rb: number, native: number) =>
    `RB ${(json / rb).toFixed(2)}x, Native ${(json / native).toFixed(2)}x smaller`;

  console.log("Simple data:");
  console.log(`  Encode: ${fmtSpeed(jsonEncodeSimple.ms, rbEncodeSimple.ms, nativeEncodeSimple.ms)}`);
  console.log(`  Decode: ${fmtSpeed(jsonDecodeSimple.ms, rbDecodeSimple.ms, nativeDecodeSimple.ms)}`);
  console.log(`  Size:   ${fmtSize(simpleJsonEncoded.length, simpleRowBinaryEncoded.length, simpleNativeEncoded.length)}`);
  console.log(`  +LZ4:   ${fmtSize(simpleJsonCompressed.length, simpleRbCompressed.length, simpleNativeCompressed.length)}`);

  console.log("\nEscape data:");
  console.log(`  Encode: ${fmtSpeed(jsonEncodeEscape.ms, rbEncodeEscape.ms, nativeEncodeEscape.ms)}`);
  console.log(`  Size:   ${fmtSize(escapeJsonEncoded.length, escapeRowBinaryEncoded.length, escapeNativeEncoded.length)}`);
  console.log(`  +LZ4:   ${fmtSize(escapeJsonCompressed.length, escapeRbCompressed.length, escapeNativeCompressed.length)}`);

  console.log("\nComplex data:");
  console.log(`  Encode: ${fmtSpeed(jsonEncodeComplex.ms, rbEncodeComplex.ms, nativeEncodeComplex.ms)}`);
  console.log(`  Decode: ${fmtSpeed(jsonDecodeComplex.ms, rbDecodeComplex.ms, nativeDecodeComplex.ms)}`);
  console.log(`  Size:   ${fmtSize(complexJsonEncoded.length, complexRowBinaryEncoded.length, complexNativeEncoded.length)}`);
  console.log(`  +LZ4:   ${fmtSize(complexJsonCompressed.length, complexRbCompressed.length, complexNativeCompressed.length)}`);

  console.log("\nComplex data (Typed):");
  console.log(`  Encode: ${fmtSpeed(jsonEncodeComplexTyped.ms, rbEncodeComplexTyped.ms, nativeEncodeComplexTyped.ms)}`);
  console.log(`  Decode: ${fmtSpeed(jsonDecodeComplexTyped.ms, rbDecodeComplexTyped.ms, nativeDecodeComplexTyped.ms)}`);
  console.log(`  Size:   ${fmtSize(complexTypedJsonEncoded.length, complexTypedRowBinaryEncoded.length, complexTypedNativeEncoded.length)}`);
  console.log(`  +LZ4:   ${fmtSize(complexTypedJsonCompressed.length, complexTypedRbCompressed.length, complexTypedNativeCompressed.length)}`);

  // === Streaming vs Sync ===
  console.log("\n=== Streaming vs Sync (Simple Data) ===\n");

  // encodeRowBinary already produces RowBinaryWithNamesAndTypes format
  const simpleWithTypesEncoded = simpleRowBinaryEncoded;

  console.log("Decoding (sync vs streaming):");
  const syncDecode = bench(
    "Sync decode",
    () => {
      decodeRowBinary(simpleWithTypesEncoded);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(syncDecode, ROWS));

  // Streaming with single chunk (best case)
  const streamDecode1Chunk = await benchAsync(
    "Stream decode (1 chunk)",
    async () => {
      await collectRowBinary(chunkedStream(simpleWithTypesEncoded, simpleWithTypesEncoded.length));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(streamDecode1Chunk, ROWS));

  // Streaming with 64KB chunks (realistic)
  const streamDecode64K = await benchAsync(
    "Stream decode (64KB chunks)",
    async () => {
      await collectRowBinary(chunkedStream(simpleWithTypesEncoded, 64 * 1024));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(streamDecode64K, ROWS));

  // Streaming with 4KB chunks (small chunks)
  const streamDecode4K = await benchAsync(
    "Stream decode (4KB chunks)",
    async () => {
      await collectRowBinary(chunkedStream(simpleWithTypesEncoded, 4 * 1024));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(streamDecode4K, ROWS));

  console.log("\nEncoding (sync vs streaming):");
  const syncEncode = bench(
    "Sync encode",
    () => {
      encodeRowBinary(simpleColumns, simpleRowsArray);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(syncEncode, ROWS));

  const streamEncode = await benchAsync(
    "Stream encode",
    async () => {
      await collectChunks(streamEncodeRowBinary(simpleColumns, simpleRowsArray));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(streamEncode, ROWS));

  console.log("\nStreaming overhead:");
  console.log(`  Decode (1 chunk): ${((streamDecode1Chunk.ms / syncDecode.ms - 1) * 100).toFixed(1)}% overhead`);
  console.log(`  Decode (64KB):    ${((streamDecode64K.ms / syncDecode.ms - 1) * 100).toFixed(1)}% overhead`);
  console.log(`  Decode (4KB):     ${((streamDecode4K.ms / syncDecode.ms - 1) * 100).toFixed(1)}% overhead`);
  console.log(`  Encode:           ${((streamEncode.ms / syncEncode.ms - 1) * 100).toFixed(1)}% overhead`);
}

main().catch(console.error);
