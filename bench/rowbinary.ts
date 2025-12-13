// Benchmark: RowBinary vs JSONEachRow for insert and query
//
// Tests encoding/decoding performance for both formats with various data types.

import { init, encodeBlock, decodeBlock, Method } from "../compression.ts";
import {
  encodeRowBinary,
  decodeRowBinary,
  streamDecodeRowBinary,
  streamEncodeRowBinary,
  type ColumnDef,
} from "../rowbinary.ts";

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

  console.log(
    `  Encoded sizes: JSON=${simpleJsonEncoded.length} bytes, RowBinary=${simpleRowBinaryEncoded.length} bytes (${((simpleRowBinaryEncoded.length / simpleJsonEncoded.length) * 100).toFixed(1)}%)\n`,
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

  // Full path with compression
  const simpleJsonCompressed = encodeBlock(simpleJsonEncoded, Method.LZ4);
  const simpleRbCompressed = encodeBlock(simpleRowBinaryEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${simpleJsonCompressed.length} bytes, RowBinary+LZ4=${simpleRbCompressed.length} bytes (${((simpleRbCompressed.length / simpleJsonCompressed.length) * 100).toFixed(1)}%)`,
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

  console.log(
    `  Encoded sizes: JSON=${escapeJsonEncoded.length} bytes, RowBinary=${escapeRowBinaryEncoded.length} bytes (${((escapeRowBinaryEncoded.length / escapeJsonEncoded.length) * 100).toFixed(1)}%)\n`,
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

  // Full path with compression
  const escapeJsonCompressed = encodeBlock(escapeJsonEncoded, Method.LZ4);
  const escapeRbCompressed = encodeBlock(escapeRowBinaryEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${escapeJsonCompressed.length} bytes, RowBinary+LZ4=${escapeRbCompressed.length} bytes (${((escapeRbCompressed.length / escapeJsonCompressed.length) * 100).toFixed(1)}%)`,
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

  console.log(
    `  Encoded sizes: JSON=${complexJsonEncoded.length} bytes, RowBinary=${complexRowBinaryEncoded.length} bytes (${((complexRowBinaryEncoded.length / complexJsonEncoded.length) * 100).toFixed(1)}%)\n`,
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

  // Full path with compression
  const complexJsonCompressed = encodeBlock(complexJsonEncoded, Method.LZ4);
  const complexRbCompressed = encodeBlock(complexRowBinaryEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${complexJsonCompressed.length} bytes, RowBinary+LZ4=${complexRbCompressed.length} bytes (${((complexRbCompressed.length / complexJsonCompressed.length) * 100).toFixed(1)}%)`,
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

  console.log(
    `  Encoded sizes: JSON=${complexTypedJsonEncoded.length} bytes, RowBinary=${complexTypedRowBinaryEncoded.length} bytes (${((complexTypedRowBinaryEncoded.length / complexTypedJsonEncoded.length) * 100).toFixed(1)}%)\n`,
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

  // Full path with compression
  const complexTypedJsonCompressed = encodeBlock(
    complexTypedJsonEncoded,
    Method.LZ4,
  );
  const complexTypedRbCompressed = encodeBlock(
    complexTypedRowBinaryEncoded,
    Method.LZ4,
  );
  console.log(
    `\nCompressed sizes: JSON+LZ4=${complexTypedJsonCompressed.length} bytes, RowBinary+LZ4=${complexTypedRbCompressed.length} bytes (${((complexTypedRbCompressed.length / complexTypedJsonCompressed.length) * 100).toFixed(1)}%)`,
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
      const data = encodeRowBinary(
        complexColumns,
        complexTypedRowsArray,
      );
      encodeBlock(data, Method.LZ4);
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(rbFullComplexTyped, ROWS));

  // === Summary ===
  console.log("\n=== Summary ===\n");
  console.log("Simple data:");
  console.log(
    `  Encode: RowBinary is ${(jsonEncodeSimple.ms / rbEncodeSimple.ms).toFixed(2)}x ${rbEncodeSimple.ms < jsonEncodeSimple.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Decode: RowBinary is ${(jsonDecodeSimple.ms / rbDecodeSimple.ms).toFixed(2)}x ${rbDecodeSimple.ms < jsonDecodeSimple.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Size: RowBinary is ${(simpleJsonEncoded.length / simpleRowBinaryEncoded.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Size+LZ4: RowBinary is ${(simpleJsonCompressed.length / simpleRbCompressed.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Full path: RowBinary+LZ4 is ${(jsonFullSimple.ms / rbFullSimple.ms).toFixed(2)}x ${rbFullSimple.ms < jsonFullSimple.ms ? "faster" : "slower"}`,
  );

  console.log("\nEscape data:");
  console.log(
    `  Encode: RowBinary is ${(jsonEncodeEscape.ms / rbEncodeEscape.ms).toFixed(2)}x ${rbEncodeEscape.ms < jsonEncodeEscape.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Size: RowBinary is ${(escapeJsonEncoded.length / escapeRowBinaryEncoded.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Size+LZ4: RowBinary is ${(escapeJsonCompressed.length / escapeRbCompressed.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Full path: RowBinary+LZ4 is ${(jsonFullEscape.ms / rbFullEscape.ms).toFixed(2)}x ${rbFullEscape.ms < jsonFullEscape.ms ? "faster" : "slower"}`,
  );

  console.log("\nComplex data:");
  console.log(
    `  Encode: RowBinary is ${(jsonEncodeComplex.ms / rbEncodeComplex.ms).toFixed(2)}x ${rbEncodeComplex.ms < jsonEncodeComplex.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Decode: RowBinary is ${(jsonDecodeComplex.ms / rbDecodeComplex.ms).toFixed(2)}x ${rbDecodeComplex.ms < jsonDecodeComplex.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Size: RowBinary is ${(complexJsonEncoded.length / complexRowBinaryEncoded.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Size+LZ4: RowBinary is ${(complexJsonCompressed.length / complexRbCompressed.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Full path: RowBinary+LZ4 is ${(jsonFullComplex.ms / rbFullComplex.ms).toFixed(2)}x ${rbFullComplex.ms < jsonFullComplex.ms ? "faster" : "slower"}`,
  );

  console.log("\nComplex data (Typed):");
  console.log(
    `  Encode: RowBinary is ${(jsonEncodeComplexTyped.ms / rbEncodeComplexTyped.ms).toFixed(2)}x ${rbEncodeComplexTyped.ms < jsonEncodeComplexTyped.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Decode: RowBinary is ${(jsonDecodeComplexTyped.ms / rbDecodeComplexTyped.ms).toFixed(2)}x ${rbDecodeComplexTyped.ms < jsonDecodeComplexTyped.ms ? "faster" : "slower"}`,
  );
  console.log(
    `  Size: RowBinary is ${(complexTypedJsonEncoded.length / complexTypedRowBinaryEncoded.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Size+LZ4: RowBinary is ${(complexTypedJsonCompressed.length / complexTypedRbCompressed.length).toFixed(2)}x smaller`,
  );
  console.log(
    `  Full path: RowBinary+LZ4 is ${(jsonFullComplexTyped.ms / rbFullComplexTyped.ms).toFixed(2)}x ${rbFullComplexTyped.ms < jsonFullComplexTyped.ms ? "faster" : "slower"}`,
  );

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
