// Benchmark: RowBinary vs JSONEachRow for insert and query
//
// Tests encoding/decoding performance for both formats with various data types.

import { init, encodeBlock, decodeBlock, Method } from '../compression.ts';
import {
  encodeRowBinaryWithNames,
  decodeRowBinaryWithNames,
  decodeRowBinaryWithNamesAndTypes,
  type ColumnDef,
} from '../rowbinary.ts';

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
      created_at: new Date('2024-01-15T10:30:00Z'),
    });
  }
  return rows;
}

// Data with strings that need JSON escaping
function generateEscapeData(count: number): Array<{ id: number; name: string; desc: string; path: string }> {
  const rows: Array<{ id: number; name: string; desc: string; path: string }> = [];
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
  const rows: Array<{ id: number; tags: string[]; scores: number[]; metadata: string | null }> = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      tags: [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`],
      scores: [Math.random() * 100, Math.random() * 100, Math.random() * 100],
      metadata: i % 3 === 0 ? null : `meta_${i}`,
    });
  }
  return rows;
}

// --- Benchmark infrastructure ---

function bench(name: string, fn: () => void, warmup = 50, iterations = 100): { name: string; ms: number } {
  // Warmup
  for (let i = 0; i < warmup; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  const elapsed = performance.now() - start;

  return { name, ms: elapsed / iterations };
}

function formatResult(result: { name: string; ms: number }, rows: number): string {
  const rowsPerSec = rows / (result.ms / 1000);
  return `  ${result.name.padEnd(30)} ${result.ms.toFixed(3).padStart(8)}ms  ${(rowsPerSec / 1_000_000).toFixed(2).padStart(6)}M rows/sec`;
}

// --- JSONEachRow helpers ---

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeJsonEachRow(rows: Record<string, unknown>[]): Uint8Array {
  let json = '';
  for (const row of rows) {
    json += JSON.stringify(row) + '\n';
  }
  return encoder.encode(json);
}

function decodeJsonEachRow<T>(data: Uint8Array): T[] {
  const text = decoder.decode(data);
  const lines = text.trim().split('\n');
  return lines.map(line => JSON.parse(line) as T);
}

// --- Run benchmarks ---

async function main() {
  await init();

  const ROWS = 10_000;
  const ITERATIONS = 50;

  console.log(`Benchmarking with ${ROWS} rows, ${ITERATIONS} iterations each\n`);

  // === Simple data ===
  console.log('=== Simple Data (6 columns: int, 2 strings, bool, float, datetime) ===\n');

  const simpleData = generateSimpleData(ROWS);
  const simpleColumns: ColumnDef[] = [
    { name: 'id', type: 'UInt32' },
    { name: 'name', type: 'String' },
    { name: 'email', type: 'String' },
    { name: 'active', type: 'Bool' },
    { name: 'score', type: 'Float64' },
    { name: 'created_at', type: 'DateTime' },
  ];
  const simpleRowsArray = simpleData.map(r => [r.id, r.name, r.email, r.active, r.score, r.created_at]);

  // Pre-encode for decode benchmarks
  const simpleJsonEncoded = encodeJsonEachRow(simpleData);
  const simpleRowBinaryEncoded = encodeRowBinaryWithNames(simpleColumns, simpleRowsArray);

  console.log(`  Encoded sizes: JSON=${simpleJsonEncoded.length} bytes, RowBinary=${simpleRowBinaryEncoded.length} bytes (${(simpleRowBinaryEncoded.length / simpleJsonEncoded.length * 100).toFixed(1)}%)\n`);

  // Encoding benchmarks
  console.log('Encoding:');
  const jsonEncodeSimple = bench('JSONEachRow encode', () => {
    encodeJsonEachRow(simpleData);
  }, 50, ITERATIONS);
  console.log(formatResult(jsonEncodeSimple, ROWS));

  const rbEncodeSimple = bench('RowBinary encode', () => {
    encodeRowBinaryWithNames(simpleColumns, simpleRowsArray);
  }, 50, ITERATIONS);
  console.log(formatResult(rbEncodeSimple, ROWS));

  // Decoding benchmarks
  console.log('\nDecoding:');
  const jsonDecodeSimple = bench('JSONEachRow decode', () => {
    decodeJsonEachRow(simpleJsonEncoded);
  }, 50, ITERATIONS);
  console.log(formatResult(jsonDecodeSimple, ROWS));

  const rbDecodeSimple = bench('RowBinary decode', () => {
    decodeRowBinaryWithNames(simpleRowBinaryEncoded, simpleColumns.map(c => c.type));
  }, 50, ITERATIONS);
  console.log(formatResult(rbDecodeSimple, ROWS));

  // Full path with compression
  console.log('\nFull path (encode + LZ4 compress):');
  const jsonFullSimple = bench('JSONEachRow + LZ4', () => {
    const data = encodeJsonEachRow(simpleData);
    encodeBlock(data, Method.LZ4);
  }, 50, ITERATIONS);
  console.log(formatResult(jsonFullSimple, ROWS));

  const rbFullSimple = bench('RowBinary + LZ4', () => {
    const data = encodeRowBinaryWithNames(simpleColumns, simpleRowsArray);
    encodeBlock(data, Method.LZ4);
  }, 50, ITERATIONS);
  console.log(formatResult(rbFullSimple, ROWS));

  // === Escape data ===
  console.log('\n=== Escape Data (strings with quotes, newlines, backslashes) ===\n');

  const escapeData = generateEscapeData(ROWS);
  const escapeColumns: ColumnDef[] = [
    { name: 'id', type: 'UInt32' },
    { name: 'name', type: 'String' },
    { name: 'desc', type: 'String' },
    { name: 'path', type: 'String' },
  ];
  const escapeRowsArray = escapeData.map(r => [r.id, r.name, r.desc, r.path]);

  const escapeJsonEncoded = encodeJsonEachRow(escapeData);
  const escapeRowBinaryEncoded = encodeRowBinaryWithNames(escapeColumns, escapeRowsArray);

  console.log(`  Encoded sizes: JSON=${escapeJsonEncoded.length} bytes, RowBinary=${escapeRowBinaryEncoded.length} bytes (${(escapeRowBinaryEncoded.length / escapeJsonEncoded.length * 100).toFixed(1)}%)\n`);

  console.log('Encoding:');
  const jsonEncodeEscape = bench('JSONEachRow encode', () => {
    encodeJsonEachRow(escapeData);
  }, 50, ITERATIONS);
  console.log(formatResult(jsonEncodeEscape, ROWS));

  const rbEncodeEscape = bench('RowBinary encode', () => {
    encodeRowBinaryWithNames(escapeColumns, escapeRowsArray);
  }, 50, ITERATIONS);
  console.log(formatResult(rbEncodeEscape, ROWS));

  // === Complex data ===
  console.log('\n=== Complex Data (arrays, nullable) ===\n');

  const complexData = generateComplexData(ROWS);
  const complexColumns: ColumnDef[] = [
    { name: 'id', type: 'UInt32' },
    { name: 'tags', type: 'Array(String)' },
    { name: 'scores', type: 'Array(Float64)' },
    { name: 'metadata', type: 'Nullable(String)' },
  ];
  const complexRowsArray = complexData.map(r => [r.id, r.tags, r.scores, r.metadata]);

  const complexJsonEncoded = encodeJsonEachRow(complexData);
  const complexRowBinaryEncoded = encodeRowBinaryWithNames(complexColumns, complexRowsArray);

  console.log(`  Encoded sizes: JSON=${complexJsonEncoded.length} bytes, RowBinary=${complexRowBinaryEncoded.length} bytes (${(complexRowBinaryEncoded.length / complexJsonEncoded.length * 100).toFixed(1)}%)\n`);

  console.log('Encoding:');
  const jsonEncodeComplex = bench('JSONEachRow encode', () => {
    encodeJsonEachRow(complexData);
  }, 50, ITERATIONS);
  console.log(formatResult(jsonEncodeComplex, ROWS));

  const rbEncodeComplex = bench('RowBinary encode', () => {
    encodeRowBinaryWithNames(complexColumns, complexRowsArray);
  }, 50, ITERATIONS);
  console.log(formatResult(rbEncodeComplex, ROWS));

  console.log('\nDecoding:');
  const jsonDecodeComplex = bench('JSONEachRow decode', () => {
    decodeJsonEachRow(complexJsonEncoded);
  }, 50, ITERATIONS);
  console.log(formatResult(jsonDecodeComplex, ROWS));

  const rbDecodeComplex = bench('RowBinary decode', () => {
    decodeRowBinaryWithNames(complexRowBinaryEncoded, complexColumns.map(c => c.type));
  }, 50, ITERATIONS);
  console.log(formatResult(rbDecodeComplex, ROWS));

  // === Summary ===
  console.log('\n=== Summary ===\n');
  console.log('Simple data:');
  console.log(`  Encode: RowBinary is ${(jsonEncodeSimple.ms / rbEncodeSimple.ms).toFixed(2)}x ${rbEncodeSimple.ms < jsonEncodeSimple.ms ? 'faster' : 'slower'}`);
  console.log(`  Decode: RowBinary is ${(jsonDecodeSimple.ms / rbDecodeSimple.ms).toFixed(2)}x ${rbDecodeSimple.ms < jsonDecodeSimple.ms ? 'faster' : 'slower'}`);
  console.log(`  Size: RowBinary is ${(simpleJsonEncoded.length / simpleRowBinaryEncoded.length).toFixed(2)}x smaller`);

  console.log('\nEscape data:');
  console.log(`  Encode: RowBinary is ${(jsonEncodeEscape.ms / rbEncodeEscape.ms).toFixed(2)}x ${rbEncodeEscape.ms < jsonEncodeEscape.ms ? 'faster' : 'slower'}`);
  console.log(`  Size: RowBinary is ${(escapeJsonEncoded.length / escapeRowBinaryEncoded.length).toFixed(2)}x smaller`);

  console.log('\nComplex data:');
  console.log(`  Encode: RowBinary is ${(jsonEncodeComplex.ms / rbEncodeComplex.ms).toFixed(2)}x ${rbEncodeComplex.ms < jsonEncodeComplex.ms ? 'faster' : 'slower'}`);
  console.log(`  Decode: RowBinary is ${(jsonDecodeComplex.ms / rbDecodeComplex.ms).toFixed(2)}x ${rbDecodeComplex.ms < jsonDecodeComplex.ms ? 'faster' : 'slower'}`);
  console.log(`  Size: RowBinary is ${(complexJsonEncoded.length / complexRowBinaryEncoded.length).toFixed(2)}x smaller`);
}

main().catch(console.error);
