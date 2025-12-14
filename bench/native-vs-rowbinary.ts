/**
 * Benchmark: Native vs RowBinary format performance
 */

import { init as initCompression } from "../compression.ts";
import { encodeRowBinary, decodeRowBinary, type ColumnDef } from "../rowbinary.ts";
import { encodeNative, decodeNative } from "../native.ts";

// Test data generators
function generateSimpleData(count: number): unknown[][] {
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    rows.push([
      i,                           // Int32
      `user_${i}`,                 // String
      `user${i}@example.com`,      // String
      i % 2 === 0,                 // Bool (as 0/1)
      Math.random() * 100,         // Float64
    ]);
  }
  return rows;
}

const simpleColumns: ColumnDef[] = [
  { name: "id", type: "Int32" },
  { name: "name", type: "String" },
  { name: "email", type: "String" },
  { name: "active", type: "UInt8" },
  { name: "score", type: "Float64" },
];

function bench(
  name: string,
  fn: () => void,
  warmup = 20,
  iterations = 50,
): { name: string; ms: number } {
  for (let i = 0; i < warmup; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  const elapsed = performance.now() - start;

  return { name, ms: elapsed / iterations };
}

function formatResult(result: { name: string; ms: number }, rows: number): string {
  const rowsPerSec = rows / (result.ms / 1000);
  return `  ${result.name.padEnd(25)} ${result.ms.toFixed(3).padStart(8)}ms  ${(rowsPerSec / 1_000_000).toFixed(2).padStart(6)}M rows/sec`;
}

async function main() {
  await initCompression();

  const ROWS = 10_000;
  const ITERATIONS = 50;

  console.log(`Benchmarking Native vs RowBinary with ${ROWS} rows, ${ITERATIONS} iterations\n`);

  const rows = generateSimpleData(ROWS);

  // Pre-encode for decode benchmarks
  const rowBinaryEncoded = encodeRowBinary(simpleColumns, rows);
  const nativeEncoded = encodeNative(simpleColumns, rows);

  console.log(`Encoded sizes:`);
  console.log(`  RowBinary: ${rowBinaryEncoded.length} bytes`);
  console.log(`  Native:    ${nativeEncoded.length} bytes`);
  console.log(`  Ratio:     ${(nativeEncoded.length / rowBinaryEncoded.length * 100).toFixed(1)}%\n`);

  // Encoding benchmarks
  console.log("Encoding:");

  const rbEncode = bench("RowBinary encode", () => {
    encodeRowBinary(simpleColumns, rows);
  }, 20, ITERATIONS);
  console.log(formatResult(rbEncode, ROWS));

  const nativeEncode = bench("Native encode", () => {
    encodeNative(simpleColumns, rows);
  }, 20, ITERATIONS);
  console.log(formatResult(nativeEncode, ROWS));

  // Decoding benchmarks
  console.log("\nDecoding:");

  const rbDecode = bench("RowBinary decode", () => {
    decodeRowBinary(rowBinaryEncoded);
  }, 20, ITERATIONS);
  console.log(formatResult(rbDecode, ROWS));

  const nativeDecode = bench("Native decode", () => {
    decodeNative(nativeEncoded);
  }, 20, ITERATIONS);
  console.log(formatResult(nativeDecode, ROWS));

  // Summary
  console.log("\n=== Summary ===\n");
  console.log(`Encode: Native is ${(rbEncode.ms / nativeEncode.ms).toFixed(2)}x ${nativeEncode.ms < rbEncode.ms ? 'faster' : 'slower'} than RowBinary`);
  console.log(`Decode: Native is ${(rbDecode.ms / nativeDecode.ms).toFixed(2)}x ${nativeDecode.ms < rbDecode.ms ? 'faster' : 'slower'} than RowBinary`);
  console.log(`Size:   Native is ${(rowBinaryEncoded.length / nativeEncoded.length).toFixed(2)}x ${nativeEncoded.length < rowBinaryEncoded.length ? 'smaller' : 'larger'} than RowBinary`);
}

main().catch(console.error);
