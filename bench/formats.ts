// Benchmark: Native vs RowBinary vs JSONEachRow
//
// Tests encoding/decoding performance for all formats with various data types.

import { init, encodeBlock, Method } from "../compression.ts";
import {
  encodeRowBinary,
  decodeRowBinary,
  streamDecodeRowBinary,
  streamEncodeRowBinary,
  type ColumnDef,
} from "../formats/rowbinary.ts";
import {
  encodeNative,
  decodeNative,
  tableFromRows,
  Table,
} from "../formats/native/index.ts";

function encodeNativeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  return encodeNative(tableFromRows(columns, rows));
}

// --- Benchmark infrastructure ---

function bench(
  name: string,
  fn: () => void,
  warmup = 50,
  iterations = 100,
): { name: string; ms: number } {
  for (let i = 0; i < warmup; i++) fn();
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  return { name, ms: (performance.now() - start) / iterations };
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
  return { name, ms: (performance.now() - start) / iterations };
}

function formatResult(
  result: { name: string; ms: number },
  rows: number,
): string {
  const rowsPerSec = rows / (result.ms / 1000);
  return `  ${result.name.padEnd(30)} ${result.ms.toFixed(3).padStart(8)}ms  ${(rowsPerSec / 1_000_000).toFixed(2).padStart(6)}M rows/sec`;
}

// --- JSON helpers ---

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeJsonEachRow(rows: Record<string, unknown>[]): Uint8Array {
  let json = "";
  for (const row of rows) json += JSON.stringify(row) + "\n";
  return encoder.encode(json);
}

function decodeJsonEachRow<T>(data: Uint8Array): T[] {
  return decoder
    .decode(data)
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line) as T);
}

// --- Streaming helpers ---

async function* chunkedStream(
  data: Uint8Array,
  chunkSize: number,
): AsyncIterable<Uint8Array> {
  for (let i = 0; i < data.length; i += chunkSize) {
    yield data.subarray(i, Math.min(i + chunkSize, data.length));
  }
}

async function collectChunks(
  gen: AsyncIterable<Uint8Array>,
): Promise<Uint8Array> {
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

async function collectRowBinary(
  chunks: AsyncIterable<Uint8Array>,
): Promise<{ columns: ColumnDef[]; rows: unknown[][] }> {
  let columns: ColumnDef[] = [];
  const rows: unknown[][] = [];
  for await (const batch of streamDecodeRowBinary(chunks)) {
    columns = batch.columns;
    rows.push(...batch.rows);
  }
  return { columns, rows };
}

// --- Scenario types ---

interface Scenario {
  name: string;
  description: string;
  columns: ColumnDef[];
  jsonData: Record<string, unknown>[];
  rowsArray: unknown[][];
}

interface ScenarioResult {
  name: string;
  encode: { json: number; rb: number; native: number };
  decode: { json: number; rb: number; native: number };
  size: { json: number; rb: number; native: number };
  compressed: { json: number; rb: number; native: number };
}

async function runScenario(
  scenario: Scenario,
  iterations: number,
): Promise<ScenarioResult> {
  const rows = scenario.rowsArray.length;
  console.log(`=== ${scenario.name} (${scenario.description}) ===\n`);

  // Pre-encode
  const jsonEncoded = encodeJsonEachRow(scenario.jsonData);
  const rbEncoded = encodeRowBinary(scenario.columns, scenario.rowsArray);
  const nativeEncoded = encodeNativeRows(scenario.columns, scenario.rowsArray);

  const pct = (val: number, base: number) => ((val / base) * 100).toFixed(1);
  console.log(
    `  Encoded sizes: JSON=${jsonEncoded.length}, RowBinary=${rbEncoded.length} (${pct(rbEncoded.length, jsonEncoded.length)}%), Native=${nativeEncoded.length} (${pct(nativeEncoded.length, jsonEncoded.length)}%)\n`,
  );

  // Encoding
  console.log("Encoding:");
  const jsonEnc = bench(
    "JSONEachRow encode",
    () => encodeJsonEachRow(scenario.jsonData),
    50,
    iterations,
  );
  console.log(formatResult(jsonEnc, rows));
  const rbEnc = bench(
    "RowBinary encode",
    () => encodeRowBinary(scenario.columns, scenario.rowsArray),
    50,
    iterations,
  );
  console.log(formatResult(rbEnc, rows));
  const nativeEnc = bench(
    "Native encode",
    () => encodeNativeRows(scenario.columns, scenario.rowsArray),
    50,
    iterations,
  );
  console.log(formatResult(nativeEnc, rows));

  // Decoding
  console.log("\nDecoding:");
  const jsonDec = bench(
    "JSONEachRow decode",
    () => decodeJsonEachRow(jsonEncoded),
    50,
    iterations,
  );
  console.log(formatResult(jsonDec, rows));
  const rbDec = bench(
    "RowBinary decode",
    () => decodeRowBinary(rbEncoded),
    50,
    iterations,
  );
  console.log(formatResult(rbDec, rows));
  const nativeDec = await benchAsync(
    "Native decode",
    async () => {
      await decodeNative(nativeEncoded);
    },
    50,
    iterations,
  );
  console.log(formatResult(nativeDec, rows));

  // Compression
  const jsonComp = encodeBlock(jsonEncoded, Method.LZ4);
  const rbComp = encodeBlock(rbEncoded, Method.LZ4);
  const nativeComp = encodeBlock(nativeEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${jsonComp.length}, RowBinary+LZ4=${rbComp.length} (${pct(rbComp.length, jsonComp.length)}%), Native+LZ4=${nativeComp.length} (${pct(nativeComp.length, jsonComp.length)}%)`,
  );

  // Full path
  console.log("\nFull path (encode + LZ4 compress):");
  const jsonFull = bench(
    "JSONEachRow + LZ4",
    () => encodeBlock(encodeJsonEachRow(scenario.jsonData), Method.LZ4),
    50,
    iterations,
  );
  console.log(formatResult(jsonFull, rows));
  const rbFull = bench(
    "RowBinary + LZ4",
    () =>
      encodeBlock(
        encodeRowBinary(scenario.columns, scenario.rowsArray),
        Method.LZ4,
      ),
    50,
    iterations,
  );
  console.log(formatResult(rbFull, rows));
  const nativeFull = bench(
    "Native + LZ4",
    () =>
      encodeBlock(
        encodeNativeRows(scenario.columns, scenario.rowsArray),
        Method.LZ4,
      ),
    50,
    iterations,
  );
  console.log(formatResult(nativeFull, rows));

  console.log("");

  return {
    name: scenario.name,
    encode: { json: jsonEnc.ms, rb: rbEnc.ms, native: nativeEnc.ms },
    decode: { json: jsonDec.ms, rb: rbDec.ms, native: nativeDec.ms },
    size: {
      json: jsonEncoded.length,
      rb: rbEncoded.length,
      native: nativeEncoded.length,
    },
    compressed: {
      json: jsonComp.length,
      rb: rbComp.length,
      native: nativeComp.length,
    },
  };
}

// --- Data generators ---

function generateSimpleData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "name", type: "String" },
    { name: "email", type: "String" },
    { name: "active", type: "Bool" },
    { name: "score", type: "Float64" },
    { name: "created_at", type: "DateTime" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    const created_at = new Date("2024-01-15T10:30:00Z");
    json.push({
      id: i,
      name: `user_${i}`,
      email: `user${i}@example.com`,
      active: i % 2 === 0,
      score: Math.random() * 100,
      created_at,
    });
    rows.push([
      i,
      `user_${i}`,
      `user${i}@example.com`,
      i % 2 === 0,
      Math.random() * 100,
      created_at,
    ]);
  }
  return { json, rows, columns };
}

function generateEscapeData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "name", type: "String" },
    { name: "desc", type: "String" },
    { name: "path", type: "String" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    json.push({
      id: i,
      name: `user "test" ${i}`,
      desc: `Line1\nLine2\tTabbed`,
      path: `C:\\Users\\test\\file${i}.txt`,
    });
    rows.push([
      i,
      `user "test" ${i}`,
      `Line1\nLine2\tTabbed`,
      `C:\\Users\\test\\file${i}.txt`,
    ]);
  }
  return { json, rows, columns };
}

function generateComplexData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "tags", type: "Array(String)" },
    { name: "scores", type: "Array(Float64)" },
    { name: "metadata", type: "Nullable(String)" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    const tags = [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`];
    const scores = Array.from({ length: 50 }, () => Math.random() * 100);
    const metadata = i % 3 === 0 ? null : `meta_${i}`;
    json.push({ id: i, tags, scores, metadata });
    rows.push([i, tags, scores, metadata]);
  }
  return { json, rows, columns };
}

function generateComplexTypedData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "tags", type: "Array(String)" },
    { name: "scores", type: "Array(Float64)" },
    { name: "metadata", type: "Nullable(String)" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    const tags = [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`];
    const scores = new Float64Array(
      Array.from({ length: 50 }, () => Math.random() * 100),
    );
    const metadata = i % 3 === 0 ? null : `meta_${i}`;
    json.push({ id: i, tags, scores, metadata });
    rows.push([i, tags, scores, metadata]);
  }
  return { json, rows, columns };
}

function generateColumnarNumericData(count: number) {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "x", type: "Float64" },
    { name: "y", type: "Float64" },
    { name: "z", type: "Float64" },
  ];

  // Columnar data as TypedArrays
  const ids = new Uint32Array(count);
  const xs = new Float64Array(count);
  const ys = new Float64Array(count);
  const zs = new Float64Array(count);

  for (let i = 0; i < count; i++) {
    ids[i] = i;
    xs[i] = Math.random();
    ys[i] = Math.random();
    zs[i] = Math.random();
  }

  // Row-oriented for comparison
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    rows.push([ids[i], xs[i], ys[i], zs[i]]);
  }

  return { columns, rows, columnar: [ids, xs, ys, zs] as unknown[][] };
}

// --- Main ---

async function main() {
  await init();

  const ROWS = 10_000;
  const ITERATIONS = 50;

  console.log(
    `Benchmarking with ${ROWS} rows, ${ITERATIONS} iterations each\n`,
  );

  // Generate all test data
  const simple = generateSimpleData(ROWS);
  const escape = generateEscapeData(ROWS);
  const complex = generateComplexData(ROWS);
  const complexTyped = generateComplexTypedData(ROWS);

  const scenarios: Scenario[] = [
    {
      name: "Simple Data",
      description: "6 columns: int, 2 strings, bool, float, datetime",
      columns: simple.columns,
      jsonData: simple.json,
      rowsArray: simple.rows,
    },
    {
      name: "Escape Data",
      description: "strings with quotes, newlines, backslashes",
      columns: escape.columns,
      jsonData: escape.json,
      rowsArray: escape.rows,
    },
    {
      name: "Complex Data",
      description: "arrays, nullable",
      columns: complex.columns,
      jsonData: complex.json,
      rowsArray: complex.rows,
    },
    {
      name: "Complex Data (Typed)",
      description: "arrays as TypedArrays",
      columns: complexTyped.columns,
      jsonData: complexTyped.json,
      rowsArray: complexTyped.rows,
    },
  ];

  const results: ScenarioResult[] = [];
  for (const scenario of scenarios) {
    results.push(await runScenario(scenario, ITERATIONS));
  }

  // Summary
  console.log("=== Summary (speedup vs JSON) ===\n");
  const fmtSpeed = (json: number, rb: number, native: number) =>
    `RB ${(json / rb).toFixed(2)}x, Native ${(json / native).toFixed(2)}x`;
  const fmtSize = (json: number, rb: number, native: number) =>
    `RB ${(json / rb).toFixed(2)}x, Native ${(json / native).toFixed(2)}x smaller`;

  for (const r of results) {
    console.log(`${r.name}:`);
    console.log(
      `  Encode: ${fmtSpeed(r.encode.json, r.encode.rb, r.encode.native)}`,
    );
    console.log(
      `  Decode: ${fmtSpeed(r.decode.json, r.decode.rb, r.decode.native)}`,
    );
    console.log(`  Size:   ${fmtSize(r.size.json, r.size.rb, r.size.native)}`);
    console.log(
      `  +LZ4:   ${fmtSize(r.compressed.json, r.compressed.rb, r.compressed.native)}`,
    );
    console.log("");
  }

  // Streaming benchmarks (only for simple data)
  console.log("=== Streaming vs Sync (Simple Data) ===\n");
  const simpleRbEncoded = encodeRowBinary(simple.columns, simple.rows);

  console.log("Decoding (sync vs streaming):");
  const syncDec = bench(
    "Sync decode",
    () => decodeRowBinary(simpleRbEncoded),
    50,
    ITERATIONS,
  );
  console.log(formatResult(syncDec, ROWS));

  const stream1 = await benchAsync(
    "Stream decode (1 chunk)",
    async () => {
      await collectRowBinary(
        chunkedStream(simpleRbEncoded, simpleRbEncoded.length),
      );
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(stream1, ROWS));

  const stream64k = await benchAsync(
    "Stream decode (64KB chunks)",
    async () => {
      await collectRowBinary(chunkedStream(simpleRbEncoded, 64 * 1024));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(stream64k, ROWS));

  const stream4k = await benchAsync(
    "Stream decode (4KB chunks)",
    async () => {
      await collectRowBinary(chunkedStream(simpleRbEncoded, 4 * 1024));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(stream4k, ROWS));

  console.log("\nEncoding (sync vs streaming):");
  const syncEnc = bench(
    "Sync encode",
    () => encodeRowBinary(simple.columns, simple.rows),
    50,
    ITERATIONS,
  );
  console.log(formatResult(syncEnc, ROWS));

  const streamEnc = await benchAsync(
    "Stream encode",
    async () => {
      await collectChunks(streamEncodeRowBinary(simple.columns, simple.rows));
    },
    50,
    ITERATIONS,
  );
  console.log(formatResult(streamEnc, ROWS));

  console.log("\nStreaming overhead:");
  console.log(
    `  Decode (1 chunk): ${((stream1.ms / syncDec.ms - 1) * 100).toFixed(1)}% overhead`,
  );
  console.log(
    `  Decode (64KB):    ${((stream64k.ms / syncDec.ms - 1) * 100).toFixed(1)}% overhead`,
  );
  console.log(
    `  Decode (4KB):     ${((stream4k.ms / syncDec.ms - 1) * 100).toFixed(1)}% overhead`,
  );
  console.log(
    `  Encode:           ${((streamEnc.ms / syncEnc.ms - 1) * 100).toFixed(1)}% overhead`,
  );

  // Columnar TypedArray benchmarks
  console.log("\n=== Native Columnar vs Row-based (numeric data) ===\n");
  const columnar = generateColumnarNumericData(ROWS);

  console.log("Native encode (row-based vs columnar TypedArray):");
  const nativeRowEnc = bench(
    "Native (row input)",
    () => encodeNativeRows(columnar.columns, columnar.rows),
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeRowEnc, ROWS));
  const nativeColEnc = bench(
    "Native (TypedArray columnar)",
    () => encodeNative(Table.fromColumnar(columnar.columns, columnar.columnar)),
    50,
    ITERATIONS,
  );
  console.log(formatResult(nativeColEnc, ROWS));

  console.log(
    `\nSpeedup: ${(nativeRowEnc.ms / nativeColEnc.ms).toFixed(2)}x faster with TypedArray columnar input`,
  );
}

main().catch(console.error);
