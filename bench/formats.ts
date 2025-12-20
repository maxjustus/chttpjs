// Benchmark: Native vs JSONEachRow
//
// Tests encoding/decoding performance for both formats with various data types.

import { init, encodeBlock, Method } from "../compression.ts";
import {
  encodeNative,
  streamEncodeNative,
  streamDecodeNative,
  batchFromRows,
  RecordBatch,
  type ColumnDef,
} from "../native/index.ts";
import { benchSync, benchAsync, readBenchOptions, reportEnvironment, type BenchOptions } from "./harness.ts";

function encodeNativeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  return encodeNative(batchFromRows(columns, rows));
}

async function* toAsync(data: Uint8Array[]): AsyncIterable<Uint8Array> {
  for (const chunk of data) yield chunk;
}

async function decodeBatch(data: Uint8Array): Promise<RecordBatch> {
  for await (const batch of streamDecodeNative(toAsync([data]))) {
    return batch;
  }
  return RecordBatch.from({ columns: [], columnData: [], rowCount: 0 });
}

// --- Benchmark infrastructure ---

function formatResult(stats: { name: string; meanMs: number }, rows: number): string {
  const rowsPerSec = rows / (stats.meanMs / 1000);
  return `  ${stats.name.padEnd(30)} ${stats.meanMs.toFixed(3).padStart(8)}ms  ${(rowsPerSec / 1_000_000).toFixed(2).padStart(6)}M rows/sec`;
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

async function collectNative(
  chunks: AsyncIterable<Uint8Array>,
): Promise<RecordBatch> {
  const blocks: RecordBatch[] = [];
  for await (const block of streamDecodeNative(chunks)) {
    blocks.push(block);
  }
  if (blocks.length === 0) {
    return RecordBatch.from({ columns: [], columnData: [], rowCount: 0 });
  }
  if (blocks.length === 1) {
    return blocks[0];
  }
  // Return first block for benchmark
  return blocks[0];
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
  encode: { json: number; native: number };
  decode: { json: number; native: number };
  size: { json: number; native: number };
  compressed: { json: number; native: number };
}

async function runScenario(
  scenario: Scenario,
  iterations: number,
  benchOptions: BenchOptions,
): Promise<ScenarioResult> {
  const rows = scenario.rowsArray.length;
  console.log(`=== ${scenario.name} (${scenario.description}) ===\n`);

  // Pre-encode
  const jsonEncoded = encodeJsonEachRow(scenario.jsonData);
  const nativeEncoded = encodeNativeRows(scenario.columns, scenario.rowsArray);

  const pct = (val: number, base: number) => ((val / base) * 100).toFixed(1);
  console.log(
    `  Encoded sizes: JSON=${jsonEncoded.length}, Native=${nativeEncoded.length} (${pct(nativeEncoded.length, jsonEncoded.length)}%)\n`,
  );

  // Encoding
  console.log("Encoding:");
  const jsonEnc = benchSync(
    "JSONEachRow encode",
    () => encodeJsonEachRow(scenario.jsonData),
    { ...benchOptions, iterations },
  );
  console.log(formatResult(jsonEnc, rows));
  const nativeEnc = benchSync(
    "Native encode",
    () => encodeNativeRows(scenario.columns, scenario.rowsArray),
    { ...benchOptions, iterations },
  );
  console.log(formatResult(nativeEnc, rows));

  // Decoding
  console.log("\nDecoding:");
  const jsonDec = benchSync(
    "JSONEachRow decode",
    () => decodeJsonEachRow(jsonEncoded),
    { ...benchOptions, iterations },
  );
  console.log(formatResult(jsonDec, rows));
  const nativeDec = await benchAsync("Native decode", async () => {
    await decodeBatch(nativeEncoded);
  }, { ...benchOptions, iterations });
  console.log(formatResult(nativeDec, rows));

  // Compression
  const jsonComp = encodeBlock(jsonEncoded, Method.LZ4);
  const nativeComp = encodeBlock(nativeEncoded, Method.LZ4);
  console.log(
    `\nCompressed sizes: JSON+LZ4=${jsonComp.length}, Native+LZ4=${nativeComp.length} (${pct(nativeComp.length, jsonComp.length)}%)`,
  );

  // Full path
  console.log("\nFull path (encode + LZ4 compress):");
  const jsonFull = benchSync(
    "JSONEachRow + LZ4",
    () => encodeBlock(encodeJsonEachRow(scenario.jsonData), Method.LZ4),
    { ...benchOptions, iterations },
  );
  console.log(formatResult(jsonFull, rows));
  const nativeFull = benchSync(
    "Native + LZ4",
    () => encodeBlock(encodeNativeRows(scenario.columns, scenario.rowsArray), Method.LZ4),
    { ...benchOptions, iterations },
  );
  console.log(formatResult(nativeFull, rows));

  console.log("");

  return {
    name: scenario.name,
    encode: { json: jsonEnc.meanMs, native: nativeEnc.meanMs },
    decode: { json: jsonDec.meanMs, native: nativeDec.meanMs },
    size: {
      json: jsonEncoded.length,
      native: nativeEncoded.length,
    },
    compressed: {
      json: jsonComp.length,
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

function generateVariantData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "v", type: "Variant(String, Int64, Float64)" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    // Rotate through the variant types
    const variant =
      i % 3 === 0 ? [0, `str_${i}`] :
        i % 3 === 1 ? [1, BigInt(i * 100)] :
          [2, Math.random() * 100];
    // JSON representation uses the raw value
    const jsonVal = i % 3 === 0 ? `str_${i}` : i % 3 === 1 ? i * 100 : Math.random() * 100;
    json.push({ id: i, v: jsonVal });
    rows.push([i, variant]);
  }
  return { json, rows, columns };
}

function generateDynamicData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "d", type: "Dynamic" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    // Mix of types: string, bigint, float, bool
    const val =
      i % 4 === 0 ? `str_${i}` :
        i % 4 === 1 ? BigInt(i) :
          i % 4 === 2 ? Math.random() * 100 :
            i % 2 === 0;
    // JSON representation
    const jsonVal = typeof val === "bigint" ? Number(val) : val;
    json.push({ id: i, d: jsonVal });
    rows.push([i, val]);
  }
  return { json, rows, columns };
}

function generateJsonColumnData(count: number): {
  json: Record<string, unknown>[];
  rows: unknown[][];
  columns: ColumnDef[];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "data", type: "JSON" },
  ];
  const json: Record<string, unknown>[] = [];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    const obj = {
      name: `user_${i}`,
      score: Math.random() * 100,
      active: i % 2 === 0,
      ...(i % 3 === 0 ? { tags: [`tag_${i % 5}`, `cat_${i % 3}`] } : {}),
    };
    json.push({ id: i, data: obj });
    rows.push([i, obj]);
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

  reportEnvironment();
  const benchOptions = readBenchOptions({ iterations: 50, warmup: 20 });
  const ROWS = 10_000;
  const ITERATIONS = benchOptions.iterations ?? 50;

  console.log(
    `Benchmarking with ${ROWS} rows, ${ITERATIONS} iterations each\n`,
  );

  // Generate all test data
  const simple = generateSimpleData(ROWS);
  const escape = generateEscapeData(ROWS);
  const complex = generateComplexData(ROWS);
  const complexTyped = generateComplexTypedData(ROWS);
  const variant = generateVariantData(ROWS);
  const dynamic = generateDynamicData(ROWS);
  const jsonCol = generateJsonColumnData(ROWS);

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
    {
      name: "Variant",
      description: "Variant(String, Int64, Float64)",
      columns: variant.columns,
      jsonData: variant.json,
      rowsArray: variant.rows,
    },
    {
      name: "Dynamic",
      description: "Dynamic with mixed types",
      columns: dynamic.columns,
      jsonData: dynamic.json,
      rowsArray: dynamic.rows,
    },
    {
      name: "JSON Column",
      description: "JSON objects with varying keys",
      columns: jsonCol.columns,
      jsonData: jsonCol.json,
      rowsArray: jsonCol.rows,
    },
  ];

  const results: ScenarioResult[] = [];
  for (const scenario of scenarios) {
    results.push(await runScenario(scenario, ITERATIONS, benchOptions));
  }

  // Summary
  console.log("=== Summary (speedup vs JSON) ===\n");
  const fmtSpeed = (json: number, native: number) =>
    `Native ${(json / native).toFixed(2)}x`;
  const fmtSize = (json: number, native: number) =>
    `Native ${(json / native).toFixed(2)}x smaller`;

  for (const r of results) {
    console.log(`${r.name}:`);
    console.log(`  Encode: ${fmtSpeed(r.encode.json, r.encode.native)}`);
    console.log(`  Decode: ${fmtSpeed(r.decode.json, r.decode.native)}`);
    console.log(`  Size:   ${fmtSize(r.size.json, r.size.native)}`);
    console.log(`  +LZ4:   ${fmtSize(r.compressed.json, r.compressed.native)}`);
    console.log("");
  }

  // Streaming benchmarks for Native
  console.log("=== Native Streaming vs Sync (Simple Data) ===\n");
  const simpleNativeEncoded = encodeNativeRows(simple.columns, simple.rows);

  console.log("Decoding (sync vs streaming):");
  const syncDec = await benchAsync("Sync decode", async () => {
    await decodeBatch(simpleNativeEncoded);
  }, { ...benchOptions, iterations: ITERATIONS });
  console.log(formatResult(syncDec, ROWS));

  const stream1 = await benchAsync("Stream decode (1 chunk)", async () => {
    await collectNative(chunkedStream(simpleNativeEncoded, simpleNativeEncoded.length));
  }, { ...benchOptions, iterations: ITERATIONS });
  console.log(formatResult(stream1, ROWS));

  const stream64k = await benchAsync("Stream decode (64KB chunks)", async () => {
    await collectNative(chunkedStream(simpleNativeEncoded, 64 * 1024));
  }, { ...benchOptions, iterations: ITERATIONS });
  console.log(formatResult(stream64k, ROWS));

  const stream4k = await benchAsync("Stream decode (4KB chunks)", async () => {
    await collectNative(chunkedStream(simpleNativeEncoded, 4 * 1024));
  }, { ...benchOptions, iterations: ITERATIONS });
  console.log(formatResult(stream4k, ROWS));

  console.log("\nEncoding (sync vs streaming):");
  const syncEnc = benchSync(
    "Sync encode",
    () => encodeNativeRows(simple.columns, simple.rows),
    { ...benchOptions, iterations: ITERATIONS },
  );
  console.log(formatResult(syncEnc, ROWS));

  async function* batchGenerator() {
    yield batchFromRows(simple.columns, simple.rows);
  }

  const streamEnc = await benchAsync("Stream encode", async () => {
    await collectChunks(streamEncodeNative(batchGenerator()));
  }, { ...benchOptions, iterations: ITERATIONS });
  console.log(formatResult(streamEnc, ROWS));

  console.log("\nStreaming overhead:");
  console.log(
    `  Decode (1 chunk): ${((stream1.meanMs / syncDec.meanMs - 1) * 100).toFixed(1)}% overhead`,
  );
  console.log(
    `  Decode (64KB):    ${((stream64k.meanMs / syncDec.meanMs - 1) * 100).toFixed(1)}% overhead`,
  );
  console.log(
    `  Decode (4KB):     ${((stream4k.meanMs / syncDec.meanMs - 1) * 100).toFixed(1)}% overhead`,
  );
  console.log(
    `  Encode:           ${((streamEnc.meanMs / syncEnc.meanMs - 1) * 100).toFixed(1)}% overhead`,
  );

  // Columnar TypedArray benchmarks
  console.log("\n=== Native Columnar vs Row-based (numeric data) ===\n");
  const columnar = generateColumnarNumericData(ROWS);

  console.log("Native encode (row-based vs columnar TypedArray):");
  const nativeRowEnc = benchSync(
    "Native (row input)",
    () => encodeNativeRows(columnar.columns, columnar.rows),
    { ...benchOptions, iterations: ITERATIONS },
  );
  console.log(formatResult(nativeRowEnc, ROWS));
  const nativeColEnc = benchSync(
    "Native (TypedArray columnar)",
    () => encodeNative(RecordBatch.fromColumnar(columnar.columns, columnar.columnar)),
    { ...benchOptions, iterations: ITERATIONS },
  );
  console.log(formatResult(nativeColEnc, ROWS));

  console.log(
    `\nSpeedup: ${(nativeRowEnc.ms / nativeColEnc.ms).toFixed(2)}x faster with TypedArray columnar input`,
  );
}

main().catch(console.error);
