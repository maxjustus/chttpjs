#!/usr/bin/env node
/**
 * Profiling tool for Native/JSON formats.
 *
 * Usage: scripts/profile.ts [options]
 * Run with -h for help.
 */

import { parseArgs } from "node:util";
import {
  batchFromCols,
  batchFromRows,
  type ColumnDef,
  encodeNative,
  getCodec,
  RecordBatch,
  streamDecodeNative,
} from "../native/index.ts";

function encodeNativeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  return encodeNative(batchFromRows(columns, rows));
}

function buildColumnar(columns: ColumnDef[], columnarData: unknown[][]): RecordBatch {
  const cols: Record<string, ReturnType<ReturnType<typeof getCodec>["fromValues"]>> = {};
  for (let i = 0; i < columns.length; i++) {
    cols[columns[i].name] = getCodec(columns[i].type).fromValues(columnarData[i] as unknown[]);
  }
  return batchFromCols(cols);
}

async function* toAsync<T>(iter: Iterable<T>): AsyncIterable<T> {
  for (const item of iter) yield item;
}

const DATA_TYPES = [
  "mixed",
  "numeric",
  "strings",
  "complex",
  "full",
  "bench-complex",
  "variant",
  "dynamic",
  "json",
] as const;

const { values } = parseArgs({
  options: {
    format: { type: "string", short: "f", default: "native" },
    operation: { type: "string", short: "o", default: "decode" },
    data: { type: "string", short: "d", default: "mixed" },
    rows: { type: "string", short: "r", default: "10000" },
    iterations: { type: "string", short: "i", default: "500" },
    columnar: { type: "boolean", short: "c", default: false },
    help: { type: "boolean", short: "h", default: false },
  },
  allowPositionals: false,
});

if (values.help) {
  console.log(`
Profile format encode/decode performance.

Usage: scripts/profile.ts [options]

Options:
  -f, --format <fmt>      Format: native, json (default: native)
  -o, --operation <op>    Operation: encode, decode (default: decode)
  -d, --data <type>       Data type (default: mixed)
  -r, --rows <n>          Row count (default: 10000)
  -i, --iterations <n>    Iterations (default: 500)
  -c, --columnar          Skip row→column transpose (native encode only)
  -h, --help              Show this help

Data types:
  mixed         UInt32, String×2, Bool, Float64, DateTime
  numeric       UInt8/16/32, Int32, Float32/64
  strings       UInt32, String×3 (varying lengths)
  complex       Array, Nullable, Tuple, Map
  full          All codecs: numeric, string, date/time, UUID, IP, LowCardinality, etc.
  bench-complex Matches bench/formats.ts "Complex Data" (50-element float arrays)
  variant       Variant(String, Int64, Float64)
  dynamic       Dynamic with mixed types
  json          JSON objects with varying keys

Examples:
  scripts/profile.ts -f native -o encode -d complex
  scripts/profile.ts -f native -o encode -c    # columnar (skip transpose)
`);
  process.exit(0);
}

const format = values.format?.toLowerCase();
const operation = values.operation?.toLowerCase();
const dataType = values.data! as (typeof DATA_TYPES)[number];
const rowCount = parseInt(values.rows!, 10);
const iterations = parseInt(values.iterations!, 10);
const columnar = values.columnar!;

if (!["native", "json"].includes(format)) {
  console.error(`Unknown format: ${format}`);
  process.exit(1);
}
if (!["encode", "decode"].includes(operation)) {
  console.error(`Unknown operation: ${operation}`);
  process.exit(1);
}
if (!DATA_TYPES.includes(dataType)) {
  console.error(`Unknown data type: ${dataType}. Use: ${DATA_TYPES.join(", ")}`);
  process.exit(1);
}
if (columnar && (format !== "native" || operation !== "encode")) {
  console.error("--columnar only applies to native encode");
  process.exit(1);
}

// --- Schema-driven data generation ---

type Schema = { columns: ColumnDef[]; generate: (i: number) => unknown[] };

const SCHEMAS: Record<(typeof DATA_TYPES)[number], Schema> = {
  mixed: {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "name", type: "String" },
      { name: "email", type: "String" },
      { name: "active", type: "Bool" },
      { name: "score", type: "Float64" },
      { name: "created", type: "DateTime" },
    ],
    generate: (i) => [
      i,
      `user_${i}`,
      `user${i}@example.com`,
      i % 2 === 0,
      Math.random() * 100,
      Math.floor(Date.now() / 1000) - i * 60,
    ],
  },

  numeric: {
    columns: [
      { name: "a", type: "UInt8" },
      { name: "b", type: "UInt16" },
      { name: "c", type: "UInt32" },
      { name: "d", type: "Int32" },
      { name: "e", type: "Float32" },
      { name: "f", type: "Float64" },
    ],
    generate: (i) => [i % 256, i % 65536, i, i - 50000, Math.random(), Math.random() * 1000],
  },

  strings: {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "short", type: "String" },
      { name: "medium", type: "String" },
      { name: "long", type: "String" },
    ],
    generate: (i) => [
      i,
      `s${i}`,
      `medium_string_value_${i}`,
      `this_is_a_longer_string_with_more_content_${i}_end`,
    ],
  },

  complex: {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "tags", type: "Array(String)" },
      { name: "scores", type: "Array(Float64)" },
      { name: "meta", type: "Nullable(String)" },
      { name: "point", type: "Tuple(Float64, Float64)" },
      { name: "attrs", type: "Map(String, Int32)" },
    ],
    generate: (i) => [
      i,
      [`tag_${i % 5}`, `cat_${i % 3}`],
      [Math.random() * 10, Math.random() * 10, Math.random() * 10],
      i % 3 === 0 ? null : `meta_${i}`,
      [Math.random() * 180 - 90, Math.random() * 360 - 180],
      new Map([
        ["a", i],
        ["b", i * 2],
      ]),
    ],
  },

  full: {
    columns: [
      // Numeric
      { name: "u8", type: "UInt8" },
      { name: "u32", type: "UInt32" },
      { name: "i64", type: "Int64" },
      { name: "f64", type: "Float64" },
      // Big integers
      { name: "u128", type: "UInt128" },
      // Decimal
      { name: "dec", type: "Decimal64(2)" },
      // String types
      { name: "str", type: "String" },
      { name: "fstr", type: "FixedString(8)" },
      // Date/time
      { name: "dt", type: "DateTime" },
      { name: "dt64", type: "DateTime64(3)" },
      { name: "date", type: "Date" },
      // Special
      { name: "uuid", type: "UUID" },
      { name: "ipv4", type: "IPv4" },
      { name: "ipv6", type: "IPv6" },
      { name: "bool", type: "Bool" },
      // LowCardinality
      { name: "lc_str", type: "LowCardinality(String)" },
      // Composite
      { name: "arr", type: "Array(Int32)" },
      { name: "nullable", type: "Nullable(String)" },
      { name: "tup", type: "Tuple(Int32, String)" },
      { name: "map", type: "Map(String, Int32)" },
    ],
    generate: (i) => {
      const now = Date.now();
      return [
        // Numeric
        i % 256,
        i,
        BigInt(i) * 1000000n,
        Math.random() * 1000,
        // Big integers
        BigInt(i) * 10000000000000000n,
        // Decimal (as number, codec handles scaling)
        Math.round(Math.random() * 10000) / 100,
        // String types
        `value_${i}`,
        `fix${(i % 100000).toString().padStart(5, "0")}`,
        // Date/time
        Math.floor(now / 1000) - i * 60,
        now - i * 60000,
        Math.floor(now / 86400000) - (i % 1000),
        // Special
        `${hex(8)}-${hex(4)}-${hex(4)}-${hex(4)}-${hex(12)}`,
        `${i % 256}.${(i >> 8) % 256}.${(i >> 16) % 256}.${i % 256}`,
        `2001:db8::${(i % 65536).toString(16)}`,
        i % 2 === 0,
        // LowCardinality (repeated values)
        `category_${i % 10}`,
        // Composite
        [i, i + 1, i + 2],
        i % 4 === 0 ? null : `nullable_${i}`,
        [i, `tuple_${i}`],
        new Map([["key", i]]),
      ];
    },
  },

  // Matches bench/formats.ts generateComplexData exactly
  "bench-complex": {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "tags", type: "Array(String)" },
      { name: "scores", type: "Array(Float64)" },
      { name: "metadata", type: "Nullable(String)" },
    ],
    generate: (i) => [
      i,
      [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`],
      Array.from({ length: 50 }, () => Math.random() * 100),
      i % 3 === 0 ? null : `meta_${i}`,
    ],
  },

  variant: {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "v", type: "Variant(String, Int64, Float64)" },
    ],
    generate: (i) => [
      i,
      i % 3 === 0
        ? [0, `str_${i}`]
        : // String
          i % 3 === 1
          ? [1, BigInt(i * 100)]
          : // Int64
            [2, Math.random()], // Float64
    ],
  },

  dynamic: {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "d", type: "Dynamic" },
    ],
    generate: (i) => [
      i,
      i % 4 === 0
        ? `str_${i}`
        : i % 4 === 1
          ? BigInt(i)
          : i % 4 === 2
            ? Math.random() * 100
            : i % 2 === 0,
    ],
  },

  json: {
    columns: [
      { name: "id", type: "UInt32" },
      { name: "data", type: "JSON" },
    ],
    generate: (i) => [
      i,
      {
        name: `user_${i}`,
        score: Math.random() * 100,
        active: i % 2 === 0,
        ...(i % 3 === 0 ? { tags: [`tag_${i % 5}`] } : {}),
      },
    ],
  },
};

function hex(len: number): string {
  return Array.from({ length: len }, () => Math.floor(Math.random() * 16).toString(16)).join("");
}

type DataSet = {
  columns: ColumnDef[];
  rows: unknown[][];
  columnarData: unknown[][];
  objects: Record<string, unknown>[];
};

function generateData(type: (typeof DATA_TYPES)[number], count: number): DataSet {
  const schema = SCHEMAS[type];
  const { columns, generate } = schema;
  const rows: unknown[][] = [];
  const objects: Record<string, unknown>[] = [];

  for (let i = 0; i < count; i++) {
    const row = generate(i);
    rows.push(row);
    const obj: Record<string, unknown> = {};
    for (let j = 0; j < columns.length; j++) {
      let val = row[j];
      // JSON can't handle Map/BigInt
      if (val instanceof Map) val = Object.fromEntries(val);
      if (typeof val === "bigint") val = val.toString();
      obj[columns[j].name] = val;
    }
    objects.push(obj);
  }

  const columnarData: unknown[][] = columns.map((_, ci) => rows.map((r) => r[ci]));
  return { columns, rows, columnarData, objects };
}

// --- JSON helpers ---

const textEnc = new TextEncoder();
const textDec = new TextDecoder();
const encodeJson = (rows: Record<string, unknown>[]) =>
  textEnc.encode(rows.map((r) => JSON.stringify(r)).join("\n"));
const decodeJson = (data: Uint8Array) =>
  textDec
    .decode(data)
    .trim()
    .split("\n")
    .map((l) => JSON.parse(l));

// --- Main ---

async function main() {
  const mode = columnar ? "columnar" : "row";
  console.log(
    `Profiling: ${format} ${operation} [${dataType}, ${mode}] ${rowCount} rows, ${iterations} iters\n`,
  );

  const { columns, rows, columnarData, objects } = generateData(dataType, rowCount);

  // Pre-encode for decode
  const encNative = encodeNativeRows(columns, rows);
  const encJson = encodeJson(objects);

  const run = async () => {
    if (format === "native") {
      if (operation === "encode") {
        if (columnar) encodeNative(buildColumnar(columns, columnarData));
        else encodeNativeRows(columns, rows);
      } else await Array.fromAsync(streamDecodeNative(toAsync([encNative])));
    } else {
      if (operation === "encode") encodeJson(objects);
      else decodeJson(encJson);
    }
  };

  console.log("Warming up...");
  for (let i = 0; i < 50; i++) await run();

  console.log("Running profiled iterations...\n");
  const start = performance.now();
  for (let i = 0; i < iterations; i++) await run();
  const elapsed = performance.now() - start;

  const avgMs = elapsed / iterations;
  const rowsPerSec = rowCount / (avgMs / 1000);
  console.log(`Completed ${iterations} iterations in ${elapsed.toFixed(0)}ms`);
  console.log(`Average: ${avgMs.toFixed(3)}ms per iteration`);
  console.log(`Throughput: ${(rowsPerSec / 1_000_000).toFixed(2)}M rows/sec`);
}

main().catch(console.error);
