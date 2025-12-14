/**
 * Integration benchmark: Native vs RowBinary vs JSONEachRow against real ClickHouse
 *
 * Measures actual insert and query performance including server-side processing.
 */

import { init, insert, query, collectBytes } from "../client.ts";
import { encodeRowBinary, decodeRowBinary, type ColumnDef } from "../rowbinary.ts";
import { encodeNative, decodeNative } from "../native.ts";
import { startClickHouse, stopClickHouse } from "../test/setup.ts";

const TEXT_ENCODER = new TextEncoder();

const consume = async (s: AsyncIterable<Uint8Array>) => { for await (const _ of s) {} };

// Encode as JSONEachRow
function encodeJSONEachRow(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const lines = rows.map(row => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columns.length; i++) {
      obj[columns[i].name] = row[i];
    }
    return JSON.stringify(obj);
  });
  return TEXT_ENCODER.encode(lines.join("\n"));
}

// Generate test data
function generateRows(count: number): unknown[][] {
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    rows.push([
      i,
      `user_${i}_${Math.random().toString(36).slice(2, 10)}`,
      `user${i}@example.com`,
      Math.random() * 1000,
      Date.now() - Math.floor(Math.random() * 86400000),
    ]);
  }
  return rows;
}

const columns: ColumnDef[] = [
  { name: "id", type: "Int32" },
  { name: "name", type: "String" },
  { name: "email", type: "String" },
  { name: "score", type: "Float64" },
  { name: "created_at", type: "Int64" },
];

async function benchInsert(
  name: string,
  tableName: string,
  format: string,
  data: Uint8Array,
  sessionId: string,
  config: { baseUrl: string; auth: { username: string; password: string } },
  iterations: number,
): Promise<{ name: string; ms: number; throughputMBps: number }> {
  const times: number[] = [];

  for (let i = 0; i < iterations; i++) {
    // Truncate table between runs
    await consume(query(`TRUNCATE TABLE ${tableName}`, sessionId, config));

    const start = performance.now();
    await insert(`INSERT INTO ${tableName} FORMAT ${format}`, data, sessionId, config);
    times.push(performance.now() - start);
  }

  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const throughputMBps = (data.length / (1024 * 1024)) / (avg / 1000);

  return { name, ms: avg, throughputMBps };
}

async function benchQuery(
  name: string,
  tableName: string,
  format: string,
  sessionId: string,
  config: { baseUrl: string; auth: { username: string; password: string } },
  iterations: number,
): Promise<{ name: string; ms: number; throughputMBps: number; bytes: number }> {
  const times: number[] = [];
  let bytes = 0;

  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    const data = await collectBytes(query(`SELECT * FROM ${tableName} FORMAT ${format}`, sessionId, config));
    times.push(performance.now() - start);
    bytes = data.length;
  }

  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  const throughputMBps = (bytes / (1024 * 1024)) / (avg / 1000);

  return { name, ms: avg, throughputMBps, bytes };
}

async function main() {
  await init();
  const ch = await startClickHouse();
  const baseUrl = ch.url + "/";
  const auth = { username: ch.username, password: ch.password };
  const sessionId = "bench_" + Date.now();
  const config = { baseUrl, auth };

  const ROW_COUNTS = [10_000, 100_000, 500_000];
  const ITERATIONS = 5;

  console.log("=== Native vs RowBinary vs JSONEachRow Integration Benchmark ===\n");

  try {
    for (const rowCount of ROW_COUNTS) {
      console.log(`\n--- ${rowCount.toLocaleString()} rows ---\n`);

      // Generate data
      const rows = generateRows(rowCount);

      // Encode in all formats
      const rowBinaryData = encodeRowBinary(columns, rows);
      const nativeData = encodeNative(columns, rows);
      const jsonData = encodeJSONEachRow(columns, rows);

      const rbSizeMB = rowBinaryData.length / 1024 / 1024;
      const nativeSizeMB = nativeData.length / 1024 / 1024;
      const jsonSizeMB = jsonData.length / 1024 / 1024;

      console.log(`Encoded sizes: RowBinary=${rbSizeMB.toFixed(2)}MB, Native=${nativeSizeMB.toFixed(2)}MB, JSON=${jsonSizeMB.toFixed(2)}MB`);

      // Create tables
      const rbTable = `bench_rb_${rowCount}`;
      const nativeTable = `bench_native_${rowCount}`;
      const jsonTable = `bench_json_${rowCount}`;

      await consume(query(`DROP TABLE IF EXISTS ${rbTable}`, sessionId, config));
      await consume(query(`DROP TABLE IF EXISTS ${nativeTable}`, sessionId, config));
      await consume(query(`DROP TABLE IF EXISTS ${jsonTable}`, sessionId, config));
      await consume(query(`CREATE TABLE ${rbTable} (id Int32, name String, email String, score Float64, created_at Int64) ENGINE = MergeTree ORDER BY id`, sessionId, config));
      await consume(query(`CREATE TABLE ${nativeTable} (id Int32, name String, email String, score Float64, created_at Int64) ENGINE = MergeTree ORDER BY id`, sessionId, config));
      await consume(query(`CREATE TABLE ${jsonTable} (id Int32, name String, email String, score Float64, created_at Int64) ENGINE = MergeTree ORDER BY id`, sessionId, config));

      // Benchmark inserts
      console.log("\nInsert benchmarks:");
      console.log("  Format          Size(MB)    Time(ms)    Rows/s");
      console.log("  " + "-".repeat(50));

      const rbInsert = await benchInsert(
        "RowBinary",
        rbTable,
        "RowBinaryWithNamesAndTypes",
        rowBinaryData,
        sessionId,
        config,
        ITERATIONS,
      );
      const rbInsertRowsPerSec = Math.round(rowCount / (rbInsert.ms / 1000));
      console.log(`  ${rbInsert.name.padEnd(14)} ${rbSizeMB.toFixed(2).padStart(8)}    ${rbInsert.ms.toFixed(1).padStart(8)}    ${rbInsertRowsPerSec.toLocaleString().padStart(10)}`);

      const nativeInsert = await benchInsert(
        "Native",
        nativeTable,
        "Native",
        nativeData,
        sessionId,
        config,
        ITERATIONS,
      );
      const nativeInsertRowsPerSec = Math.round(rowCount / (nativeInsert.ms / 1000));
      console.log(`  ${nativeInsert.name.padEnd(14)} ${nativeSizeMB.toFixed(2).padStart(8)}    ${nativeInsert.ms.toFixed(1).padStart(8)}    ${nativeInsertRowsPerSec.toLocaleString().padStart(10)}`);

      const jsonInsert = await benchInsert(
        "JSONEachRow",
        jsonTable,
        "JSONEachRow",
        jsonData,
        sessionId,
        config,
        ITERATIONS,
      );
      const jsonInsertRowsPerSec = Math.round(rowCount / (jsonInsert.ms / 1000));
      console.log(`  ${jsonInsert.name.padEnd(14)} ${jsonSizeMB.toFixed(2).padStart(8)}    ${jsonInsert.ms.toFixed(1).padStart(8)}    ${jsonInsertRowsPerSec.toLocaleString().padStart(10)}`);

      // Insert data for query benchmarks
      await consume(query(`TRUNCATE TABLE ${rbTable}`, sessionId, config));
      await consume(query(`TRUNCATE TABLE ${nativeTable}`, sessionId, config));
      await consume(query(`TRUNCATE TABLE ${jsonTable}`, sessionId, config));
      await insert(`INSERT INTO ${rbTable} FORMAT RowBinaryWithNamesAndTypes`, rowBinaryData, sessionId, config);
      await insert(`INSERT INTO ${nativeTable} FORMAT Native`, nativeData, sessionId, config);
      await insert(`INSERT INTO ${jsonTable} FORMAT JSONEachRow`, jsonData, sessionId, config);

      // Benchmark queries
      console.log("\nQuery benchmarks:");
      console.log("  Format          Size(MB)    Time(ms)    Rows/s");
      console.log("  " + "-".repeat(50));

      const rbQuery = await benchQuery(
        "RowBinary",
        rbTable,
        "RowBinaryWithNamesAndTypes",
        sessionId,
        config,
        ITERATIONS,
      );
      const rbQueryRowsPerSec = Math.round(rowCount / (rbQuery.ms / 1000));
      console.log(`  ${rbQuery.name.padEnd(14)} ${(rbQuery.bytes/1024/1024).toFixed(2).padStart(8)}    ${rbQuery.ms.toFixed(1).padStart(8)}    ${rbQueryRowsPerSec.toLocaleString().padStart(10)}`);

      const nativeQuery = await benchQuery(
        "Native",
        nativeTable,
        "Native",
        sessionId,
        config,
        ITERATIONS,
      );
      const nativeQueryRowsPerSec = Math.round(rowCount / (nativeQuery.ms / 1000));
      console.log(`  ${nativeQuery.name.padEnd(14)} ${(nativeQuery.bytes/1024/1024).toFixed(2).padStart(8)}    ${nativeQuery.ms.toFixed(1).padStart(8)}    ${nativeQueryRowsPerSec.toLocaleString().padStart(10)}`);

      const jsonQuery = await benchQuery(
        "JSONEachRow",
        jsonTable,
        "JSONEachRow",
        sessionId,
        config,
        ITERATIONS,
      );
      const jsonQueryRowsPerSec = Math.round(rowCount / (jsonQuery.ms / 1000));
      console.log(`  ${jsonQuery.name.padEnd(14)} ${(jsonQuery.bytes/1024/1024).toFixed(2).padStart(8)}    ${jsonQuery.ms.toFixed(1).padStart(8)}    ${jsonQueryRowsPerSec.toLocaleString().padStart(10)}`);

      // Summary for this row count
      console.log("\nSummary (vs JSON baseline):");
      console.log(`  Insert: RowBinary ${(jsonInsert.ms / rbInsert.ms).toFixed(2)}x, Native ${(jsonInsert.ms / nativeInsert.ms).toFixed(2)}x faster than JSON`);
      console.log(`  Query:  RowBinary ${(jsonQuery.ms / rbQuery.ms).toFixed(2)}x, Native ${(jsonQuery.ms / nativeQuery.ms).toFixed(2)}x faster than JSON`);

      // Cleanup
      await consume(query(`DROP TABLE IF EXISTS ${rbTable}`, sessionId, config));
      await consume(query(`DROP TABLE IF EXISTS ${nativeTable}`, sessionId, config));
      await consume(query(`DROP TABLE IF EXISTS ${jsonTable}`, sessionId, config));
    }

  } finally {
    await stopClickHouse();
  }
}

main().catch(console.error);
