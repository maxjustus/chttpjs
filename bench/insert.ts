import { createClient } from "@clickhouse/client";
import { gzipSync } from "node:zlib";
import { insert, init } from "../client.ts";

const encoder = new TextEncoder();

const BASE_URL = "http://localhost:8123";

interface Dataset {
  name: string;
  rows: Record<string, unknown>[];
  description: string;
}

function generateDatasets(): Dataset[] {
  const datasets: Dataset[] = [];

  // Identical JSON rows (low entropy - best case for compression)
  const repeatedRows = [];
  for (let i = 0; i < 1000000; i++) {
    repeatedRows.push({ id: 1, name: "test", value: 100 });
  }
  datasets.push({
    name: "json-repeat",
    rows: repeatedRows,
    description: "Identical JSON rows (low entropy)",
  });

  // Varied JSON rows (medium entropy - typical case)
  const variedRows = [];
  for (let i = 0; i < 1000000; i++) {
    variedRows.push({
      id: i,
      timestamp: Date.now(),
      user_id: `user_${i % 1000}`,
      event_type: ["click", "view", "purchase", "signup"][i % 4],
      metadata: JSON.stringify({ page: `/page/${i % 100}`, duration: Math.random() * 1000 }),
    });
  }
  datasets.push({
    name: "json-varied",
    rows: variedRows,
    description: "Varied JSON rows (medium entropy)",
  });

  // Log-like data
  const logRows = [];
  const levels = ["INFO", "DEBUG", "WARN", "ERROR"];
  const messages = [
    "Request processed successfully",
    "Database connection established",
    "Cache miss for key",
    "User authentication failed",
    "File not found",
  ];
  for (let i = 0; i < 1000000; i++) {
    const ts = new Date(Date.now() + i * 1000).toISOString();
    logRows.push({
      timestamp: ts,
      level: levels[i % 4],
      message: `${messages[i % 5]} id=${i}`,
    });
  }
  datasets.push({
    name: "logs",
    rows: logRows,
    description: "Log lines with timestamps",
  });

  return datasets;
}

// Official ClickHouse client instances
const officialClientGzip = createClient({
  url: BASE_URL,
  compression: { request: true, response: false },
});

const officialClientPlain = createClient({
  url: BASE_URL,
  compression: { request: false, response: false },
});

interface BenchResult {
  method: string;
  timeMs: number;
  rowsPerSec: number;
  mbPerSec: number;
  transferred: number;
  cpuUserMs: number;
  cpuSystemMs: number;
  cpuTotalMs: number;
}

type InsertFn = (query: string, data: Record<string, unknown>[]) => Promise<number>;

async function benchMethod(
  name: string,
  insertFn: InsertFn,
  query: string,
  data: Record<string, unknown>[],
  rawSize: number,
  iterations: number
): Promise<BenchResult> {
  // Warmup
  await insertFn(query, data);

  // Benchmark with CPU measurement
  const cpuStart = process.cpuUsage();
  const start = performance.now();
  let transferred = 0;
  for (let i = 0; i < iterations; i++) {
    transferred = await insertFn(query, data);
  }
  const elapsed = (performance.now() - start) / iterations;
  const cpuEnd = process.cpuUsage(cpuStart);

  // Convert microseconds to milliseconds, divide by iterations
  const cpuUserMs = cpuEnd.user / 1000 / iterations;
  const cpuSystemMs = cpuEnd.system / 1000 / iterations;

  return {
    method: name,
    timeMs: elapsed,
    rowsPerSec: (data.length / elapsed) * 1000,
    mbPerSec: (rawSize / 1024 / 1024 / elapsed) * 1000,
    transferred,
    cpuUserMs,
    cpuSystemMs,
    cpuTotalMs: cpuUserMs + cpuSystemMs,
  };
}

async function setupTable(): Promise<void> {
  await fetch(`${BASE_URL}`, {
    method: "POST",
    body: "DROP TABLE IF EXISTS bench_insert",
  });

  await fetch(`${BASE_URL}`, {
    method: "POST",
    body: `CREATE TABLE bench_insert (
      id Int64,
      timestamp String,
      user_id String,
      event_type String,
      metadata String,
      level String,
      message String,
      name String,
      value Int64
    ) ENGINE = Null`,
  });
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

async function main() {
  await init();

  const iterations = 5;
  const datasets = generateDatasets();
  const sessionId = Date.now().toString();

  console.log("Insert Benchmark: chttp vs @clickhouse/client");
  console.log(`Target: ${BASE_URL}`);
  console.log(`Iterations: ${iterations}\n`);

  // Check ClickHouse is available
  try {
    const resp = await fetch(`${BASE_URL}?query=SELECT%201`);
    if (!resp.ok) throw new Error("ClickHouse not responding");
  } catch {
    console.error("ERROR: ClickHouse not available at localhost:8123");
    console.error("Start ClickHouse first: docker run -d -p 8123:8123 clickhouse/clickhouse-server");
    process.exit(1);
  }

  await setupTable();

  const query = "INSERT INTO bench_insert FORMAT JSONEachRow";

  for (const dataset of datasets) {
    const rawBody = dataset.rows.map((d) => JSON.stringify(d)).join("\n") + "\n";
    const rawSize = encoder.encode(rawBody).length;

    console.log(`\n${"=".repeat(75)}`);
    console.log(`${dataset.name.toUpperCase()}: ${dataset.description}`);
    console.log(`Rows: ${dataset.rows.length.toLocaleString()}, Raw size: ${formatBytes(rawSize)}`);
    console.log("=".repeat(75));
    console.log("Method          Time(ms)    Rows/sec      MB/sec    CPU(ms)    Transferred");
    console.log("-".repeat(80));

    const bufferSizes = [
      { size: 128 * 1024, label: "128KB" },
      { size: 256 * 1024, label: "256KB" },
      { size: 512 * 1024, label: "512KB" },
      { size: 1 * 1024 * 1024, label: "1MB" },
      { size: 2 * 1024 * 1024, label: "2MB" },
    ];

    const methods: { name: string; fn: InsertFn }[] = [];

    // chttp methods with different buffer sizes
    for (const { size, label } of bufferSizes) {
      methods.push(
        {
          name: `chttp lz4 ${label}`,
          fn: async (q, d) => {
            await insert(q, d, sessionId, { baseUrl: BASE_URL + "/", compression: "lz4", bufferSize: size });
            return 0; // We don't track transferred bytes for chttp easily
          },
        },
        {
          name: `chttp zstd ${label}`,
          fn: async (q, d) => {
            await insert(q, d, sessionId, { baseUrl: BASE_URL + "/", compression: "zstd", bufferSize: size });
            return 0;
          },
        },
        {
          name: `chttp none ${label}`,
          fn: async (q, d) => {
            await insert(q, d, sessionId, { baseUrl: BASE_URL + "/", compression: "none", bufferSize: size });
            return 0;
          },
        }
      );
    }

    // Official client methods (no buffer size option)
    methods.push(
      {
        name: "official gzip",
        fn: async (_q, d) => {
          await officialClientGzip.insert({
            table: "bench_insert",
            values: d,
            format: "JSONEachRow",
          });
          return 0;
        },
      },
      {
        name: "official plain",
        fn: async (_q, d) => {
          await officialClientPlain.insert({
            table: "bench_insert",
            values: d,
            format: "JSONEachRow",
          });
          return 0;
        },
      }
    );

    // Direct fetch with gzip level 1 (fastest compression)
    methods.push({
      name: "fetch gzip L1",
      fn: async (q, d) => {
        const body = d.map((row) => JSON.stringify(row)).join("\n") + "\n";
        const compressed = gzipSync(body, { level: 1 });
        const url = `${BASE_URL}/?query=${encodeURIComponent(q)}`;
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Encoding": "gzip",
            "Content-Type": "application/octet-stream",
          },
          body: compressed,
        });
        await response.text();
        return compressed.length;
      },
    });

    for (const { name, fn } of methods) {
      const result = await benchMethod(name, fn, query, dataset.rows, rawSize, iterations);
      const transferred = result.transferred > 0 ? formatBytes(result.transferred) : "n/a";
      console.log(
        `${result.method.padEnd(14)}  ${result.timeMs.toFixed(1).padStart(8)}  ${result.rowsPerSec.toFixed(0).padStart(10)}  ${result.mbPerSec.toFixed(2).padStart(10)}  ${result.cpuTotalMs.toFixed(1).padStart(9)}  ${transferred.padStart(12)}`
      );
    }
  }

  // Cleanup
  await fetch(`${BASE_URL}`, {
    method: "POST",
    body: "DROP TABLE IF EXISTS bench_insert",
  });

  await officialClientGzip.close();
  await officialClientPlain.close();
}

main().catch(console.error);
