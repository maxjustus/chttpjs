import { gzip, gunzip, zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { promisify } from "node:util";
import { compressFrame, decompressFrame } from "lz4-napi";
import {
  compress as zstdNativeCompress,
  decompress as zstdNativeDecompress,
} from "zstd-napi";
import * as bokuweb from "@bokuweb/zstd-wasm";
import {
  init,
  encodeBlock,
  decodeBlock,
  Method,
  usingNativeZstd,
} from "../compression.ts";
import { benchAsync, readBenchOptions, reportEnvironment } from "./harness.ts";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);

const encoder = new TextEncoder();

interface TestDataSet {
  name: string;
  data: Uint8Array;
  description: string;
}

function generateTestDataSets(): TestDataSet[] {
  const datasets: TestDataSet[] = [];

  // 1. High entropy - random bytes (incompressible)
  const randomBytes = new Uint8Array(5_000_000);
  // getRandomValues has 64KB limit, fill in chunks
  for (let i = 0; i < randomBytes.length; i += 65536) {
    const chunk = randomBytes.subarray(
      i,
      Math.min(i + 65536, randomBytes.length),
    );
    crypto.getRandomValues(chunk);
  }
  datasets.push({
    name: "random",
    data: randomBytes,
    description: "Random bytes (high entropy)",
  });

  // 2. Very low entropy - repeated pattern
  const repeated = new Uint8Array(5_000_000);
  const pattern = encoder.encode("AAAAAAAAAA");
  for (let i = 0; i < repeated.length; i++) {
    repeated[i] = pattern[i % pattern.length];
  }
  datasets.push({
    name: "repeated",
    data: repeated,
    description: "Repeated 'A' pattern (very low entropy)",
  });

  // 3. Low entropy - repeated JSON objects
  const repeatedJson: string[] = [];
  const sameObj = JSON.stringify({ id: 1, name: "test", value: 100 });
  for (let i = 0; i < 50_000; i++) {
    repeatedJson.push(sameObj);
  }
  datasets.push({
    name: "json-repeat",
    data: encoder.encode(repeatedJson.join("\n")),
    description: "Identical JSON rows (low entropy)",
  });

  // 4. Medium entropy - typical JSON with variation
  const variedJson: string[] = [];
  for (let i = 0; i < 50_000; i++) {
    variedJson.push(
      JSON.stringify({
        id: i,
        timestamp: Date.now(),
        user_id: `user_${i % 1000}`,
        event_type: ["click", "view", "purchase", "signup"][i % 4],
        metadata: { page: `/page/${i % 100}`, duration: Math.random() * 1000 },
      }),
    );
  }
  datasets.push({
    name: "json-varied",
    data: encoder.encode(variedJson.join("\n")),
    description: "Varied JSON rows (medium entropy)",
  });

  // 5. Medium-high entropy - UUIDs
  const uuids: string[] = [];
  for (let i = 0; i < 100_000; i++) {
    uuids.push(crypto.randomUUID());
  }
  datasets.push({
    name: "uuids",
    data: encoder.encode(uuids.join("\n")),
    description: "UUIDs (medium-high entropy)",
  });

  // 6. Log-like data - timestamps + messages
  const logs: string[] = [];
  const levels = ["INFO", "DEBUG", "WARN", "ERROR"];
  const messages = [
    "Request processed successfully",
    "Database connection established",
    "Cache miss for key",
    "User authentication failed",
    "File not found",
  ];
  for (let i = 0; i < 50_000; i++) {
    const ts = new Date(Date.now() + i * 1000).toISOString();
    logs.push(`${ts} ${levels[i % 4]} ${messages[i % 5]} id=${i}`);
  }
  datasets.push({
    name: "logs",
    data: encoder.encode(logs.join("\n")),
    description: "Log lines with timestamps (medium entropy)",
  });

  return datasets;
}

interface BenchResult {
  method: string;
  compressMs: number;
  decompressMs: number;
  ratio: number;
  compressedSize: number;
}

async function benchMethod(
  name: string,
  data: Uint8Array,
  compress: (d: Uint8Array) => Promise<Uint8Array>,
  decompress: (d: Uint8Array) => Promise<Uint8Array>,
  iterations: number,
  warmup: number,
): Promise<BenchResult> {
  let compressed: Uint8Array = await compress(data);
  await decompress(compressed);

  const compressStats = await benchAsync(
    `${name} compress`,
    async () => {
      compressed = await compress(data);
    },
    { iterations, warmup },
  );

  const decompressStats = await benchAsync(
    `${name} decompress`,
    async () => {
      await decompress(compressed);
    },
    { iterations, warmup },
  );

  return {
    method: name,
    compressMs: compressStats.meanMs,
    decompressMs: decompressStats.meanMs,
    ratio: data.length / compressed.length,
    compressedSize: compressed.length,
  };
}

type CompressionMethod = {
  name: string;
  compress: (d: Uint8Array, origLen: number) => Promise<Uint8Array>;
  decompress: (d: Uint8Array, origLen: number) => Promise<Uint8Array>;
};

function getMethods(): CompressionMethod[] {
  return [
    {
      name: "LZ4 wasm",
      compress: async (d) => encodeBlock(d, Method.LZ4),
      decompress: async (d) => decodeBlock(d, true),
    },
    {
      name: "LZ4 native",
      compress: async (d) =>
        new Uint8Array(await compressFrame(Buffer.from(d))),
      decompress: async (d) =>
        new Uint8Array(await decompressFrame(Buffer.from(d))),
    },
    {
      name: "ZSTD chttp",
      compress: async (d) => encodeBlock(d, Method.ZSTD),
      decompress: async (d) => decodeBlock(d, true),
    },
    {
      name: "ZSTD wasm",
      compress: async (d) => bokuweb.compress(d, 3),
      decompress: async (d) => bokuweb.decompress(d),
    },
    {
      name: "ZSTD napi",
      compress: async (d) => new Uint8Array(zstdNativeCompress(d)),
      decompress: async (d) => new Uint8Array(zstdNativeDecompress(d)),
    },
    {
      name: "ZSTD node",
      compress: async (d) => new Uint8Array(zstdCompressSync(d, { level: 3 })),
      decompress: async (d) => new Uint8Array(zstdDecompressSync(d)),
    },
    {
      name: "gzip",
      compress: async (d) => new Uint8Array(await gzipAsync(d)),
      decompress: async (d) => new Uint8Array(await gunzipAsync(d)),
    },
  ];
}

async function main() {
  await init();
  await bokuweb.init();

  reportEnvironment();
  const benchOptions = readBenchOptions({ iterations: 5, warmup: 2 });
  const iterations = benchOptions.iterations ?? 5;
  const warmup = benchOptions.warmup ?? 2;
  const datasets = generateTestDataSets();
  const methods = getMethods();

  console.log(
    `ZSTD backend: ${usingNativeZstd ? "native (zstd-napi)" : "WASM (@bokuweb/zstd-wasm)"}`,
  );
  console.log(`Iterations: ${iterations}, Warmup: ${warmup}\n`);

  for (const dataset of datasets) {
    console.log(`\n${"=".repeat(70)}`);
    console.log(`${dataset.name.toUpperCase()}: ${dataset.description}`);
    console.log(`Size: ${(dataset.data.length / 1024).toFixed(1)} KB`);
    console.log("=".repeat(70));
    console.log("Method         Compress(ms)  Decompress(ms)  Ratio   Size");
    console.log("-".repeat(70));

    for (const method of methods) {
      const result = await benchMethod(
        method.name,
        dataset.data,
        (d) => method.compress(d, dataset.data.length),
        (d) => method.decompress(d, dataset.data.length),
        iterations,
        warmup,
      );
      console.log(
        `${result.method.padEnd(14)} ${result.compressMs.toFixed(2).padStart(12)}  ${result.decompressMs.toFixed(2).padStart(14)}  ${result.ratio.toFixed(2).padStart(5)}x  ${result.compressedSize}`,
      );
    }
  }
}

main().catch(console.error);
