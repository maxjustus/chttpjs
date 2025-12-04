import { gzip, gunzip, deflate, inflate } from "node:zlib";
import { promisify } from "node:util";
import { compressFrame, decompressFrame } from "lz4-napi";
import { compress as zstdNativeCompress, decompress as zstdNativeDecompress } from "zstd-napi";
import * as bokuweb from "@bokuweb/zstd-wasm";
import * as lichtblick from "@lichtblick/wasm-zstd";
import { Zstd as HpccZstd } from "@hpcc-js/wasm-zstd";
import { init, encodeBlock, decodeBlock, Method } from "../compression.ts";

const gzipAsync = promisify(gzip);
const gunzipAsync = promisify(gunzip);
const deflateAsync = promisify(deflate);
const inflateAsync = promisify(inflate);

const encoder = new TextEncoder();

function generateTestData(rows: number): Uint8Array {
  const data = [];
  for (let i = 0; i < rows; i++) {
    data.push(JSON.stringify({
      id: i,
      timestamp: Date.now(),
      user_id: `user_${i % 1000}`,
      event_type: ["click", "view", "purchase", "signup"][i % 4],
      metadata: { page: `/page/${i % 100}`, duration: Math.random() * 1000 },
    }));
  }
  return encoder.encode(data.join("\n") + "\n");
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
): Promise<BenchResult> {
  // Warmup
  const warmupCompressed = await compress(data);
  await decompress(warmupCompressed);

  // Benchmark compress
  const compressStart = performance.now();
  let compressed: Uint8Array = new Uint8Array(0);
  for (let i = 0; i < iterations; i++) {
    compressed = await compress(data);
  }
  const compressMs = (performance.now() - compressStart) / iterations;

  // Benchmark decompress
  const decompressStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    await decompress(compressed);
  }
  const decompressMs = (performance.now() - decompressStart) / iterations;

  return {
    method: name,
    compressMs,
    decompressMs,
    ratio: data.length / compressed.length,
    compressedSize: compressed.length,
  };
}

async function main() {
  await init();
  await bokuweb.init();
  await lichtblick.isLoaded;
  const hpcc = await HpccZstd.load();

  const rows = 10000;
  const iterations = 10;
  const data = generateTestData(rows);

  console.log(`Benchmark: ${rows} JSON rows, ${data.length} bytes, ${iterations} iterations\n`);

  const results: BenchResult[] = [];

  // LZ4 (WASM)
  results.push(await benchMethod(
    "LZ4 wasm",
    data,
    async (d) => encodeBlock(d, Method.LZ4),
    async (d) => decodeBlock(d, true),
    iterations,
  ));

  // LZ4 (native)
  results.push(await benchMethod(
    "LZ4 native",
    data,
    async (d) => new Uint8Array(await compressFrame(Buffer.from(d))),
    async (d) => new Uint8Array(await decompressFrame(Buffer.from(d))),
    iterations,
  ));

  // ZSTD @dweb-browser (current)
  results.push(await benchMethod(
    "ZSTD dweb",
    data,
    async (d) => encodeBlock(d, Method.ZSTD),
    async (d) => decodeBlock(d, true),
    iterations,
  ));

  // ZSTD @bokuweb
  results.push(await benchMethod(
    "ZSTD bokuweb",
    data,
    async (d) => bokuweb.compress(d, 3),
    async (d) => bokuweb.decompress(d),
    iterations,
  ));

  // ZSTD @lichtblick (Facebook official)
  const dataLen = data.length;
  results.push(await benchMethod(
    "ZSTD lichtblk",
    data,
    async (d) => new Uint8Array(lichtblick.compress(d, 3)),
    async (d) => new Uint8Array(lichtblick.decompress(d, dataLen)),
    iterations,
  ));

  // ZSTD @hpcc-js
  results.push(await benchMethod(
    "ZSTD hpcc",
    data,
    async (d) => hpcc.compress(d, 3),
    async (d) => hpcc.decompress(d),
    iterations,
  ));

  // ZSTD (native)
  results.push(await benchMethod(
    "ZSTD native",
    data,
    async (d) => new Uint8Array(zstdNativeCompress(d)),
    async (d) => new Uint8Array(zstdNativeDecompress(d)),
    iterations,
  ));

  // gzip
  results.push(await benchMethod(
    "gzip",
    data,
    async (d) => new Uint8Array(await gzipAsync(d)),
    async (d) => new Uint8Array(await gunzipAsync(d)),
    iterations,
  ));

  // deflate
  results.push(await benchMethod(
    "deflate",
    data,
    async (d) => new Uint8Array(await deflateAsync(d)),
    async (d) => new Uint8Array(await inflateAsync(d)),
    iterations,
  ));

  // Print results
  console.log("Method         Compress(ms)  Decompress(ms)  Ratio   Size");
  console.log("------         ------------  --------------  -----   ----");
  for (const r of results) {
    console.log(
      `${r.method.padEnd(14)} ${r.compressMs.toFixed(2).padStart(12)}  ${r.decompressMs.toFixed(2).padStart(14)}  ${r.ratio.toFixed(2).padStart(5)}x  ${r.compressedSize}`,
    );
  }
}

main().catch(console.error);
