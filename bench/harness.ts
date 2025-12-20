import os from "node:os";

export type BenchOptions = {
  warmup?: number;
  iterations?: number;
  batchSize?: number;
};

export type BenchStats = {
  name: string;
  samplesMs: number[];
  meanMs: number;
  medianMs: number;
  p95Ms: number;
  stdevMs: number;
  minMs: number;
  maxMs: number;
  warmup: number;
  iterations: number;
  batchSize: number;
};

export function readBenchOptions(defaults: BenchOptions = {}): BenchOptions {
  const warmup = parseInt(process.env.BENCH_WARMUP ?? "", 10);
  const iterations = parseInt(process.env.BENCH_ITERATIONS ?? "", 10);
  const batchSize = parseInt(process.env.BENCH_BATCH ?? "", 10);
  return {
    warmup: Number.isFinite(warmup) ? warmup : defaults.warmup,
    iterations: Number.isFinite(iterations) ? iterations : defaults.iterations,
    batchSize: Number.isFinite(batchSize) ? batchSize : defaults.batchSize,
  };
}

export function reportEnvironment(): void {
  const cpus = os.cpus();
  const cpuName = cpus[0]?.model?.trim() ?? "unknown";
  const flags = process.execArgv.length ? ` (${process.execArgv.join(" ")})` : "";
  console.log(`Node: ${process.version} ${process.platform}/${process.arch}${flags}`);
  console.log(`CPU:  ${cpuName} (${cpus.length} cores)`);
}

export function benchSync(
  name: string,
  fn: () => void,
  options: BenchOptions = {},
): BenchStats {
  const warmup = options.warmup ?? 20;
  const iterations = options.iterations ?? 50;
  const batchSize = options.batchSize ?? 1;

  for (let i = 0; i < warmup; i++) {
    for (let b = 0; b < batchSize; b++) fn();
  }

  const samples: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    for (let b = 0; b < batchSize; b++) fn();
    const elapsed = performance.now() - start;
    samples.push(elapsed / batchSize);
  }

  return summarize(name, samples, { warmup, iterations, batchSize });
}

export async function benchAsync(
  name: string,
  fn: () => Promise<void>,
  options: BenchOptions = {},
): Promise<BenchStats> {
  const warmup = options.warmup ?? 20;
  const iterations = options.iterations ?? 50;
  const batchSize = options.batchSize ?? 1;

  for (let i = 0; i < warmup; i++) {
    for (let b = 0; b < batchSize; b++) await fn();
  }

  const samples: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    for (let b = 0; b < batchSize; b++) await fn();
    const elapsed = performance.now() - start;
    samples.push(elapsed / batchSize);
  }

  return summarize(name, samples, { warmup, iterations, batchSize });
}

export function formatStats(stats: BenchStats): string {
  return `${stats.name.padEnd(30)} ${stats.meanMs.toFixed(3).padStart(8)}ms  p95 ${stats.p95Ms.toFixed(3)}ms`;
}

function summarize(
  name: string,
  samplesMs: number[],
  meta: { warmup: number; iterations: number; batchSize: number },
): BenchStats {
  const sorted = [...samplesMs].sort((a, b) => a - b);
  const meanMs = samplesMs.reduce((a, b) => a + b, 0) / samplesMs.length;
  const medianMs = percentile(sorted, 0.5);
  const p95Ms = percentile(sorted, 0.95);
  const minMs = sorted[0] ?? 0;
  const maxMs = sorted[sorted.length - 1] ?? 0;
  const stdevMs = Math.sqrt(
    samplesMs.reduce((sum, x) => sum + (x - meanMs) ** 2, 0) / samplesMs.length,
  );

  return {
    name,
    samplesMs,
    meanMs,
    medianMs,
    p95Ms,
    stdevMs,
    minMs,
    maxMs,
    warmup: meta.warmup,
    iterations: meta.iterations,
    batchSize: meta.batchSize,
  };
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const weight = idx - lo;
  return sorted[lo] * (1 - weight) + sorted[hi] * weight;
}
