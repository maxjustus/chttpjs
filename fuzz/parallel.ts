#!/usr/bin/env tsx
/**
 * Parallel fuzz test runner.
 *
 * Usage:
 *   tsx fuzz/parallel.ts [options]
 *
 * Options:
 *   --unit       Run unit tests
 *   --corruption Run byte-mutation decode tests
 *   --insert     Run insert() option tests
 *   --lz4        Run LZ4 frame decoder oracle tests
 *   --http       Run HTTP integration tests
 *   --tcp        Run TCP integration tests
 *   --generated  Run client-generated CH-anchored tests
 *   --all        Run all tests (default if none specified)
 *   --verbose    Stream test output in real-time
 *
 * Environment:
 *   FUZZ_ITERATIONS - number of iterations (default: 25)
 *   FUZZ_ROWS - row count for integration tests (default: 10000)
 *   FUZZ_TYPE_SOURCE - generated-suite type source: ch | local | mix (default: mix).
 *                      ch = CH generateRandomStructure; local = offline genType;
 *                      mix = per-seed choice of either.
 *   FUZZ_MEMORY_POLL_MS - child RSS sampling interval (default: 250, 0 disables).
 *   FUZZ_MEMORY_WARN_MB - print jobs whose peak RSS exceeds this many MiB (default: off).
 *
 * Examples:
 *   tsx fuzz/parallel.ts --all
 *   FUZZ_ITERATIONS=5 tsx fuzz/parallel.ts --http
 *   tsx fuzz/parallel.ts --tcp --verbose
 */

import { spawn, spawnSync } from "node:child_process";
import { startClickHouse, stopClickHouse } from "../test/setup.ts";
import { config } from "./config.ts";

interface Job {
  name: string;
  file: string;
  env: Record<string, string>;
}

interface JobResult {
  job: Job;
  success: boolean;
  duration: number;
  output: string;
  peakRssBytes: number | null;
}

type Suite = "unit" | "corruption" | "insert" | "lz4" | "http" | "tcp" | "generated";

/** Suites that run offline — no ClickHouse server, no compression matrix. */
const LOCAL_SUITES: ReadonlySet<Suite> = new Set(["unit", "corruption", "insert", "lz4"]);

const memoryPollMs = readPositiveIntEnv("FUZZ_MEMORY_POLL_MS", 250);
const memoryWarnBytes = readPositiveIntEnv("FUZZ_MEMORY_WARN_MB", 0) * 1024 * 1024;

function readPositiveIntEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) return fallback;
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function readRssBytes(pid: number): number | null {
  if (process.platform === "win32") return null;
  const res = spawnSync("ps", ["-o", "rss=", "-p", String(pid)], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  if (res.status !== 0) return null;
  const rssKb = parseInt(res.stdout.trim(), 10);
  return Number.isFinite(rssKb) ? rssKb * 1024 : null;
}

function formatBytes(bytes: number | null): string {
  if (bytes === null) return "n/a";
  return `${(bytes / 1024 / 1024).toFixed(1)}MiB`;
}

function parseArgs(): {
  suites: Suite[];
  verbose: boolean;
} {
  const args = process.argv.slice(2);
  const suites: Suite[] = [];
  let verbose = false;

  for (const arg of args) {
    if (arg === "--unit") {
      suites.push("unit");
    } else if (arg === "--corruption") {
      suites.push("corruption");
    } else if (arg === "--insert") {
      suites.push("insert");
    } else if (arg === "--lz4") {
      suites.push("lz4");
    } else if (arg === "--http") {
      suites.push("http");
    } else if (arg === "--tcp") {
      suites.push("tcp");
    } else if (arg === "--generated") {
      suites.push("generated");
    } else if (arg === "--all") {
      suites.push("unit", "corruption", "insert", "lz4", "http", "tcp", "generated");
    } else if (arg === "--verbose" || arg === "-v") {
      verbose = true;
    }
  }

  if (suites.length === 0) {
    suites.push("unit", "corruption", "insert", "lz4", "http", "tcp", "generated");
  }

  return { suites: [...new Set(suites)], verbose };
}

function buildJobs(suites: Suite[]): Job[] {
  const jobs: Job[] = [];

  for (const suite of suites) {
    if (LOCAL_SUITES.has(suite)) {
      for (let i = 0; i < config.iterations; i++) {
        jobs.push({
          name: `${suite}[${i}]`,
          file: `fuzz/${suite}.ts`,
          env: { FUZZ_ITERATION_INDEX: String(i) },
        });
      }
    } else {
      for (const comp of config.compressions) {
        for (let i = 0; i < config.iterations; i++) {
          const compName = comp === false ? "none" : comp;
          jobs.push({
            name: `${suite}:${compName}[${i}]`,
            file: `fuzz/${suite}.ts`,
            env: {
              FUZZ_COMPRESSION: String(comp),
              FUZZ_ITERATION_INDEX: String(i),
            },
          });
        }
      }
    }
  }

  return jobs;
}

async function runJob(job: Job, verbose: boolean): Promise<JobResult> {
  const start = Date.now();
  let output = "";

  return new Promise((resolve) => {
    let peakRssBytes: number | null = null;
    let memoryTimer: ReturnType<typeof setInterval> | null = null;
    const sampleMemory = () => {
      if (proc.pid === undefined) return;
      const rss = readRssBytes(proc.pid);
      if (rss !== null && (peakRssBytes === null || rss > peakRssBytes)) peakRssBytes = rss;
    };
    const finish = (success: boolean) => {
      if (memoryTimer) clearInterval(memoryTimer);
      sampleMemory();
      resolve({
        job,
        success,
        duration: Date.now() - start,
        output,
        peakRssBytes,
      });
    };

    const proc = spawn("tsx", ["--test", job.file], {
      env: { ...process.env, ...job.env },
      cwd: process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });

    if (memoryPollMs > 0) {
      sampleMemory();
      memoryTimer = setInterval(sampleMemory, memoryPollMs);
    }

    proc.stdout?.on("data", (data) => {
      const text = data.toString();
      output += text;
      if (verbose) {
        process.stdout.write(`[${job.name}] ${text}`);
      }
    });
    proc.stderr?.on("data", (data) => {
      const text = data.toString();
      output += text;
      if (verbose) {
        process.stderr.write(`[${job.name}] ${text}`);
      }
    });

    proc.on("close", (code) => {
      finish(code === 0);
    });

    proc.on("error", (err) => {
      output += `\nProcess error: ${err.message}`;
      finish(false);
    });
  });
}

async function runParallel(
  jobs: Job[],
  maxConcurrency: number,
  verbose: boolean,
): Promise<JobResult[]> {
  const results: JobResult[] = [];
  const pending = [...jobs];
  const running: Promise<void>[] = [];

  console.log(
    `Running ${jobs.length} jobs with concurrency ${maxConcurrency}${verbose ? " (verbose)" : ""}\n`,
  );

  async function startNext(): Promise<void> {
    const job = pending.shift();
    if (!job) return;

    console.log(`[start] ${job.name}`);
    const result = await runJob(job, verbose);
    results.push(result);

    const status = result.success ? "pass" : "FAIL";
    const duration = (result.duration / 1000).toFixed(1);
    console.log(
      `[${status}] ${job.name} (${duration}s, peak RSS ${formatBytes(result.peakRssBytes)})`,
    );

    if (!result.success && !verbose) {
      console.log(`\n--- ${job.name} output ---\n${result.output}\n---\n`);
    }

    if (pending.length > 0) {
      running.push(startNext());
    }
  }

  for (let i = 0; i < Math.min(maxConcurrency, jobs.length); i++) {
    running.push(startNext());
  }

  while (running.length > 0) {
    await running.shift();
  }

  return results;
}

async function main() {
  const { suites, verbose } = parseArgs();
  const jobs = buildJobs(suites);

  console.log(`Fuzz test runner`);
  console.log(`  Suites: ${suites.join(", ")}`);
  console.log(`  Iterations: ${config.iterations}`);
  console.log(`  Total jobs: ${jobs.length}`);
  console.log(
    `  Memory: ${memoryPollMs > 0 ? `poll ${memoryPollMs}ms` : "disabled"}${
      memoryWarnBytes > 0 ? `, warn > ${formatBytes(memoryWarnBytes)}` : ""
    }`,
  );
  console.log();

  // Start one ClickHouse server shared by every worker process (each connects
  // via FUZZ_CH_URL instead of starting its own container). "unit" needs none.
  const needsClickHouse = suites.some((s) => !LOCAL_SUITES.has(s)) && !process.env.FUZZ_CH_URL;
  if (needsClickHouse) {
    const ch = await startClickHouse();
    process.env.FUZZ_CH_URL = ch.url;
    process.env.FUZZ_CH_TCP_PORT = String(ch.tcpPort);
  }

  try {
    const startTime = Date.now();
    const results = await runParallel(jobs, config.maxConcurrentProcesses, verbose);
    const totalDuration = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log("\n=== Summary ===");
    const passed = results.filter((r) => r.success).length;
    const failed = results.filter((r) => !r.success).length;
    console.log(`Passed: ${passed}/${results.length}`);
    console.log(`Failed: ${failed}`);
    console.log(`Total time: ${totalDuration}s`);
    const memoryResults = results.filter((r) => r.peakRssBytes !== null);
    if (memoryResults.length > 0) {
      const sortedByPeak = [...memoryResults].sort(
        (a, b) => (b.peakRssBytes ?? 0) - (a.peakRssBytes ?? 0),
      );
      console.log(`Peak RSS max: ${formatBytes(sortedByPeak[0]!.peakRssBytes)}`);
      const warned =
        memoryWarnBytes > 0
          ? sortedByPeak.filter((r) => (r.peakRssBytes ?? 0) > memoryWarnBytes)
          : [];
      if (warned.length > 0) {
        console.log(`\nMemory warnings (> ${formatBytes(memoryWarnBytes)}):`);
        for (const r of warned.slice(0, 10)) {
          console.log(`  - ${r.job.name}: ${formatBytes(r.peakRssBytes)}`);
        }
        if (warned.length > 10) console.log(`  ... ${warned.length - 10} more`);
      }
      console.log("\nTop peak RSS jobs:");
      for (const r of sortedByPeak.slice(0, 10)) {
        console.log(`  - ${r.job.name}: ${formatBytes(r.peakRssBytes)}`);
      }
    }

    if (failed > 0) {
      console.log("\nFailed jobs:");
      for (const r of results.filter((r) => !r.success)) {
        console.log(`  - ${r.job.name}`);
      }
      process.exitCode = 1;
    }
  } finally {
    if (needsClickHouse) await stopClickHouse();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
