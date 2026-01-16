#!/usr/bin/env node --experimental-strip-types
/**
 * Parallel fuzz test runner.
 *
 * Usage:
 *   node --experimental-strip-types fuzz/parallel.ts [options]
 *
 * Options:
 *   --level=quick|standard|thorough  Set fuzz level (default: standard)
 *   --unit                           Run unit tests
 *   --http                           Run HTTP integration tests
 *   --tcp                            Run TCP integration tests
 *   --all                            Run all tests (default if none specified)
 *   --compression=false,lz4,zstd     Specific compressions to test
 *   --verbose                        Stream test output in real-time
 *
 * Examples:
 *   node --experimental-strip-types fuzz/parallel.ts --level=thorough --all
 *   node --experimental-strip-types fuzz/parallel.ts --http --compression=lz4
 *   node --experimental-strip-types fuzz/parallel.ts --unit --verbose
 */

import { spawn } from "node:child_process";
import { cpus } from "node:os";

interface Job {
  name: string;
  file: string;
  env: Record<string, string>;
  iterationIndex?: number;
}

interface JobResult {
  job: Job;
  success: boolean;
  duration: number;
  output: string;
}

function parseArgs(): {
  level: string;
  suites: ("unit" | "http" | "tcp")[];
  compressions: string[] | null;
  verbose: boolean;
} {
  const args = process.argv.slice(2);
  let level = "standard";
  const suites: ("unit" | "http" | "tcp")[] = [];
  let compressions: string[] | null = null;
  let verbose = false;

  for (const arg of args) {
    if (arg.startsWith("--level=")) {
      level = arg.slice(8);
    } else if (arg === "--unit") {
      suites.push("unit");
    } else if (arg === "--http") {
      suites.push("http");
    } else if (arg === "--tcp") {
      suites.push("tcp");
    } else if (arg === "--all") {
      suites.push("unit", "http", "tcp");
    } else if (arg.startsWith("--compression=")) {
      compressions = arg.slice(14).split(",");
    } else if (arg === "--verbose" || arg === "-v") {
      verbose = true;
    }
  }

  if (suites.length === 0) {
    suites.push("unit", "http", "tcp");
  }

  return { level, suites: [...new Set(suites)], compressions, verbose };
}

function getIterationCount(level: string, type: "unit" | "integration" | "tcp"): number {
  const envOverride = process.env.FUZZ_ITERATIONS;
  if (envOverride) return parseInt(envOverride, 10);

  const defaults: Record<string, Record<string, number>> = {
    quick: { unit: 10, integration: 3, tcp: 3 },
    standard: { unit: 50, integration: 25, tcp: 25 },
    thorough: { unit: 100, integration: 50, tcp: 50 },
  };

  return defaults[level]?.[type] ?? defaults.standard[type];
}

function buildJobs(level: string, suites: string[], compressions: string[] | null): Job[] {
  const jobs: Job[] = [];

  // Default compressions per level
  const defaultCompressions: Record<string, string[]> = {
    quick: ["false"],
    standard: ["false", "lz4"],
    thorough: ["false", "lz4", "zstd"],
  };

  const effectiveCompressions = compressions ?? defaultCompressions[level] ?? ["false"];

  for (const suite of suites) {
    if (suite === "unit") {
      const iterations = getIterationCount(level, "unit");
      for (let i = 0; i < iterations; i++) {
        jobs.push({
          name: `unit[${i}]`,
          file: "fuzz/unit.ts",
          env: { FUZZ_LEVEL: level, FUZZ_ITERATION_INDEX: String(i) },
          iterationIndex: i,
        });
      }
    } else {
      const iterations =
        suite === "http"
          ? getIterationCount(level, "integration")
          : getIterationCount(level, "tcp");

      for (const comp of effectiveCompressions) {
        for (let i = 0; i < iterations; i++) {
          const compName = comp === "false" ? "none" : comp;
          jobs.push({
            name: `${suite}:${compName}[${i}]`,
            file: `fuzz/${suite}.ts`,
            env: { FUZZ_LEVEL: level, FUZZ_COMPRESSION: comp, FUZZ_ITERATION_INDEX: String(i) },
            iterationIndex: i,
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
    const proc = spawn("node", ["--experimental-strip-types", "--test", job.file], {
      env: { ...process.env, ...job.env },
      cwd: process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });

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
      resolve({
        job,
        success: code === 0,
        duration: Date.now() - start,
        output,
      });
    });

    proc.on("error", (err) => {
      output += `\nProcess error: ${err.message}`;
      resolve({
        job,
        success: false,
        duration: Date.now() - start,
        output,
      });
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
    console.log(`[${status}] ${job.name} (${duration}s)`);

    if (!result.success && !verbose) {
      // Print failure output (unless already shown via verbose)
      console.log(`\n--- ${job.name} output ---\n${result.output}\n---\n`);
    }

    // Start next job if available
    if (pending.length > 0) {
      running.push(startNext());
    }
  }

  // Start initial batch
  for (let i = 0; i < Math.min(maxConcurrency, jobs.length); i++) {
    running.push(startNext());
  }

  // Wait for all to complete
  while (running.length > 0) {
    await running.shift();
  }

  return results;
}

function getMaxConcurrency(level: string): number {
  const envOverride = process.env.FUZZ_MAX_CONCURRENT;
  if (envOverride) {
    const parsed = parseInt(envOverride, 10);
    if (!isNaN(parsed) && parsed > 0) return parsed;
  }

  const defaults: Record<string, number> = {
    quick: Math.max(2, Math.floor(cpus().length / 2)),
    standard: cpus().length,
    thorough: cpus().length,
  };

  return defaults[level] ?? defaults.standard;
}

async function main() {
  const { level, suites, compressions, verbose } = parseArgs();
  const jobs = buildJobs(level, suites, compressions);

  console.log(`Fuzz test runner`);
  console.log(`  Level: ${level}`);
  console.log(`  Suites: ${suites.join(", ")}`);
  console.log(`  Total jobs: ${jobs.length}`);
  console.log();

  const maxConcurrency = getMaxConcurrency(level);

  const startTime = Date.now();
  const results = await runParallel(jobs, maxConcurrency, verbose);
  const totalDuration = ((Date.now() - startTime) / 1000).toFixed(1);

  // Summary
  console.log("\n=== Summary ===");
  const passed = results.filter((r) => r.success).length;
  const failed = results.filter((r) => !r.success).length;
  console.log(`Passed: ${passed}/${results.length}`);
  console.log(`Failed: ${failed}`);
  console.log(`Total time: ${totalDuration}s`);

  if (failed > 0) {
    console.log("\nFailed jobs:");
    for (const r of results.filter((r) => !r.success)) {
      console.log(`  - ${r.job.name}`);
    }
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
