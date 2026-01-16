/**
 * Shared configuration for fuzz tests.
 *
 * FUZZ_LEVEL controls test thoroughness:
 *   quick    - minimal iterations, no compression variants (CI/fast feedback)
 *   standard - normal iterations, one compression variant per transport
 *   thorough - more iterations, all compression variants
 *
 * Individual overrides:
 *   FUZZ_ITERATIONS - override iteration count for all test types
 *   FUZZ_ROWS - override row count for integration tests
 *   FUZZ_COMPRESSION - single compression to test (for CI matrix jobs)
 */

import { cpus } from "node:os";

export type FuzzLevel = "quick" | "standard" | "thorough";
export type Compression = false | "lz4" | "zstd";

export const FUZZ_LEVEL = (process.env.FUZZ_LEVEL as FuzzLevel) || "standard";

interface FuzzConfig {
  unitIterations: number;
  integrationIterations: number;
  tcpIterations: number;
  rows: number;
  maxConcurrentProcesses: number;
  httpCompressions: Compression[];
  tcpCompressions: Compression[];
}

const FUZZ_CONFIGS: Record<FuzzLevel, FuzzConfig> = {
  quick: {
    unitIterations: 10,
    integrationIterations: 3,
    tcpIterations: 3,
    rows: 1000,
    maxConcurrentProcesses: Math.max(2, Math.floor(cpus().length / 2)),
    httpCompressions: [false],
    tcpCompressions: [false],
  },
  standard: {
    unitIterations: 50,
    integrationIterations: 25,
    tcpIterations: 25,
    rows: 20000,
    maxConcurrentProcesses: cpus().length,
    httpCompressions: [false, "lz4"],
    tcpCompressions: [false, "lz4"],
  },
  thorough: {
    unitIterations: 100,
    integrationIterations: 50,
    tcpIterations: 50,
    rows: 50000,
    maxConcurrentProcesses: cpus().length,
    httpCompressions: [false, "lz4", "zstd"],
    tcpCompressions: [false, "lz4", "zstd"],
  },
};

const baseConfig = FUZZ_CONFIGS[FUZZ_LEVEL];

// Parse single compression override for CI matrix jobs
function parseCompression(val: string | undefined): Compression | undefined {
  if (!val) return undefined;
  if (val === "false" || val === "none") return false;
  if (val === "lz4" || val === "zstd") return val;
  return undefined;
}

const singleCompression = parseCompression(process.env.FUZZ_COMPRESSION);

export const config = {
  unitIterations: parseInt(process.env.FUZZ_ITERATIONS ?? String(baseConfig.unitIterations), 10),
  integrationIterations: parseInt(
    process.env.FUZZ_ITERATIONS ?? String(baseConfig.integrationIterations),
    10,
  ),
  tcpIterations: parseInt(process.env.FUZZ_ITERATIONS ?? String(baseConfig.tcpIterations), 10),
  rows: parseInt(process.env.FUZZ_ROWS ?? String(baseConfig.rows), 10),
  maxConcurrentProcesses: parseInt(
    process.env.FUZZ_MAX_CONCURRENT ?? String(baseConfig.maxConcurrentProcesses),
    10,
  ),
  httpCompressions:
    singleCompression !== undefined ? [singleCompression] : baseConfig.httpCompressions,
  tcpCompressions:
    singleCompression !== undefined ? [singleCompression] : baseConfig.tcpCompressions,
};

/**
 * Get the iteration index for this process.
 * Returns null if not running as a single iteration (process-based model).
 */
export function getIterationIndex(): number | null {
  const envVal = process.env.FUZZ_ITERATION_INDEX;
  if (!envVal) return null;

  const parsed = parseInt(envVal, 10);
  if (isNaN(parsed) || parsed < 0) {
    throw new Error(`Invalid FUZZ_ITERATION_INDEX: ${envVal}`);
  }
  return parsed;
}

export interface FuzzErrorContext {
  testType: "tcp" | "http";
  iteration: number;
  totalIterations: number;
  compression: Compression;
  rows: number;
  structure?: string;
  jsonType?: string;
  srcTable?: string;
  dstTable?: string;
}

export function logFuzzError(ctx: FuzzErrorContext, err: unknown): void {
  const lines = [
    ``,
    `${"=".repeat(60)}`,
    `FUZZ TEST FAILURE`,
    `${"=".repeat(60)}`,
    `Test:        ${ctx.testType} fuzz`,
    `Iteration:   ${ctx.iteration + 1}/${ctx.totalIterations}`,
    `Compression: ${ctx.compression === false ? "none" : ctx.compression}`,
    `Rows:        ${ctx.rows.toLocaleString()}`,
  ];

  if (ctx.structure) {
    lines.push(`Structure:   ${ctx.structure}`);
  }
  if (ctx.jsonType) {
    lines.push(`JSON Type:   ${ctx.jsonType}`);
  }
  if (ctx.srcTable) {
    lines.push(`Src Table:   ${ctx.srcTable}`);
  }
  if (ctx.dstTable) {
    lines.push(`Dst Table:   ${ctx.dstTable}`);
  }

  lines.push(`${"─".repeat(60)}`);

  if (err instanceof Error) {
    lines.push(`Error:       ${err.message}`);
    if (err.stack) {
      lines.push(`Stack:`);
      lines.push(err.stack.split("\n").slice(1).join("\n"));
    }
  } else {
    lines.push(`Error:       ${String(err)}`);
  }

  lines.push(`${"=".repeat(60)}`);
  console.error(lines.join("\n"));
}

export function logConfig(testType: "unit" | "http" | "tcp"): void {
  const iterations =
    testType === "unit"
      ? config.unitIterations
      : testType === "http"
        ? config.integrationIterations
        : config.tcpIterations;
  const compressions =
    testType === "unit"
      ? ["n/a"]
      : testType === "http"
        ? config.httpCompressions
        : config.tcpCompressions;

  const iterIdx = getIterationIndex();
  const mode =
    iterIdx !== null ? `iteration=${iterIdx + 1}/${iterations}` : `iterations=${iterations}`;

  console.log(
    `[fuzz ${testType}] level=${FUZZ_LEVEL}, ${mode}, compressions=${JSON.stringify(compressions)}, rows=${config.rows}`,
  );
}
