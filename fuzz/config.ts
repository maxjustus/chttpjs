/**
 * Shared configuration for fuzz tests.
 *
 * Environment variables:
 *   FUZZ_ITERATIONS - number of iterations (default: 25)
 *   FUZZ_ROWS - row count for integration tests (default: 10000)
 */

import { cpus } from "node:os";

export type Compression = false | "lz4" | "zstd";

const CPU_COUNT = cpus().length;

function readIntEnv(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) return fallback;

  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed)) return fallback;
  return parsed;
}

function parseCompression(): Compression[] {
  const value = process.env.FUZZ_COMPRESSION;
  if (value === undefined) return [false, "lz4", "zstd"];
  if (value === "false") return [false];
  if (value === "lz4" || value === "zstd") return [value];
  return [false, "lz4", "zstd"];
}

export const config = {
  iterations: readIntEnv("FUZZ_ITERATIONS", 25),
  rows: readIntEnv("FUZZ_ROWS", 10000),
  maxConcurrentProcesses: Math.max(4, Math.min(8, Math.floor(CPU_COUNT / 2))),
  compressions: parseCompression(),
};

/**
 * Get the iteration index for this process.
 * Returns null if not running as a single iteration (process-based model).
 */
export function getIterationIndex(): number | null {
  const envVal = process.env.FUZZ_ITERATION_INDEX;
  if (!envVal) return null;

  const parsed = parseInt(envVal, 10);
  if (Number.isNaN(parsed) || parsed < 0) {
    throw new Error(`Invalid FUZZ_ITERATION_INDEX: ${envVal}`);
  }
  return parsed;
}

export interface FuzzErrorContext {
  testType: "tcp" | "http" | "generated";
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
  let compression = String(ctx.compression);
  if (ctx.compression === false) {
    compression = "none";
  }

  const lines = [
    ``,
    `${"=".repeat(60)}`,
    `FUZZ TEST FAILURE`,
    `${"=".repeat(60)}`,
    `Test:        ${ctx.testType} fuzz`,
    `Iteration:   ${ctx.iteration + 1}/${ctx.totalIterations}`,
    `Compression: ${compression}`,
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

export function logConfig(
  testType: "unit" | "http" | "tcp" | "generated" | "corruption" | "insert" | "lz4",
): void {
  let mode = `iterations=${config.iterations}`;
  const iterIdx = getIterationIndex();
  if (iterIdx !== null) {
    mode = `iteration=${iterIdx + 1}/${config.iterations}`;
  }

  const local =
    testType === "unit" || testType === "corruption" || testType === "insert" || testType === "lz4";
  const compressions = local ? "n/a" : JSON.stringify(config.compressions);
  console.log(`[fuzz ${testType}] ${mode}, compressions=${compressions}, rows=${config.rows}`);
}
