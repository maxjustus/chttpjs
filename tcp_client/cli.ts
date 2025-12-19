#!/usr/bin/env node
/**
 * TCP Client CLI - streams protocol packets as NDJSON to stdout.
 *
 * Usage:
 *   node --experimental-strip-types tcp_client/cli.ts 'SELECT 1'  # single query
 *   node --experimental-strip-types tcp_client/cli.ts             # interactive REPL
 *
 * Environment:
 *   CH_HOST     - ClickHouse host (default: localhost)
 *   CH_PORT     - ClickHouse TCP port (default: 9000)
 *   CH_USER     - Username (default: default)
 *   CH_PASSWORD - Password (default: "")
 */

import * as readline from "node:readline";
import { TcpClient } from "./client.ts";
import type { Packet } from "./types.ts";

const options = {
  host: process.env.CH_HOST ?? "localhost",
  port: parseInt(process.env.CH_PORT ?? "9000", 10),
  user: process.env.CH_USER ?? "default",
  password: process.env.CH_PASSWORD ?? "",
};

// Convert non-JSON-safe types for serialization
function toJSON(obj: unknown): unknown {
  if (typeof obj === "bigint") return obj.toString();
  if (obj instanceof Date) return obj.toISOString();
  if (obj instanceof Map) return Object.fromEntries([...obj.entries()].map(([k, v]) => [k, toJSON(v)]));
  if (Array.isArray(obj)) return obj.map(toJSON);
  if (obj !== null && typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj)) {
      result[k] = toJSON(v);
    }
    return result;
  }
  return obj;
}

function formatPacket(packet: Packet): Record<string, unknown> {
  switch (packet.type) {
    case "Data":
    case "Totals":
    case "Extremes": {
      const rows = [...packet.batch.rows()].map(r => r.toObject());
      return {
        type: packet.type,
        columns: packet.batch.columns,
        rows: rows,
      };
    }
    case "Progress":
      return { type: "Progress", ...packet.progress };
    case "ProfileInfo":
      return { type: "ProfileInfo", ...packet.info };
    case "ProfileEvents":
      return {
        type: "ProfileEvents",
        accumulated: Object.fromEntries(packet.accumulated),
      };
    case "Log":
      return { type: "Log", entries: packet.entries };
    case "EndOfStream":
      return { type: "EndOfStream" };
    default:
      return packet as Record<string, unknown>;
  }
}

async function runQuery(client: TcpClient, query: string): Promise<void> {
  for await (const packet of client.query(query, { send_logs_level: "trace" })) {
    console.log(JSON.stringify(toJSON(formatPacket(packet))));
  }
}

async function runInteractive(client: TcpClient): Promise<void> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "ch> ",
  });

  console.log(`Connected to ${options.host}:${options.port}`);
  console.log('Type queries, or "exit" to quit.\n');
  rl.prompt();

  for await (const line of rl) {
    const query = line.trim();
    if (!query) {
      rl.prompt();
      continue;
    }
    if (query.toLowerCase() === "exit" || query.toLowerCase() === "quit") {
      break;
    }

    try {
      await runQuery(client, query);
    } catch (err) {
      console.error(`Error: ${(err as Error).message}`);
    }
    console.log();
    rl.prompt();
  }

  rl.close();
}

async function main() {
  const client = new TcpClient(options);
  await client.connect();

  try {
    const query = process.argv[2];
    if (query) {
      await runQuery(client, query);
    } else {
      await runInteractive(client);
    }
  } finally {
    client.close();
  }
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
