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
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { TcpClient } from "./client.ts";
import type { Packet } from "./types.ts";

const HISTORY_FILE = path.join(os.homedir(), ".ch_cli_history");
const MAX_HISTORY = 500;

function loadHistory(): string[] {
  try {
    return fs.readFileSync(HISTORY_FILE, "utf-8").split("\n").filter(Boolean);
  } catch {
    return [];
  }
}

function saveHistory(history: string[]): void {
  try {
    fs.writeFileSync(HISTORY_FILE, history.slice(-MAX_HISTORY).join("\n") + "\n");
  } catch {
    // Ignore write errors
  }
}

const options = {
  host: process.env.CH_HOST ?? "localhost",
  port: parseInt(process.env.CH_PORT ?? "9000", 10),
  user: process.env.CH_USER ?? "default",
  password: process.env.CH_PASSWORD ?? "",
  settings: {
    output_format_native_use_flattened_dynamic_and_json_serialization: 1,
  }
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

function formatPacket(packet: Packet, compact: boolean = false): Record<string, unknown> {
  switch (packet.type) {
    case "Data":
    case "Totals":
    case "Extremes": {
      if (compact) {
        return {
          type: packet.type,
          columns: packet.batch.columns,
          rowCount: packet.batch.rowCount,
        };
      }
      return {
        type: packet.type,
        columns: packet.batch.columns,
        rows: packet.batch.toArray(),
      };
    }
    case "Progress":
      return { type: "Progress", delta: packet.progress, accumulated: packet.accumulated };
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

async function runQuery(client: TcpClient, query: string, pretty: boolean = false): Promise<void> {
  const compact = /\bFORMAT\s+Null\b/i.test(query);
  for await (const packet of client.query(query, { settings: { send_logs_level: "trace" } })) {
    const json = toJSON(formatPacket(packet, compact));
    console.log(pretty ? JSON.stringify(json, null, 2) : JSON.stringify(json));
  }
}

async function runLoad(client: TcpClient, filePath: string, tableName: string): Promise<void> {
  const content = fs.readFileSync(filePath, "utf-8");
  const lines = content.split("\n").filter(line => line.trim());

  function* rows() {
    for (const line of lines) {
      yield JSON.parse(line);
    }
  }

  const count = lines.length;
  console.log(`Loading ${count} rows from ${filePath} into ${tableName}...`);

  let writtenRows = 0n;
  for await (const packet of client.insert(`INSERT INTO ${tableName} VALUES`, rows(), { batchSize: 10000 })) {
    if (packet.type === "Progress") {
      writtenRows = packet.accumulated.writtenRows;
    }
  }
  console.log(`Inserted ${writtenRows > 0n ? writtenRows : count} rows.`);
}

async function runInteractive(client: TcpClient): Promise<void> {
  const history = loadHistory();

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "ch> ",
    history,
    historySize: MAX_HISTORY,
  });

  console.log(`Connected to ${options.host}:${options.port}`);
  console.log('Commands: \\load <file.jsonl> INTO <table>, exit\n');
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

    // \load /path/to/file.jsonl INTO table_name
    const loadMatch = query.match(/^\\load\s+(\S+)\s+into\s+(\S+)$/i);
    if (loadMatch) {
      try {
        await runLoad(client, loadMatch[1], loadMatch[2]);
      } catch (err) {
        console.error(`Error: ${(err as Error).message}`);
      }
      console.log();
      rl.prompt();
      continue;
    }

    try {
      await runQuery(client, query, true);
    } catch (err) {
      console.error(`Error: ${(err as Error).message}`);
    }
    console.log();
    rl.prompt();
  }

  // Save history on exit
  saveHistory((rl as any).history || []);

  rl.close();
}

async function main() {
  // TODO: make v3 JSON setting a default here / maybe in the client library or document how more clearly.
  // requires making settings top level in the client options
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
