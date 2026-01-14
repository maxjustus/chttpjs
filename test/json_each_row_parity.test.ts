import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { collectBytes, init, insert, query, streamEncodeJsonEachRow } from "../client.ts";
import { ClickHouseDateTime64, streamDecodeNative } from "../native/index.ts";
import { TcpClient } from "../tcp_client/client.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { consume, toArrayRows, toAsync } from "./test_utils.ts";

function normalizeValue(value: unknown): unknown {
  if (value instanceof ClickHouseDateTime64) return value.toJSON();
  if (value instanceof Map) {
    const entries = [...value.entries()].map(([k, v]) => [normalizeValue(k), normalizeValue(v)]);
    entries.sort((a, b) => String(a[0]).localeCompare(String(b[0])));
    return entries;
  }
  if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
    return Array.from(value as unknown as ArrayLike<number>);
  }
  if (Array.isArray(value)) return value.map(normalizeValue);
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    const out: Record<string, unknown> = {};
    for (const k of keys) out[k] = normalizeValue(obj[k]);
    return out;
  }
  return value;
}

async function collectNativeRows(data: Uint8Array): Promise<unknown[][]> {
  const out: unknown[][] = [];
  for await (const batch of streamDecodeNative(toAsync([data]), { enumAsNumber: true })) {
    out.push(...toArrayRows(batch));
  }
  return out;
}

describe("JSONEachRow parity (row-object insert)", { timeout: 120000 }, () => {
  let baseUrl: string;
  let auth: { username: string; password: string };
  let tcp: { host: string; port: number; user: string; password: string };
  const sessionId = `json_each_row_parity_${Date.now()}`;

  before(async () => {
    await init();
    const ch = await startClickHouse();
    baseUrl = `${ch.url}/`;
    auth = { username: ch.username, password: ch.password };
    tcp = { host: ch.host, port: ch.tcpPort, user: ch.username, password: ch.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  it("matches ClickHouse JSONEachRow defaults for omitted/unknown/null fields", async () => {
    const tableNative = `test_parity_native_${Date.now()}`;
    const tableJson = `test_parity_json_${Date.now()}`;
    const schemaSql = (table: string) => `
      CREATE TABLE ${table} (
        id UInt32,
        i32 Int32,
        b Bool,
        s_obj String,
        s_arr String,
        opt Nullable(Int32),
        arr Array(Int32),
        m Map(String, Int32),
        t Tuple(Int32, String),
        e Enum8('a' = 2, 'b' = 1, 'none' = 0),
        ts DateTime64(3)
      ) ENGINE = Memory
    `;

    await consume(query(`DROP TABLE IF EXISTS ${tableNative}`, sessionId, { baseUrl, auth }));
    await consume(query(`DROP TABLE IF EXISTS ${tableJson}`, sessionId, { baseUrl, auth }));
    await consume(query(schemaSql(tableNative), sessionId, { baseUrl, auth }));
    await consume(query(schemaSql(tableJson), sessionId, { baseUrl, auth }));

    try {
      const rows: Record<string, unknown>[] = [
        {
          id: 1,
          i32: 123,
          b: true,
          s_obj: { a: 1 },
          s_arr: [1, 2, 3],
          opt: 7,
          arr: [1, 2],
          m: { a: 1, b: 2 },
          t: [5, "x"],
          e: "b",
          // Use a numeric epoch value because ClickHouse JSONEachRow doesn't accept ISO strings with "Z".
          ts: 0,
          extra: "ignored",
        },
        {
          id: 2,
          // omit i32/b/opt/arr/m/t/e/ts (defaults)
          s_obj: { b: 2 },
          s_arr: [],
          extra: 123,
        },
        {
          id: 3,
          i32: null,
          b: null,
          s_obj: null,
          s_arr: null,
          opt: null,
          arr: null,
          m: null,
          t: null,
          e: null,
          ts: null,
        },
      ];

      const tcpClient = new TcpClient(tcp);
      await tcpClient.connect();
      try {
        for await (const _ of tcpClient.insert(`INSERT INTO ${tableNative} VALUES`, rows)) {
        }
      } finally {
        tcpClient.close();
      }

      await insert(
        `INSERT INTO ${tableJson} FORMAT JSONEachRow`,
        streamEncodeJsonEachRow(rows),
        sessionId,
        { baseUrl, auth },
      );

      const nativeBytes = await collectBytes(
        query(`SELECT * FROM ${tableNative} ORDER BY id FORMAT Native`, sessionId, {
          baseUrl,
          auth,
        }),
      );
      const jsonBytes = await collectBytes(
        query(`SELECT * FROM ${tableJson} ORDER BY id FORMAT Native`, sessionId, { baseUrl, auth }),
      );

      const nativeRows = (await collectNativeRows(nativeBytes)).map((r) => r.map(normalizeValue));
      const jsonRows = (await collectNativeRows(jsonBytes)).map((r) => r.map(normalizeValue));

      assert.deepStrictEqual(nativeRows, jsonRows);
    } finally {
      await consume(query(`DROP TABLE ${tableNative}`, sessionId, { baseUrl, auth }));
      await consume(query(`DROP TABLE ${tableJson}`, sessionId, { baseUrl, auth }));
    }
  });
});
