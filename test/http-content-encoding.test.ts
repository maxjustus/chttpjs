/**
 * HTTP content coding (`httpCompression`): ClickHouse flushes zstd and lz4 per
 * block, so rows arrive as produced instead of at the end of the query.
 */

import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { ClickHouseException, init, query } from "../client.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";

const ENCODINGS = ["zstd", "lz4"] as const;

describe("httpCompression", () => {
  let url: string;
  let auth: { username: string; password: string };

  before(async () => {
    await init();
    const clickhouse = await startClickHouse();
    url = `${clickhouse.url}/`;
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  async function rowLines(sql: string, options: Record<string, unknown>): Promise<string[]> {
    const parts: string[] = [];
    for await (const packet of query(sql, { url, auth, ...options })) {
      if (packet.type === "Data") parts.push(new TextDecoder().decode(packet.chunk));
    }
    return parts
      .join("")
      .split("\n")
      .filter((line) => line.startsWith('{"row"'));
  }

  /** How a failing query surfaced: thrown exception, or an error row in the body. */
  async function failureOf(sql: string, options: Record<string, unknown>) {
    let body = "";
    let error: unknown;
    try {
      for await (const packet of query(sql, { url, auth, ...options })) {
        if (packet.type === "Data") body += new TextDecoder().decode(packet.chunk);
      }
    } catch (err) {
      error = err;
    }
    const inBody = body.includes("FUNCTION_THROW_IF_VALUE_IS_NON_ZERO");
    return {
      code: error instanceof ClickHouseException ? error.code : undefined,
      threw: error !== undefined,
      reported: error instanceof ClickHouseException || inBody,
    };
  }

  for (const encoding of ENCODINGS) {
    it(`${encoding} yields the same rows as an uncompressed response`, async () => {
      const sql = "SELECT number, toString(number) AS s FROM numbers(50000)";
      const expected = await rowLines(sql, { compression: false });
      const actual = await rowLines(sql, { httpCompression: encoding });
      assert.equal(actual.length, 50000);
      assert.deepEqual(actual, expected);
    });

    it(`${encoding} delivers rows before the query completes`, async () => {
      const sql = "SELECT number, sleepEachRow(0.2) FROM numbers(6) SETTINGS max_block_size=1";
      const start = Date.now();
      const arrivals: number[] = [];
      for await (const packet of query(sql, { url, auth, httpCompression: encoding })) {
        if (packet.type === "Data") arrivals.push(Date.now() - start);
      }
      const total = arrivals[arrivals.length - 1] as number;
      assert.ok(total > 900, `query should span the sleeps, took ${total}ms`);
      assert.ok(
        (arrivals[0] as number) < total / 2,
        `first row arrived at ${arrivals[0]}ms of ${total}ms, so the server buffered`,
      );
    });

    it(`${encoding} reports a mid-stream failure the same way an uncompressed response does`, async () => {
      // 25.8 delivers the failure as a JSONEachRow `exception` row, 26.x as a
      // framed __exception__ trailer. Both must survive the content coding.
      const sql =
        "SELECT throwIf(number = 50000, 'boom') FROM numbers(100000) SETTINGS max_block_size=1000";
      const baseline = await failureOf(sql, { compression: false });
      assert.ok(baseline.reported, "uncompressed baseline lost the failure");

      // The failure races the response teardown, so repeat to catch flakiness.
      for (let attempt = 0; attempt < 3; attempt++) {
        const actual = await failureOf(sql, { httpCompression: encoding });
        assert.deepEqual(actual, baseline, `attempt ${attempt}`);
      }
    });
  }
});
