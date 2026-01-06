/**
 * Tests for HTTP Content-Encoding compression of query bodies.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";

import { startClickHouse, stopClickHouse } from "./setup.ts";
import { init, query, collectText } from "../client.ts";
import { generateSessionId } from "./test_utils.ts";

describe("HTTP query body compression", { timeout: 60000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = generateSessionId("http-compress");

  before(async () => {
    await init();
    clickhouse = await startClickHouse();
    baseUrl = clickhouse.url + "/";
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  it("sends zstd-compressed query body", async () => {
    // Large query with many values to make compression worthwhile
    const values = Array(500)
      .fill(0)
      .map((_, i) => i)
      .join(",");
    const queryStr = `SELECT number FROM system.numbers WHERE number IN (${values}) LIMIT 500 FORMAT JSONEachRow`;

    const result = await collectText(
      query(queryStr, sessionId, {
        baseUrl,
        auth,
        compression: "none",
        compressQuery: "zstd",
      }),
    );

    // Should return 500 rows
    const rows = result
      .trim()
      .split("\n")
      .filter((l) => l.startsWith("{"));
    assert.strictEqual(rows.length, 500);
  });

  it("sends lz4-compressed query body", async () => {
    const values = Array(500)
      .fill(0)
      .map((_, i) => i)
      .join(",");
    const queryStr = `SELECT number FROM system.numbers WHERE number IN (${values}) LIMIT 500 FORMAT JSONEachRow`;

    const result = await collectText(
      query(queryStr, sessionId, {
        baseUrl,
        auth,
        compression: "none",
        compressQuery: "lz4",
      }),
    );

    const rows = result
      .trim()
      .split("\n")
      .filter((l) => l.startsWith("{"));
    assert.strictEqual(rows.length, 500);
  });

  it("works with both query and response compression", async () => {
    // compressQuery compresses the request body
    // compression compresses the response (native blocks)
    const values = Array(100)
      .fill(0)
      .map((_, i) => i)
      .join(",");
    const queryStr = `SELECT number FROM system.numbers WHERE number IN (${values}) LIMIT 100 FORMAT JSONEachRow`;

    const result = await collectText(
      query(queryStr, sessionId, {
        baseUrl,
        auth,
        compression: "lz4", // response compression (native blocks)
        compressQuery: "zstd", // request body compression
      }),
    );

    const rows = result
      .trim()
      .split("\n")
      .filter((l) => l.startsWith("{"));
    assert.strictEqual(rows.length, 100);
  });

  it("compresses large queries efficiently", async () => {
    // Generate a query with ~50KB of IN clause values
    const values = Array(5000)
      .fill(0)
      .map((_, i) => i)
      .join(",");
    const queryStr = `SELECT count() FROM system.numbers WHERE number IN (${values}) FORMAT JSONEachRow`;

    // This query string is about 28KB uncompressed
    assert.ok(queryStr.length > 20000, `Query should be large: ${queryStr.length}`);

    const result = await collectText(
      query(queryStr, sessionId, {
        baseUrl,
        auth,
        compression: "none",
        compressQuery: "zstd",
      }),
    );

    // Should return count result
    const parsed = JSON.parse(result.trim());
    assert.strictEqual(parsed["count()"], 5000);
  });
});
