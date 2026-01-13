/**
 * Test for error response handling with compression enabled.
 *
 * Bug: When compression is enabled (compress=1), ClickHouse compresses error
 * responses too. The client was reading error bodies with response.text()
 * which doesn't decompress, resulting in garbled error messages.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";

import { startClickHouse, stopClickHouse } from "./setup.ts";
import { init, query, collectText } from "../client.ts";
import { generateSessionId } from "./test_utils.ts";

describe("HTTP error response with compression", { timeout: 60000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = generateSessionId("error-compress");

  before(async () => {
    await init();
    clickhouse = await startClickHouse();
    baseUrl = clickhouse.url + "/";
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  for (const compression of ["lz4", "zstd"] as const) {
    it(`should return readable error message with ${compression} compression`, async () => {
      // This query references a nonexistent table which triggers an error response
      const invalidQuery = "SELECT * FROM nonexistent_table_xyz123";

      try {
        await collectText(
          query(invalidQuery, sessionId, {
            baseUrl,
            auth,
            compression,
          })
        );
        assert.fail("Query should have thrown an error");
      } catch (err) {
        const message = (err as Error).message;

        // The error message MUST contain the ClickHouse error format
        // If compression isn't handled, we get garbled binary instead
        assert.ok(
          message.includes("DB::Exception"),
          `Error message should contain 'DB::Exception', got: ${message.substring(0, 300)}`
        );

        // Error should mention the table name or error code
        assert.ok(
          message.includes("UNKNOWN_TABLE") || message.includes("nonexistent_table_xyz123") || message.includes("Code:"),
          `Error message should contain error details, got: ${message.substring(0, 300)}`
        );

        // The message should NOT start with "Query failed: 4xx - " followed by
        // replacement characters (�) which indicate failed decompression
        const startsWithGarbled = /Query failed: \d+ - [^\w\s]{3,}/.test(message);
        assert.ok(
          !startsWithGarbled,
          `Error message should not start with binary garbage: ${message.substring(0, 300)}`
        );
      }
    });
  }

  it("should return readable error message without compression", async () => {
    const invalidQuery = "SELECT * FROM nonexistent_table_xyz123";

    try {
      await collectText(
        query(invalidQuery, sessionId, {
          baseUrl,
          auth,
          compression: false,
        })
      );
      assert.fail("Query should have thrown an error");
    } catch (err) {
      const message = (err as Error).message;

      // Without compression, this should always work
      assert.ok(
        message.includes("Code:") || message.includes("UNKNOWN_TABLE"),
        `Error message should be readable, got: ${message.substring(0, 200)}`
      );
    }
  });
});
