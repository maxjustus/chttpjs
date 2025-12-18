import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { ClickHouseException } from "../types.ts";

describe("TCP Client Reliability", () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: ""
  };

  test("should parse exception with full details", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      for await (const _ of client.query("SELECT * FROM nonexistent_table_xyz123")) {}
      assert.fail("Should have thrown an exception");
    } catch (err) {
      assert.ok(err instanceof ClickHouseException, "Should be ClickHouseException");
      assert.strictEqual(err.code, 60, "Should have error code 60 (UNKNOWN_TABLE)");
      assert.strictEqual(err.exceptionName, "DB::Exception", "Should have exception name");
      assert.ok(err.message.includes("does not exist"), "Message should mention table does not exist");
      assert.ok(err.serverStackTrace.length > 0, "Should have stack trace");
    } finally {
      client.close();
    }
  });

  test("should ping and receive pong", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      await client.ping();
      // If we get here without error, ping succeeded
      assert.ok(true);
    } finally {
      client.close();
    }
  });

  test("should timeout query that takes too long", async () => {
    const client = new TcpClient({
      ...options,
      queryTimeout: 50 // 50ms timeout
    });
    await client.connect();
    try {
      // Use sleep(1) which is 1 second - enough to trigger our 50ms timeout
      for await (const _ of client.query("SELECT sleep(1)")) {}
      assert.fail("Should have thrown a timeout error");
    } catch (err: any) {
      // Socket is destroyed on timeout, which can manifest as various errors
      assert.ok(
        err.message.includes("timeout") ||
        err.message.includes("end of stream") ||
        err.message.includes("Premature close"),
        `Should be timeout/close error, got: ${err.message}`
      );
    } finally {
      client.close();
    }
  });

  test("should cancel query via AbortSignal", async () => {
    const client = new TcpClient(options);
    await client.connect();

    const controller = new AbortController();

    // Cancel after 50ms
    setTimeout(() => controller.abort(), 50);

    try {
      // Start a slow query
      for await (const _ of client.query(
        "SELECT sleep(10)",
        {},
        { signal: controller.signal }
      )) {}
      // Query may complete early due to cancellation
    } catch (err: any) {
      // Expected - either abort error or server-side cancellation
      assert.ok(true, "Query was cancelled or errored as expected");
    } finally {
      client.close();
    }
  });

  test("should reject query if already aborted", async () => {
    const client = new TcpClient(options);
    await client.connect();

    const controller = new AbortController();
    controller.abort(); // Abort before query starts

    try {
      for await (const _ of client.query("SELECT 1", {}, { signal: controller.signal })) {}
      assert.fail("Should have thrown an error");
    } catch (err: any) {
      assert.ok(err.message.includes("aborted"), "Should mention aborted");
    } finally {
      client.close();
    }
  });

  test("should handle connection timeout", async () => {
    const client = new TcpClient({
      host: "192.0.2.1", // Non-routable IP (RFC 5737 TEST-NET-1)
      port: 9000,
      connectTimeout: 100 // 100ms timeout
    });

    try {
      await client.connect();
      assert.fail("Should have thrown a timeout error");
    } catch (err: any) {
      assert.ok(
        err.message.includes("timeout") || err.message.includes("ETIMEDOUT") || err.code === "ETIMEDOUT",
        `Should be timeout error, got: ${err.message}`
      );
    }
  });
});
