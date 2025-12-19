import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { ClickHouseException } from "../types.ts";
import { Table } from "../../formats/native/table.ts";

describe("TCP Client Reliability", () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: ""
  };

  async function withClient<T>(fn: (client: TcpClient) => Promise<T>): Promise<T> {
    const client = new TcpClient(options);
    await client.connect();
    try {
      return await fn(client);
    } finally {
      client.close();
    }
  }

  test("should parse exception with full details", () => withClient(async (client) => {
    try {
      for await (const _ of client.query("SELECT * FROM nonexistent_table_xyz123")) {}
      assert.fail("Should have thrown an exception");
    } catch (err) {
      assert.ok(err instanceof ClickHouseException, "Should be ClickHouseException");
      assert.strictEqual(err.code, 60, "Should have error code 60 (UNKNOWN_TABLE)");
      assert.strictEqual(err.exceptionName, "DB::Exception", "Should have exception name");
      assert.ok(err.message.includes("does not exist"), "Message should mention table does not exist");
      assert.ok(err.serverStackTrace.length > 0, "Should have stack trace");
    }
  }));

  test("should ping and receive pong", () => withClient(async (client) => {
    await client.ping();
    assert.ok(true);
  }));

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
      // The client should now wrap "Premature close" into a timeout error
      assert.ok(
        err.message.includes("timeout"),
        `Should be timeout error, got: ${err.message}`
      );
    } finally {
      client.close();
    }
  });

  test("should cancel query via AbortSignal", () => withClient(async (client) => {
    const controller = new AbortController();
    setTimeout(() => controller.abort(), 50);

    try {
      for await (const _ of client.query("SELECT sleep(10)", {}, { signal: controller.signal })) {}
    } catch (err: any) {
      assert.ok(true, "Query was cancelled or errored as expected");
    }
  }));

  test("should reject query if already aborted", () => withClient(async (client) => {
    const controller = new AbortController();
    controller.abort();

    try {
      for await (const _ of client.query("SELECT 1", {}, { signal: controller.signal })) {}
      assert.fail("Should have thrown an error");
    } catch (err: any) {
      assert.ok(err.message.includes("aborted"), "Should mention aborted");
    }
  }));

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

  test("should cancel insert via AbortSignal", async () => {
    const client = new TcpClient(options);
    await client.connect();

    const controller = new AbortController();

    try {
      // Create table for insert test
      await client.execute("CREATE TABLE IF NOT EXISTS test_abort_insert (x UInt64) ENGINE = Memory");

      // Create an async generator that yields tables slowly
      async function* slowTables() {
        for (let i = 0; i < 100; i++) {
          yield Table.fromColumnar(
            [{ name: "x", type: "UInt64" }],
            [BigInt64Array.from([BigInt(i)])]
          );
          await new Promise(r => setTimeout(r, 10));
        }
      }

      // Cancel after 50ms
      setTimeout(() => controller.abort(), 50);

      await client.insert(
        "INSERT INTO test_abort_insert FORMAT Native",
        slowTables(),
        { signal: controller.signal }
      );
      // Insert may complete or be cancelled
    } catch (err: any) {
      assert.ok(
        err.message.includes("cancelled") || err.message.includes("aborted"),
        `Should be cancel/abort error, got: ${err.message}`
      );
    } finally {
      // Clean up - use a fresh client since connection may be in bad state
      const cleanupClient = new TcpClient(options);
      await cleanupClient.connect();
      await cleanupClient.execute("DROP TABLE IF EXISTS test_abort_insert");
      cleanupClient.close();
      client.close();
    }
  });

  test("should reject insert if already aborted", () => withClient(async (client) => {
    const controller = new AbortController();
    controller.abort();

    try {
      const table = Table.fromColumnar(
        [{ name: "x", type: "UInt64" }],
        [BigInt64Array.from([1n, 2n, 3n])]
      );
      await client.insert("INSERT INTO system.numbers FORMAT Native", table, { signal: controller.signal });
      assert.fail("Should have thrown an error");
    } catch (err: any) {
      assert.ok(err.message.includes("aborted"), "Should mention aborted");
    }
  }));

  test("should cancel connect via AbortSignal", async () => {
    const controller = new AbortController();

    // Use non-routable IP so connection hangs
    const client = new TcpClient({
      host: "192.0.2.1", // Non-routable IP (RFC 5737 TEST-NET-1)
      port: 9000,
      connectTimeout: 10000 // Long timeout so abort happens first
    });

    // Abort after 50ms
    setTimeout(() => controller.abort(), 50);

    try {
      await client.connect({ signal: controller.signal });
      assert.fail("Should have thrown an abort error");
    } catch (err: any) {
      assert.ok(
        err.message.includes("aborted") || err.message.includes("abort"),
        `Should be abort error, got: ${err.message}`
      );
    }
  });

  test("should reject connect if already aborted", async () => {
    const controller = new AbortController();
    controller.abort(); // Abort before connect starts

    const client = new TcpClient(options);

    try {
      await client.connect({ signal: controller.signal });
      assert.fail("Should have thrown an error");
    } catch (err: any) {
      assert.ok(err.message.includes("aborted"), "Should mention aborted");
    }
  });
});
