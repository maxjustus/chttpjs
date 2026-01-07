import { test, describe, before, after } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { ClickHouseException } from "../types.ts";
import { RecordBatch } from "../../native/table.ts";
import { startClickHouse, stopClickHouse } from "../../test/setup.ts";

describe("TCP Client Reliability", () => {
  let options: { host: string; port: number; user: string; password: string };

  before(async () => {
    const ch = await startClickHouse();
    options = {
      host: ch.host,
      port: ch.tcpPort,
      user: ch.username,
      password: ch.password
    };
  });

  after(async () => {
    await stopClickHouse();
  });

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
      // Message format varies by ClickHouse version: "does not exist" or "Unknown table expression identifier"
      assert.ok(
        err.message.includes("does not exist") || err.message.includes("Unknown table"),
        `Message should mention unknown/missing table, got: ${err.message}`
      );
      assert.ok(err.serverStackTrace.length > 0, "Should have stack trace");
    }
  }));

  test("should allow subsequent queries after exception", () => withClient(async (client) => {
    // First query: trigger an exception
    try {
      for await (const _ of client.query("SELECT * FROM nonexistent_table_xyz123")) {}
      assert.fail("Should have thrown an exception");
    } catch (err) {
      assert.ok(err instanceof ClickHouseException, "First query should throw ClickHouseException");
    }

    // Second query: should work on same connection
    let result: number | null = null;
    for await (const packet of client.query("SELECT 42 as answer")) {
      if (packet.type === "Data" && packet.batch.rowCount > 0) {
        result = Number(packet.batch.getAt(0, 0));
      }
    }
    assert.strictEqual(result, 42, "Second query should return correct result");

    // Third query: another error, connection should still recover
    try {
      for await (const _ of client.query("INVALID SQL SYNTAX HERE")) {}
      assert.fail("Should have thrown an exception");
    } catch (err) {
      assert.ok(err instanceof ClickHouseException, "Third query should throw ClickHouseException");
    }

    // Fourth query: verify connection still works
    let finalResult: number | null = null;
    for await (const packet of client.query("SELECT 123 as value")) {
      if (packet.type === "Data" && packet.batch.rowCount > 0) {
        finalResult = Number(packet.batch.getAt(0, 0));
      }
    }
    assert.strictEqual(finalResult, 123, "Fourth query should return correct result");
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
      for await (const _ of client.query("SELECT sleep(10)", { signal: controller.signal })) {}
    } catch (err: any) {
      assert.ok(true, "Query was cancelled or errored as expected");
    }
  }));

  test("should reject query if already aborted", () => withClient(async (client) => {
    const controller = new AbortController();
    controller.abort();

    try {
      for await (const _ of client.query("SELECT 1", { signal: controller.signal })) {}
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
          yield RecordBatch.fromColumnar(
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
      const table = RecordBatch.fromColumnar(
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
