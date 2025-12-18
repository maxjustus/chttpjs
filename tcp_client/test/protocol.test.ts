import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";

describe("TCP Client Protocol Features", () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: ""
  };

  test("should handle WITH TOTALS", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      let gotTotals = false;
      let dataRows = 0;
      for await (const packet of client.query(
        "SELECT count() as cnt FROM numbers(100) GROUP BY number % 10 WITH TOTALS"
      )) {
        if (packet.type === "Data") dataRows += packet.table.rowCount;
        if (packet.type === "Totals") gotTotals = true;
      }
      assert.ok(gotTotals, "Should receive Totals packet");
      assert.strictEqual(dataRows, 10, "Should have 10 data rows");
    } finally {
      client.close();
    }
  });

  test("should handle extremes setting", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      let gotExtremes = false;
      for await (const packet of client.query(
        "SELECT number FROM numbers(100)",
        { extremes: 1 }
      )) {
        if (packet.type === "Extremes") gotExtremes = true;
      }
      assert.ok(gotExtremes, "Should receive Extremes packet");
    } finally {
      client.close();
    }
  });

  test("should use ZSTD compression", async () => {
    const client = new TcpClient({ ...options, compression: 'zstd' });
    await client.connect();
    try {
      let rows = 0;
      for await (const packet of client.query("SELECT * FROM numbers(1000)")) {
        if (packet.type === "Data") rows += packet.table.rowCount;
      }
      assert.strictEqual(rows, 1000, "Should receive all rows with ZSTD compression");
    } finally {
      client.close();
    }
  });

  test("should use LZ4 compression by default when enabled", async () => {
    const client = new TcpClient({ ...options, compression: true });
    await client.connect();
    try {
      let rows = 0;
      for await (const packet of client.query("SELECT * FROM numbers(1000)")) {
        if (packet.type === "Data") rows += packet.table.rowCount;
      }
      assert.strictEqual(rows, 1000, "Should receive all rows with LZ4 compression");
    } finally {
      client.close();
    }
  });

  test("should handle typed settings (number/boolean)", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      let rows = 0;
      // Using typed settings values
      for await (const packet of client.query(
        "SELECT * FROM numbers(10)",
        { max_threads: 2, log_queries: false }
      )) {
        if (packet.type === "Data") rows += packet.table.rowCount;
      }
      assert.strictEqual(rows, 10, "Should work with typed settings");
    } finally {
      client.close();
    }
  });

  test("should handle Log packets when send_logs_level is set", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      let gotLog = false;
      for await (const packet of client.query(
        "SELECT 1",
        { send_logs_level: "trace" }
      )) {
        if (packet.type === "Log") {
          gotLog = true;
          assert.ok(packet.entries.length > 0, "Log should have entries");
          assert.ok(typeof packet.entries[0].text === "string", "Log entry should have text");
        }
      }
      // Log packets are optional - server may or may not send them
      // Just verify we can handle them without error
      console.log(`  (Log packets received: ${gotLog})`);
    } finally {
      client.close();
    }
  });

  test("should accumulate ProfileEvents across packets", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      let packetCount = 0;
      let lastAccumulated: Map<string, bigint> | null = null;

      // Use frequent profile events to get multiple packets
      for await (const packet of client.query(
        "SELECT sleep(0.05), number FROM numbers(10)",
        { send_profile_events: 1, profile_events_delay_ms: 25 }
      )) {
        if (packet.type === "ProfileEvents") {
          packetCount++;
          lastAccumulated = packet.accumulated;
          // Verify accumulated is a Map with entries
          assert.ok(packet.accumulated instanceof Map, "accumulated should be a Map");
        }
      }

      assert.ok(packetCount > 0, "Should receive at least one ProfileEvents packet");
      assert.ok(lastAccumulated!.size > 0, "Should have accumulated events");
      // SelectedRows should be present and match our query
      assert.strictEqual(lastAccumulated!.get("SelectedRows"), 10n, "SelectedRows should match");
      console.log(`  (ProfileEvents packets: ${packetCount}, accumulated entries: ${lastAccumulated!.size})`);
    } finally {
      client.close();
    }
  });

  test("should expose timezone getter", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      // Run a query - timezone may or may not be sent depending on server
      for await (const _ of client.query("SELECT now()")) {}
      // Just verify the getter works without error
      const tz = client.timezone;
      console.log(`  (Session timezone: ${tz ?? "not set"})`);
    } finally {
      client.close();
    }
  });

  test("should enable TCP keep-alive when configured", async () => {
    const client = new TcpClient({ ...options, keepAliveIntervalMs: 5000 });
    await client.connect();
    try {
      // Connection should work with TCP keep-alive enabled
      let rows = 0;
      for await (const packet of client.query("SELECT 1")) {
        if (packet.type === "Data") rows += packet.table.rowCount;
      }
      assert.strictEqual(rows, 1);
    } finally {
      client.close();
    }
  });

  test("should connect with TLS when configured", async () => {
    const client = new TcpClient({
      host: "localhost",
      port: 9440,
      tls: { rejectUnauthorized: false }
    });
    try {
      await client.connect();
      let rows = 0;
      for await (const packet of client.query("SELECT 1")) {
        if (packet.type === "Data") rows += packet.table.rowCount;
      }
      assert.strictEqual(rows, 1);
    } catch (err: any) {
      // TLS port may not be configured - skip gracefully
      if (err.code === 'ECONNREFUSED') {
        console.log("  (TLS port 9440 not available, skipping)");
        return;
      }
      throw err;
    } finally {
      client.close();
    }
  });
});
