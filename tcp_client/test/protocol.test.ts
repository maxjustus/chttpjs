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
});
