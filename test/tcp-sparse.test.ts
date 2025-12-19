/**
 * Integration tests for Sparse serialization and Compression in Native format via TCP.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../tcp_client/client.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { toArrayRows } from "./test_utils.ts";

describe("TCP sparse deserialization", { timeout: 120000 }, () => {
  let chConfig: { host: string; tcpPort: number; username: string; password: string };

  before(async () => {
    const ch = await startClickHouse();
    chConfig = { host: ch.host, tcpPort: ch.tcpPort, username: ch.username, password: ch.password };
    // Wait for container to be fully ready
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Create test table
    const setupClient = new TcpClient({
      host: chConfig.host,
      port: chConfig.tcpPort,
      user: chConfig.username,
      password: chConfig.password,
    });
    await setupClient.connect();

    const table = "test_tcp_sparse";
    await setupClient.execute(`DROP TABLE IF EXISTS ${table}`);
    await setupClient.execute(`
      CREATE TABLE ${table} (
        id UInt32,
        val UInt64
      ) ENGINE = MergeTree
      ORDER BY id
      SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0001
    `);

    // Insert 10000 rows, only 2 non-zero
    const rows = [];
    const rowCount = 10000;
    for (let i = 0; i < rowCount; i++) {
      if (i === 10) rows.push(`(${i}, 123456789)`);
      else if (i === 5000) rows.push(`(${i}, 987654321)`);
      else rows.push(`(${i}, 0)`);
    }
    await setupClient.execute(`INSERT INTO ${table} VALUES ${rows.join(",")}`);
    await setupClient.execute(`OPTIMIZE TABLE ${table} FINAL`);
    setupClient.close();
  });

  after(async () => {
    await stopClickHouse();
  });

  async function runSparseTest(compression: boolean) {
    const client = new TcpClient({
      host: chConfig.host,
      port: chConfig.tcpPort,
      user: chConfig.username,
      password: chConfig.password,
      compression,
      debug: true,
    });
    await client.connect();

    try {
      const packets = client.query(
        `SELECT * FROM test_tcp_sparse ORDER BY id`,
        { "allow_special_serialization_kinds_in_output_formats": "1" }
      );

      let totalRows = 0;
      for await (const packet of packets) {
        if (packet.type === "Data") {
          const batch = packet.batch;
          const decodedRows = toArrayRows(batch);
          totalRows += batch.rowCount;

          // Check specific values if we have enough rows
          if (decodedRows.length > 10) {
            assert.strictEqual(decodedRows[10][1], 123456789n, "Row 10 should have value 123456789");
          }
          if (decodedRows.length > 5000) {
            assert.strictEqual(decodedRows[5000][1], 987654321n, "Row 5000 should have value 987654321");
          }
        }
      }
      assert.strictEqual(totalRows, 10000, "Should receive 10000 rows total");
    } finally {
      client.close();
    }
  }

  it("reads sparse data without compression", async () => {
    await runSparseTest(false);
  });

  it("reads sparse data with compression", async () => {
    await runSparseTest(true);
  });
});
