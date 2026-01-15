/**
 * Integration tests for Sparse serialization and Compression in Native format via TCP.
 */

import assert from "node:assert";
import { after, before, describe, it } from "node:test";
import { TcpClient } from "../tcp_client/client.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { toArrayRows } from "./test_utils.ts";

describe("TCP sparse deserialization", { timeout: 120000 }, () => {
  let chConfig: { host: string; tcpPort: number; username: string; password: string };

  before(async () => {
    const ch = await startClickHouse();
    chConfig = { host: ch.host, tcpPort: ch.tcpPort, username: ch.username, password: ch.password };
    // Wait for container to be fully ready
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Create test table
    const setupClient = new TcpClient({
      host: chConfig.host,
      port: chConfig.tcpPort,
      user: chConfig.username,
      password: chConfig.password,
    });
    await setupClient.connect();

    const table = "test_tcp_sparse";
    await setupClient.query(`DROP TABLE IF EXISTS ${table}`);
    await setupClient.query(`
      CREATE TABLE ${table} (
        id UInt32,
        val UInt64
      ) ENGINE = MergeTree
      ORDER BY id
      SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0001
    `);

    // Insert 10000 rows, only 2 non-zero
    const rows: any[] = [];
    const rowCount = 10000;
    for (let i = 0; i < rowCount; i++) {
      if (i === 10) rows.push(`(${i}, 123456789)`);
      else if (i === 5000) rows.push(`(${i}, 987654321)`);
      else rows.push(`(${i}, 0)`);
    }
    await setupClient.query(`INSERT INTO ${table} VALUES ${rows.join(",")}`);
    await setupClient.query(`OPTIMIZE TABLE ${table} FINAL`);

    // Create Variant sparse test table
    const variantTable = "test_tcp_variant_sparse";
    await setupClient.query(`DROP TABLE IF EXISTS ${variantTable}`);
    await setupClient.query(`
      CREATE TABLE ${variantTable} (
        id UInt32,
        v Variant(String, UInt64)
      ) ENGINE = MergeTree
      ORDER BY id
      SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0001
    `);

    // Insert 10000 rows, mostly NULL to trigger sparse serialization
    const variantRows: string[] = [];
    for (let i = 0; i < rowCount; i++) {
      if (i === 50) variantRows.push(`(${i}, 'hello')`);
      else if (i === 5000) variantRows.push(`(${i}, 42)`);
      else variantRows.push(`(${i}, NULL)`);
    }
    await setupClient.query(`INSERT INTO ${variantTable} VALUES ${variantRows.join(",")}`);
    await setupClient.query(`OPTIMIZE TABLE ${variantTable} FINAL`);

    setupClient.close();
  });

  after(async () => {
    await stopClickHouse();
  });

  for (const compression of [false, "lz4"] as const) {
    it(`reads sparse data ${compression ? "with" : "without"} compression`, async () => {
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
        const packets = client.query(`SELECT * FROM test_tcp_sparse ORDER BY id`);

        let totalRows = 0;
        for await (const packet of packets) {
          if (packet.type === "Data") {
            const batch = packet.batch;
            const decodedRows = toArrayRows(batch);
            totalRows += batch.rowCount;

            // Check specific values if we have enough rows
            if (decodedRows.length > 10) {
              assert.strictEqual(
                decodedRows[10][1],
                123456789n,
                "Row 10 should have value 123456789",
              );
            }
            if (decodedRows.length > 5000) {
              assert.strictEqual(
                decodedRows[5000][1],
                987654321n,
                "Row 5000 should have value 987654321",
              );
            }
          }
        }
        assert.strictEqual(totalRows, 10000, "Should receive 10000 rows total");
      } finally {
        client.close();
      }
    });
  }

  it("reads sparse Variant data (tests skipSerializationTree with Variant)", async () => {
    const client = new TcpClient({
      host: chConfig.host,
      port: chConfig.tcpPort,
      user: chConfig.username,
      password: chConfig.password,
    });
    await client.connect();

    try {
      const packets = client.query(`SELECT * FROM test_tcp_variant_sparse ORDER BY id`);

      let totalRows = 0;
      for await (const packet of packets) {
        if (packet.type === "Data") {
          const batch = packet.batch;
          const decodedRows = toArrayRows(batch);
          totalRows += batch.rowCount;

          // Check specific values if we have enough rows
          if (decodedRows.length > 50) {
            assert.deepStrictEqual(
              decodedRows[50][1],
              [0, "hello"],
              "Row 50 should have Variant(String) = 'hello'",
            );
          }
          if (decodedRows.length > 5000) {
            assert.deepStrictEqual(
              decodedRows[5000][1],
              [1, 42n],
              "Row 5000 should have Variant(UInt64) = 42",
            );
          }
        }
      }
      assert.strictEqual(totalRows, 10000, "Should receive 10000 rows total");
    } finally {
      client.close();
    }
  });
});
