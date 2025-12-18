
import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { asRows } from "../../formats/native/index.ts";
import { tableFromRows } from "../../formats/native/table.ts";

describe("TCP Client Multi-block Integration", () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: "",
    debug: true
  };

  test("should handle multi-block SELECT queries", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const tableName = `test_tcp_multi_${Date.now()}`;
      // Use 100k rows to ensure multiple blocks (default block size is 65536)
      const rowCount = 100000;
      
      await client.execute(`CREATE TABLE ${tableName} (id UInt64, name String) ENGINE = Memory`);
      await client.execute(`INSERT INTO ${tableName} SELECT number, 'row_' || toString(number) FROM numbers(${rowCount})`);
      
      console.log(`Querying ${rowCount} rows...`);
      const stream = client.query(`SELECT * FROM ${tableName}`);
      
      let totalRows = 0;
      let blockCount = 0;
      for await (const packet of stream) {
        if (packet.type === "Data") {
          blockCount++;
          totalRows += packet.table.rowCount;
        }
      }
      
      console.log(`Received ${totalRows} rows in ${blockCount} blocks.`);
      assert.strictEqual(totalRows, rowCount, "Total row count mismatch");
      assert.ok(blockCount > 1, "Should have received multiple blocks");
      
      await client.execute(`DROP TABLE ${tableName}`);
    } finally {
      client.close();
    }
  });

  test("should handle multi-block INSERT queries", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const tableName = `test_tcp_multi_ins_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt64, name String) ENGINE = Memory`);
      
      const blockCount = 10;
      const rowsPerBlock = 1000;
      
      async function* generateBlocks() {
        for (let i = 0; i < blockCount; i++) {
          const rows = [];
          for (let j = 0; j < rowsPerBlock; j++) {
            const id = BigInt(i * rowsPerBlock + j);
            rows.push([id, `name_${id}`]);
          }
          yield tableFromRows([
            { name: "id", type: "UInt64" },
            { name: "name", type: "String" }
          ], rows);
        }
      }

      console.log(`Inserting ${blockCount * rowsPerBlock} rows in ${blockCount} blocks...`);
      await client.insert(`INSERT INTO ${tableName} VALUES`, generateBlocks());
      
      // Verify
      const stream = client.query(`SELECT count() FROM ${tableName}`);
      let totalCount = 0n;
      for await (const packet of stream) {
        if (packet.type === "Data") {
          for (const row of asRows(packet.table)) {
            totalCount = row["count()"] as bigint;
          }
        }
      }
      
      console.log(`Verified ${totalCount} rows in table.`);
      assert.strictEqual(Number(totalCount), blockCount * rowsPerBlock);
      
      await client.execute(`DROP TABLE ${tableName}`);
    } finally {
      client.close();
    }
  });
});
