
import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { tableFromRows } from "../../formats/native/table.ts";
import { asRows } from "../../formats/native/index.ts";

describe("TCP Client Integration", () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: ""
  };

  test("should connect and run a simple SELECT query", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const stream = client.query("SELECT 1 as id, 'hello' as str");
      let rowsFound = 0;
      for await (const packet of stream) {
        if (packet.type === "Data") {
          rowsFound += packet.table.rowCount;
        }
      }
      assert.strictEqual(rowsFound, 1);
    } finally {
      client.close();
    }
  });

  test("should connect and run an INSERT query", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const tableName = `test_tcp_insert_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt64, name String) ENGINE = Memory`);
      
      const table = tableFromRows([
        { name: "id", type: "UInt64" },
        { name: "name", type: "String" }
      ], [
        [1n, "Alice"],
        [2n, "Bob"]
      ]);

      await client.insert(`INSERT INTO ${tableName} VALUES`, table);
      
      // Verify
      const stream = client.query(`SELECT * FROM ${tableName} ORDER BY id`);
      const allRows: any[] = [];
      for await (const packet of stream) {
        if (packet.type === "Data") {
          for (const row of asRows(packet.table)) {
            allRows.push(row);
          }
        }
      }
      
      assert.strictEqual(allRows.length, 2, "Should have 2 rows");
      assert.deepStrictEqual(allRows[0], { id: 1n, name: "Alice" });
      assert.deepStrictEqual(allRows[1], { id: 2n, name: "Bob" });
      
      await client.execute(`DROP TABLE ${tableName}`);
    } finally {
      client.close();
    }
  });

  test("should run a larger query from system.numbers", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const stream = client.query("SELECT * FROM system.numbers LIMIT 100");
      let count = 0;
      for await (const packet of stream) {
        if (packet.type === "Data") count += packet.table.rowCount;
      }
      assert.strictEqual(count, 100);
    } finally {
      client.close();
    }
  });
});
