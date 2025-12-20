
import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { batchFromRows } from "../../native/table.ts";
import { type ColumnDef } from "../../native/types.ts";

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
          rowsFound += packet.batch.rowCount;
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

      const batch = batchFromRows([
        { name: "id", type: "UInt64" },
        { name: "name", type: "String" }
      ], [
        [1n, "Alice"],
        [2n, "Bob"]
      ]);

      await client.insert(`INSERT INTO ${tableName} VALUES`, batch);

      // Verify
      const stream = client.query(`SELECT * FROM ${tableName} ORDER BY id`);
      const allRows: any[] = [];
      for await (const packet of stream) {
        if (packet.type === "Data") {
          for (const row of packet.batch.rows()) {
            allRows.push(row.toObject());
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
        if (packet.type === "Data") count += packet.batch.rowCount;
      }
      assert.strictEqual(count, 100);
    } finally {
      client.close();
    }
  });

  test("should insert row objects with auto-coercion", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const tableName = `test_row_objects_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt32, name String, value Float64) ENGINE = Memory`);

      // Insert row objects - types will be coerced based on server schema
      await client.insert(`INSERT INTO ${tableName} VALUES`, [
        { id: 1, name: "alice", value: 1.5 },
        { id: 2, name: "bob", value: 2.5 },
        { id: 3, name: "charlie", value: 3.5 },
      ]);

      // Verify
      const stream = client.query(`SELECT * FROM ${tableName} ORDER BY id`);
      const allRows: any[] = [];
      for await (const packet of stream) {
        if (packet.type === "Data") {
          for (const row of packet.batch.rows()) {
            allRows.push(row.toObject());
          }
        }
      }

      assert.strictEqual(allRows.length, 3);
      assert.strictEqual(allRows[0].id, 1);
      assert.strictEqual(allRows[0].name, "alice");
      assert.strictEqual(allRows[1].id, 2);
      assert.strictEqual(allRows[2].value, 3.5);

      await client.execute(`DROP TABLE ${tableName}`);
    } finally {
      client.close();
    }
  });

  test("should insert row objects from generator with batching", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const tableName = `test_generator_rows_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt32, value String) ENGINE = Memory`);

      function* generateRows() {
        for (let i = 0; i < 250; i++) {
          yield { id: i, value: `row_${i}` };
        }
      }

      await client.insert(`INSERT INTO ${tableName} VALUES`, generateRows(), { batchSize: 100 });

      const stream = client.query(`SELECT count() as cnt FROM ${tableName}`);
      let count = 0;
      for await (const packet of stream) {
        if (packet.type === "Data") {
          count = Number(packet.batch.getAt(0, 0));
        }
      }

      assert.strictEqual(count, 250);

      await client.execute(`DROP TABLE ${tableName}`);
    } finally {
      client.close();
    }
  });

  test("should validate schema when provided", async () => {
    const client = new TcpClient(options);
    await client.connect();
    try {
      const tableName = `test_schema_valid_${Date.now()}`;
      await client.execute(`CREATE TABLE ${tableName} (id UInt32, name String) ENGINE = Memory`);

      const schema: ColumnDef[] = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];

      await client.insert(`INSERT INTO ${tableName} VALUES`, [
        { id: 1, name: "test" },
      ], { schema });

      const stream = client.query(`SELECT count() as cnt FROM ${tableName}`);
      let count = 0;
      for await (const packet of stream) {
        if (packet.type === "Data") {
          count = Number(packet.batch.getAt(0, 0));
        }
      }
      assert.strictEqual(count, 1);

      await client.execute(`DROP TABLE ${tableName}`);
    } finally {
      client.close();
    }
  });

  test("should throw on schema mismatch - wrong type", async () => {
    const tableName = `test_schema_mismatch_${Date.now()}`;

    const setupClient = new TcpClient(options);
    await setupClient.connect();
    await setupClient.execute(`CREATE TABLE ${tableName} (id UInt32, name String) ENGINE = Memory`);
    setupClient.close();

    const client = new TcpClient(options);
    await client.connect();
    const wrongSchema: ColumnDef[] = [
      { name: "id", type: "UInt64" },  // Wrong type!
      { name: "name", type: "String" },
    ];

    await assert.rejects(
      () => client.insert(`INSERT INTO ${tableName} VALUES`, [{ id: 1, name: "test" }], { schema: wrongSchema }),
      /Schema mismatch.*UInt64.*UInt32/
    );
    client.close();

    const cleanupClient = new TcpClient(options);
    await cleanupClient.connect();
    await cleanupClient.execute(`DROP TABLE ${tableName}`);
    cleanupClient.close();
  });

  test("should throw on schema mismatch - wrong column name", async () => {
    const tableName = `test_schema_name_${Date.now()}`;

    const setupClient = new TcpClient(options);
    await setupClient.connect();
    await setupClient.execute(`CREATE TABLE ${tableName} (id UInt32, name String) ENGINE = Memory`);
    setupClient.close();

    const client = new TcpClient(options);
    await client.connect();
    const wrongSchema: ColumnDef[] = [
      { name: "user_id", type: "UInt32" },  // Wrong name!
      { name: "name", type: "String" },
    ];

    await assert.rejects(
      () => client.insert(`INSERT INTO ${tableName} VALUES`, [{ id: 1, name: "test" }], { schema: wrongSchema }),
      /Schema mismatch.*user_id.*id/
    );
    client.close();

    const cleanupClient = new TcpClient(options);
    await cleanupClient.connect();
    await cleanupClient.execute(`DROP TABLE ${tableName}`);
    cleanupClient.close();
  });

  test("should throw on schema mismatch - wrong column count", async () => {
    const tableName = `test_schema_count_${Date.now()}`;

    const setupClient = new TcpClient(options);
    await setupClient.connect();
    await setupClient.execute(`CREATE TABLE ${tableName} (id UInt32, name String) ENGINE = Memory`);
    setupClient.close();

    const client = new TcpClient(options);
    await client.connect();
    const wrongSchema: ColumnDef[] = [
      { name: "id", type: "UInt32" },
      // Missing 'name' column
    ];

    await assert.rejects(
      () => client.insert(`INSERT INTO ${tableName} VALUES`, [{ id: 1 }], { schema: wrongSchema }),
      /Schema mismatch.*expected 1 columns.*got 2/
    );
    client.close();

    const cleanupClient = new TcpClient(options);
    await cleanupClient.connect();
    await cleanupClient.execute(`DROP TABLE ${tableName}`);
    cleanupClient.close();
  });
});
