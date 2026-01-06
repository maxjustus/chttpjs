import { describe, it, after } from "node:test";
import assert from "node:assert";
import {
  ClickHouseContainer,
  StartedClickHouseContainer,
} from "@testcontainers/clickhouse";
import { TcpClient } from "../tcp_client/client.ts";
import { ChunkedProtocolMode } from "../tcp_client/types.ts";

// ClickHouse config that enables chunked protocol
const CHUNKED_CONFIG = `<clickhouse>
    <listen_host>::</listen_host>
    <tcp_port>9000</tcp_port>
    <http_port>8123</http_port>
    <max_table_size_to_drop>0</max_table_size_to_drop>
    <path>/var/lib/clickhouse/</path>
    <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
    <format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
    <logger>
        <level>warning</level>
        <console>1</console>
    </logger>
    <user_directories>
        <users_xml>
            <path>/etc/clickhouse-server/users.xml</path>
        </users_xml>
    </user_directories>
    <proto_caps>
        <send>chunked_optional</send>
        <recv>chunked_optional</recv>
    </proto_caps>
</clickhouse>
`;

let container: StartedClickHouseContainer | undefined;

async function startChunkedClickHouse(version = "25.8") {
  console.log("Starting ClickHouse container with chunked protocol...");

  container = await new ClickHouseContainer(
    `clickhouse/clickhouse-server:${version}`,
  )
    .withDatabase("default")
    .withUsername("default")
    .withPassword("password")
    .withCopyContentToContainer([
      {
        content: CHUNKED_CONFIG,
        target: "/etc/clickhouse-server/config.xml",
      },
    ])
    .start();

  const host = container.getHost();
  const tcpPort = container.getMappedPort(9000);
  console.log(`ClickHouse started with chunked protocol at ${host}:${tcpPort}`);

  return {
    host,
    tcpPort,
    username: "default",
    password: "password",
  };
}

async function queryScalar(client: TcpClient, sql: string): Promise<unknown> {
  const stream = client.query(sql);
  for await (const packet of stream) {
    if (packet.type === "Data" && packet.batch.rowCount > 0) {
      return packet.batch.getAt(0, 0);
    }
  }
  throw new Error(`No rows returned for: ${sql}`);
}

async function collectRows(client: TcpClient, sql: string): Promise<unknown[][]> {
  const rows: unknown[][] = [];
  for await (const packet of client.query(sql)) {
    if (packet.type === "Data") {
      for (let i = 0; i < packet.batch.rowCount; i++) {
        const row: unknown[] = [];
        for (let j = 0; j < packet.batch.columnData.length; j++) {
          row.push(packet.batch.getAt(i, j));
        }
        rows.push(row);
      }
    }
  }
  return rows;
}

describe("TCP chunked protocol", { timeout: 120000 }, () => {
  after(async () => {
    if (container) {
      console.log("Stopping ClickHouse container...");
      await container.stop();
    }
  });

  it("negotiates chunked mode when server supports it", async () => {
    const ch = await startChunkedClickHouse();
    const client = new TcpClient({
      host: ch.host,
      port: ch.tcpPort,
      user: ch.username,
      password: ch.password,
      chunkedMode: ChunkedProtocolMode.ChunkedOptional,
    });

    try {
      await client.connect();
      assert.ok(client.serverHello, "serverHello should be set after connect");

      // Verify chunked mode was negotiated
      assert.strictEqual(
        client.serverHello.chunkedSend,
        ChunkedProtocolMode.Chunked,
        "Expected chunked mode for send direction",
      );
      assert.strictEqual(
        client.serverHello.chunkedRecv,
        ChunkedProtocolMode.Chunked,
        "Expected chunked mode for recv direction",
      );

      // Run a simple query to verify communication works
      const version = await queryScalar(client, "SELECT version()");
      assert.ok(typeof version === "string" && version.length > 0);
      console.log(`Query succeeded with chunked protocol, server version: ${version}`);
    } finally {
      client.close();
    }
  });

  it("queries work with chunked framing", async () => {
    const ch = await startChunkedClickHouse();
    const client = new TcpClient({
      host: ch.host,
      port: ch.tcpPort,
      user: ch.username,
      password: ch.password,
      chunkedMode: ChunkedProtocolMode.ChunkedOptional,
    });

    try {
      await client.connect();

      // Query that returns multiple rows
      const rows = await collectRows(
        client,
        "SELECT number, toString(number) as str FROM system.numbers LIMIT 100",
      );

      assert.strictEqual(rows.length, 100, "Expected 100 rows");
      assert.deepStrictEqual(rows[0], [0n, "0"]);
      assert.deepStrictEqual(rows[99], [99n, "99"]);
    } finally {
      client.close();
    }
  });

  it("inserts work with chunked framing", async () => {
    const ch = await startChunkedClickHouse();
    const client = new TcpClient({
      host: ch.host,
      port: ch.tcpPort,
      user: ch.username,
      password: ch.password,
      chunkedMode: ChunkedProtocolMode.ChunkedOptional,
    });

    try {
      await client.connect();

      const tableName = `test_chunked_${Date.now()}`;

      // Create table
      for await (const _ of client.query(`CREATE TABLE ${tableName} (id UInt64, name String) ENGINE = Memory`)) {}

      // Insert data using RecordBatch
      const { RecordBatchBuilder } = await import("../native/index.ts");
      const builder = new RecordBatchBuilder([
        { name: "id", type: "UInt64" },
        { name: "name", type: "String" },
      ]);
      builder.appendRow([1n, "Alice"]);
      builder.appendRow([2n, "Bob"]);
      builder.appendRow([3n, "Charlie"]);
      const batch = builder.finish();

      await client.insert(`INSERT INTO ${tableName} VALUES`, batch);

      // Verify inserted data
      const rows = await collectRows(client, `SELECT * FROM ${tableName} ORDER BY id`);
      assert.strictEqual(rows.length, 3);
      assert.deepStrictEqual(rows[0], [1n, "Alice"]);
      assert.deepStrictEqual(rows[1], [2n, "Bob"]);
      assert.deepStrictEqual(rows[2], [3n, "Charlie"]);

      // Cleanup
      for await (const _ of client.query(`DROP TABLE ${tableName}`)) {}
    } finally {
      client.close();
    }
  });

  it("falls back to notchunked with older server versions", async () => {
    // ClickHouse 23.8 doesn't support chunked protocol
    console.log("Starting ClickHouse 23.8 (no chunked support)...");

    const oldContainer = await new ClickHouseContainer(
      "clickhouse/clickhouse-server:23.8",
    )
      .withDatabase("default")
      .withUsername("default")
      .withPassword("password")
      .start();

    const client = new TcpClient({
      host: oldContainer.getHost(),
      port: oldContainer.getMappedPort(9000),
      user: "default",
      password: "password",
      chunkedMode: ChunkedProtocolMode.ChunkedOptional,
    });

    try {
      await client.connect();
      assert.ok(client.serverHello);

      // Older servers should negotiate notchunked
      assert.strictEqual(
        client.serverHello.chunkedSend,
        ChunkedProtocolMode.NotChunked,
        "Expected notchunked for send with old server",
      );
      assert.strictEqual(
        client.serverHello.chunkedRecv,
        ChunkedProtocolMode.NotChunked,
        "Expected notchunked for recv with old server",
      );

      // Query should still work
      const version = await queryScalar(client, "SELECT version()");
      assert.ok(typeof version === "string" && version.startsWith("23.8"));
    } finally {
      client.close();
      await oldContainer.stop();
    }
  });
});
