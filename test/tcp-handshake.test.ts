import { describe, it } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../tcp_client/client.ts";
import { DBMS_TCP_PROTOCOL_VERSION } from "../tcp_client/types.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";

function parseVersions(): string[] {
  const raw = process.env.CLICKHOUSE_VERSIONS;
  if (raw && raw.trim()) {
    return raw.split(",").map((v) => v.trim()).filter(Boolean);
  }
  return ["25.8", "24.8", "23.8"];
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

describe("TCP handshake revision gating", { timeout: 300000, concurrency: 1 }, () => {
  const versions = parseVersions();

  for (const version of versions) {
    it(`caps server revision for ClickHouse ${version}`, async () => {
      const ch = await startClickHouse(version);
      const client = new TcpClient({
        host: ch.host,
        port: ch.tcpPort,
        user: ch.username,
        password: ch.password,
      });

      try {
        await client.connect();
        assert.ok(client.serverHello, "serverHello should be set after connect");

        const revision = client.serverHello.revision;
        assert.ok(
          revision <= DBMS_TCP_PROTOCOL_VERSION,
          `Server revision ${revision} should not exceed client max ${DBMS_TCP_PROTOCOL_VERSION}`,
        );

        const major = client.serverHello.major;
        const minor = client.serverHello.minor;
        const isAtLeastMax = major > 25n || (major === 25n && minor >= 8n);
        if (isAtLeastMax) {
          assert.strictEqual(
            revision,
            DBMS_TCP_PROTOCOL_VERSION,
            `Expected revision cap at ${DBMS_TCP_PROTOCOL_VERSION} for ${major}.${minor}`,
          );
        } else {
          assert.ok(
            revision < DBMS_TCP_PROTOCOL_VERSION,
            `Expected revision < ${DBMS_TCP_PROTOCOL_VERSION} for ${major}.${minor}`,
          );
        }

        const versionValue = await queryScalar(client, "SELECT version()");
        assert.ok(typeof versionValue === "string" && (versionValue as string).length > 0);
      } finally {
        client.close();
        await stopClickHouse();
      }
    });
  }
});
