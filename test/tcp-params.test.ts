import assert from "node:assert";
import { describe, it, before, after } from "node:test";
import { TcpClient, type QueryParamValue } from "../tcp_client/client.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";

let ch: Awaited<ReturnType<typeof startClickHouse>>;
let client: TcpClient;

async function queryScalar(
  sql: string,
  params?: Record<string, QueryParamValue>,
): Promise<unknown> {
  const stream = client.query(sql, { params });
  for await (const packet of stream) {
    if (packet.type === "Data" && packet.batch.rowCount > 0) {
      return packet.batch.getAt(0, 0);
    }
  }
  throw new Error(`No rows returned for: ${sql}`);
}

describe("TCP Query Parameters", { timeout: 60000 }, () => {
  before(async () => {
    ch = await startClickHouse();
    client = new TcpClient({
      host: ch.host,
      port: ch.tcpPort,
      user: ch.username,
      password: ch.password,
    });
    await client.connect();
  });

  after(async () => {
    await client.close();
    await stopClickHouse();
  });

  it("handles UInt64 param", async () => {
    const result = await queryScalar("SELECT {val: UInt64}", { val: 42 });
    assert.strictEqual(Number(result), 42);
  });

  it("handles String param", async () => {
    const result = await queryScalar("SELECT {s: String}", { s: "hello world" });
    assert.strictEqual(result, "hello world");
  });

  it("handles String param with special chars", async () => {
    const testString = "it's 5 o'clock\nnewline\ttab\\backslash";
    const result = await queryScalar("SELECT {s: String}", { s: testString });
    assert.strictEqual(result, testString);
  });

  it("handles Array(UInt64) param", async () => {
    const result = await queryScalar("SELECT arraySum({ids: Array(UInt64)})", {
      ids: [1, 2, 3, 4, 5],
    });
    assert.strictEqual(Number(result), 15);
  });

  it("handles Tuple(Int32, Int32) param", async () => {
    const result = await queryScalar(
      "SELECT tupleElement({point: Tuple(Int32, Int32)}, 1) + tupleElement({point: Tuple(Int32, Int32)}, 2)",
      { point: [10, 20] },
    );
    assert.strictEqual(Number(result), 30);
  });

  it("handles Map(String, UInt32) param", async () => {
    const result = await queryScalar("SELECT {m: Map(String, UInt32)}['a']", {
      m: { a: 42, b: 99 },
    });
    assert.strictEqual(Number(result), 42);
  });

  it("handles nested Array(Tuple(Int32, Int32)) param", async () => {
    const result = await queryScalar(
      "SELECT arraySum(arrayMap(t -> tupleElement(t, 1) + tupleElement(t, 2), {points: Array(Tuple(Int32, Int32))}))",
      {
        points: [
          [1, 2],
          [3, 4],
          [5, 6],
        ],
      },
    );
    assert.strictEqual(Number(result), 21); // (1+2) + (3+4) + (5+6)
  });

  it("handles DateTime64(3) param", async () => {
    const testDate = new Date("2024-06-15T10:30:45.123Z");
    const result = await queryScalar("SELECT toUnixTimestamp64Milli({ts: DateTime64(3)})", {
      ts: testDate,
    });
    assert.strictEqual(Number(result), testDate.getTime());
  });

  it("handles same param multiple times", async () => {
    const result = await queryScalar("SELECT {id: UInt64} + {id: UInt64}", { id: 21 });
    assert.strictEqual(Number(result), 42);
  });

  it("handles Enum param", async () => {
    const result = await queryScalar("SELECT {status: Enum8('active' = 1, 'inactive' = 2)}", {
      status: "active",
    });
    assert.strictEqual(result, "active");
  });

  it("handles UUID param", async () => {
    const testUUID = "550e8400-e29b-41d4-a716-446655440000";
    const result = await queryScalar("SELECT {id: UUID}", { id: testUUID });
    assert.strictEqual(result, testUUID);
  });

  it("handles IPv4 param", async () => {
    const result = await queryScalar("SELECT {ip: IPv4}", { ip: "192.168.1.1" });
    assert.strictEqual(result, "192.168.1.1");
  });

  it("handles IPv6 param", async () => {
    const result = await queryScalar("SELECT {ip: IPv6}", { ip: "2001:db8::1" });
    assert.ok(String(result).includes("2001:db8"));
  });

  it("handles Date param", async () => {
    const result = await queryScalar("SELECT {d: Date}", { d: new Date("2024-06-15") });
    assert.ok(result instanceof Date);
    assert.strictEqual((result as Date).toISOString().slice(0, 10), "2024-06-15");
  });

  it("handles Decimal param", async () => {
    const result = await queryScalar("SELECT {d: Decimal(10, 2)}", { d: "123.45" });
    assert.strictEqual(Number(result), 123.45);
  });

  // Verifies that IPv4 encoding/decoding via native format works correctly.
  // This test was added after fixing an endianness bug where IPv4 addresses
  // were decoded with reversed octets (192.168.1.1 became 1.1.168.192).
  it("round-trips IPv4 through insert and select", async () => {
    const tableName = `test_ipv4_${Date.now()}`;
    const testIPs = ["192.168.1.1", "10.0.0.1", "172.16.254.99", "255.255.255.0"];

    // Create table
    for await (const _ of client.query(`CREATE TABLE ${tableName} (ip IPv4) ENGINE = Memory`)) {
    }

    // Insert using native format
    const rows = testIPs.map((ip) => ({ ip }));
    for await (const _ of client.insert(`INSERT INTO ${tableName} VALUES`, rows)) {
    }

    // Read back via native format
    const results: string[] = [];
    const stream = client.query(`SELECT ip FROM ${tableName} ORDER BY ip`);
    for await (const packet of stream) {
      if (packet.type === "Data") {
        for (let i = 0; i < packet.batch.rowCount; i++) {
          results.push(packet.batch.getAt(i, 0) as string);
        }
      }
    }

    // Sort both arrays for comparison (ORDER BY ip gives numeric order, not string order)
    const sortedExpected = [...testIPs].sort();
    const sortedResults = [...results].sort();

    assert.deepStrictEqual(sortedResults, sortedExpected);

    // Cleanup (Memory tables are dropped automatically when server restarts anyway)
    for await (const _ of client.query(`DROP TABLE IF EXISTS ${tableName}`)) {
    }
  });
});
