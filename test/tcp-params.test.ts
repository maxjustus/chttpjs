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
});
