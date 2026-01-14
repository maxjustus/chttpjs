import assert from "node:assert";
import { describe, it } from "node:test";
import { decodeBatch, encodeNativeRows, toArrayRows } from "../test_utils.ts";

type MatrixCase = {
  type: string;
  value: unknown;
  assert: (decoded: unknown) => void;
};

async function roundTripValue(type: string, value: unknown): Promise<unknown> {
  const columns = [{ name: "v", type }];
  const rows = [[value]];
  const encoded = encodeNativeRows(columns, rows);
  const decoded = await decodeBatch(encoded);
  return toArrayRows(decoded)[0][0];
}

describe("Native type matrix", () => {
  it("covers every supported codec type", async () => {
    const decoder = new TextDecoder();

    const cases: MatrixCase[] = [
      { type: "Int8", value: -5, assert: (v) => assert.strictEqual(v, -5) },
      { type: "Int16", value: -1234, assert: (v) => assert.strictEqual(v, -1234) },
      { type: "Int32", value: -123456, assert: (v) => assert.strictEqual(v, -123456) },
      {
        type: "Int64",
        value: -1234567890123n,
        assert: (v) => assert.strictEqual(v, -1234567890123n),
      },
      { type: "UInt8", value: 200, assert: (v) => assert.strictEqual(v, 200) },
      { type: "UInt16", value: 60000, assert: (v) => assert.strictEqual(v, 60000) },
      { type: "UInt32", value: 4000000000, assert: (v) => assert.strictEqual(v, 4000000000) },
      {
        type: "UInt64",
        value: 9000000000000n,
        assert: (v) => assert.strictEqual(v, 9000000000000n),
      },
      {
        type: "Float32",
        value: 1.25,
        assert: (v) => assert.ok(Math.abs((v as number) - 1.25) < 1e-5),
      },
      {
        type: "Float64",
        value: -1.25,
        assert: (v) => assert.ok(Math.abs((v as number) + 1.25) < 1e-12),
      },
      { type: "Bool", value: true, assert: (v) => assert.strictEqual(v, 1) },
      {
        type: "Date",
        value: new Date("2024-01-15T00:00:00Z"),
        assert: (v) =>
          assert.strictEqual((v as Date).getTime(), new Date("2024-01-15T00:00:00Z").getTime()),
      },
      {
        type: "Date32",
        value: new Date("2024-06-30T00:00:00Z"),
        assert: (v) =>
          assert.strictEqual((v as Date).getTime(), new Date("2024-06-30T00:00:00Z").getTime()),
      },
      {
        type: "DateTime",
        value: new Date("2024-01-15T10:30:00Z"),
        assert: (v) =>
          assert.strictEqual((v as Date).getTime(), new Date("2024-01-15T10:30:00Z").getTime()),
      },
      {
        type: "DateTime64(3)",
        value: new Date("2024-01-15T10:30:00.123Z"),
        assert: (v) =>
          assert.strictEqual(
            (v as { toDate(): Date }).toDate().getTime(),
            new Date("2024-01-15T10:30:00.123Z").getTime(),
          ),
      },
      { type: "String", value: "hello", assert: (v) => assert.strictEqual(v, "hello") },
      {
        type: "FixedString(5)",
        value: "hello",
        assert: (v) => assert.strictEqual(decoder.decode(v as Uint8Array), "hello"),
      },
      {
        type: "UUID",
        value: "550e8400-e29b-41d4-a716-446655440000",
        assert: (v) => assert.strictEqual(v, "550e8400-e29b-41d4-a716-446655440000"),
      },
      { type: "IPv4", value: "192.168.0.1", assert: (v) => assert.strictEqual(v, "192.168.0.1") },
      {
        type: "IPv6",
        value: "2001:db8::1",
        assert: (v) => assert.ok(typeof v === "string" && (v as string).length > 0),
      },
      { type: "Enum8('a' = 1, 'b' = 2)", value: 2, assert: (v) => assert.strictEqual(v, "b") },
      { type: "Enum16('a' = 1, 'b' = 2)", value: 1, assert: (v) => assert.strictEqual(v, "a") },
      { type: "Decimal32(2)", value: "12.34", assert: (v) => assert.strictEqual(v, "12.34") },
      {
        type: "Decimal64(4)",
        value: "-999.9999",
        assert: (v) => assert.strictEqual(v, "-999.9999"),
      },
      {
        type: "Decimal128(6)",
        value: "12345.678901",
        assert: (v) => assert.strictEqual(v, "12345.678901"),
      },
      {
        type: "Decimal256(10)",
        value: "-1234567890.0123456789",
        assert: (v) => assert.strictEqual(v, "-1234567890.0123456789"),
      },
      {
        type: "Int128",
        value: 170141183460469231731687303715884105727n,
        assert: (v) => assert.strictEqual(v, 170141183460469231731687303715884105727n),
      },
      {
        type: "UInt128",
        value: (1n << 128n) - 1n,
        assert: (v) => assert.strictEqual(v, (1n << 128n) - 1n),
      },
      {
        type: "Int256",
        value: (1n << 255n) - 1n,
        assert: (v) => assert.strictEqual(v, (1n << 255n) - 1n),
      },
      {
        type: "UInt256",
        value: (1n << 256n) - 1n,
        assert: (v) => assert.strictEqual(v, (1n << 256n) - 1n),
      },
      { type: "Nullable(Int32)", value: null, assert: (v) => assert.strictEqual(v, null) },
      {
        type: "Array(Int32)",
        value: [1, 2, 3],
        assert: (v) => assert.deepStrictEqual(Array.from(v as ArrayLike<number>), [1, 2, 3]),
      },
      {
        type: "Map(String, Int32)",
        value: { a: 1, b: 2 },
        assert: (v) =>
          assert.deepStrictEqual(Array.from(v as Map<string, number>), [
            ["a", 1],
            ["b", 2],
          ]),
      },
      {
        type: "Tuple(Int32, String)",
        value: [7, "x"],
        assert: (v) => assert.deepStrictEqual(v, [7, "x"]),
      },
      {
        type: "Tuple(x Int32, y String)",
        value: { x: 7, y: "x" },
        assert: (v) => assert.deepStrictEqual(v, { x: 7, y: "x" }),
      },
      {
        type: "Nested(id Int32, name String)",
        value: [
          { id: 1, name: "a" },
          { id: 2, name: "b" },
        ],
        assert: (v) =>
          assert.deepStrictEqual(v, [
            { id: 1, name: "a" },
            { id: 2, name: "b" },
          ]),
      },
      {
        type: "LowCardinality(String)",
        value: "active",
        assert: (v) => assert.strictEqual(v, "active"),
      },
      {
        type: "Variant(String, UInt64)",
        value: [1, 42n],
        assert: (v) => assert.deepStrictEqual(v, [1, 42n]),
      },
      {
        type: "Dynamic",
        value: 42,
        assert: (v) => assert.strictEqual(v, 42n),
      },
      {
        type: "JSON",
        value: { a: 1, b: "x" },
        assert: (v) => {
          const obj = v as Record<string, unknown>;
          assert.strictEqual(obj.a, 1n);
          assert.strictEqual(obj.b, "x");
        },
      },
      {
        type: "JSON(name String, age Int64)",
        value: { name: "alice", age: 30 },
        assert: (v) => {
          const obj = v as Record<string, unknown>;
          assert.strictEqual(obj.name, "alice");
          assert.strictEqual(obj.age, 30n);
        },
      },
      {
        type: "Point",
        value: [1.5, 2.5],
        assert: (v) => assert.deepStrictEqual(v, [1.5, 2.5]),
      },
      {
        type: "Ring",
        value: [
          [0, 0],
          [1, 0],
          [1, 1],
          [0, 1],
          [0, 0],
        ],
        assert: (v) =>
          assert.deepStrictEqual(v, [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 1],
            [0, 0],
          ]),
      },
      {
        type: "Polygon",
        value: [
          [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 1],
            [0, 0],
          ],
        ],
        assert: (v) =>
          assert.deepStrictEqual(v, [
            [
              [0, 0],
              [1, 0],
              [1, 1],
              [0, 1],
              [0, 0],
            ],
          ]),
      },
      {
        type: "MultiPolygon",
        value: [
          [
            [
              [0, 0],
              [1, 0],
              [1, 1],
              [0, 1],
              [0, 0],
            ],
          ],
        ],
        assert: (v) =>
          assert.deepStrictEqual(v, [
            [
              [
                [0, 0],
                [1, 0],
                [1, 1],
                [0, 1],
                [0, 0],
              ],
            ],
          ]),
      },
    ];

    for (const entry of cases) {
      const decoded = await roundTripValue(entry.type, entry.value);
      try {
        entry.assert(decoded);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        throw new Error(`Type matrix failed for ${entry.type}: ${message}`);
      }
    }
  });
});
