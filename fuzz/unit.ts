/**
 * Unit fuzz tests for Native format encoder/decoder.
 * No ClickHouse required - generates random data locally and round-trips through encode/decode.
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import {
  batchFromRows,
  type ColumnDef,
  encodeNative,
  streamDecodeNative,
} from "../native/index.ts";
import { decodeBatch, toArrayRows } from "../test/test_utils.ts";
import { config, logConfig, getIterationIndex } from "./config.ts";

logConfig("unit");

function encodeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  return encodeNative(batchFromRows(columns, rows));
}

describe("Native Unit Fuzz Tests", { timeout: 60000 }, () => {
  const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;
  const randomBigInt = (bits: number) => {
    const max = (1n << BigInt(bits - 1)) - 1n;
    const min = -(1n << BigInt(bits - 1));
    const range = max - min;
    return min + BigInt(Math.floor(Math.random() * Number(range)));
  };
  const randomFloat = () => (Math.random() - 0.5) * 1e10;
  const randomString = (maxLen = 100) => {
    const len = randomInt(0, maxLen);
    const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 \t\n!@#$%^&*()";
    return Array.from({ length: len }, () => chars[randomInt(0, chars.length - 1)]).join("");
  };
  const randomUnicode = (maxLen = 50) => {
    const len = randomInt(0, maxLen);
    const codePoints = [
      () => randomInt(0x20, 0x7e),
      () => randomInt(0x00c0, 0x00ff),
      () => randomInt(0x0400, 0x04ff),
      () => randomInt(0x4e00, 0x9fff),
      () => randomInt(0x1f600, 0x1f64f),
    ];
    return Array.from({ length: len }, () => {
      const gen = codePoints[randomInt(0, codePoints.length - 1)];
      return String.fromCodePoint(gen());
    }).join("");
  };
  const randomUUID = () => {
    const hex = () => randomInt(0, 15).toString(16);
    return `${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}-${hex()}${hex()}${hex()}${hex()}-4${hex()}${hex()}${hex()}-${["8", "9", "a", "b"][randomInt(0, 3)]}${hex()}${hex()}${hex()}-${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}${hex()}`;
  };

  type TypeGen = {
    type: string;
    gen: () => unknown;
    compare?: (a: unknown, b: unknown) => boolean;
  };

  const scalarTypes: TypeGen[] = [
    { type: "Int8", gen: () => randomInt(-128, 127) },
    { type: "Int16", gen: () => randomInt(-32768, 32767) },
    { type: "Int32", gen: () => randomInt(-2147483648, 2147483647) },
    { type: "Int64", gen: () => randomBigInt(64) },
    { type: "UInt8", gen: () => randomInt(0, 255) },
    { type: "UInt16", gen: () => randomInt(0, 65535) },
    { type: "UInt32", gen: () => randomInt(0, 4294967295) },
    { type: "UInt64", gen: () => BigInt(randomInt(0, Number.MAX_SAFE_INTEGER)) },
    {
      type: "Float32",
      gen: () => randomFloat() * 1e-5,
      compare: (a, b) => {
        if (Number.isNaN(a) && Number.isNaN(b)) return true;
        const relDiff =
          Math.abs((a as number) - (b as number)) /
          Math.max(Math.abs(a as number), Math.abs(b as number), 1);
        return relDiff < 1e-5;
      },
    },
    {
      type: "Float64",
      gen: randomFloat,
      compare: (a, b) => {
        if (Number.isNaN(a) && Number.isNaN(b)) return true;
        const relDiff =
          Math.abs((a as number) - (b as number)) /
          Math.max(Math.abs(a as number), Math.abs(b as number), 1);
        return relDiff < 1e-10;
      },
    },
    { type: "String", gen: () => randomString() },
    { type: "String", gen: () => randomUnicode() },
    { type: "UUID", gen: randomUUID },
  ];

  const dateTypes: TypeGen[] = [
    {
      type: "Date",
      gen: () => new Date(randomInt(0, 65535) * 86400000),
      compare: (a, b) => (a as Date).getTime() === (b as Date).getTime(),
    },
    {
      type: "DateTime",
      gen: () => new Date(randomInt(0, 4294967295) * 1000),
      compare: (a, b) => (a as Date).getTime() === (b as Date).getTime(),
    },
  ];

  const ipTypes: TypeGen[] = [
    {
      type: "IPv4",
      gen: () =>
        `${randomInt(0, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}`,
    },
    {
      type: "IPv6",
      gen: () => {
        const parts = Array.from({ length: 8 }, () => randomInt(0, 65535).toString(16));
        return parts.join(":");
      },
    },
  ];

  function generateRows(
    types: TypeGen[],
    rowCount: number,
  ): { columns: ColumnDef[]; rows: unknown[][]; types: TypeGen[] } {
    const columns: ColumnDef[] = types.map((t, i) => ({ name: `col_${i}`, type: t.type }));
    const rows: unknown[][] = [];
    for (let i = 0; i < rowCount; i++) {
      rows.push(types.map((t) => t.gen()));
    }
    return { columns, rows, types };
  }

  const stringify = (v: unknown): string => {
    if (typeof v === "bigint") return `${v}n`;
    if (v instanceof Date) return v.toISOString();
    if (v instanceof Map)
      return `Map(${[...v.entries()].map(([k, val]) => `${stringify(k)}=>${stringify(val)}`).join(", ")})`;
    if (ArrayBuffer.isView(v) && !(v instanceof DataView))
      return `[${[...(v as any)].map(stringify).join(", ")}]`;
    if (Array.isArray(v)) return `[${v.map(stringify).join(", ")}]`;
    return JSON.stringify(v);
  };

  function compareRows(original: unknown[][], decoded: unknown[][], types: TypeGen[]): void {
    assert.strictEqual(decoded.length, original.length, "Row count mismatch");
    for (let i = 0; i < original.length; i++) {
      for (let j = 0; j < types.length; j++) {
        const compare = types[j].compare ?? ((a, b) => a === b);
        const origVal = original[i][j];
        const decVal = decoded[i][j];
        assert.ok(
          compare(origVal, decVal),
          `Mismatch at row ${i}, col ${j} (${types[j].type}): ${stringify(origVal)} vs ${stringify(decVal)}`,
        );
      }
    }
  }

  it("fuzz scalar types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const typeCount = randomInt(1, scalarTypes.length);
      const selectedTypes = Array.from(
        { length: typeCount },
        () => scalarTypes[randomInt(0, scalarTypes.length - 1)],
      );
      const rowCount = randomInt(1, 100);

      const { columns, rows, types } = generateRows(selectedTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz date types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const rowCount = randomInt(1, 100);
      const { columns, rows, types } = generateRows(dateTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz IP types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const rowCount = randomInt(1, 100);
      const { columns, rows, types } = generateRows(ipTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz Nullable types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const baseType = scalarTypes[randomInt(0, scalarTypes.length - 1)];
      const nullableType: TypeGen = {
        type: `Nullable(${baseType.type})`,
        gen: () => (Math.random() < 0.3 ? null : baseType.gen()),
        compare: (a, b) => {
          if (a === null && b === null) return true;
          if (a === null || b === null) return false;
          return baseType.compare ? baseType.compare(a, b) : a === b;
        },
      };

      const rowCount = randomInt(1, 100);
      const { columns, rows, types } = generateRows([nullableType], rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz Array types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const baseType = scalarTypes[randomInt(0, 5)];
      const arrayType: TypeGen = {
        type: `Array(${baseType.type})`,
        gen: () => {
          const len = randomInt(0, 10);
          return Array.from({ length: len }, () => baseType.gen());
        },
        compare: (a, b) => {
          const arrA = a as unknown[];
          const arrB = b as unknown[];
          if (arrA.length !== arrB.length) return false;
          for (let i = 0; i < arrA.length; i++) {
            const cmp = baseType.compare ?? ((x, y) => x === y);
            if (!cmp(arrA[i], arrB[i])) return false;
          }
          return true;
        },
      };

      const rowCount = randomInt(1, 50);
      const { columns, rows, types } = generateRows([arrayType], rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz Tuple types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const elementCount = randomInt(2, 4);
      const elementTypes = Array.from({ length: elementCount }, () => scalarTypes[randomInt(0, 5)]);

      const tupleType: TypeGen = {
        type: `Tuple(${elementTypes.map((t) => t.type).join(", ")})`,
        gen: () => elementTypes.map((t) => t.gen()),
        compare: (a, b) => {
          const arrA = a as unknown[];
          const arrB = b as unknown[];
          if (arrA.length !== arrB.length) return false;
          for (let i = 0; i < arrA.length; i++) {
            const cmp = elementTypes[i].compare ?? ((x, y) => x === y);
            if (!cmp(arrA[i], arrB[i])) return false;
          }
          return true;
        },
      };

      const rowCount = randomInt(1, 50);
      const { columns, rows, types } = generateRows([tupleType], rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz Map types", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const keyType = scalarTypes[randomInt(0, 2)];
      const valueType = scalarTypes[randomInt(0, 5)];

      const mapType: TypeGen = {
        type: `Map(${keyType.type}, ${valueType.type})`,
        gen: () => {
          const size = randomInt(0, 5);
          const seen = new Set<string>();
          const entries: [unknown, unknown][] = [];
          for (let i = 0; i < size; i++) {
            const key = keyType.gen();
            const keyStr = JSON.stringify(key);
            if (seen.has(keyStr)) continue;
            seen.add(keyStr);
            entries.push([key, valueType.gen()]);
          }
          return entries;
        },
        compare: (a, b) => {
          const arrA = a as [unknown, unknown][];
          const mapB = b as Map<unknown, unknown>;
          if (!(mapB instanceof Map)) return false;
          if (arrA.length !== mapB.size) return false;
          const keyCmp = keyType.compare ?? ((x, y) => x === y);
          const valCmp = valueType.compare ?? ((x, y) => x === y);
          for (const [k, v] of arrA) {
            let found = false;
            for (const [mk, mv] of mapB) {
              if (keyCmp(k, mk) && valCmp(v, mv)) {
                found = true;
                break;
              }
            }
            if (!found) return false;
          }
          return true;
        },
      };

      const rowCount = randomInt(1, 30);
      const { columns, rows, types } = generateRows([mapType], rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz mixed column types", async () => {
    const iterationIndex = getIterationIndex();
    const mixedIterations = Math.ceil(config.unitIterations / 2);
    const iterations = iterationIndex !== null ? 1 : mixedIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      if (iter >= mixedIterations) {
        console.log(
          `Skipping iteration ${iter} (only ${mixedIterations} iterations for this test)`,
        );
        continue;
      }

      const allTypes = [...scalarTypes.slice(0, 6), ...dateTypes, ...ipTypes];
      const colCount = randomInt(3, 8);
      const selectedTypes = Array.from(
        { length: colCount },
        () => allTypes[randomInt(0, allTypes.length - 1)],
      );
      const rowCount = randomInt(10, 200);

      const { columns, rows, types } = generateRows(selectedTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz stream decode with random chunking", async () => {
    const iterationIndex = getIterationIndex();
    const streamIterations = Math.ceil(config.unitIterations / 2);
    const iterations = iterationIndex !== null ? 1 : streamIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      if (iter >= streamIterations) {
        console.log(
          `Skipping iteration ${iter} (only ${streamIterations} iterations for this test)`,
        );
        continue;
      }

      const typeCount = randomInt(2, 5);
      const selectedTypes = Array.from({ length: typeCount }, () => scalarTypes[randomInt(0, 5)]);
      const rowCount = randomInt(10, 100);

      const { columns, rows, types } = generateRows(selectedTypes, rowCount);

      const blockSize = randomInt(5, 20);
      const blocks: Uint8Array[] = [];
      for (let i = 0; i < rows.length; i += blockSize) {
        const chunk = rows.slice(i, i + blockSize);
        blocks.push(encodeRows(columns, chunk));
      }

      async function* toAsync(arr: Uint8Array[]): AsyncIterable<Uint8Array> {
        for (const item of arr) yield item;
      }

      const decodedRows: unknown[][] = [];
      let decodedColumns: ColumnDef[] = [];
      for await (const result of streamDecodeNative(toAsync(blocks))) {
        decodedColumns = result.columns;
        decodedRows.push(...toArrayRows(result));
      }

      assert.deepStrictEqual(decodedColumns, columns);
      compareRows(rows, decodedRows, types);
    }
  });

  it("fuzz LowCardinality(String)", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const uniqueValues = Array.from({ length: randomInt(3, 20) }, () => randomString(30));
      const lcType: TypeGen = {
        type: "LowCardinality(String)",
        gen: () => uniqueValues[randomInt(0, uniqueValues.length - 1)],
      };

      const rowCount = randomInt(10, 200);
      const { columns, rows, types } = generateRows([lcType], rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz empty and single-row edge cases", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const typeCount = randomInt(1, 5);
      const selectedTypes = Array.from(
        { length: typeCount },
        () => scalarTypes[randomInt(0, scalarTypes.length - 1)],
      );

      // Test empty
      {
        const { columns } = generateRows(selectedTypes, 0);
        const encoded = encodeRows(columns, []);
        const decoded = await decodeBatch(encoded);
        assert.deepStrictEqual(decoded.columns, columns);
        assert.strictEqual(decoded.rowCount, 0);
      }

      // Test single row
      {
        const { columns, rows, types } = generateRows(selectedTypes, 1);
        const encoded = encodeRows(columns, rows);
        const decoded = await decodeBatch(encoded);
        assert.deepStrictEqual(decoded.columns, columns);
        compareRows(rows, toArrayRows(decoded), types);
      }
    }
  });

  it("fuzz JSON with typed paths", async () => {
    const iterationIndex = getIterationIndex();
    const iterations = iterationIndex !== null ? 1 : config.unitIterations;
    const startIdx = iterationIndex ?? 0;

    const randomBigInt64 = () => {
      const max = (1n << 63n) - 1n;
      const min = -(1n << 63n);
      const range = max - min;
      return min + BigInt(Math.floor(Math.random() * Number(range)));
    };

    const typedPathTypes = [
      { type: "String", gen: () => randomString(20), nullable: false },
      { type: "Int64", gen: () => randomBigInt64(), nullable: false },
      { type: "Int32", gen: () => randomInt(-2147483648, 2147483647), nullable: false },
      { type: "Float64", gen: randomFloat, nullable: false },
      {
        type: "LowCardinality(String)",
        gen: () => ["active", "inactive", "pending"][randomInt(0, 2)],
        nullable: false,
      },
      {
        type: "Array(String)",
        gen: () => Array.from({ length: randomInt(0, 5) }, () => randomString(10)),
        nullable: false,
      },
      {
        type: "Nullable(String)",
        gen: () => (Math.random() < 0.3 ? null : randomString(15)),
        nullable: true,
      },
      {
        type: "Nullable(Int64)",
        gen: () => (Math.random() < 0.3 ? null : randomBigInt64()),
        nullable: true,
      },
    ];

    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const numTypedPaths = randomInt(1, 3);
      const selectedTypedPaths = Array.from({ length: numTypedPaths }, (_, i) => {
        const tp = typedPathTypes[randomInt(0, typedPathTypes.length - 1)];
        return { name: `typed_${i}`, ...tp };
      });

      const typeArgs = selectedTypedPaths.map((p) => `${p.name} ${p.type}`).join(", ");
      const jsonType = `JSON(${typeArgs})`;

      const rowCount = randomInt(1, 50);
      const rows: unknown[][] = [];
      for (let r = 0; r < rowCount; r++) {
        const obj: Record<string, unknown> = {};
        for (const tp of selectedTypedPaths) {
          if (tp.nullable) {
            if (Math.random() > 0.2) obj[tp.name] = tp.gen();
          } else {
            obj[tp.name] = tp.gen();
          }
        }
        const numDynamic = randomInt(0, 2);
        for (let d = 0; d < numDynamic; d++) {
          obj[`dyn_${d}`] = randomString(10);
        }
        rows.push([obj]);
      }

      const columns: ColumnDef[] = [{ name: "j", type: jsonType }];
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      assert.strictEqual(decoded.rowCount, rowCount);

      const decodedRows = toArrayRows(decoded);
      for (let r = 0; r < rowCount; r++) {
        const orig = rows[r][0] as Record<string, unknown>;
        const dec = decodedRows[r][0] as Record<string, unknown>;
        for (const key of Object.keys(orig)) {
          const origVal = orig[key];
          const decVal = dec[key];
          if (origVal === null) {
            assert.strictEqual(
              decVal,
              undefined,
              `Row ${r}, key ${key}: null should become undefined`,
            );
          } else if (Array.isArray(origVal)) {
            assert.deepStrictEqual(decVal, origVal, `Row ${r}, key ${key}: array mismatch`);
          } else if (typeof origVal === "number") {
            assert.ok(
              decVal === origVal || decVal === BigInt(Math.floor(origVal as number)),
              `Row ${r}, key ${key}: numeric mismatch ${origVal} vs ${decVal}`,
            );
          } else {
            assert.strictEqual(decVal, origVal, `Row ${r}, key ${key}: value mismatch`);
          }
        }
      }
    }
  });
});
