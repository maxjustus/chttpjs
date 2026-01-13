/**
 * Fuzz tests for Native format encoder/decoder.
 *
 * Two test modes:
 * 1. Unit fuzz: Generate random data locally and round-trip through encode/decode
 * 2. Integration fuzz: Use ClickHouse's generateRandom() for comprehensive testing
 */
import { describe, it } from "node:test";
import assert from "node:assert";
import { encodeNative, streamDecodeNative, RecordBatchBuilder, RecordBatch, type ColumnDef } from "../../native/index.ts";
import { decodeBatch, toArrayRows } from "../test_utils.ts";

// Helper to encode rows via RecordBatchBuilder
function encodeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  const builder = new RecordBatchBuilder(columns);
  for (const row of rows) builder.appendRow(row);
  return encodeNative(builder.finish());
}

// ============================================================================
// Unit Fuzz Tests (no ClickHouse required)
// ============================================================================

describe("Native Unit Fuzz Tests", { timeout: 60000 }, () => {
  // Random generators
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
      () => randomInt(0x20, 0x7E),      // ASCII
      () => randomInt(0x00C0, 0x00FF),  // Latin Extended
      () => randomInt(0x0400, 0x04FF),  // Cyrillic
      () => randomInt(0x4E00, 0x9FFF),  // CJK
      () => randomInt(0x1F600, 0x1F64F), // Emoji
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

  // Type definitions with generators
  type TypeGen = { type: string; gen: () => unknown; compare?: (a: unknown, b: unknown) => boolean };

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
      type: "Float32", gen: () => randomFloat() * 1e-5, compare: (a, b) => {
        if (Number.isNaN(a) && Number.isNaN(b)) return true;
        const relDiff = Math.abs((a as number) - (b as number)) / Math.max(Math.abs(a as number), Math.abs(b as number), 1);
        return relDiff < 1e-5;
      }
    },
    {
      type: "Float64", gen: randomFloat, compare: (a, b) => {
        if (Number.isNaN(a) && Number.isNaN(b)) return true;
        const relDiff = Math.abs((a as number) - (b as number)) / Math.max(Math.abs(a as number), Math.abs(b as number), 1);
        return relDiff < 1e-10;
      }
    },
    { type: "String", gen: () => randomString() },
    { type: "String", gen: () => randomUnicode() }, // Unicode variant
    { type: "UUID", gen: randomUUID },
  ];

  const dateTypes: TypeGen[] = [
    {
      type: "Date",
      gen: () => new Date(randomInt(0, 65535) * 86400000),
      compare: (a, b) => (a as Date).getTime() === (b as Date).getTime()
    },
    {
      type: "DateTime",
      gen: () => new Date(randomInt(0, 4294967295) * 1000),
      compare: (a, b) => (a as Date).getTime() === (b as Date).getTime()
    },
  ];

  const ipTypes: TypeGen[] = [
    {
      type: "IPv4",
      gen: () => `${randomInt(0, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}.${randomInt(0, 255)}`,
    },
    {
      type: "IPv6",
      gen: () => {
        const parts = Array.from({ length: 8 }, () => randomInt(0, 65535).toString(16));
        return parts.join(":");
      },
    },
  ];

  function generateRows(types: TypeGen[], rowCount: number): { columns: ColumnDef[]; rows: unknown[][]; types: TypeGen[] } {
    const columns: ColumnDef[] = types.map((t, i) => ({ name: `col_${i}`, type: t.type }));
    const rows: unknown[][] = [];
    for (let i = 0; i < rowCount; i++) {
      rows.push(types.map(t => t.gen()));
    }
    return { columns, rows, types };
  }

  const stringify = (v: unknown): string => {
    if (typeof v === "bigint") return `${v}n`;
    if (v instanceof Date) return v.toISOString();
    if (v instanceof Map) return `Map(${[...v.entries()].map(([k, val]) => `${stringify(k)}=>${stringify(val)}`).join(", ")})`;
    if (ArrayBuffer.isView(v) && !(v instanceof DataView)) return `[${[...v as any].map(stringify).join(", ")}]`;
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
          `Mismatch at row ${i}, col ${j} (${types[j].type}): ${stringify(origVal)} vs ${stringify(decVal)}`
        );
      }
    }
  }

  it("fuzz scalar types", async () => {
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      // Pick random subset of scalar types
      const typeCount = randomInt(1, scalarTypes.length);
      const selectedTypes = Array.from({ length: typeCount }, () => scalarTypes[randomInt(0, scalarTypes.length - 1)]);
      const rowCount = randomInt(1, 100);

      const { columns, rows, types } = generateRows(selectedTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz date types", async () => {
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      const rowCount = randomInt(1, 100);
      const { columns, rows, types } = generateRows(dateTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz IP types", async () => {
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      const rowCount = randomInt(1, 100);
      const { columns, rows, types } = generateRows(ipTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz Nullable types", async () => {
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      // Pick a random base type and make it nullable
      const baseType = scalarTypes[randomInt(0, scalarTypes.length - 1)];
      const nullableType: TypeGen = {
        type: `Nullable(${baseType.type})`,
        gen: () => Math.random() < 0.3 ? null : baseType.gen(),
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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      const baseType = scalarTypes[randomInt(0, 5)]; // Limit to simpler types
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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      // Create random tuple with 2-4 elements
      const elementCount = randomInt(2, 4);
      const elementTypes = Array.from({ length: elementCount }, () => scalarTypes[randomInt(0, 5)]);

      const tupleType: TypeGen = {
        type: `Tuple(${elementTypes.map(t => t.type).join(", ")})`,
        gen: () => elementTypes.map(t => t.gen()),
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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      const keyType = scalarTypes[randomInt(0, 2)]; // String or small ints
      const valueType = scalarTypes[randomInt(0, 5)];

      const mapType: TypeGen = {
        type: `Map(${keyType.type}, ${valueType.type})`,
        // Generate as Array<[K, V]> - Maps dedupe keys so use unique keys
        gen: () => {
          const size = randomInt(0, 5);
          const seen = new Set<string>();
          const entries: [unknown, unknown][] = [];
          for (let i = 0; i < size; i++) {
            let key = keyType.gen();
            const keyStr = JSON.stringify(key);
            // Ensure unique keys to match Map behavior
            if (seen.has(keyStr)) continue;
            seen.add(keyStr);
            entries.push([key, valueType.gen()]);
          }
          return entries;
        },
        // Compare input array against output Map
        compare: (a, b) => {
          const arrA = a as [unknown, unknown][];
          const mapB = b as Map<unknown, unknown>;
          if (!(mapB instanceof Map)) return false;
          if (arrA.length !== mapB.size) return false;
          const keyCmp = keyType.compare ?? ((x, y) => x === y);
          const valCmp = valueType.compare ?? ((x, y) => x === y);
          for (const [k, v] of arrA) {
            // Find matching key in Map
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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "25", 10);
    for (let iter = 0; iter < iterations; iter++) {
      // Mix different type categories
      const allTypes = [...scalarTypes.slice(0, 6), ...dateTypes, ...ipTypes];
      const colCount = randomInt(3, 8);
      const selectedTypes = Array.from({ length: colCount }, () => allTypes[randomInt(0, allTypes.length - 1)]);
      const rowCount = randomInt(10, 200);

      const { columns, rows, types } = generateRows(selectedTypes, rowCount);
      const encoded = encodeRows(columns, rows);
      const decoded = await decodeBatch(encoded);

      assert.deepStrictEqual(decoded.columns, columns);
      compareRows(rows, toArrayRows(decoded), types);
    }
  });

  it("fuzz stream decode with random chunking", async () => {
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "25", 10);
    for (let iter = 0; iter < iterations; iter++) {
      const typeCount = randomInt(2, 5);
      const selectedTypes = Array.from({ length: typeCount }, () => scalarTypes[randomInt(0, 5)]);
      const rowCount = randomInt(10, 100);

      const { columns, rows, types } = generateRows(selectedTypes, rowCount);

      // Encode multiple blocks
      const blockSize = randomInt(5, 20);
      const blocks: Uint8Array[] = [];
      for (let i = 0; i < rows.length; i += blockSize) {
        const chunk = rows.slice(i, i + blockSize);
        blocks.push(encodeRows(columns, chunk));
      }

      // Stream decode
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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      // Generate strings with high repetition (good for LowCardinality)
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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);
    for (let iter = 0; iter < iterations; iter++) {
      const typeCount = randomInt(1, 5);
      const selectedTypes = Array.from({ length: typeCount }, () => scalarTypes[randomInt(0, scalarTypes.length - 1)]);

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
    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "50", 10);

    // Available typed path types - non-nullable types must always be present
    const typedPathTypes = [
      { type: "String", gen: () => randomString(20), nullable: false },
      { type: "Int64", gen: () => randomBigInt(64), nullable: false },
      { type: "Int32", gen: () => randomInt(-2147483648, 2147483647), nullable: false },
      { type: "Float64", gen: randomFloat, nullable: false },
      { type: "LowCardinality(String)", gen: () => ["active", "inactive", "pending"][randomInt(0, 2)], nullable: false },
      { type: "Array(String)", gen: () => Array.from({ length: randomInt(0, 5) }, () => randomString(10)), nullable: false },
      { type: "Nullable(String)", gen: () => Math.random() < 0.3 ? null : randomString(15), nullable: true },
      { type: "Nullable(Int64)", gen: () => Math.random() < 0.3 ? null : randomBigInt(64), nullable: true },
    ];

    for (let iter = 0; iter < iterations; iter++) {
      // Generate 1-3 typed paths
      const numTypedPaths = randomInt(1, 3);
      const selectedTypedPaths = Array.from({ length: numTypedPaths }, (_, i) => {
        const tp = typedPathTypes[randomInt(0, typedPathTypes.length - 1)];
        return { name: `typed_${i}`, ...tp };
      });

      // Build JSON type string
      const typeArgs = selectedTypedPaths.map(p => `${p.name} ${p.type}`).join(", ");
      const jsonType = `JSON(${typeArgs})`;

      // Generate rows with typed paths + random dynamic paths
      const rowCount = randomInt(1, 50);
      const rows: unknown[][] = [];
      for (let r = 0; r < rowCount; r++) {
        const obj: Record<string, unknown> = {};
        // Add typed path values - nullable types can be omitted, non-nullable must be present
        for (const tp of selectedTypedPaths) {
          if (tp.nullable) {
            if (Math.random() > 0.2) obj[tp.name] = tp.gen();
          } else {
            obj[tp.name] = tp.gen(); // Always present for non-nullable
          }
        }
        // Add 0-2 dynamic paths
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

      // Verify each row's structure is preserved
      const decodedRows = toArrayRows(decoded);
      for (let r = 0; r < rowCount; r++) {
        const orig = rows[r][0] as Record<string, unknown>;
        const dec = decodedRows[r][0] as Record<string, unknown>;
        for (const key of Object.keys(orig)) {
          const origVal = orig[key];
          const decVal = dec[key];
          // Null values are omitted from decoded object
          if (origVal === null) {
            assert.strictEqual(decVal, undefined, `Row ${r}, key ${key}: null should become undefined`);
          } else if (Array.isArray(origVal)) {
            assert.deepStrictEqual(decVal, origVal, `Row ${r}, key ${key}: array mismatch`);
          } else if (typeof origVal === "number") {
            // Int64 becomes bigint, floats stay as numbers
            assert.ok(
              decVal === origVal || decVal === BigInt(Math.floor(origVal as number)),
              `Row ${r}, key ${key}: numeric mismatch ${origVal} vs ${decVal}`
            );
          } else {
            assert.strictEqual(decVal, origVal, `Row ${r}, key ${key}: value mismatch`);
          }
        }
      }
    }
  });
});

// ============================================================================
// Integration Fuzz Tests (requires ClickHouse)
// ============================================================================

import { startClickHouse, stopClickHouse } from "../setup.ts";
import { init, insert, query, collectText, dataChunks, type QueryPacket } from "../../client.ts";

describe("Native Integration Fuzz Tests", { timeout: 600000 }, () => {
  it("round-trips random data N times", async () => {
    // Setup
    await init();
    const clickhouse = await startClickHouse();
    const baseUrl = clickhouse.url + "/";
    const auth = { username: clickhouse.username, password: clickhouse.password };
    const sessionId = "native_fuzz_" + Date.now().toString();
    // Separate session for inserts to avoid lock contention during streaming
    const insertSessionId = sessionId + "_insert";

    try {
      const N = parseInt(process.env.INTEGRATION_FUZZ_ITERATIONS ?? process.env.FUZZ_ITERATIONS ?? "25", 10);

      for (let i = 0; i < N; i++) {
        const srcTable = `native_fuzz_src_${i}`;
        const dstTable = `native_fuzz_dst_${i}`;
        let structure = "";

        try {
          // 1. Generate random structure
          const structResult = await collectText(
            query(`SELECT generateRandomStructure() FORMAT TabSeparated`, sessionId, {
              baseUrl,
              auth,
            }),
          );
          structure = structResult.trim();
          console.log(`[native fuzz ${i + 1}/${N}] structure: ${structure}`);

          // 2. Create source table with random rows
          // IMPORTANT: Multi-block testing is required to verify decodeNative handles
          // multiple blocks correctly. ClickHouse sends data in blocks (~65k rows default).
          // We use 80k rows to guarantee multiple blocks (exceeds 65k default block size).
          // Note: 100k+ rows with complex nested types can exceed memory limits.
          const unescaped = structure.replace(/\\'/g, "'");
          const escapedStructure = unescaped.replace(/'/g, "''");
          const rowCount = parseInt(process.env.INTEGRATION_FUZZ_ROWS ?? process.env.FUZZ_ROWS ?? "80000", 10);
          await consume(
            query(
              `CREATE TABLE ${srcTable} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM generateRandom('${escapedStructure}') LIMIT ${rowCount}`,
              sessionId,
              { baseUrl, auth, compression: false },
            ),
          );

          // 2. Create empty dest table first
          await consume(
            query(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`, sessionId, {
              baseUrl,
              auth,
              compression: false,
            }),
          );

          // 4. Stream decode and insert block-by-block to avoid memory pressure
          // This keeps only 1-2 blocks in memory at a time instead of 80k rows
          const queryResult = query(
            `SELECT * FROM ${srcTable} FORMAT Native SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=1`,
            sessionId,
            { baseUrl, auth },
          );

          let columns: ColumnDef[] = [];
          let blocksProcessed = 0;
          let rowsProcessed = 0;
          const startTime = Date.now();
          let lastProgressTime = startTime;

          for await (const block of streamDecodeNative(dataChunks(queryResult), { mapAsArray: true, debug: true })) {
            columns = block.columns;
            blocksProcessed++;
            rowsProcessed += block.rowCount;

            // Log progress every 3 seconds
            const now = Date.now();
            if (now - lastProgressTime >= 3000) {
              const elapsed = ((now - startTime) / 1000).toFixed(1);
              console.log(`  [${i + 1}/${N}] ${rowsProcessed.toLocaleString()} rows, ${blocksProcessed} blocks (${elapsed}s)`);
              lastProgressTime = now;
            }

            // Encode the Table directly
            const encoded = encodeNative(block);
            await insert(
              `INSERT INTO ${dstTable} FORMAT Native`,
              encoded,
              insertSessionId,
              { baseUrl, auth },
            );
          }

          // Final progress
          const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
          console.log(`  [${i + 1}/${N}] done: ${rowsProcessed.toLocaleString()} rows, ${blocksProcessed} blocks (${elapsed}s)`);

          // 5. Verify exact row equality using cityHash64(*)
          // NaN bit patterns are preserved through round-trip since we return TypedArray from decode
          const diff1 = await collectText(
            query(
              `SELECT count() FROM (SELECT cityHash64(*) AS h FROM ${srcTable} EXCEPT SELECT cityHash64(*) AS h FROM ${dstTable}) FORMAT TabSeparated`,
              sessionId,
              { baseUrl, auth },
            ),
          );
          const diff2 = await collectText(
            query(
              `SELECT count() FROM (SELECT cityHash64(*) AS h FROM ${dstTable} EXCEPT SELECT cityHash64(*) AS h FROM ${srcTable}) FORMAT TabSeparated`,
              sessionId,
              { baseUrl, auth },
            ),
          );

          if (diff1.trim() !== "0" || diff2.trim() !== "0") {
            // Find first differing column for debugging
            let firstDiffCol = "";
            let firstDiffColName = "";
            for (const col of columns) {
              const colDiff = await collectText(
                query(
                  `SELECT count() FROM (SELECT cityHash64(\`${col.name}\`) AS h FROM ${srcTable} EXCEPT SELECT cityHash64(\`${col.name}\`) AS h FROM ${dstTable}) FORMAT TabSeparated`,
                  sessionId, { baseUrl, auth },
                ),
              );
              if (colDiff.trim() !== "0") {
                firstDiffCol = `${col.name} (${col.type})`;
                firstDiffColName = col.name;
                break;
              }
            }

            // Sample values from differing column
            if (firstDiffColName) {
              const srcSample = await collectText(
                query(
                  `SELECT \`${firstDiffColName}\` FROM ${srcTable} LIMIT 10 FORMAT TabSeparated`,
                  sessionId, { baseUrl, auth },
                ),
              );
              const dstSample = await collectText(
                query(
                  `SELECT \`${firstDiffColName}\` FROM ${dstTable} LIMIT 10 FORMAT TabSeparated`,
                  sessionId, { baseUrl, auth },
                ),
              );
              console.log(`Source ${firstDiffCol} sample:\n${srcSample}`);
              console.log(`Dest ${firstDiffCol} sample:\n${dstSample}`);

              // Count distinct values
              const srcDistinct = await collectText(
                query(
                  `SELECT count(DISTINCT \`${firstDiffColName}\`) FROM ${srcTable} FORMAT TabSeparated`,
                  sessionId, { baseUrl, auth },
                ),
              );
              const dstDistinct = await collectText(
                query(
                  `SELECT count(DISTINCT \`${firstDiffColName}\`) FROM ${dstTable} FORMAT TabSeparated`,
                  sessionId, { baseUrl, auth },
                ),
              );
              console.log(`Distinct values - src: ${srcDistinct.trim()}, dst: ${dstDistinct.trim()}`);
            }

            // Get specific differing rows
            const diffRows = await collectText(
              query(
                `SELECT * FROM (SELECT cityHash64(*) AS h, * FROM ${srcTable} EXCEPT SELECT cityHash64(*) AS h, * FROM ${dstTable}) LIMIT 5 FORMAT TabSeparated`,
                sessionId, { baseUrl, auth },
              ),
            );
            throw new Error(
              `Native fuzz mismatch in iteration ${i}: ${diff1.trim()}/${diff2.trim()} rows differ. First differing column: ${firstDiffCol || "unknown"}\nSample diff rows:\n${diffRows}`,
            );
          }
        } catch (err) {
          console.error(
            `[native fuzz ${i + 1}/${N}] FAILED with structure: ${structure}`,
          );
          throw err;
        } finally {
          // Use insertSessionId for cleanup to avoid session lock from streaming query
          await consume(
            query(`DROP TABLE IF EXISTS ${srcTable}`, insertSessionId, {
              baseUrl,
              auth,
              compression: false,
            }),
          );
          await consume(
            query(`DROP TABLE IF EXISTS ${dstTable}`, insertSessionId, {
              baseUrl,
              auth,
              compression: false,
            }),
          );
        }
      }
    } finally {
      // Teardown
      await stopClickHouse();
    }
  });
});

async function consume(input: AsyncIterable<QueryPacket>) {
  for await (const _ of input) {}
}

// ============================================================================
// TCP Client Integration Fuzz Tests (requires ClickHouse)
// ============================================================================

import { TcpClient } from "../../tcp_client/client.ts";

describe("TCP Native Integration Fuzz Tests", { timeout: 300000 }, () => {
  const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;

  /**
   * Generalized JSON typed paths fuzz test using generateRandomStructure().
   * Tests arbitrary nested types as JSON typed paths via ClickHouse's random type generator.
   * Full round-trip: read from ClickHouse → decode → encode → insert back → verify with cityHash64
   */
  it("round-trips JSON with random typed paths through ClickHouse", async () => {
    const iterations = parseInt(process.env.TCP_FUZZ_ITERATIONS ?? process.env.FUZZ_ITERATIONS ?? "10", 10);
    const rowCount = parseInt(process.env.TCP_FUZZ_ROWS ?? process.env.FUZZ_ROWS ?? "500", 10);

    // Helper to get scalar result from query
    async function queryScalar(client: TcpClient, sql: string): Promise<string> {
      const stream = client.query(sql);
      for await (const packet of stream) {
        if (packet.type === "Data" && packet.batch.rowCount > 0) {
          return String(packet.batch.getAt(0, 0));
        }
      }
      return "";
    }

    const client = new TcpClient({ host: "localhost", port: 9000, user: "default", password: "" });
    await client.connect();

    try {
      for (let iter = 0; iter < iterations; iter++) {
        const srcTable = `fuzz_json_src_${Date.now()}_${iter}`;
        const dstTable = `fuzz_json_dst_${Date.now()}_${iter}`;
        let jsonType = "";

        try {
          // Generate 1-3 random types for typed paths
          const numPaths = randomInt(1, 3);
          const typedPathDefs: string[] = [];
          const pathTypes: string[] = [];
          for (let p = 0; p < numPaths; p++) {
            const result = await queryScalar(client, `SELECT generateRandomStructure(1, 1)`);
            const match = result.match(/^\S+\s+(.+)$/);
            if (match) {
              const idx = typedPathDefs.length;
              typedPathDefs.push(`tp_${idx} Nullable(${match[1]})`);
              pathTypes.push(`Nullable(${match[1]})`);
            }
          }

          if (typedPathDefs.length === 0) continue;

          jsonType = `JSON(${typedPathDefs.join(", ")})`;
          console.log(`[json random fuzz ${iter + 1}/${iterations}] ${jsonType}`);

          // Create source table with JSON column and random data
          const helperCols = pathTypes.map((t, i) => `tp_${i} ${t}`).join(", ");
          const pathSelect = pathTypes.map((_, i) => `'tp_${i}', tp_${i}`).join(", ");

          // Create source table and insert random data
          await client.execute(`CREATE TABLE ${srcTable} (id UInt64, data ${jsonType}) ENGINE = Memory`);
          await client.execute(
            `INSERT INTO ${srcTable} SELECT rowNumberInAllBlocks() as id, map(${pathSelect})::${jsonType} as data ` +
            `FROM generateRandom('${helperCols.replace(/'/g, "''")}') LIMIT ${rowCount}`
          );

          // Create empty destination table
          await client.execute(`CREATE TABLE ${dstTable} (id UInt64, data ${jsonType}) ENGINE = Memory`);

          // Read from source via TCP (tests decoder) - collect all batches first
          const batches: RecordBatch[] = [];
          const stream = client.query(
            `SELECT * FROM ${srcTable} ORDER BY id`,
            { settings: { output_format_native_use_flattened_dynamic_and_json_serialization: 1 } }
          );
          for await (const packet of stream) {
            if (packet.type === "Data" && packet.batch.rowCount > 0) {
              batches.push(packet.batch);
            }
          }

          // Insert batches to dest (tests encoder)
          let insertedRows = 0;
          for (const batch of batches) {
            await client.insert(`INSERT INTO ${dstTable} VALUES`, batch);
            insertedRows += batch.rowCount;
          }

          // Verify row counts first
          const srcCount = parseInt(await queryScalar(client, `SELECT count() FROM ${srcTable}`) || "0", 10);
          const dstCount = parseInt(await queryScalar(client, `SELECT count() FROM ${dstTable}`) || "0", 10);

          if (srcCount !== dstCount) {
            throw new Error(`Row count mismatch: src=${srcCount}, dst=${dstCount} for ${jsonType}`);
          }

          // Verify by extracting typed paths and comparing with cityHash64
          // JSON columns don't support cityHash64 directly, so we extract paths
          const pathList = typedPathDefs.map((_, i) => `data.tp_${i}`).join(", ");
          const d1 = parseInt(await queryScalar(client,
            `SELECT count() FROM (SELECT id, cityHash64(${pathList}) AS h FROM ${srcTable} EXCEPT SELECT id, cityHash64(${pathList}) AS h FROM ${dstTable})`
          ) || "0", 10);
          const d2 = parseInt(await queryScalar(client,
            `SELECT count() FROM (SELECT id, cityHash64(${pathList}) AS h FROM ${dstTable} EXCEPT SELECT id, cityHash64(${pathList}) AS h FROM ${srcTable})`
          ) || "0", 10);

          if (d1 !== 0 || d2 !== 0) {
            throw new Error(`Hash mismatch: ${d1}/${d2} rows differ for ${jsonType}`);
          }
        } catch (err) {
          console.error(`[json random fuzz ${iter + 1}] FAILED with: ${jsonType}`);
          throw err;
        } finally {
          try {
            await client.execute(`DROP TABLE IF EXISTS ${srcTable}`);
            await client.execute(`DROP TABLE IF EXISTS ${dstTable}`);
          } catch (_) { /* ignore cleanup errors */ }
        }
      }
    } finally {
      client.close();
    }
  });
});
