import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { init, insert, query, collectBytes, collectText } from "../client.ts";
import {
  encodeRowBinaryWithNames,
  decodeRowBinaryWithNamesAndTypes,
} from "../rowbinary.ts";

describe("RowBinary Fuzz Tests", { timeout: 300000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let baseUrl: string;
  let auth: { username: string; password: string };
  const sessionId = "fuzz_" + Date.now().toString();

  before(async () => {
    await init();
    clickhouse = await startClickHouse();
    baseUrl = clickhouse.url + "/";
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  it("round-trips random data N times", async () => {
    const N = parseInt(process.env.FUZZ_ITERATIONS ?? "25", 10);

    for (let i = 0; i < N; i++) {
      const srcTable = `fuzz_src_${i}`;
      const dstTable = `fuzz_dst_${i}`;
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
        console.log(`[fuzz ${i + 1}/${N}] structure: ${structure}`);

        // 2. Create source table with 50k rows (multi-block)
        // TabSeparated escapes quotes as \', unescape then re-escape for SQL
        const unescaped = structure.replace(/\\'/g, "'");
        const escapedStructure = unescaped.replace(/'/g, "''");
        await consume(
          query(
            `CREATE TABLE ${srcTable} ENGINE = Memory AS SELECT * FROM generateRandom('${escapedStructure}') LIMIT 50000`,
            sessionId,
            { baseUrl, auth, compression: "none" },
          ),
        );

        // 3. Query source in RowBinary format
        const srcBytes = await collectBytes(
          query(
            `SELECT * FROM ${srcTable} FORMAT RowBinaryWithNamesAndTypes`,
            sessionId,
            { baseUrl, auth },
          ),
        );

        // 4. Decode
        const decoded = decodeRowBinaryWithNamesAndTypes(srcBytes);

        // 5. Create empty dest table
        await consume(
          query(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`, sessionId, {
            baseUrl,
            auth,
            compression: "none",
          }),
        );

        // 6. Encode and insert
        const encoded = encodeRowBinaryWithNames(decoded.columns, decoded.rows);
        await insert(
          `INSERT INTO ${dstTable} FORMAT RowBinaryWithNames`,
          encoded,
          sessionId,
          { baseUrl, auth },
        );

        // 7. Verify both directions
        const diff1 = await collectText(
          query(
            `SELECT count() FROM (SELECT * FROM ${srcTable} EXCEPT SELECT * FROM ${dstTable}) FORMAT TabSeparated`,
            sessionId,
            { baseUrl, auth },
          ),
        );
        const diff2 = await collectText(
          query(
            `SELECT count() FROM (SELECT * FROM ${dstTable} EXCEPT SELECT * FROM ${srcTable}) FORMAT TabSeparated`,
            sessionId,
            { baseUrl, auth },
          ),
        );

        assert.strictEqual(
          diff1.trim(),
          "0",
          `src→dst mismatch in iteration ${i}: ${diff1.trim()} rows differ`,
        );
        assert.strictEqual(
          diff2.trim(),
          "0",
          `dst→src mismatch in iteration ${i}: ${diff2.trim()} rows differ`,
        );
      } catch (err) {
        console.error(
          `[fuzz ${i + 1}/${N}] FAILED with structure: ${structure}`,
        );
        throw err;
      } finally {
        await consume(
          query(`DROP TABLE IF EXISTS ${srcTable}`, sessionId, {
            baseUrl,
            auth,
            compression: "none",
          }),
        );
        await consume(
          query(`DROP TABLE IF EXISTS ${dstTable}`, sessionId, {
            baseUrl,
            auth,
            compression: "none",
          }),
        );
      }
    }
  });
});

async function consume(stream: AsyncIterable<Uint8Array>) {
  for await (const _ of stream) {
  }
}
