/**
 * HTTP integration fuzz tests for Native format.
 * Uses ClickHouse's generateRandom() and cityHash64() for verification.
 */

import { describe, it } from "node:test";
import { type ColumnDef, encodeNative, streamDecodeNative } from "../native/index.ts";
import { collectText, dataChunks, init, insert, type QueryPacket, query } from "../client.ts";
import { startClickHouse, stopClickHouse } from "../test/setup.ts";
import { type Compression, config, logConfig, logFuzzError, getIterationIndex } from "./config.ts";

logConfig("http");

const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;

async function consume(input: AsyncIterable<QueryPacket>) {
  for await (const _ of input) {
  }
}

describe("Native HTTP Integration Fuzz Tests", { timeout: 600000 }, () => {
  for (const compression of config.httpCompressions) {
    it(`round-trips random data (compression=${compression})`, async () => {
      await init();
      const clickhouse = await startClickHouse();
      const baseUrl = `${clickhouse.url}/`;
      const auth = { username: clickhouse.username, password: clickhouse.password };

      try {
        const iterationIndex = getIterationIndex();
        const N = config.integrationIterations;
        const iterations = iterationIndex !== null ? 1 : N;
        const startIdx = iterationIndex ?? 0;

        for (let i = startIdx; i < startIdx + iterations; i++) {
          const rowCount = config.rows;

          // Each iteration gets its own session to avoid conflicts
          const sessionId = `native_fuzz_${compression}_${Date.now()}_${i}_${Math.random().toString(36).slice(2)}`;
          const insertSessionId = `${sessionId}_insert`;
          const srcTable = `native_fuzz_src_${Date.now()}_${i}_${Math.random().toString(36).slice(2)}`;
          const dstTable = `native_fuzz_dst_${Date.now()}_${i}_${Math.random().toString(36).slice(2)}`;
          let structure = "";

          try {
            // Generate random structure
            const structResult = await collectText(
              query(`SELECT generateRandomStructure() FORMAT TabSeparated`, sessionId, {
                baseUrl,
                auth,
              }),
            );
            structure = structResult.trim();
            console.log(`[http fuzz ${i + 1}/${N} compression=${compression}] ${structure}`);

            // Create source table with random rows
            const unescaped = structure.replace(/\\'/g, "'");
            const escapedStructure = unescaped.replace(/'/g, "''");
            await consume(
              query(
                `CREATE TABLE ${srcTable} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM generateRandom('${escapedStructure}') LIMIT ${rowCount}`,
                sessionId,
                { baseUrl, auth, compression: false },
              ),
            );

            // Create empty dest table
            await consume(
              query(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`, sessionId, {
                baseUrl,
                auth,
                compression: false,
              }),
            );

            // Stream decode and insert block-by-block
            const queryResult = query(
              `SELECT * FROM ${srcTable} FORMAT Native SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=1`,
              sessionId,
              { baseUrl, auth, compression },
            );

            let columns: ColumnDef[] = [];
            let blocksProcessed = 0;
            let rowsProcessed = 0;
            const startTime = Date.now();
            let lastProgressTime = startTime;

            for await (const block of streamDecodeNative(dataChunks(queryResult), {
              mapAsArray: true,
              debug: false,
            })) {
              columns = block.columns;
              blocksProcessed++;
              rowsProcessed += block.rowCount;

              const now = Date.now();
              if (now - lastProgressTime >= 3000) {
                const elapsed = ((now - startTime) / 1000).toFixed(1);
                console.log(
                  `  [${i + 1}/${N}] ${rowsProcessed.toLocaleString()} rows, ${blocksProcessed} blocks (${elapsed}s)`,
                );
                lastProgressTime = now;
              }

              const encoded = encodeNative(block);
              await insert(`INSERT INTO ${dstTable} FORMAT Native`, encoded, insertSessionId, {
                baseUrl,
                auth,
              });
            }

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log(
              `  [${i + 1}/${N}] done: ${rowsProcessed.toLocaleString()} rows, ${blocksProcessed} blocks (${elapsed}s)`,
            );

            // Verify with cityHash64
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
              // Debug: find first differing column
              let firstDiffCol = "";
              for (const col of columns) {
                const colDiff = await collectText(
                  query(
                    `SELECT count() FROM (SELECT cityHash64(\`${col.name}\`) AS h FROM ${srcTable} EXCEPT SELECT cityHash64(\`${col.name}\`) AS h FROM ${dstTable}) FORMAT TabSeparated`,
                    sessionId,
                    { baseUrl, auth },
                  ),
                );
                if (colDiff.trim() !== "0") {
                  firstDiffCol = `${col.name} (${col.type})`;
                  break;
                }
              }
              throw new Error(
                `Native fuzz mismatch: ${diff1.trim()}/${diff2.trim()} rows differ. First differing column: ${firstDiffCol || "unknown"}`,
              );
            }
          } catch (err) {
            logFuzzError(
              {
                testType: "http",
                iteration: i,
                totalIterations: N,
                compression: compression as Compression,
                rows: rowCount,
                structure,
                srcTable,
                dstTable,
              },
              err,
            );
            throw err;
          } finally {
            await consume(
              query(`DROP TABLE IF EXISTS ${srcTable} SYNC`, insertSessionId, {
                baseUrl,
                auth,
                compression: false,
              }),
            );
            await consume(
              query(`DROP TABLE IF EXISTS ${dstTable} SYNC`, insertSessionId, {
                baseUrl,
                auth,
                compression: false,
              }),
            );
          }
        }
      } finally {
        await stopClickHouse();
      }
    });

    // Test 2: JSON with random typed paths
    it(`round-trips JSON with random typed paths (compression=${compression})`, async () => {
      await init();
      const clickhouse = await startClickHouse();
      const baseUrl = `${clickhouse.url}/`;
      const auth = { username: clickhouse.username, password: clickhouse.password };

      try {
        const iterationIndex = getIterationIndex();
        const N = config.integrationIterations;
        const iterations = iterationIndex !== null ? 1 : N;
        const startIdx = iterationIndex ?? 0;

        for (let i = startIdx; i < startIdx + iterations; i++) {
          const rowCount = config.rows;

          // Each iteration gets its own session to avoid conflicts
          const sessionId = `json_fuzz_${compression}_${Date.now()}_${i}_${Math.random().toString(36).slice(2)}`;
          const insertSessionId = `${sessionId}_insert`;
          const srcTable = `json_fuzz_src_${Date.now()}_${i}_${Math.random().toString(36).slice(2)}`;
          const dstTable = `json_fuzz_dst_${Date.now()}_${i}_${Math.random().toString(36).slice(2)}`;
          let jsonType = "";

          try {
            // Generate 1-3 random types for typed paths
            const numPaths = randomInt(1, 3);
            const typedPathDefs: string[] = [];
            const pathTypes: string[] = [];
            for (let p = 0; p < numPaths; p++) {
              const result = await collectText(
                query(`SELECT generateRandomStructure(1, 1) FORMAT TabSeparated`, sessionId, {
                  baseUrl,
                  auth,
                }),
              );
              const match = result.trim().match(/^\S+\s+(.+)$/);
              if (match) {
                const idx = typedPathDefs.length;
                typedPathDefs.push(`tp_${idx} Nullable(${match[1]})`);
                pathTypes.push(`Nullable(${match[1]})`);
              }
            }

            if (typedPathDefs.length === 0) return;

            jsonType = `JSON(${typedPathDefs.join(", ")})`;
            console.log(`[http json fuzz ${i + 1}/${N} compression=${compression}] ${jsonType}`);

            // Create source table with JSON column
            const helperCols = pathTypes.map((t, idx) => `tp_${idx} ${t}`).join(", ");
            const pathSelect = pathTypes.map((_, idx) => `'tp_${idx}', tp_${idx}`).join(", ");

            await consume(
              query(
                `CREATE TABLE ${srcTable} (id UInt64, data ${jsonType}) ENGINE = Memory`,
                sessionId,
                { baseUrl, auth, compression: false },
              ),
            );
            await consume(
              query(
                `INSERT INTO ${srcTable} SELECT rowNumberInAllBlocks() as id, map(${pathSelect})::${jsonType} as data ` +
                  `FROM generateRandom('${helperCols.replace(/'/g, "''")}') LIMIT ${rowCount}`,
                sessionId,
                { baseUrl, auth, compression: false },
              ),
            );

            await consume(
              query(
                `CREATE TABLE ${dstTable} (id UInt64, data ${jsonType}) ENGINE = Memory`,
                sessionId,
                { baseUrl, auth, compression: false },
              ),
            );

            // Stream decode and insert block-by-block
            const queryResult = query(
              `SELECT * FROM ${srcTable} ORDER BY id FORMAT Native SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=1`,
              sessionId,
              { baseUrl, auth, compression },
            );

            for await (const block of streamDecodeNative(dataChunks(queryResult), {
              mapAsArray: true,
              debug: false,
            })) {
              const encoded = encodeNative(block);
              await insert(`INSERT INTO ${dstTable} FORMAT Native`, encoded, insertSessionId, {
                baseUrl,
                auth,
              });
            }

            // Verify row counts
            const srcCount = await collectText(
              query(`SELECT count() FROM ${srcTable} FORMAT TabSeparated`, sessionId, {
                baseUrl,
                auth,
              }),
            );
            const dstCount = await collectText(
              query(`SELECT count() FROM ${dstTable} FORMAT TabSeparated`, sessionId, {
                baseUrl,
                auth,
              }),
            );

            if (srcCount.trim() !== dstCount.trim()) {
              throw new Error(
                `Row count mismatch: src=${srcCount.trim()}, dst=${dstCount.trim()} for ${jsonType}`,
              );
            }

            // Verify with cityHash64 on extracted paths
            const pathList = typedPathDefs.map((_, idx) => `data.tp_${idx}`).join(", ");
            const diff1 = await collectText(
              query(
                `SELECT count() FROM (SELECT id, cityHash64(${pathList}) AS h FROM ${srcTable} EXCEPT SELECT id, cityHash64(${pathList}) AS h FROM ${dstTable}) FORMAT TabSeparated`,
                sessionId,
                { baseUrl, auth },
              ),
            );
            const diff2 = await collectText(
              query(
                `SELECT count() FROM (SELECT id, cityHash64(${pathList}) AS h FROM ${dstTable} EXCEPT SELECT id, cityHash64(${pathList}) AS h FROM ${srcTable}) FORMAT TabSeparated`,
                sessionId,
                { baseUrl, auth },
              ),
            );

            if (diff1.trim() !== "0" || diff2.trim() !== "0") {
              throw new Error(
                `Hash mismatch: ${diff1.trim()}/${diff2.trim()} rows differ for ${jsonType}`,
              );
            }
          } catch (err) {
            logFuzzError(
              {
                testType: "http",
                iteration: i,
                totalIterations: N,
                compression: compression as Compression,
                rows: rowCount,
                jsonType,
                srcTable,
                dstTable,
              },
              err,
            );
            throw err;
          } finally {
            await consume(
              query(`DROP TABLE IF EXISTS ${srcTable} SYNC`, insertSessionId, {
                baseUrl,
                auth,
                compression: false,
              }),
            );
            await consume(
              query(`DROP TABLE IF EXISTS ${dstTable} SYNC`, insertSessionId, {
                baseUrl,
                auth,
                compression: false,
              }),
            );
          }
        }
      } finally {
        await stopClickHouse();
      }
    });
  }
});
