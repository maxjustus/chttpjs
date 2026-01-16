/**
 * TCP integration fuzz tests for Native format.
 * Tests both random schemas (generateRandomStructure) and random JSON typed paths.
 */

import { describe, it } from "node:test";
import { type ColumnDef, encodeNative, RecordBatch } from "../native/index.ts";
import { TcpClient } from "../tcp_client/client.ts";
import { startClickHouse, stopClickHouse } from "../test/setup.ts";
import { type Compression, config, logConfig, logFuzzError, getIterationIndex } from "./config.ts";

logConfig("tcp");

const randomInt = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;

async function queryScalar(client: TcpClient, sql: string): Promise<string> {
  const stream = client.query(sql);
  for await (const packet of stream) {
    if (packet.type === "Data" && packet.batch.rowCount > 0) {
      return String(packet.batch.getAt(0, 0));
    }
  }
  return "";
}

describe("Native TCP Integration Fuzz Tests", { timeout: 600000 }, () => {
  for (const compression of config.tcpCompressions) {
    // Test 1: Random schemas via generateRandomStructure()
    it(`round-trips random data (compression=${compression})`, async () => {
      const iterationIndex = getIterationIndex();
      const iterations = config.tcpIterations;
      const iterCount = iterationIndex !== null ? 1 : iterations;
      const startIdx = iterationIndex ?? 0;

      const ch = await startClickHouse();

      try {
        for (let iter = startIdx; iter < startIdx + iterCount; iter++) {
          const rowCount = config.rows;

          // Each iteration gets its own client (TcpClient doesn't support concurrent ops)
          const client = new TcpClient({
            host: ch.host,
            port: ch.tcpPort,
            user: ch.username,
            password: ch.password,
            compression: compression,
            queryTimeout: 120000, // 2 minutes for large fuzz tests
            debug: !!process.env.FUZZ_DEBUG,
          });
          await client.connect();

          const srcTable = `tcp_fuzz_src_${Date.now()}_${iter}_${Math.random().toString(36).slice(2)}`;
          const dstTable = `tcp_fuzz_dst_${Date.now()}_${iter}_${Math.random().toString(36).slice(2)}`;
          let structure = "";

          try {
            // Generate random structure
            structure = await queryScalar(client, `SELECT generateRandomStructure()`);
            console.log(
              `[tcp fuzz ${iter + 1}/${iterations} compression=${compression}] ${structure}`,
            );

            // Create source table with random rows
            const unescaped = structure.replace(/\\'/g, "'");
            const escapedStructure = unescaped.replace(/'/g, "''");
            await client.query(
              `CREATE TABLE ${srcTable} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM generateRandom('${escapedStructure}') LIMIT ${rowCount}`,
            );

            // Create empty dest table
            await client.query(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`);

            // Stream via TCP - collect batches
            const batches: RecordBatch[] = [];
            let columns: ColumnDef[] = [];
            const stream = client.query(`SELECT * FROM ${srcTable}`, {
              settings: { output_format_native_use_flattened_dynamic_and_json_serialization: 1 },
            });
            for await (const packet of stream) {
              if (packet.type === "Data" && packet.batch.rowCount > 0) {
                columns = packet.batch.columns;
                batches.push(packet.batch);
              }
            }

            // Insert batches to dest
            for (const batch of batches) {
              for await (const _ of client.insert(`INSERT INTO ${dstTable} VALUES`, batch)) {
              }
            }

            // Verify with cityHash64
            const d1 = parseInt(
              (await queryScalar(
                client,
                `SELECT count() FROM (SELECT cityHash64(*) AS h FROM ${srcTable} EXCEPT SELECT cityHash64(*) AS h FROM ${dstTable})`,
              )) || "0",
              10,
            );
            const d2 = parseInt(
              (await queryScalar(
                client,
                `SELECT count() FROM (SELECT cityHash64(*) AS h FROM ${dstTable} EXCEPT SELECT cityHash64(*) AS h FROM ${srcTable})`,
              )) || "0",
              10,
            );

            if (d1 !== 0 || d2 !== 0) {
              // Debug: find first differing column
              let firstDiffCol = "";
              for (const col of columns) {
                const colDiff = parseInt(
                  (await queryScalar(
                    client,
                    `SELECT count() FROM (SELECT cityHash64(\`${col.name}\`) AS h FROM ${srcTable} EXCEPT SELECT cityHash64(\`${col.name}\`) AS h FROM ${dstTable})`,
                  )) || "0",
                  10,
                );
                if (colDiff !== 0) {
                  firstDiffCol = `${col.name} (${col.type})`;
                  break;
                }
              }
              throw new Error(
                `TCP fuzz mismatch: ${d1}/${d2} rows differ. First differing column: ${firstDiffCol || "unknown"}`,
              );
            }
          } catch (err) {
            logFuzzError(
              {
                testType: "tcp",
                iteration: iter,
                totalIterations: iterations,
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
            try {
              await client.query(`DROP TABLE IF EXISTS ${srcTable} SYNC`);
              await client.query(`DROP TABLE IF EXISTS ${dstTable} SYNC`);
            } catch {
              /* ignore cleanup errors */
            }
            client.close();
          }
        }
      } finally {
        await stopClickHouse();
      }
    });

    // Test 2: JSON with random typed paths
    it(`round-trips JSON with random typed paths (compression=${compression})`, async () => {
      const iterationIndex = getIterationIndex();
      const iterations = config.tcpIterations;
      const iterCount = iterationIndex !== null ? 1 : iterations;
      const startIdx = iterationIndex ?? 0;

      const ch = await startClickHouse();

      try {
        for (let iter = startIdx; iter < startIdx + iterCount; iter++) {
          const rowCount = config.rows;

          // Each iteration gets its own client (TcpClient doesn't support concurrent ops)
          const client = new TcpClient({
            host: ch.host,
            port: ch.tcpPort,
            user: ch.username,
            password: ch.password,
            compression: compression,
            queryTimeout: 600000,
            debug: !!process.env.FUZZ_DEBUG,
          });
          await client.connect();

          const srcTable = `fuzz_json_src_${Date.now()}_${iter}_${Math.random().toString(36).slice(2)}`;
          const dstTable = `fuzz_json_dst_${Date.now()}_${iter}_${Math.random().toString(36).slice(2)}`;
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

            if (typedPathDefs.length === 0) return;

            jsonType = `JSON(${typedPathDefs.join(", ")})`;
            console.log(
              `[tcp fuzz ${iter + 1}/${iterations} compression=${compression}] ${jsonType}`,
            );

            // Create source table with JSON column
            const helperCols = pathTypes.map((t, i) => `tp_${i} ${t}`).join(", ");
            const pathSelect = pathTypes.map((_, i) => `'tp_${i}', tp_${i}`).join(", ");

            await client.query(
              `CREATE TABLE ${srcTable} (id UInt64, data ${jsonType}) ENGINE = Memory`,
            );
            await client.query(
              `INSERT INTO ${srcTable} SELECT rowNumberInAllBlocks() as id, map(${pathSelect})::${jsonType} as data ` +
                `FROM generateRandom('${helperCols.replace(/'/g, "''")}') LIMIT ${rowCount}`,
            );

            await client.query(
              `CREATE TABLE ${dstTable} (id UInt64, data ${jsonType}) ENGINE = Memory`,
            );

            // Read from source via TCP - collect all batches
            const batches: RecordBatch[] = [];
            const stream = client.query(`SELECT * FROM ${srcTable} ORDER BY id`, {
              settings: { output_format_native_use_flattened_dynamic_and_json_serialization: 1 },
            });
            for await (const packet of stream) {
              if (packet.type === "Data" && packet.batch.rowCount > 0) {
                batches.push(packet.batch);
              }
            }

            // Insert batches to dest
            for (const batch of batches) {
              for await (const _ of client.insert(`INSERT INTO ${dstTable} VALUES`, batch)) {
              }
            }

            // Verify row counts
            const srcCount = parseInt(
              (await queryScalar(client, `SELECT count() FROM ${srcTable}`)) || "0",
              10,
            );
            const dstCount = parseInt(
              (await queryScalar(client, `SELECT count() FROM ${dstTable}`)) || "0",
              10,
            );

            if (srcCount !== dstCount) {
              throw new Error(
                `Row count mismatch: src=${srcCount}, dst=${dstCount} for ${jsonType}`,
              );
            }

            // Verify with cityHash64 on extracted paths
            const pathList = typedPathDefs.map((_, i) => `data.tp_${i}`).join(", ");
            const d1 = parseInt(
              (await queryScalar(
                client,
                `SELECT count() FROM (SELECT id, cityHash64(${pathList}) AS h FROM ${srcTable} EXCEPT SELECT id, cityHash64(${pathList}) AS h FROM ${dstTable})`,
              )) || "0",
              10,
            );
            const d2 = parseInt(
              (await queryScalar(
                client,
                `SELECT count() FROM (SELECT id, cityHash64(${pathList}) AS h FROM ${dstTable} EXCEPT SELECT id, cityHash64(${pathList}) AS h FROM ${srcTable})`,
              )) || "0",
              10,
            );

            if (d1 !== 0 || d2 !== 0) {
              throw new Error(`Hash mismatch: ${d1}/${d2} rows differ for ${jsonType}`);
            }
          } catch (err) {
            logFuzzError(
              {
                testType: "tcp",
                iteration: iter,
                totalIterations: iterations,
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
            try {
              await client.query(`DROP TABLE IF EXISTS ${srcTable} SYNC`);
              await client.query(`DROP TABLE IF EXISTS ${dstTable} SYNC`);
            } catch {
              /* ignore cleanup errors */
            }
            client.close();
          }
        }
      } finally {
        await stopClickHouse();
      }
    });
  }
});
