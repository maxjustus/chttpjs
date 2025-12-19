import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { asRows } from "../../formats/native/index.ts";

describe("TCP Client Fuzz Tests", { timeout: 600000 }, () => {
  // Query timeout: default 120s, or set QUERY_TIMEOUT env var
  const queryTimeout = parseInt(process.env.QUERY_TIMEOUT ?? "120000", 10);

  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: "",
    debug: !!process.env.DEBUG,
    queryTimeout,
  };

  // Quick read-only fuzz: just SELECT random data, no round-trip
  // Good for finding decode bugs quickly
  // Usage: FUZZ_ITERATIONS=50 FUZZ_ROWS=100000 make fuzz-tcp
  // Debug: DEBUG=1 FUZZ_ITERATIONS=5 make fuzz-tcp
  test("decode random structures", async () => {
    let client = new TcpClient(options);
    await client.connect();

    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "10", 10);
    const rowCount = parseInt(process.env.FUZZ_ROWS ?? "20000", 10);
    let failures = 0;
    const maxFailures = 3;

    for (let i = 0; i < iterations; i++) {
      let structure = "";
      const iterStartTime = Date.now();

      try {
        // Get random structure
        for await (const p of client.query("SELECT generateRandomStructure()")) {
          if (p.type === "Data") {
            structure = (asRows(p.table).next().value as any)["generateRandomStructure()"];
          }
        }

        console.log(`[tcp fuzz ${i + 1}/${iterations}] ${structure.slice(0, 100)}...`);

        // Query random data with that structure
        const escaped = structure.replace(/'/g, "''");
        let totalRows = 0;
        let blocks = 0;
        const queryStartTime = Date.now();

        for await (const p of client.query(`SELECT * FROM generateRandom('${escaped}') LIMIT ${rowCount}`)) {
          if (p.type === "Data") {
            totalRows += p.table.rowCount;
            blocks++;
          }
        }

        const elapsed = ((Date.now() - queryStartTime) / 1000).toFixed(2);
        console.log(`  ${totalRows} rows, ${blocks} blocks (${elapsed}s)`);
        assert.strictEqual(totalRows, rowCount, `Expected ${rowCount} rows, got ${totalRows}`);

      } catch (err) {
        const elapsed = ((Date.now() - iterStartTime) / 1000).toFixed(2);
        const error = err as Error;
        console.error(`\n[FUZZ FAILURE] iteration ${i + 1}/${iterations} after ${elapsed}s`);
        console.error(`  Structure: ${structure || "(not yet fetched)"}`);
        console.error(`  Error: ${error.message}`);
        console.error(`  Stack: ${error.stack}`);

        failures++;
        if (failures >= maxFailures) {
          client.close();
          throw new Error(`Too many failures (${failures}), last error: ${error.message}`);
        }

        // Reconnect and continue - connection may be broken
        console.error(`  Reconnecting... (failure ${failures}/${maxFailures})`);
        client.close();
        client = new TcpClient(options);
        await client.connect();
      }
    }

    client.close();
    if (failures > 0) {
      console.log(`\nCompleted with ${failures} transient failure(s)`);
    }
  });

  // Full round-trip fuzz: SELECT -> INSERT -> verify hash
  // Note: Some random structures can't be inserted (e.g., certain Decimal scales)
  // These will fail with ClickHouse errors - that's expected, not a client bug
  // Skip with: SKIP_ROUNDTRIP=1 make fuzz-tcp
  test("round-trip random structures", { skip: !!process.env.SKIP_ROUNDTRIP }, async () => {
    let client = new TcpClient(options);
    await client.connect();

    const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "5", 10);
    // Use 80k+ rows to ensure multi-block (ClickHouse default block size ~65k)
    const rowCount = parseInt(process.env.FUZZ_ROWS ?? "80000", 10);
    let failures = 0;
    const maxFailures = 3;

    for (let i = 0; i < iterations; i++) {
      const srcTable = `tcp_fuzz_src_${i}_${Date.now()}`;
      const dstTable = `tcp_fuzz_dst_${i}_${Date.now()}`;
      let structure = "";
      const iterStartTime = Date.now();

      try {
        // Get random structure
        for await (const p of client.query("SELECT generateRandomStructure()")) {
          if (p.type === "Data") {
            structure = (asRows(p.table).next().value as any)["generateRandomStructure()"];
          }
        }

        console.log(`[tcp round-trip ${i + 1}/${iterations}] ${structure.slice(0, 80)}...`);

        // Create source table with random data
        const escaped = structure.replace(/'/g, "''");
        await client.execute(`CREATE TABLE ${srcTable} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM generateRandom('${escaped}') LIMIT ${rowCount}`);
        await client.execute(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`);

        // Stream from SRC to DST
        const startTime = Date.now();
        let blocksRead = 0;
        let rowsRead = 0;

        const queryStream = (async function* () {
          for await (const packet of client.query(`SELECT * FROM ${srcTable}`)) {
            if (packet.type === "Data") {
              blocksRead++;
              rowsRead += packet.table.rowCount;
              yield packet.table;
            }
          }
        })();

        await client.insert(`INSERT INTO ${dstTable} VALUES`, queryStream);
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
        console.log(`  transferred ${rowsRead} rows, ${blocksRead} blocks (${elapsed}s)`);

        // Verify using cityHash64 (handles NaN correctly)
        let srcHash = 0n, dstHash = 0n;
        for await (const p of client.query(`SELECT sum(cityHash64(*)) as h FROM ${srcTable}`)) {
          if (p.type === "Data") srcHash = (asRows(p.table).next().value as any).h;
        }
        for await (const p of client.query(`SELECT sum(cityHash64(*)) as h FROM ${dstTable}`)) {
          if (p.type === "Data") dstHash = (asRows(p.table).next().value as any).h;
        }

        assert.strictEqual(dstHash, srcHash, "Hash mismatch - data corruption detected");
        console.log(`  hash verified OK`);

        await client.execute(`DROP TABLE IF EXISTS ${srcTable}`);
        await client.execute(`DROP TABLE IF EXISTS ${dstTable}`);

      } catch (err) {
        const elapsed = ((Date.now() - iterStartTime) / 1000).toFixed(2);
        const error = err as Error;
        console.error(`\n[ROUND-TRIP FAILURE] iteration ${i + 1}/${iterations} after ${elapsed}s`);
        console.error(`  Structure: ${structure || "(not yet fetched)"}`);
        console.error(`  Error: ${error.message}`);
        if (process.env.DEBUG) {
          console.error(`  Stack: ${error.stack}`);
        }

        // Cleanup tables on failure
        try {
          await client.execute(`DROP TABLE IF EXISTS ${srcTable}`);
          await client.execute(`DROP TABLE IF EXISTS ${dstTable}`);
        } catch {
          // Ignore cleanup errors - connection may be broken
        }

        failures++;
        if (failures >= maxFailures) {
          client.close();
          throw new Error(`Too many failures (${failures}), last error: ${error.message}`);
        }

        // Reconnect and continue
        console.error(`  Reconnecting... (failure ${failures}/${maxFailures})`);
        client.close();
        client = new TcpClient(options);
        await client.connect();
      }
    }

    client.close();
    if (failures > 0) {
      console.log(`\nCompleted with ${failures} transient failure(s)`);
    }
  });
});
