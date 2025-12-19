import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";

// Settings for complex/experimental types
const QUERY_SETTINGS = {
  "use_variant_as_common_type": "1",
  "allow_experimental_variant_type": "1",
  "allow_experimental_dynamic_type": "1",
  "allow_experimental_json_type": "1",
  "output_format_native_use_flattened_dynamic_and_json_serialization": "1",
};

describe("TCP Client Fuzz Tests", { timeout: 600000, concurrency: 1 }, () => {
  const queryTimeout = parseInt(process.env.QUERY_TIMEOUT ?? "120000", 10);
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: "",
    debug: !!process.env.DEBUG,
    queryTimeout,
  };

  // ============================================================================
  // Test Helpers
  // ============================================================================

  function fuzzConfig(defaults: { iterations?: number; rows?: number } = {}) {
    return {
      iterations: parseInt(process.env.FUZZ_ITERATIONS ?? String(defaults.iterations ?? 5), 10),
      rowCount: parseInt(process.env.FUZZ_ROWS ?? String(defaults.rows ?? 10000), 10),
      verbose: !!process.env.VERBOSE,
    };
  }

  async function withClient<T>(fn: (client: TcpClient) => Promise<T>): Promise<T> {
    const client = new TcpClient(options);
    await client.connect();
    try {
      return await fn(client);
    } finally {
      client.close();
    }
  }

  async function withDualClients<T>(fn: (read: TcpClient, write: TcpClient) => Promise<T>): Promise<T> {
    const read = new TcpClient(options);
    const write = new TcpClient(options);
    await Promise.all([read.connect(), write.connect()]);
    try {
      return await fn(read, write);
    } finally {
      read.close();
      write.close();
    }
  }

  async function consumeQuery(
    client: TcpClient,
    sql: string,
    settings?: Record<string, string>
  ): Promise<{ totalRows: number; blocks: number }> {
    let totalRows = 0, blocks = 0;
    const verbose = !!process.env.VERBOSE;
    for await (const p of client.query(sql, settings)) {
      if (p.type === "Data") {
        totalRows += p.batch.rowCount;
        blocks++;
        if (verbose) {
          const ms = p.batch.decodeTimeMs?.toFixed(2) ?? "?";
          console.log(`    block ${blocks}: ${p.batch.rowCount} rows (${ms}ms decode)`);
        }
      }
    }
    return { totalRows, blocks };
  }

  async function streamRoundTrip(
    readClient: TcpClient,
    writeClient: TcpClient,
    srcTable: string,
    dstTable: string,
    settings?: Record<string, string>
  ): Promise<number> {
    let rowsRead = 0;
    const stream = (async function* () {
      for await (const p of readClient.query(`SELECT * FROM ${srcTable}`, settings)) {
        if (p.type === "Data") {
          rowsRead += p.batch.rowCount;
          yield p.batch;
        }
      }
    })();
    await writeClient.insert(`INSERT INTO ${dstTable} VALUES`, stream);
    console.log(`  transferred ${rowsRead} rows`);
    await verifyTableEquality(writeClient, srcTable, dstTable, settings);
    console.log(`  verified OK`);
    return rowsRead;
  }

  async function verifyTableEquality(
    client: TcpClient,
    srcTable: string,
    dstTable: string,
    settings?: Record<string, string>
  ): Promise<void> {
    let srcCount = 0n, dstCount = 0n;
    for await (const p of client.query(`SELECT count() as c FROM ${srcTable}`, settings ?? QUERY_SETTINGS)) {
      if (p.type === "Data" && p.batch.rowCount > 0) srcCount = (p.batch.rows().next().value as any).c;
    }
    for await (const p of client.query(`SELECT count() as c FROM ${dstTable}`, settings ?? QUERY_SETTINGS)) {
      if (p.type === "Data" && p.batch.rowCount > 0) dstCount = (p.batch.rows().next().value as any).c;
    }
    if (srcCount !== dstCount) {
      throw new Error(`Row count mismatch: src=${srcCount}, dst=${dstCount}`);
    }
  }

  /** Execute a statement (DDL/DML) with optional settings via query() */
  async function exec(client: TcpClient, sql: string, settings?: Record<string, string>): Promise<void> {
    for await (const _ of client.query(sql, settings)) { /* drain */ }
  }

  async function getRandomStructure(
    client: TcpClient,
    numColumns?: number,
    settings?: Record<string, string>
  ): Promise<string> {
    const query = numColumns
      ? `SELECT generateRandomStructure(${numColumns}) AS s`
      : `SELECT generateRandomStructure() AS s`;
    let result = "";
    for await (const p of client.query(query, settings)) {
      if (p.type === "Data" && p.batch.rowCount > 0 && !result) {
        const row = p.batch.rows().next().value as any;
        if (row && row.s) result = row.s;
      }
    }
    if (!result) throw new Error("Failed to get random structure");
    return result;
  }

  // Quick read-only fuzz: just SELECT random data, no round-trip
  // Usage: FUZZ_ITERATIONS=50 FUZZ_ROWS=100000 make fuzz-tcp
  test("decode random structures", async () => {
    const { iterations, rowCount } = fuzzConfig({ iterations: 10, rows: 20000 });
    let client = new TcpClient(options);
    await client.connect();
    let failures = 0;
    const maxFailures = 3;

    for (let i = 0; i < iterations; i++) {
      let structure = "";
      try {
        structure = await getRandomStructure(client);
        console.log(`[tcp fuzz ${i + 1}/${iterations}] ${structure.slice(0, 100)}...`);

        const escaped = structure.replace(/'/g, "''");
        const start = Date.now();
        const { totalRows, blocks } = await consumeQuery(
          client,
          `SELECT * FROM generateRandom('${escaped}') LIMIT ${rowCount}`
        );
        console.log(`  ${totalRows} rows, ${blocks} blocks (${((Date.now() - start) / 1000).toFixed(2)}s)`);
        assert.strictEqual(totalRows, rowCount, `Expected ${rowCount} rows, got ${totalRows}`);
      } catch (err) {
        const error = err as Error;
        console.error(`\n[FUZZ FAILURE] iteration ${i + 1}/${iterations}`);
        console.error(`  Structure: ${structure || "(not yet fetched)"}`);
        console.error(`  Error: ${error.message}`);
        failures++;
        if (failures >= maxFailures) {
          client.close();
          throw new Error(`Too many failures (${failures}), last error: ${error.message}`);
        }
        console.error(`  Reconnecting... (failure ${failures}/${maxFailures})`);
        client.close();
        client = new TcpClient(options);
        await client.connect();
      }
    }
    client.close();
  });

  // Full round-trip fuzz: SELECT -> INSERT -> verify
  // Skip with: SKIP_ROUNDTRIP=1 make fuzz-tcp
  test("round-trip random structures", { skip: !!process.env.SKIP_ROUNDTRIP }, async () => {
    const { iterations, rowCount } = fuzzConfig({ iterations: 5, rows: 80000 });
    let readClient = new TcpClient(options);
    let writeClient = new TcpClient(options);
    await Promise.all([readClient.connect(), writeClient.connect()]);
    let failures = 0;
    const maxFailures = 3;

    for (let i = 0; i < iterations; i++) {
      const srcTable = `tcp_fuzz_src_${i}_${Date.now()}`;
      const dstTable = `tcp_fuzz_dst_${i}_${Date.now()}`;
      let structure = "";

      try {
        structure = await getRandomStructure(writeClient);
        console.log(`[tcp round-trip ${i + 1}/${iterations}] ${structure.slice(0, 80)}...`);

        const escaped = structure.replace(/'/g, "''");
        await writeClient.execute(`CREATE TABLE ${srcTable} ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM generateRandom('${escaped}') LIMIT ${rowCount}`);
        await writeClient.execute(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`);

        const start = Date.now();
        let blocksRead = 0, rowsRead = 0;
        const stream = (async function* () {
          for await (const p of readClient.query(`SELECT * FROM ${srcTable}`)) {
            if (p.type === "Data") {
              blocksRead++;
              rowsRead += p.batch.rowCount;
              yield p.batch;
            }
          }
        })();
        await writeClient.insert(`INSERT INTO ${dstTable} VALUES`, stream);
        console.log(`  transferred ${rowsRead} rows, ${blocksRead} blocks (${((Date.now() - start) / 1000).toFixed(2)}s)`);

        await verifyTableEquality(writeClient, srcTable, dstTable);
        console.log(`  verified OK`);
        await writeClient.execute(`DROP TABLE IF EXISTS ${srcTable}`);
        await writeClient.execute(`DROP TABLE IF EXISTS ${dstTable}`);
      } catch (err) {
        const error = err as Error;
        console.error(`\n[ROUND-TRIP FAILURE] ${structure || "(no structure)"}: ${error.message}`);
        try {
          await writeClient.execute(`DROP TABLE IF EXISTS ${srcTable}`);
          await writeClient.execute(`DROP TABLE IF EXISTS ${dstTable}`);
        } catch { /* ignore */ }
        failures++;
        if (failures >= maxFailures) {
          readClient.close();
          writeClient.close();
          throw new Error(`Too many failures (${failures}), last error: ${error.message}`);
        }
        console.error(`  Reconnecting... (failure ${failures}/${maxFailures})`);
        readClient.close();
        writeClient.close();
        readClient = new TcpClient(options);
        writeClient = new TcpClient(options);
        await Promise.all([readClient.connect(), writeClient.connect()]);
      }
    }
    readClient.close();
    writeClient.close();
  });

  // ============================================================================
  // Complex Types Fuzz Tests (Variant, Dynamic, JSON)
  // ============================================================================

  test("decode Variant types", () => withClient(async (client) => {
    const { iterations, rowCount } = fuzzConfig({ iterations: 5, rows: 10000 });
    for (let i = 0; i < iterations; i++) {
      const t1 = `variant_t1_${i}_${Date.now()}`;
      const t2 = `variant_t2_${i}_${Date.now()}`;
      try {
        const struct1 = await getRandomStructure(client, 3, QUERY_SETTINGS);
        const struct2 = await getRandomStructure(client, 3, QUERY_SETTINGS);
        console.log(`[variant decode ${i + 1}/${iterations}] ${struct1.slice(0, 50)}...`);

        const esc1 = struct1.replace(/'/g, "''"), esc2 = struct2.replace(/'/g, "''");
        const half = Math.floor(rowCount / 2);
        await exec(client, `CREATE TABLE ${t1} ENGINE=Memory AS SELECT rowNumberInAllBlocks() as idx, tuple(*) as data FROM generateRandom('${esc1}') LIMIT ${half}`, QUERY_SETTINGS);
        await exec(client, `CREATE TABLE ${t2} ENGINE=Memory AS SELECT rowNumberInAllBlocks() as idx, tuple(*) as data FROM generateRandom('${esc2}') LIMIT ${half}`, QUERY_SETTINGS);

        const { totalRows, blocks } = await consumeQuery(client,
          `SELECT multiIf(rand() % 3 = 0, ${t1}.data, rand() % 3 = 1, ${t2}.data, NULL) AS v
           FROM numbers(${rowCount}) AS n
           LEFT JOIN ${t1} ON ${t1}.idx = n.number % ${half}
           LEFT JOIN ${t2} ON ${t2}.idx = n.number % ${half}`,
          QUERY_SETTINGS
        );
        console.log(`  ${totalRows} rows, ${blocks} blocks`);
        assert.strictEqual(totalRows, rowCount);
      } finally {
        await client.execute(`DROP TABLE IF EXISTS ${t1}`);
        await client.execute(`DROP TABLE IF EXISTS ${t2}`);
      }
    }
  }));

  test("decode Dynamic types", () => withClient(async (client) => {
    const { iterations, rowCount } = fuzzConfig({ iterations: 5, rows: 10000 });
    for (let i = 0; i < iterations; i++) {
      console.log(`[dynamic decode ${i + 1}/${iterations}]`);
      const { totalRows, blocks } = await consumeQuery(client,
        `SELECT multiIf(
          n % 7 = 0, tuple(rand64(), rand64(), randomStringUTF8(10))::Dynamic,
          n % 7 = 1, [rand(), rand(), rand()]::Dynamic,
          n % 7 = 2, map('a', rand64(), 'b', rand64())::Dynamic,
          n % 7 = 3, rand64()::Dynamic,
          n % 7 = 4, randomStringUTF8(50)::Dynamic,
          n % 7 = 5, tuple(rand(), [rand64(), rand64()], randomStringUTF8(5))::Dynamic,
          NULL::Dynamic
        ) AS d FROM (SELECT number AS n FROM numbers(${rowCount}))`,
        QUERY_SETTINGS
      );
      console.log(`  ${totalRows} rows, ${blocks} blocks`);
      assert.strictEqual(totalRows, rowCount);
    }
  }));

  // Note: ClickHouse refuses UInt128/Int128/UInt256/Int256 in JSON. We use map()
  // which requires homogeneous value types, so we stringify for mixed-type objects.
  test("decode JSON types", () => withClient(async (client) => {
    const { iterations, rowCount } = fuzzConfig({ iterations: 5, rows: 10000 });
    for (let i = 0; i < iterations; i++) {
      console.log(`[json decode ${i + 1}/${iterations}]`);
      const { totalRows, blocks } = await consumeQuery(client,
        `SELECT map(
          'id', toString(rowNumberInAllBlocks()),
          'c1', toString(rand64()),
          'c2', toString(rand()),
          'c3', randomPrintableASCII(10),
          'nested', toString(map('x', rand(), 'y', rand()))
        )::JSON AS j FROM numbers(${rowCount})`,
        QUERY_SETTINGS
      );
      console.log(`  ${totalRows} rows, ${blocks} blocks`);
      assert.strictEqual(totalRows, rowCount);
    }
  }));

  // Verify JSON correctly handles UInt64 values beyond JS MAX_SAFE_INTEGER as BigInt
  test("decode JSON with large UInt64", () => withClient(async (client) => {
    const largeValue = 18446744073709551615n; // UInt64 max
    for await (const p of client.query(
      `SELECT map('big', toUInt64('18446744073709551615'))::JSON AS j`,
      QUERY_SETTINGS
    )) {
      if (p.type === "Data") {
        for (const row of p.batch.rows()) {
          const val = (row as any).j?.big;
          assert.strictEqual(typeof val, "bigint", "Large UInt64 should be BigInt");
          assert.strictEqual(val, largeValue);
        }
      }
    }
  }));

  test("round-trip Variant types", { skip: !!process.env.SKIP_ROUNDTRIP }, () =>
    withDualClients(async (read, write) => {
      const { iterations, rowCount } = fuzzConfig({ iterations: 3, rows: 20000 });
      for (let i = 0; i < iterations; i++) {
        const src = `variant_src_${i}_${Date.now()}`, dst = `variant_dst_${i}_${Date.now()}`;
        const t1 = `variant_t1_${i}_${Date.now()}`, t2 = `variant_t2_${i}_${Date.now()}`;
        try {
          const s1 = await getRandomStructure(write, 3, QUERY_SETTINGS);
          const s2 = await getRandomStructure(write, 3, QUERY_SETTINGS);
          console.log(`[variant round-trip ${i + 1}/${iterations}]`);

          const esc1 = s1.replace(/'/g, "''"), esc2 = s2.replace(/'/g, "''");
          const half = Math.floor(rowCount / 2);
          await exec(write, `CREATE TABLE ${t1} ENGINE=Memory AS SELECT rowNumberInAllBlocks() as idx, tuple(*) as data FROM generateRandom('${esc1}') LIMIT ${half}`, QUERY_SETTINGS);
          await exec(write, `CREATE TABLE ${t2} ENGINE=Memory AS SELECT rowNumberInAllBlocks() as idx, tuple(*) as data FROM generateRandom('${esc2}') LIMIT ${half}`, QUERY_SETTINGS);
          await exec(write, `CREATE TABLE ${src} ENGINE=Memory AS SELECT multiIf(rand() % 3 = 0, ${t1}.data, rand() % 3 = 1, ${t2}.data, NULL) AS v FROM numbers(${rowCount}) AS n LEFT JOIN ${t1} ON ${t1}.idx = n.number % ${half} LEFT JOIN ${t2} ON ${t2}.idx = n.number % ${half}`, QUERY_SETTINGS);
          await exec(write, `CREATE TABLE ${dst} EMPTY AS ${src}`, QUERY_SETTINGS);

          await streamRoundTrip(read, write, src, dst, QUERY_SETTINGS);
        } finally {
          await write.execute(`DROP TABLE IF EXISTS ${src}`);
          await write.execute(`DROP TABLE IF EXISTS ${dst}`);
          await write.execute(`DROP TABLE IF EXISTS ${t1}`);
          await write.execute(`DROP TABLE IF EXISTS ${t2}`);
        }
      }
    })
  );

  test("round-trip Dynamic types", { skip: !!process.env.SKIP_ROUNDTRIP }, () =>
    withDualClients(async (read, write) => {
      const { iterations, rowCount } = fuzzConfig({ iterations: 3, rows: 20000 });
      for (let i = 0; i < iterations; i++) {
        const src = `dynamic_src_${i}_${Date.now()}`, dst = `dynamic_dst_${i}_${Date.now()}`;
        try {
          console.log(`[dynamic round-trip ${i + 1}/${iterations}]`);
          await exec(write, `CREATE TABLE ${src} ENGINE=Memory AS SELECT multiIf(
            n % 7 = 0, tuple(rand64(), rand64())::Dynamic,
            n % 7 = 1, [rand(), rand(), rand()]::Dynamic,
            n % 7 = 2, map('a', rand64(), 'b', rand64())::Dynamic,
            n % 7 = 3, rand64()::Dynamic,
            n % 7 = 4, toString(rand64())::Dynamic,
            n % 7 = 5, tuple(rand(), [rand64(), rand64()])::Dynamic,
            NULL::Dynamic
          ) AS d FROM (SELECT number AS n FROM numbers(${rowCount}))`, QUERY_SETTINGS);
          await exec(write, `CREATE TABLE ${dst} EMPTY AS ${src}`, QUERY_SETTINGS);

          await streamRoundTrip(read, write, src, dst, QUERY_SETTINGS);
        } finally {
          await write.execute(`DROP TABLE IF EXISTS ${src}`);
          await write.execute(`DROP TABLE IF EXISTS ${dst}`);
        }
      }
    })
  );

  test("round-trip JSON types", { skip: !!process.env.SKIP_ROUNDTRIP }, () =>
    withDualClients(async (read, write) => {
      const { iterations, rowCount } = fuzzConfig({ iterations: 3, rows: 20000 });
      for (let i = 0; i < iterations; i++) {
        const src = `json_src_${i}_${Date.now()}`, dst = `json_dst_${i}_${Date.now()}`;
        try {
          console.log(`[json round-trip ${i + 1}/${iterations}]`);
          await exec(write, `CREATE TABLE ${src} ENGINE=Memory AS SELECT map(
            'id', toString(rowNumberInAllBlocks()),
            'c1', toString(rand64()),
            'c2', toString(rand()),
            'c3', randomPrintableASCII(10),
            'c4', toString(rand() % 2 = 0)
          )::JSON AS j FROM numbers(${rowCount})`, QUERY_SETTINGS);
          await exec(write, `CREATE TABLE ${dst} EMPTY AS ${src}`, QUERY_SETTINGS);

          await streamRoundTrip(read, write, src, dst, QUERY_SETTINGS);
        } finally {
          await write.execute(`DROP TABLE IF EXISTS ${src}`);
          await write.execute(`DROP TABLE IF EXISTS ${dst}`);
        }
      }
    })
  );
});
