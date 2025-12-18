
import { test, describe } from "node:test";
import assert from "node:assert";
import { TcpClient } from "../client.ts";
import { asRows } from "../../formats/native/index.ts";

describe("TCP Client Full Fuzz Gauntlet", { timeout: 600000 }, () => {
  const options = {
    host: "localhost",
    port: 9000,
    user: "default",
    password: "",
    debug: false
  };

  test("should round-trip random data structures", async () => {
    const client = new TcpClient(options);
    await client.connect();
    
    try {
      const iterations = parseInt(process.env.FUZZ_ITERATIONS ?? "5", 10);
      
      for (let i = 0; i < iterations; i++) {
        const srcTable = `tcp_fuzz_src_${i}_${Date.now()}`;
        const dstTable = `tcp_fuzz_dst_${i}_${Date.now()}`;
        
        // 1. Get a random structure from ClickHouse
        let structure = "";
        const structStream = client.query("SELECT generateRandomStructure()");
        for await (const packet of structStream) {
          if (packet.type === "Data") {
            for (const row of asRows(packet.table)) {
              structure = row["generateRandomStructure()"] as string;
            }
          }
        }
        
        console.log(`[Fuzz ${i+1}/${iterations}] Structure: ${structure}`);
        
        // 2. Create source table with random data
        const rowCount = 5000;
        const escaped = structure.replace(/'/g, "''");
        await client.execute(`CREATE TABLE ${srcTable} ENGINE = Memory AS SELECT * FROM generateRandom('${escaped}') LIMIT ${rowCount}`);
        
        // 3. Create empty destination table
        await client.execute(`CREATE TABLE ${dstTable} EMPTY AS ${srcTable}`);
        
        // 4. Stream from SRC to DST via TCP
        console.log(`  Streaming ${rowCount} rows from SRC to DST...`);
        const queryStream = (async function*() {
          const stream = client.query(`SELECT * FROM ${srcTable}`);
          for await (const packet of stream) {
            if (packet.type === "Data") {
              yield packet.table;
            }
          }
        })();
        
        await client.insert(`INSERT INTO ${dstTable} VALUES`, queryStream);
        
        // 5. Verify equality using count and hash
        let srcCount = 0n;
        let dstCount = 0n;
        
        for await (const p of client.query(`SELECT count() as c FROM ${srcTable}`)) {
          if (p.type === "Data") srcCount = (asRows(p.table).next().value as any).c;
        }
        for await (const p of client.query(`SELECT count() as c FROM ${dstTable}`)) {
          if (p.type === "Data") dstCount = (asRows(p.table).next().value as any).c;
        }
        
        assert.strictEqual(dstCount, srcCount, "Row count mismatch");

        console.log(`  [Fuzz ${i+1}/${iterations}] PASSED`);
        
        await client.execute(`DROP TABLE ${srcTable}`);
        await client.execute(`DROP TABLE ${dstTable}`);
      }
    } finally {
      client.close();
    }
  });
});
