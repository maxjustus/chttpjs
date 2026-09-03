/**
 * Tests for framed HTTP responses (framing_output_format, ClickHouse 26.8+).
 *
 * Framing multiplexes data, totals, extremes, progress, logs, profile events
 * and exceptions into one response stream. The client parses the frames back
 * into the standard QueryPacket model, so the concatenation of Data chunks
 * must be byte-identical to the unframed format output.
 */

import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import {
  ClickHouseException,
  collectJsonEachRow,
  collectRows,
  collectText,
  dataChunks,
  init,
  query,
  streamDecodeNative,
  type QueryPacket,
} from "../client.ts";
import { startClickHouse, stopClickHouse } from "./setup.ts";
import { generateSessionId } from "./test_utils.ts";

// Framing formats land in 26.8, newer than the pinned suite default, so this
// file starts its own container. Override with CH_FRAMING_VERSION.
const FRAMING_CH_VERSION = process.env.CH_FRAMING_VERSION || "26.8";

describe("HTTP framing formats", { timeout: 120000 }, () => {
  let clickhouse: Awaited<ReturnType<typeof startClickHouse>>;
  let url: string;
  let auth: { username: string; password: string };
  const sessionId = generateSessionId("framing");

  before(async () => {
    await init();
    clickhouse = await startClickHouse(FRAMING_CH_VERSION);
    url = `${clickhouse.url}/`;
    auth = { username: clickhouse.username, password: clickhouse.password };
  });

  after(async () => {
    await stopClickHouse();
  });

  async function collectPackets(sql: string, options: Parameters<typeof query>[1]) {
    const packets: QueryPacket[] = [];
    for await (const packet of query(sql, options)) {
      packets.push(packet);
    }
    return packets;
  }

  describe("Auxiliary packets", () => {
    for (const framing of [
      "EventStream",
      "JSONEachPacketBase64",
      "JSONEachPacketString",
    ] as const) {
      it(`surfaces log and profile-events packets under ${framing}`, async () => {
        const packets = await collectPackets("SELECT number FROM numbers(10) FORMAT JSONEachRow", {
          url,
          auth,
          sessionId,
          framing,
          compression: false,
          settings: { send_logs_level: "trace" },
        });

        const logs = packets.filter((p) => p.type === "Log");
        assert.ok(logs.length > 0, "should surface log packets");
        for (const l of logs) {
          assert.ok(l.entries.length > 0);
          for (const entry of l.entries) {
            assert.strictEqual(typeof entry.text, "string");
            assert.strictEqual(typeof entry.source, "string");
            assert.strictEqual(typeof entry.priority, "string");
          }
        }

        const profileEvents = packets.filter((p) => p.type === "ProfileEvents");
        assert.ok(profileEvents.length > 0, "should surface profile-events packets");
        const selected = profileEvents
          .flatMap((p) => p.events)
          .find((e) => e.name === "SelectedRows");
        assert.ok(selected, "should report SelectedRows");
        assert.strictEqual(selected.value, "10");
        assert.ok(selected.type === "gauge" || selected.type === "increment");
      });
    }
  });

  describe("EventStream", () => {
    it("multiplexes data and progress packets in order", async () => {
      const packets = await collectPackets("SELECT number FROM numbers(1000) FORMAT JSONEachRow", {
        url,
        auth,
        sessionId,
        framing: "EventStream",
        compression: false,
      });

      assert.ok(
        packets.some((p) => p.type === "Progress"),
        "should surface progress packets",
      );
      const summary = packets.at(-1);
      assert.ok(summary?.type === "Summary");

      const data = packets
        .filter((p) => p.type === "Data")
        .map((p) => Buffer.from(p.chunk).toString());
      const rows = data.join("").split("\n").filter(Boolean);
      assert.strictEqual(rows.length, 1000);
      assert.strictEqual(JSON.parse(rows[0]!).number, 0);
      assert.strictEqual(JSON.parse(rows[rows.length - 1]!).number, 999);
    });

    it("reproduces the unframed format output byte for byte", async () => {
      const sql = "SELECT number, toString(number) AS s FROM numbers(500) FORMAT JSONEachRow";
      const framed = await collectText(
        query(sql, { url, auth, sessionId, framing: "EventStream" }),
      );
      const plain = await collectText(
        query(sql, { url, auth, sessionId, framing: "EventStream", compression: false }),
      );
      assert.strictEqual(framed, plain);
    });

    it("carries binary formats intact", async () => {
      const sql = "SELECT number, number + 1 AS next FROM numbers(100) FORMAT Native";
      const framed = await collectRows(
        streamDecodeNative(
          dataChunks(query(sql, { url, auth, sessionId, framing: "EventStream" })),
        ),
      );
      const plain = await collectRows(
        streamDecodeNative(
          dataChunks(
            query(sql, { url, auth, sessionId, framing: "EventStream", compression: false }),
          ),
        ),
      );
      assert.deepStrictEqual(framed, plain);
    });

    it("surfaces a mid-stream error from the exception packet", async () => {
      await assert.rejects(
        collectText(
          query("SELECT throwIf(number = 5, 'framed boom') FROM numbers(10) FORMAT JSONEachRow", {
            url,
            auth,
            sessionId,
            framing: "EventStream",
            compression: false,
          }),
        ),
        (err: unknown) => {
          assert.ok(err instanceof ClickHouseException);
          assert.match(err.message, /framed boom/);
          return true;
        },
      );
    });

    it("works with block compression", async () => {
      const sql = "SELECT number FROM numbers(2000) FORMAT CSV";
      const framed = await collectText(
        query(sql, { url, auth, sessionId, framing: "EventStream", compression: "lz4" }),
      );
      const plain = await collectText(
        query(sql, { url, auth, sessionId, framing: "EventStream", compression: false }),
      );
      assert.strictEqual(framed, plain);
      assert.strictEqual(framed.split("\n").filter(Boolean).length, 2000);
    });
  });

  describe("JSONEachPacketBase64", () => {
    it("reproduces the unframed format output byte for byte", async () => {
      const sql = "SELECT number, toString(number) AS s FROM numbers(500) FORMAT JSONEachRow";
      const framed = await collectText(
        query(sql, { url, auth, sessionId, framing: "JSONEachPacketBase64", compression: false }),
      );
      const plain = await collectText(
        query(sql, { url, auth, sessionId, framing: "EventStream", compression: false }),
      );
      assert.strictEqual(framed, plain);
    });
  });

  describe("JSONEachPacketString", () => {
    it("reproduces the unframed format output byte for byte", async () => {
      const sql =
        "SELECT number, concat('v', toString(number)) AS s FROM numbers(500) FORMAT JSONEachRow";
      const framed = await collectText(
        query(sql, { url, auth, sessionId, framing: "JSONEachPacketString", compression: false }),
      );
      const plain = await collectText(
        query(sql, { url, auth, sessionId, framing: "EventStream", compression: false }),
      );
      assert.strictEqual(framed, plain);
    });

    it("keeps JSON escapes in the payload intact", async () => {
      const rows = await collectJsonEachRow<{ s: string }>(
        query("SELECT 'a\"b\\c\nd' AS s FROM numbers(1) FORMAT JSONEachRow", {
          url,
          auth,
          sessionId,
          framing: "JSONEachPacketString",
          compression: false,
        }),
      );
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0]!.s, 'a"b\\c\nd');
    });
  });
});
