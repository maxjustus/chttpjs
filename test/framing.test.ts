/**
 * Unit tests for the framed-response parsers. No server: these pin the wire
 * grammar and the chunk-boundary handling that integration tests cannot force.
 */

import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { parseFramedStream, type FramedPacket, type FramingFormat } from "../framing.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Feed `body` through the parser in fixed-size chunks. */
async function parse(body: string, format: FramingFormat, chunkSize = body.length) {
  const bytes = encoder.encode(body);
  async function* chunks() {
    for (let i = 0; i < bytes.length; i += chunkSize) {
      yield bytes.subarray(i, i + chunkSize);
    }
  }
  const packets: FramedPacket[] = [];
  for await (const packet of parseFramedStream(chunks(), format)) packets.push(packet);
  return packets;
}

const b64 = (s: string) => Buffer.from(s).toString("base64");

const EVENT_STREAM_BODY =
  `event: data\ndata: ${b64('{"n":0}\n')}\n\n` +
  `event: progress\ndata: {"read_rows":"1"}\n\n` +
  `event: log\ndata: {"text":"hello","source":"executeQuery"}\n\n` +
  `event: profile_events\ndata: [{"name":"SelectedRows","value":"1"}]\n\n` +
  `event: data\ndata: ${b64('{"n":1}\n')}\n\n`;

describe("framed response parsers", () => {
  describe("EventStream", () => {
    it("parses every packet kind", async () => {
      const packets = await parse(EVENT_STREAM_BODY, "EventStream");
      assert.deepStrictEqual(
        packets.map((p) => p.kind),
        ["data", "progress", "log", "profile_events", "data"],
      );
      const payloads = packets
        .filter((p) => "payload" in p)
        .map((p) => decoder.decode(p.payload))
        .join("");
      assert.strictEqual(payloads, '{"n":0}\n{"n":1}\n');
    });

    it("parses identically when events are split across chunk boundaries", async () => {
      for (const chunkSize of [1, 2, 3, 7, 64]) {
        const packets = await parse(EVENT_STREAM_BODY, "EventStream", chunkSize);
        assert.deepStrictEqual(
          packets.map((p) => p.kind),
          ["data", "progress", "log", "profile_events", "data"],
          `chunk size ${chunkSize}`,
        );
      }
    });

    it("joins multiple data fields of one event", async () => {
      const [packet] = await parse(
        'event: progress\ndata: {"read_rows"\ndata: :"1"}\n\n',
        "EventStream",
      );
      assert.deepStrictEqual(packet, { kind: "progress", progress: { read_rows: "1" } });
    });

    it("ignores unknown packet kinds", async () => {
      const packets = await parse("event: future_kind\ndata: {}\n\n", "EventStream");
      assert.deepStrictEqual(packets, []);
    });

    it("rejects a stream that ends mid-packet", async () => {
      await assert.rejects(
        parse(`event: data\ndata: ${b64('{"n":0}\n')}`, "EventStream"),
        /truncated/i,
      );
    });
  });

  describe("JSONEachPacket", () => {
    const base64Body =
      `{"packet":"data","data":"${b64('{"n":0}\n')}"}\n` +
      `{"packet":"progress","progress":{"read_rows":"1"}}\n` +
      `{"packet":"log","log":{"text":"hello"}}\n` +
      `{"packet":"profile_events","profile_events":[{"name":"SelectedRows"}]}\n`;

    it("parses every packet kind", async () => {
      const packets = await parse(base64Body, "JSONEachPacketBase64");
      assert.deepStrictEqual(
        packets.map((p) => p.kind),
        ["data", "progress", "log", "profile_events"],
      );
    });

    it("parses identically when lines are split across chunk boundaries", async () => {
      for (const chunkSize of [1, 5, 33]) {
        const packets = await parse(base64Body, "JSONEachPacketBase64", chunkSize);
        assert.deepStrictEqual(
          packets.map((p) => p.kind),
          ["data", "progress", "log", "profile_events"],
          `chunk size ${chunkSize}`,
        );
      }
    });

    it("encodes string payloads as UTF-8, including across chunk boundaries", async () => {
      const body = `{"packet":"data","data":"\\u00e9\\u00e8 \\ud83d\\ude80\\n"}\n`;
      for (const chunkSize of [1, 4, body.length]) {
        const [packet] = await parse(body, "JSONEachPacketString", chunkSize);
        assert.ok(packet?.kind === "data");
        assert.strictEqual(decoder.decode(packet.payload), "éè 🚀\n");
      }
    });

    it("rejects a stream that ends mid-packet", async () => {
      await assert.rejects(
        parse('{"packet":"progress","progress":{"read_rows"', "JSONEachPacketBase64"),
        /truncated/i,
      );
    });
  });
});
