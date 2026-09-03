/**
 * Parsers for framed HTTP responses (ClickHouse 26.8+, setting
 * `framing_output_format`). A framing format multiplexes data, totals,
 * extremes, progress, logs, profile events, and exceptions into one response
 * stream. The concatenation of the decoded payloads of the data, totals, and
 * extremes packets is exactly what the output format would have produced
 * without framing.
 */

import { concat } from "./compression.ts";

export type FramingFormat = "EventStream" | "JSONEachPacketBase64" | "JSONEachPacketString";

/** One row of a `log` packet; all fields are strings on the wire. */
export interface HttpLogEntry {
  event_time: string;
  event_time_microseconds: string;
  host_name: string;
  query_id: string;
  thread_id: string;
  priority: string;
  source: string;
  text: string;
}

/** One row of a `profile_events` packet; all fields are strings on the wire. */
export interface HttpProfileEvent {
  host_name: string;
  current_time: string;
  thread_id: string;
  type: "gauge" | "increment";
  name: string;
  value: string;
}

export type FramedPacket =
  | { kind: "data" | "totals" | "extremes"; payload: Uint8Array }
  | { kind: "progress"; progress: Record<string, string> }
  | { kind: "log"; entry: HttpLogEntry }
  | { kind: "profile_events"; events: HttpProfileEvent[] }
  | { kind: "exception"; message: string };

const encoder = new TextEncoder();

function base64Decode(s: string): Uint8Array {
  if (typeof Buffer !== "undefined") return new Uint8Array(Buffer.from(s, "base64"));
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * Every packet ends with its delimiter, so bytes left over at end of stream mean
 * the server closed the connection mid-packet.
 */
function truncated(bytes: number): Error {
  return new Error(
    `Truncated framed response: stream ended mid-packet with ${bytes} bytes buffered`,
  );
}

/** Index of the first occurrence of `delim` at or after `from`, or -1. */
function findDelimiter(buf: Uint8Array, from: number, delim: Uint8Array): number {
  outer: for (let i = from; i + delim.length <= buf.length; i++) {
    for (let j = 0; j < delim.length; j++) {
      if (buf[i + j] !== delim[j]) continue outer;
    }
    return i;
  }
  return -1;
}

function framedPacketFromJson(kind: string, body: string): FramedPacket | undefined {
  switch (kind) {
    case "data":
    case "totals":
    case "extremes":
      return { kind, payload: base64Decode(body) };
    case "progress":
      return { kind: "progress", progress: JSON.parse(body) };
    case "log":
      return { kind: "log", entry: JSON.parse(body) };
    case "profile_events":
      return { kind: "profile_events", events: JSON.parse(body) };
    case "exception":
      return { kind: "exception", message: JSON.parse(body).exception };
    // unknown packet kinds are ignored for forward compatibility
    default:
      return undefined;
  }
}

/**
 * Server-sent events framing: `event: <kind>\ndata: <payload>\n\n`. Payload
 * packets are base64 (the formatted block has no line breaks, so it is always
 * one data field); auxiliary packets are JSON.
 */
async function* parseEventStream(chunks: AsyncIterable<Uint8Array>): AsyncGenerator<FramedPacket> {
  const decoder = new TextDecoder();
  const delimiter = new Uint8Array([10, 10]);
  let pending: Uint8Array = new Uint8Array(0);
  let start = 0; // start of the incomplete event
  let scanned = 0; // first byte not yet searched for the delimiter
  for await (const chunk of chunks) {
    pending = pending.length === 0 ? chunk : concat([pending, chunk]);
    for (;;) {
      const end = findDelimiter(pending, scanned, delimiter);
      if (end < 0) break;
      const packet = parseSseEvent(decoder.decode(pending.subarray(start, end)));
      if (packet) yield packet;
      start = scanned = end + delimiter.length;
    }
    pending = pending.subarray(start);
    start = 0;
    // a delimiter can span the chunk boundary; rescan the overhang next round
    scanned = Math.max(0, pending.length - (delimiter.length - 1));
  }
  if (pending.length > 0) throw truncated(pending.length);
}

function parseSseEvent(text: string): FramedPacket | undefined {
  let kind = "";
  const data: string[] = [];
  for (const line of text.split("\n")) {
    if (line.startsWith("event:")) kind = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
    // other SSE fields (id, retry, comments) do not occur; skip them
  }
  if (!kind || data.length === 0) return undefined;
  return framedPacketFromJson(kind, data.join("\n"));
}

/**
 * JSONEachPacket framing: one JSON object per line, `packet` selects the kind.
 * Payload data is base64 (`JSONEachPacketBase64`) or a JSON string
 * (`JSONEachPacketString`, text output formats only).
 */
async function* parseJsonEachPacket(
  chunks: AsyncIterable<Uint8Array>,
  base64: boolean,
): AsyncGenerator<FramedPacket> {
  const decoder = new TextDecoder();
  let pending = "";
  for await (const chunk of chunks) {
    pending += decoder.decode(chunk, { stream: true });
    let newline: number;
    while ((newline = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      if (line) {
        const packet = jsonPacketLine(line, base64);
        if (packet) yield packet;
      }
    }
  }
  pending += decoder.decode();
  if (pending) throw truncated(pending.length);
}

function jsonPacketLine(line: string, base64: boolean): FramedPacket | undefined {
  const obj = JSON.parse(line) as Record<string, unknown>;
  const kind = obj.packet as string;
  switch (kind) {
    case "data":
    case "totals":
    case "extremes":
      return {
        kind,
        payload: base64 ? base64Decode(obj.data as string) : encoder.encode(obj.data as string),
      };
    case "progress":
      return { kind: "progress", progress: obj.progress as Record<string, string> };
    case "log":
      return { kind: "log", entry: obj.log as HttpLogEntry };
    case "profile_events":
      return { kind: "profile_events", events: obj.profile_events as HttpProfileEvent[] };
    case "exception":
      return { kind: "exception", message: obj.exception as string };
    // unknown packet kinds are ignored for forward compatibility
    default:
      return undefined;
  }
}

/** Split a framed response body into packets. */
export function parseFramedStream(
  chunks: AsyncIterable<Uint8Array>,
  format: FramingFormat,
): AsyncGenerator<FramedPacket> {
  if (format === "EventStream") return parseEventStream(chunks);
  return parseJsonEachPacket(chunks, format === "JSONEachPacketBase64");
}
