import assert from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { createLz4FrameDecoder } from "../lz4_frame.ts";

// Captured from ClickHouse: `Accept-Encoding: lz4` with enable_http_compression=1.
// 4 linked blocks (frame FLG bit 5 clear), so matches reach across block edges.
const FRAME = new Uint8Array(readFileSync(new URL("fixtures/ch-lz4-frame.bin", import.meta.url)));
const DECODED_LEN = 822146;
const ROW = "ab".repeat(60);

function decodeAll(frame: Uint8Array, chunkSize: number): Uint8Array {
  const decoder = createLz4FrameDecoder();
  const out: Uint8Array[] = [];
  for (let i = 0; i < frame.length; i += chunkSize) {
    out.push(decoder.push(frame.subarray(i, i + chunkSize)));
  }
  const total = out.reduce((n, c) => n + c.length, 0);
  const joined = new Uint8Array(total);
  let off = 0;
  for (const c of out) {
    joined.set(c, off);
    off += c.length;
  }
  return joined;
}

test("decodes a ClickHouse frame whose matches span block boundaries", () => {
  const text = new TextDecoder().decode(decodeAll(FRAME, FRAME.length));
  assert.equal(text.length, DECODED_LEN);
  assert.equal(text.split(`{"s":"${ROW}"}`).length - 1, 6000);
  assert.ok(text.startsWith('{"progress"'));
});

test("decodes to the same bytes regardless of chunk boundaries", () => {
  const whole = decodeAll(FRAME, FRAME.length);
  for (const size of [1, 7, 64, 1024]) {
    assert.deepEqual(decodeAll(FRAME, size), whole, `chunk size ${size}`);
  }
});

test("returns every complete block when the frame ends without an endmark", () => {
  // A mid-stream server error closes the connection with the last block partial.
  const truncated = FRAME.subarray(0, FRAME.length - 200);
  const text = new TextDecoder().decode(decodeAll(truncated, 512));
  assert.ok(text.length > 0 && text.length < DECODED_LEN);
  assert.ok(text.includes(ROW));
});

test("decodes a frame whose blocks each carry a checksum", () => {
  // `lz4 -BX -BD -B4`: block checksums, linked blocks, 64 KiB blocks (11 of them).
  const frame = new Uint8Array(
    readFileSync(new URL("fixtures/lz4-block-checksum.bin", import.meta.url)),
  );
  const expected =
    Array.from({ length: 20000 }, (_, i) => `line ${i % 100} of the lz4 checksum fixture`).join(
      "\n",
    ) + "\n";
  assert.equal(new TextDecoder().decode(decodeAll(frame, 4096)), expected);
});

test("rejects input that is not an LZ4 frame", () => {
  assert.throws(() => createLz4FrameDecoder().push(new Uint8Array(8)), /magic/i);
});
