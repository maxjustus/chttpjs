/**
 * LZ4 frame decoder oracle fuzz.
 *
 * Compress random payloads with lz4-napi's frame compressor, decode them with
 * the streaming frame decoder at arbitrary chunk boundaries, and compare the
 * output byte-for-byte against lz4-napi's frame decompressor.
 *
 * lz4-napi (lz4-flex) emits independent 64 KiB blocks with no checksums, which
 * the ClickHouse capture fixture (linked blocks) and the CLI checksum fixtures
 * do not cover; this suite cross-checks every decode path — literals, short,
 * long and overlapping matches, uncompressed blocks, multi-block frames —
 * against an independent implementation.
 *
 * Reproduce a failure with FUZZ_ITERATION_INDEX=<iter> tsx --test fuzz/lz4.ts.
 * Every payload and chunk boundary derives from the per-iteration seeded RNG.
 */

import assert from "node:assert";
import { describe, it } from "node:test";
import { concat } from "../compression.ts";
import { createLz4FrameDecoder } from "../lz4_frame.ts";
import type { Rng } from "../native/codecs/base.ts";
import { config, getIterationIndex, logConfig } from "./config.ts";
import { makeRng } from "./rng.ts";
import { pick, randomString } from "./util.ts";

logConfig("lz4");

const SMALL_FRAME_LEN = 1024; // 1-byte chunk cap; pending copies per push
const TINY_STEP_FRAME_LEN = 8192; // fixed 2-8 byte step chunk cap

function randomBytes(rng: Rng, len: number): Uint8Array {
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = rng.int(0, 255);
  return out;
}

/** Short period repetition: runs with a small match offset. */
function periodic(rng: Rng, len: number): Uint8Array {
  const seedLen = rng.int(1, 16);
  const seed = randomBytes(rng, seedLen);
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = seed[i % seedLen]!;
  return out;
}

/** Random bytes over a small alphabet: many short matches. */
function smallAlphabet(rng: Rng, len: number): Uint8Array {
  const span = rng.int(2, 8);
  const base = rng.int(0, 255 - span);
  const out = new Uint8Array(len);
  for (let i = 0; i < len; i++) out[i] = base + rng.int(0, span);
  return out;
}

/** JSONEachRow-like lines: literals plus matches at structural distance. */
function textLines(rng: Rng, len: number): Uint8Array {
  const out = new Uint8Array(len);
  let off = 0;
  for (let i = 0; off < len; i++) {
    const line = `{"row":${i},"s":"${randomString(rng, 12)}"}\n`;
    const bytes = new TextEncoder().encode(line);
    const n = Math.min(bytes.length, len - off);
    out.set(bytes.subarray(0, n), off);
    off += n;
  }
  return out;
}

/** Random segment mix: a block that contains both stored and matched content. */
function mixed(rng: Rng, len: number): Uint8Array {
  const builders = [randomBytes, periodic, smallAlphabet, textLines];
  const out = new Uint8Array(len);
  let off = 0;
  while (off < len) {
    const builder = pick(rng, builders);
    const segLen = Math.min(rng.int(1, 8192), len - off);
    out.set(builder(rng, segLen), off);
    off += segLen;
  }
  return out;
}

const BUILDERS: Array<(rng: Rng, len: number) => Uint8Array> = [
  randomBytes,
  periodic,
  smallAlphabet,
  textLines,
  mixed,
];

/** Around and across the 64 KiB block boundary, plus a couple of larger frames. */
const PAYLOAD_LENGTHS = [0, 1, 3, 7, 63, 255, 4096, 65535, 65536, 65537, 200000, 500000];

function decodeAll(rng: Rng, frame: Uint8Array): Uint8Array {
  const decoder = createLz4FrameDecoder();
  const parts: Uint8Array[] = [];
  const push = (chunk: Uint8Array) => {
    const out = decoder.push(chunk);
    if (out.length > 0) parts.push(out);
  };

  let mode = rng.int(0, 3);
  if (
    (mode === 2 && frame.length > SMALL_FRAME_LEN) ||
    (mode === 3 && frame.length > TINY_STEP_FRAME_LEN)
  ) {
    mode = 1; // fall back to random cuts: tiny chunks would copy pending per push
  }

  if (mode === 0) {
    push(frame);
  } else if (mode === 1) {
    for (let offset = 0; offset < frame.length; ) {
      const size = rng.int(1, frame.length - offset);
      push(frame.subarray(offset, offset + size));
      offset += size;
    }
  } else if (mode === 2) {
    const step = rng.int(2, 8);
    for (let offset = 0; offset < frame.length; offset += step) {
      push(frame.subarray(offset, Math.min(offset + step, frame.length)));
    }
  } else {
    for (let i = 0; i < frame.length; i++) push(frame.subarray(i, i + 1));
  }

  return concat(parts);
}

describe("LZ4 frame decoder oracle", { timeout: 120000 }, () => {
  const iterationIndex = getIterationIndex();
  const iterations = iterationIndex !== null ? 1 : config.iterations;
  const startIdx = iterationIndex ?? 0;

  it("decodes lz4-napi frames byte-identically at any chunk boundary", async () => {
    const lz4 = await import("lz4-napi");
    for (let iter = startIdx; iter < startIdx + iterations; iter++) {
      const rng = makeRng(iter);
      const payloadCount = rng.int(2, 5);
      for (let p = 0; p < payloadCount; p++) {
        const len = PAYLOAD_LENGTHS[rng.int(0, PAYLOAD_LENGTHS.length - 1)]!;
        const builder = pick(rng, BUILDERS);
        const payload = builder(rng, len);
        const frame = lz4.compressFrameSync(Buffer.from(payload));
        const expected = lz4.decompressFrameSync(frame);
        const ctx = `iter=${iter} payload=${p} len=${len} builder=${builder.name} frame=${frame.length}`;
        const decoded = decodeAll(rng, frame);
        assert.equal(decoded.length, expected.length, `${ctx}: decoded length`);
        assert.equal(Buffer.compare(Buffer.from(decoded), expected), 0, ctx);
      }
    }
  });
});
