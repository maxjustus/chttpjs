import assert from "node:assert";
import { test } from "node:test";
import { Xxhash32Stream, xxhash32 } from "../xxhash32.ts";

const enc = new TextEncoder();

// Reference vectors from the xxHash test suite (xxhsum -H0), seed 0.
test("matches known xxHash-32 vectors", () => {
  assert.equal(xxhash32(enc.encode("")), 0x02cc5d05);
  assert.equal(xxhash32(enc.encode("a")), 0x550d7456);
  assert.equal(xxhash32(enc.encode("abc")), 0x32d153ff);
  assert.equal(xxhash32(enc.encode("abcdefghijklmnopqrstuvwxyz")), 0x63a14d5f);
});

test("matches a one-shot digest when fed in arbitrary-size chunks", () => {
  const data = enc.encode("abcdefghijklmnopqrstuvwxyz0123456789".repeat(50));
  const expected = xxhash32(data);

  for (const chunkSize of [1, 3, 16, 17, 4096]) {
    const stream = new Xxhash32Stream();
    for (let i = 0; i < data.length; i += chunkSize) {
      stream.update(data.subarray(i, i + chunkSize));
    }
    assert.equal(stream.digest(), expected, `chunk size ${chunkSize}`);
  }
});

test("empty stream matches empty one-shot digest", () => {
  assert.equal(new Xxhash32Stream().digest(), xxhash32(new Uint8Array(0)));
});
