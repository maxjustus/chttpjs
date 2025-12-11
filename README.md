# chttp

ClickHouse HTTP client with native compression (LZ4/ZSTD).

## Install

```bash
npm install @maxjustus/chttp
```

For smaller bundle (LZ4 only, no ZSTD):
```ts
import { ... } from "@maxjustus/chttp/lz4";
```

## Quick Start

```ts
import { insert, query, streamJsonEachRow } from "@maxjustus/chttp";

const config = {
  baseUrl: "http://localhost:8123/",
  auth: { username: "default", password: "" }
};

// Insert with JSON data (using streamJsonEachRow helper)
await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  streamJsonEachRow([{ id: 1, name: "test" }]),
  "session123",
  config  // compression defaults to "lz4"
);

// Insert raw bytes (any format)
const encoder = new TextEncoder();
const csvData = encoder.encode("1,test\n2,other\n");
await insert(
  "INSERT INTO table FORMAT CSV",
  csvData,
  "session123",
  config
);

// Query (compression enabled by default)
for await (const chunk of query(
  "SELECT * FROM table FORMAT JSON",
  "session123",
  config,
)) {
  console.log(chunk);
}

// DDL statements (consume the iterator)
for await (const _ of query("CREATE TABLE ...", "session123", config)) {}
```

## Streaming Large Inserts

The `insert` function accepts `Uint8Array`, `Uint8Array[]`, or `AsyncIterable<Uint8Array>`. Use `streamJsonEachRow` for JSON data:

```ts
// Streaming JSON objects
async function* generateRows() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, value: `data_${i}` };
  }
}

await insert(
  "INSERT INTO large_table FORMAT JSONEachRow",
  streamJsonEachRow(generateRows()),
  "session123",
  { compression: "zstd", onProgress: (p) => console.log(`${p.bytesUncompressed} bytes`) }
);

// Streaming raw bytes (any format)
async function* generateCsvChunks() {
  const encoder = new TextEncoder();
  for (let batch = 0; batch < 1000; batch++) {
    let chunk = "";
    for (let i = 0; i < 1000; i++) {
      chunk += `${batch * 1000 + i},value_${i}\n`;
    }
    yield encoder.encode(chunk);
  }
}

await insert(
  "INSERT INTO large_table FORMAT CSV",
  generateCsvChunks(),
  "session123",
  { compression: "lz4" }
);
```

## Parsing Query Results

The `query()` function yields decompressed chunks aligned to compression blocks, not rows. Use helpers to parse:

```ts
import { query, streamLines, streamJsonLines, collectResponse } from "@maxjustus/chttp";

// JSONEachRow - streaming parsed objects
for await (const row of streamJsonLines(query("SELECT * FROM t FORMAT JSONEachRow", session, config))) {
  console.log(row.id, row.name);
}

// CSV/TSV - streaming raw lines
for await (const line of streamLines(query("SELECT * FROM t FORMAT CSV", session, config))) {
  const [id, name] = line.split(",");
}

// JSON format - buffer entire response with helper
const json = await collectResponse(query("SELECT * FROM t FORMAT JSON", session, config));
const data = JSON.parse(json);

// Or buffer manually
let result = "";
for await (const chunk of query("SELECT * FROM t FORMAT JSON", session, config)) {
  result += chunk;
}
const data2 = JSON.parse(result);
```

## Timeout and Cancellation

Configure with `timeout` (ms) or provide an `AbortSignal` for manual cancellation:

```ts
// Custom timeout
await insert(query, data, sessionId, { timeout: 60_000 });

// Manual cancellation
const controller = new AbortController();
setTimeout(() => controller.abort(), 5000);
await insert(query, data, sessionId, { signal: controller.signal });

// Both (whichever triggers first)
await insert(query, data, sessionId, {
  signal: controller.signal,
  timeout: 60_000
});
```

Requires Node.js 20+ or modern browsers (Chrome 116+, Firefox 124+, Safari 17.4+) for `AbortSignal.any()`.

## Compression

Set `compression` in options:
- `"lz4"` - fast, WASM (default)
- `"zstd"` - smaller output, native in Node.js with WASM fallback
- `"none"` - no compression

ZSTD automatically uses native bindings (`zstd-napi`) in Node.js, falling back to WASM (`@bokuweb/zstd-wasm`) in browsers or if native fails to load. Native is ~2x faster than WASM. The native bindings are an optional dependency and install automatically on supported platforms.

### Benchmark

Compression ratio by data type:

| Data Type      | LZ4   | ZSTD     | gzip   |
|----------------|-------|----------|--------|
| Random bytes   | 1.0x  | 1.0x     | 1.0x   |
| Repeated       | 250x  | 14,706x  | 963x   |
| JSON (varied)  | 4.3x  | 9.0x     | 8.0x   |
| UUIDs          | 1.0x  | 1.9x     | 1.7x   |
| Log lines      | 5.5x  | 17.6x    | 11.5x  |

Speed (686KB varied JSON):

| Method     | Compress | Decompress |
|------------|----------|------------|
| LZ4 wasm   | 0.6ms    | 0.2ms      |
| ZSTD napi  | 0.6ms    | 0.3ms      |
| ZSTD wasm  | 1.4ms    | 0.4ms      |
| gzip       | 4.2ms    | 0.9ms      |

ZSTD with native bindings (auto-detected in Node.js) matches LZ4 speed with 2x better compression. Run `npm run bench` to reproduce.

## Development

```bash
npm test  # runs integration tests against ClickHouse via testcontainers
```

Requires Node.js 24+ (uses `--experimental-strip-types` for direct TS execution).

## Wire Format

ClickHouse native compression blocks:
- 16-byte CityHash128 checksum (v1.0.2)
- 1-byte method (0x82=LZ4, 0x90=ZSTD)
- 4-byte compressed size (includes 9-byte header)
- 4-byte uncompressed size
- compressed data
