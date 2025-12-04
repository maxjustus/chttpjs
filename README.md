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
import { insert, query } from "@maxjustus/chttp";

const config = {
  baseUrl: "http://localhost:8123/",
  auth: { username: "default", password: "" }
};

// Insert with compression
await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  [{ id: 1, name: "test" }],
  "session123",
  config  // compression defaults to "lz4"
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

```ts
async function* generateData() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, value: `data_${i}` };
  }
}

await insert(
  "INSERT INTO large_table FORMAT JSONEachRow",
  generateData(),
  "session123",
  { compression: "zstd", onProgress: (p) => console.log(`${p.rowsProcessed} rows`) }
);
```

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
