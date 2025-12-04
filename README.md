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
import { insert, query, Method } from "@maxjustus/chttp";

const config = {
  baseUrl: "http://localhost:8123/",
  auth: { username: "default", password: "" }
};

// Insert with compression
await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  [{ id: 1, name: "test" }],
  "session123",
  Method.LZ4,
  config
);

// Query with compressed response
for await (const chunk of query(
  "SELECT * FROM table FORMAT JSON",
  "session123",
  true,
  config,
)) {
  console.log(chunk);
}
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
  Method.ZSTD,
  { onProgress: (p) => console.log(`${p.rowsProcessed} rows`) }  // add baseUrl/auth as needed
);
```

## Compression

- `Method.LZ4` - fast (default)
- `Method.ZSTD` - smaller output
- `Method.None` - no compression

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
