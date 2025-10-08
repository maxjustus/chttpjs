# ClickHouse HTTP Client with Native Compression

A TypeScript/Node.js client for ClickHouse that implements the native compression protocol for efficient data insertion and querying.

## Features

- **Native compression**: LZ4 and ZSTD compression compatible with ClickHouse format
- **Streaming inserts**: Memory-efficient insertion of large datasets using async generators
- **Streaming queries**: Process query results as they arrive with optional compression
- **TypeScript support**: Full type safety with strict TypeScript compilation
- **Comprehensive testing**: Integration tests with real ClickHouse via testcontainers

## Installation

```bash
npm install lz4 bling-hashes zstd-napi @testcontainers/clickhouse
```

## Usage

```ts
import { insertCompressed, execQuery, Method } from "./client.ts";

// Insert data with compression
await insertCompressed(
  "INSERT INTO table FORMAT JSONEachRow",
  [{ id: 1, name: "test" }],
  "session123",
  Method.LZ4,
  { 
    baseUrl: "http://localhost:8123/",
    auth: { username: "default", password: "password" }
  }
);

// Stream query results
for await (const chunk of execQuery(
  "SELECT * FROM table FORMAT JSON",
  "session123",
  true, // compressed response
  {
    baseUrl: "http://localhost:8123/",
    auth: { username: "default", password: "password" },
  },
)) {
  console.log(chunk);
}
```

## Streaming Inserts

Handle large datasets efficiently with async generators:

```ts
async function* generateData() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, value: `data_${i}` };
  }
}

await insertCompressed(
  "INSERT INTO large_table FORMAT JSONEachRow",
  generateData(),
  "session123",
  Method.ZSTD,
  {
    onProgress: (progress) => {
      console.log(`Processed ${progress.rowsProcessed} rows`);
    }
  }
);
```

## Compression Methods

- `Method.LZ4` - Fast compression (default)
- `Method.ZSTD` - Better compression ratios
- `Method.None` - No compression

## Development

### TypeScript Compilation

```bash
tsc  # Compile TypeScript to JavaScript
tsc --noEmit  # Type check without output
```

### Testing

Run the test suite:

```bash
npm test
```

Tests use testcontainers to spin up a real ClickHouse instance for integration testing.

> **Note**  
> Run all scripts with Node.js 24+ using the `--experimental-strip-types` flag (the provided npm scripts already include it) so that the runtime can execute the TypeScript sources directly.

## Implementation Details

ClickHouse uses a custom compression format:
- 16-byte CityHash128 checksum (v1.0.2)
- 1-byte magic number (0x82 for LZ4, 0x90 for ZSTD)
- 4-byte compressed size (includes 9-byte header)
- 4-byte uncompressed size
- Raw compressed data

The client currently bypasses checksum verification using `http_native_compression_disable_checksumming_on_decompress=1` due to CityHash version incompatibility. Proper CityHash128 implementation will be added in a future update.
