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

// Query (yields Uint8Array, compression enabled by default)
import { query, streamText, collectText } from "@maxjustus/chttp";

// Stream text chunks
for await (const text of streamText(query(
  "SELECT * FROM table FORMAT JSON",
  "session123",
  config,
))) {
  console.log(text);
}

// Or collect entire response
const json = await collectText(query(
  "SELECT * FROM table FORMAT JSON",
  "session123",
  config,
));

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

The `query()` function yields raw `Uint8Array` chunks aligned to compression blocks, not rows. Use helpers to parse:

```ts
import { query, streamText, streamLines, streamJsonLines, collectText, collectBytes } from "@maxjustus/chttp";

// JSONEachRow - streaming parsed objects
for await (const row of streamJsonLines(query("SELECT * FROM t FORMAT JSONEachRow", session, config))) {
  console.log(row.id, row.name);
}

// CSV/TSV - streaming raw lines
for await (const line of streamLines(query("SELECT * FROM t FORMAT CSV", session, config))) {
  const [id, name] = line.split(",");
}

// JSON format - buffer entire response
const json = await collectText(query("SELECT * FROM t FORMAT JSON", session, config));
const data = JSON.parse(json);

// Binary formats (RowBinary, etc.)
const bytes = await collectBytes(query("SELECT * FROM t FORMAT RowBinaryWithNamesAndTypes", session, config));
```

## RowBinary Format (Experimental)

For high-performance inserts, use the binary `RowBinaryWithNames` format instead of JSON:

```ts
import { insert, encodeRowBinaryWithNames, type ColumnDef } from "@maxjustus/chttp";

const columns: ColumnDef[] = [
  { name: "id", type: "UInt32" },
  { name: "name", type: "String" },
  { name: "value", type: "Float64" },
];

const rows = [
  [1, "alice", 1.5],
  [2, "bob", 2.5],
];

const data = encodeRowBinaryWithNames(columns, rows);

await insert(
  "INSERT INTO table FORMAT RowBinaryWithNames",
  data,
  "session123",
  config
);
```

Supported types:
- Integers: `Int8`-`Int64`, `UInt8`-`UInt64`, `Int128`, `UInt128`, `Int256`, `UInt256`
- Floats: `Float32`, `Float64`
- Decimals: `Decimal32(P,S)`, `Decimal64(P,S)`, `Decimal128(P,S)`, `Decimal256(P,S)`
- Strings: `String`, `FixedString(N)`
- Date/Time: `Date`, `Date32`, `DateTime`, `DateTime64(precision)`
- Other: `Bool`, `UUID`, `IPv4`, `IPv6`, `Enum8(...)`, `Enum16(...)`
- Containers: `Nullable(T)`, `Array(T)`, `Tuple(T1, T2, ...)`, `Map(K, V)`, `Variant(T1, T2, ...)`
- Self-describing: `Dynamic`, `JSON`, `Object('json')`

Types can be arbitrarily nested: `Tuple(String, Array(Int32), Map(String, Float64))`.

Typed arrays (`Int32Array`, `Float64Array`, etc.) are supported for array columns. Maps accept JS objects or `Map` instances. BigInt values are used for `Int128`/`UInt128`/`Int256`/`UInt256`. Decimal types return strings for precision preservation.

Named tuples (`Tuple(a Int32, b String)`) encode from and decode to JS objects with matching field names.

### Dynamic Type

The `Dynamic` type carries its own type information. You can pass plain JS values (type is inferred) or explicit `{type, value}` objects:

```ts
// Inferred types
const rows = [
  [42],           // -> Int64
  [3.14],         // -> Float64
  ["hello"],      // -> String
  [true],         // -> Bool
  [new Date()],   // -> DateTime64(3)
  [[1, 2, 3]],    // -> Array(Int64)
];

// Explicit types (for anything not auto-inferred)
const rows = [
  [{ type: "UInt8", value: 255 }],
  [{ type: "Decimal64(18, 4)", value: "123.4567" }],
];
```

### Decoding Query Results

Use `collectBytes` with `RowBinaryWithNamesAndTypes` format to decode query results:

```ts
import { query, collectBytes, decodeRowBinaryWithNamesAndTypes } from "@maxjustus/chttp";

const data = await collectBytes(query(
  "SELECT * FROM table FORMAT RowBinaryWithNamesAndTypes",
  "session123",
  config
));

const { columns, rows } = decodeRowBinaryWithNamesAndTypes(data);
// columns: [{ name: "id", type: "UInt32" }, { name: "name", type: "String" }, ...]
// rows: [[1, "alice"], [2, "bob"], ...]
```

The `decodeRowBinaryWithNamesAndTypes` function returns column names, types, and row data. For `RowBinaryWithNames` format (types not included), use `decodeRowBinaryWithNames(data, types)` and provide column types.

## JSONCompactEachRowWithNames Format

Compact JSON format where the first row contains column names and subsequent rows are value arrays:

```ts
import { insert, query, streamJsonCompactEachRowWithNames, parseJsonCompactEachRowWithNames } from "@maxjustus/chttp";

// Insert - objects are automatically converted to compact arrays
const rows = [
  { id: 1, name: "alice", value: 1.5 },
  { id: 2, name: "bob", value: 2.5 },
];

await insert(
  "INSERT INTO table FORMAT JSONCompactEachRowWithNames",
  streamJsonCompactEachRowWithNames(rows),  // columns extracted from first object
  "session123",
  config
);

// Query - parse compact format back to objects
for await (const row of parseJsonCompactEachRowWithNames(
  query("SELECT * FROM table FORMAT JSONCompactEachRowWithNames", "session123", config)
)) {
  console.log(row.id, row.name);
}
```

Optionally specify column order: `streamJsonCompactEachRowWithNames(rows, ["name", "id"])`.

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
