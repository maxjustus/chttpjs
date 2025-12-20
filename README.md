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
  auth: { username: "default", password: "" },
};

// Insert with JSON data (using streamJsonEachRow helper)
await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  streamJsonEachRow([{ id: 1, name: "test" }]),
  "session123",
  config, // compression defaults to "lz4"
);

// Insert raw bytes (any format)
const encoder = new TextEncoder();
const csvData = encoder.encode("1,test\n2,other\n");
await insert("INSERT INTO table FORMAT CSV", csvData, "session123", config);

// Query (yields Uint8Array, compression enabled by default)
import { query, streamText, collectText } from "@maxjustus/chttp";

// Stream text chunks
for await (const text of streamText(
  query("SELECT * FROM table FORMAT JSON", "session123", config),
)) {
  console.log(text);
}

// Or collect entire response
const json = await collectText(
  query("SELECT * FROM table FORMAT JSON", "session123", config),
);

// DDL statements (consume the iterator)
for await (const _ of query("CREATE TABLE ...", "session123", config)) {
}
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
  {
    compression: "zstd",
    onProgress: (p) => console.log(`${p.bytesUncompressed} bytes`),
  },
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
  { compression: "lz4" },
);
```

## Parsing Query Results

The `query()` function yields raw `Uint8Array` chunks aligned to compression blocks, not rows. Use helpers to parse:

```ts
import {
  query,
  streamText,
  streamLines,
  streamJsonLines,
  collectText,
  collectBytes,
} from "@maxjustus/chttp";

// JSONEachRow - streaming parsed objects
for await (const row of streamJsonLines(
  query("SELECT * FROM t FORMAT JSONEachRow", session, config),
)) {
  console.log(row.id, row.name);
}

// CSV/TSV - streaming raw lines
for await (const line of streamLines(
  query("SELECT * FROM t FORMAT CSV", session, config),
)) {
  const [id, name] = line.split(",");
}

// JSON format - buffer entire response
const json = await collectText(
  query("SELECT * FROM t FORMAT JSON", session, config),
);
const data = JSON.parse(json);
```

## Native Format

ClickHouse's internal wire format. Returns columnar data (RecordBatch) rather than materializing all rows upfront.

### RecordBatch Construction

```ts
import {
  insert,
  query,
  encodeNative,
  streamDecodeNative,
  rows,
  collectRows,
  batchFromArrays,
  batchFromRows,
  batchFromCols,
  batchBuilder,
  makeBuilder,
} from "@maxjustus/chttp";

const schema = [
  { name: "id", type: "UInt32" },
  { name: "name", type: "String" },
];

// From columnar data (named columns)
const batch = batchFromArrays(schema, {
  id: new Uint32Array([1, 2, 3]),
  name: ["alice", "bob", "charlie"],
});

// From row arrays
const batch2 = batchFromRows(schema, [
  [1, "alice"],
  [2, "bob"],
  [3, "charlie"],
]);

// Row-by-row builder
const builder = batchBuilder(schema);
builder.appendRow([1, "alice"]);
builder.appendRow([2, "bob"]);
builder.appendRow([3, "charlie"]);
const batch3 = builder.finish();

// Encode and insert
await insert(
  "INSERT INTO t FORMAT Native",
  encodeNative(batch),
  "session",
  config,
);

// Query returns columnar data as RecordBatch - stream and iterate
for await (const row of rows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", "session", config)),
)) {
  console.log(row.id, row.name);
}

// Or collect all rows at once
const allRows = await collectRows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", "session", config)),
);

// Work with batches directly for columnar access
for await (const batch of streamDecodeNative(
  query("SELECT * FROM t FORMAT Native", "session", config),
)) {
  const ids = batch.getColumn("id")!;
  for (let i = 0; i < ids.length; i++) {
    console.log(ids.get(i));
  }
}
```

### Column Builders

Build columns independently with `makeBuilder`:

```ts
const idCol = makeBuilder("UInt32").append(1).append(2).append(3).finish();
const nameCol = makeBuilder("String")
  .append("alice")
  .append("bob")
  .append("charlie")
  .finish();

// Columns carry their type - schema is derived automatically
const batch = batchFromCols({ id: idCol, name: nameCol });
// batch.schema = [{ name: "id", type: "UInt32" }, { name: "name", type: "String" }]
```

### Complex Types

```ts
// Array(Int32)
batchFromArrays([{ name: "tags", type: "Array(Int32)" }], {
  tags: [[1, 2], [3, 4, 5], [6]],
});

// Tuple(Float64, Float64) - positional
batchFromArrays([{ name: "point", type: "Tuple(Float64, Float64)" }], {
  point: [
    [1.0, 2.0],
    [3.0, 4.0],
  ],
});

// Tuple(x Float64, y Float64) - named tuples use objects
batchFromArrays([{ name: "point", type: "Tuple(x Float64, y Float64)" }], {
  point: [
    { x: 1.0, y: 2.0 },
    { x: 3.0, y: 4.0 },
  ],
});

// Map(String, Int32)
batchFromArrays([{ name: "meta", type: "Map(String, Int32)" }], {
  meta: [{ a: 1, b: 2 }, new Map([["c", 3]])],
});

// Nullable(String)
batchFromArrays([{ name: "note", type: "Nullable(String)" }], {
  note: ["hello", null, "world"],
});

// Variant(String, Int64, Bool) - type inferred from values
batchFromArrays([{ name: "val", type: "Variant(String, Int64, Bool)" }], {
  val: ["hello", 42n, true, null],
});

// Variant with explicit discriminators (for ambiguous cases)
batchFromArrays(
  [{ name: "val", type: "Variant(String, Int64, Bool)" }],
  { val: [[0, "hello"], [1, 42n], [2, true], null] }, // [discriminator, value]
);

// Dynamic - types inferred automatically
batchFromArrays([{ name: "dyn", type: "Dynamic" }], {
  dyn: ["hello", 42, true, [1, 2, 3], null],
});

// JSON - plain objects
batchFromArrays([{ name: "data", type: "JSON" }], {
  data: [
    { a: 1, b: "x" },
    { a: 2, c: true },
  ],
});

// Building complex columns
const pointCol = makeBuilder("Tuple(Float64, Float64)")
  .append([1.0, 2.0])
  .append([3.0, 4.0])
  .finish();
```

### Streaming

```ts
import {
  streamEncodeNative,
  streamDecodeNative,
  rows,
  batchFromArrays,
} from "@maxjustus/chttp";

// Streaming decode - rows as objects (lazy)
for await (const row of rows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", "session", config)),
)) {
  console.log(row.id, row.name);
}

// Or work with RecordBatch blocks directly
for await (const batch of streamDecodeNative(
  query("SELECT * FROM t FORMAT Native", "session", config),
)) {
  // Iterate rows from a RecordBatch
  for (const row of batch) {
    console.log(row.id, row.name);
  }
}

async function* generateBatches() {
  const schema = [
    { name: "id", type: "UInt32" },
    { name: "value", type: "Float64" },
  ];
  const batchSize = 10000;
  for (let i = 0; i < 100; i++) {
    const ids = new Uint32Array(batchSize);
    const values = new Float64Array(batchSize);
    for (let j = 0; j < batchSize; j++) {
      ids[j] = i * batchSize + j;
      values[j] = Math.random();
    }
    yield batchFromArrays(schema, { id: ids, value: values });
  }
}

await insert(
  "INSERT INTO t FORMAT Native",
  streamEncodeNative(generateBatches()),
  "session",
  config,
);
```

Supports all ClickHouse types including integers (Int8-Int256, UInt8-UInt256), floats, decimals, strings, date/time, containers (Array, Tuple, Map, Nullable), Variant, Dynamic, JSON, and geo types.

**Limitation**: `Dynamic` and `JSON` types require V3 flattened format. On ClickHouse 25.6+, set `output_format_native_use_flattened_dynamic_and_json_serialization=1`.

## TCP Client (Experimental)

Direct TCP protocol for lower latency. Single connection per client - use separate clients for concurrent operations.

### Basic Usage

```ts
import { TcpClient } from "@maxjustus/chttp/tcp";

const client = new TcpClient({
  host: "localhost",
  port: 9000,
  database: "default",
  user: "default",
  password: "",
});
await client.connect();

for await (const packet of client.query("SELECT * FROM table")) {
  if (packet.type === "Data") {
    for (const row of packet.batch) {
      console.log(row.id, row.name);
    }
  }
}

// Execute DDL
await client.execute("CREATE TABLE ...");

// Insert (see Insert API section for details)
await client.insert("INSERT INTO table", [{ id: 1, name: "alice" }]);

client.close();
```

### Connection Options

```ts
const client = new TcpClient({
  host: "localhost",
  port: 9000,
  database: "default",
  user: "default",
  password: "",
  compression: "lz4", // 'lz4' | 'zstd' | false
  connectTimeout: 10000, // ms
  queryTimeout: 30000, // ms
  tls: true, // or tls.ConnectionOptions
});
```

### Streaming Results

Query yields packets - handle by type:

```ts
for await (const packet of client.query(sql, { send_logs_level: "trace" })) {
  switch (packet.type) {
    case "Data":
      console.log(`${packet.batch.rowCount} rows`);
      break;
    case "Progress":
      console.log(`${packet.progress.readRows} rows read`);
      break;
    case "Log":
      for (const entry of packet.entries) {
        console.log(`[${entry.source}] ${entry.text}`);
      }
      break;
    case "ProfileInfo":
      console.log(`${packet.info.rows} total rows`);
      break;
    case "EndOfStream":
      break;
  }
}
```

### Insert API

The `insert()` method accepts RecordBatches or row objects:

```ts
await client.insert("INSERT INTO t", batch);

await client.insert("INSERT INTO t", [batch1, batch2]);

// Row objects with auto-coercion (types inferred from server schema)
await client.insert("INSERT INTO t", [
  { id: 1, name: "alice" },
  { id: 2, name: "bob" },
]);

async function* generateRows() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, name: `user${i}` };
  }
}

// batchSize dictates number of rows per RecordBatch (native insert block) sent
await client.insert("INSERT INTO t", generateRows(), { batchSize: 10000 });

// Schema validation (fail fast if types don't match the schema the server sends for the insert table)
await client.insert("INSERT INTO t", rows, {
  schema: [
    { name: "id", type: "UInt32" },
    { name: "name", type: "String" },
  ],
});
```

### Streaming Between Tables

Use separate connections for concurrent read/write:

```ts
import { TcpClient, recordBatches } from "@maxjustus/chttp/tcp";

const readClient = new TcpClient(options);
const writeClient = new TcpClient(options);
await readClient.connect();
await writeClient.connect();

// Stream RecordBatches from one table to another
await writeClient.insert(
  "INSERT INTO dst",
  recordBatches(readClient.query("SELECT * FROM src")),
);
```

### Cancellation

```ts
const controller = new AbortController();
setTimeout(() => controller.abort(), 5000);

await client.connect({ signal: controller.signal });

for await (const p of client.query(sql, {}, { signal: controller.signal })) {
  // ...
}
```

### Auto-Close TCP connection on scope exit

```ts
await using client = await TcpClient.connect(options);
// automatically closed when scope exits
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
  timeout: 60_000,
});
```

Requires Node.js 20+ or modern browsers (Chrome 116+, Firefox 124+, Safari 17.4+) for `AbortSignal.any()`.

## Compression

Set `compression` in options:

- `"lz4"` - fast, native in Node.js with WASM fallback (default)
- `"zstd"` - ~2x better compression, native in Node.js with WASM fallback
- `"none"` - no compression

ZSTD and LZ4 use native bindings in Node.js when available, falling back to WASM in browsers. Run `npm run bench` to see compression ratios and speeds for your data.

## Development

```bash
npm test       # runs integration tests against ClickHouse via testcontainers
make test-tcp  # TCP client tests (requires local ClickHouse on port 9000)
make fuzz-tcp  # TCP fuzz tests (FUZZ_ITERATIONS=10 FUZZ_ROWS=20000)
```

Requires Node.js 24+ (uses `--experimental-strip-types` for direct TS execution).
