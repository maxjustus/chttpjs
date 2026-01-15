# chttp

ClickHouse HTTP/TCP client with native compression (LZ4/ZSTD) and Native format support.

## Install

```bash
npm install @maxjustus/chttp
```

## HTTP vs TCP

| Feature | HTTP | TCP |
|---------|------|-----|
| Real-time progress | No | Yes (rows read/written, bytes, memory, CPU) |
| Server logs | No | Yes (via `send_logs_level` setting) |
| Profile events | No | Yes (detailed execution metrics) |
| Long-running queries | Timeout-prone | Robust (persistent connection) |
| Browser support | Yes | No (Node/Bun/Deno only) |

Use **HTTP** for browser apps or simple queries. Use **TCP** for observability, long-running queries, and lower latency.

## Quick Start

### HTTP Client

```ts
import { insert, query, streamEncodeJsonEachRow, collectText } from "@maxjustus/chttp";

const config = {
  baseUrl: "http://localhost:8123/",
  auth: { username: "default", password: "" },
};

// Insert
const { summary } = await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  streamEncodeJsonEachRow([{ id: 1, name: "test" }]),
  "session123",
  config,
);
console.log(`Wrote ${summary.written_rows} rows`);

// Query
const json = await collectText(query("SELECT * FROM table FORMAT JSON", "session123", config));

// DDL
for await (const _ of query("CREATE TABLE ...", "session123", config)) {}
```

### TCP Client

```ts
import { TcpClient } from "@maxjustus/chttp/tcp";

const client = new TcpClient({ host: "localhost", port: 9000 });
await client.connect();

// Query
for await (const packet of client.query("SELECT * FROM table")) {
  if (packet.type === "Data") {
    for (const row of packet.batch) console.log(row.id, row.name);
  }
}

// Insert
await client.insert("INSERT INTO table", [{ id: 1, name: "alice" }]);

client.close();
```

## Query Parameters

Both HTTP and TCP clients use ClickHouse's native query parameters with identical `{name: Type}` syntax:

```ts
// HTTP
const result = await collectText(
  query("SELECT {id: UInt64} as id, {name: String} as name FORMAT JSON", sessionId, {
    ...config,
    params: { id: 42, name: "Alice" },
  }),
);

// TCP (same syntax)
for await (const packet of client.query(
  "SELECT * FROM users WHERE age > {min_age: UInt32}",
  { params: { min_age: 18 } }
)) { /* ... */ }
```

Parameters are type-safe and prevent SQL injection. The type annotation (e.g., `{name: String}`) tells ClickHouse how to parse the value.

## Streaming Large Inserts

The `insert` function accepts `Uint8Array`, `Uint8Array[]`, or `AsyncIterable<Uint8Array>`. Use `streamEncodeJsonEachRow` for JSON data:

```ts
// Streaming JSON objects
async function* generateRows() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, value: `data_${i}` };
  }
}

await insert(
  "INSERT INTO large_table FORMAT JSONEachRow",
  streamEncodeJsonEachRow(generateRows()),
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
  streamDecodeJsonEachRow,
  collectJsonEachRow,
  collectText,
  collectBytes,
} from "@maxjustus/chttp";

// JSONEachRow - streaming parsed objects
for await (const row of streamDecodeJsonEachRow(
  query("SELECT * FROM t FORMAT JSONEachRow", session, config),
)) {
  console.log(row.id, row.name);
}

const res = await collectJsonEachRow(
  query("SELECT * FROM t FORMAT JSONEachRow", session, config),
);

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
  batchFromRows,
  batchFromCols,
  getCodec,
} from "@maxjustus/chttp";

const schema = [
  { name: "id", type: "UInt32" },
  { name: "name", type: "String" },
];

// From row arrays
const batch = batchFromRows(schema, [
  [1, "alice"],
  [2, "bob"],
  [3, "charlie"],
]);

// From pre-built columns (zero-copy for TypedArrays)
const batch2 = batchFromCols({
  id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
  name: getCodec("String").fromValues(["alice", "bob", "charlie"]),
});

// From generators (streaming row construction)
function* generateRows() {
  yield [1, "alice"];
  yield [2, "bob"];
  yield [3, "charlie"];
}
const batch3 = batchFromRows(schema, generateRows());

// Encode and insert
await insert(
  "INSERT INTO t FORMAT Native",
  encodeNative(batch),
  "session",
  config,
);

// Query returns columnar data as RecordBatch - stream rows directly
for await (const row of rows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", "session", config)),
)) {
  console.log(row.id, row.name);
}

// Or collect all rows at once (materialized to plain objects)
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

### Building Columns from Values

Build columns independently with `getCodec().fromValues()`:

```ts
const idCol = getCodec("UInt32").fromValues([1, 2, 3]);
const nameCol = getCodec("String").fromValues(["alice", "bob", "charlie"]);

// Columns carry their type - schema is derived automatically
const batch = batchFromCols({ id: idCol, name: nameCol });
// batch.schema = [{ name: "id", type: "UInt32" }, { name: "name", type: "String" }]
```

For numeric columns, pass TypedArrays (e.g., `Uint32Array`, `Float64Array`) for zero-copy construction.

### Complex Types

```ts
// Array(Int32)
batchFromCols({
  tags: getCodec("Array(Int32)").fromValues([[1, 2], [3, 4, 5], [6]]),
});

// Tuple(Float64, Float64) - positional
batchFromCols({
  point: getCodec("Tuple(Float64, Float64)").fromValues([[1.0, 2.0], [3.0, 4.0]]),
});

// Tuple(x Float64, y Float64) - named tuples use objects
batchFromCols({
  point: getCodec("Tuple(x Float64, y Float64)").fromValues([
    { x: 1.0, y: 2.0 },
    { x: 3.0, y: 4.0 },
  ]),
});

// Map(String, Int32)
batchFromCols({
  meta: getCodec("Map(String, Int32)").fromValues([{ a: 1, b: 2 }, new Map([["c", 3]])]),
});

// Nullable(String)
batchFromCols({
  note: getCodec("Nullable(String)").fromValues(["hello", null, "world"]),
});

// Variant(String, Int64, Bool) - type inferred from values
batchFromCols({
  val: getCodec("Variant(String, Int64, Bool)").fromValues(["hello", 42n, true, null]),
});

// Variant with explicit discriminators (for ambiguous cases)
batchFromCols({
  val: getCodec("Variant(String, Int64, Bool)").fromValues([
    [0, "hello"], [1, 42n], [2, true], null
  ]),
});

// Dynamic - types inferred automatically
batchFromCols({
  dyn: getCodec("Dynamic").fromValues(["hello", 42, true, [1, 2, 3], null]),
});

// JSON - plain objects
batchFromCols({
  data: getCodec("JSON").fromValues([{ a: 1, b: "x" }, { a: 2, c: true }]),
});
```

### Streaming Insert

```ts
import { insert, streamEncodeNative, batchFromCols, getCodec } from "@maxjustus/chttp";

async function* generateBatches() {
  const batchSize = 10000;
  for (let i = 0; i < 100; i++) {
    const ids = new Uint32Array(batchSize);
    const values = new Float64Array(batchSize);
    for (let j = 0; j < batchSize; j++) {
      ids[j] = i * batchSize + j;
      values[j] = Math.random();
    }
    yield batchFromCols({
      id: getCodec("UInt32").fromValues(ids),
      value: getCodec("Float64").fromValues(values),
    });
  }
}

await insert(
  "INSERT INTO t FORMAT Native",
  streamEncodeNative(generateBatches()),
  "session",
  config,
);
```

Supports all ClickHouse types.

**Limitation**: `Dynamic` and `JSON` types require V3 flattened format. On ClickHouse 25.6+, set `output_format_native_use_flattened_dynamic_and_json_serialization=1`.

### BigInt Handling

ClickHouse 64-bit+ integers (Int64, UInt64, Int128, etc.) are returned as JavaScript BigInt. Pass `{ bigIntAsString: true }` to convert to strings for JSON serialization:

```ts
const row = batch.get(0, { bigIntAsString: true });
const obj = row.toObject({ bigIntAsString: true });
const allRows = batch.toArray({ bigIntAsString: true });
```

> **Global alternative**: Add `BigInt.prototype.toJSON = function() { return this.toString(); };` at startup. See [MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt#use_within_json).

## TCP Client (Experimental)

Direct TCP protocol. Single connection per client - use separate clients for concurrent operations.

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

// DDL statements
await client.query("CREATE TABLE ...");

// Insert - returns Packet[] (TCP streams progress during insert)
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
for await (const packet of client.query(sql, { settings: { send_logs_level: "trace" } })) {
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

### Progress Tracking

Progress packets contain **delta values** (increments since the last packet). The client accumulates these into running totals available via `packet.accumulated`:

```ts
for await (const packet of client.query(sql)) {
  if (packet.type === "Progress") {
    const { accumulated } = packet;
    console.log(`${accumulated.percent}% complete`);
    console.log(`Read: ${accumulated.readRows} rows, ${accumulated.readBytes} bytes`);
    console.log(`Elapsed: ${Number(accumulated.elapsedNs) / 1e9}s`);
  }
}
```

### ProfileEvents and Resource Metrics

ProfileEvents provide execution metrics. Memory and CPU stats are merged into accumulated progress:

```ts
for await (const packet of client.query(sql)) {
  if (packet.type === "Progress") {
    const { accumulated } = packet;
    console.log(`Memory: ${accumulated.memoryUsage} bytes`);
    console.log(`Peak memory: ${accumulated.peakMemoryUsage} bytes`);
    console.log(`CPU time: ${accumulated.cpuTimeMicroseconds}µs`);
    console.log(`CPU cores utilized: ${accumulated.cpuUsage.toFixed(1)}`);
  }

  if (packet.type === "ProfileEvents") {
    // Raw accumulated event counters
    console.log(`Selected rows: ${packet.accumulated.get("SelectedRows")}`);
    console.log(`Read bytes: ${packet.accumulated.get("ReadCompressedBytes")}`);
  }
}
```

`memoryUsage` is the latest value; `peakMemoryUsage` is the max seen. `cpuUsage` shows equivalent CPUs utilized.

### Insert API

The `insert()` method accepts RecordBatches or row objects:

```ts
// Single batch
await client.insert("INSERT INTO t", batch);

// Multiple batches
await client.insert("INSERT INTO t", [batch1, batch2]);

// Row objects with auto-coercion (types inferred from server schema; unknown keys ignored; omitted keys use defaults)
await client.insert("INSERT INTO t", [
  { id: 1, name: "alice" },
  { id: 2, name: "bob" },
]);

// Streaming rows with generator
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

#### Insert Progress Tracking

Both `query()` and `insert()` return a `CollectableAsyncGenerator<Packet>`:
- `await` collects all packets into an array
- `for await` streams packets one at a time

```ts
// Collect all packets
const packets = await client.insert("INSERT INTO t", rows);
const progress = packets.findLast(p => p.type === "Progress");
if (progress?.type === "Progress") {
  console.log(`Wrote ${progress.accumulated.writtenRows} rows`);
}

// Stream packets (useful for real-time progress on large inserts)
for await (const packet of client.insert("INSERT INTO t", generateRows())) {
  if (packet.type === "Progress") {
    console.log(`Written: ${packet.accumulated.writtenRows} rows`);
  }
}
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

## External Tables

Send temporary in-memory tables with your query. Schema is auto-extracted from RecordBatch.

### Unified API (RecordBatch)

Pass RecordBatches directly to either client:

```ts
import { batchFromCols, getCodec, query, collectText } from "@maxjustus/chttp";

const users = batchFromCols({
  id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
  name: getCodec("String").fromValues(["Alice", "Bob", "Charlie"]),
});

// TCP
for await (const packet of client.query(
  "SELECT * FROM users WHERE id > 1",
  { externalTables: { users } }
)) {
  if (packet.type === "Data") {
    for (const row of packet.batch) console.log(row.name);
  }
}

// HTTP - same API
const result = await collectText(query(
  "SELECT * FROM users WHERE id > 1 FORMAT JSON",
  sessionId,
  { baseUrl, auth, externalTables: { users } }
));
```

Supports streaming via iterables/async iterables of RecordBatch:

```ts
async function* generateBatches() {
  for (let i = 0; i < 10; i++) {
    yield batchFromCols({ id: getCodec("UInt32").fromValues([i]) });
  }
}

// Works with both TCP and HTTP
await client.query("SELECT sum(id) FROM data", {
  externalTables: { data: generateBatches() }
});
```

### HTTP with Raw Data

For raw TSV/CSV/JSON data, use the explicit structure form:

```ts
const result = await collectText(query(
  "SELECT * FROM mydata ORDER BY id FORMAT JSON",
  sessionId,
  {
    baseUrl, auth,
    externalTables: {
      mydata: {
        structure: "id UInt32, name String",
        format: "TabSeparated",  // or JSONEachRow, CSV, etc.
        data: "1\tAlice\n2\tBob\n"
      }
    }
  }
));
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

Requires Node.js 20+, Bun, Deno, or modern browsers (Chrome 116+, Firefox 124+, Safari 17.4+) for `AbortSignal.any()`.

## Error Handling

### HTTP Client

The HTTP client throws standard `Error` objects with the HTTP status and response body:

```ts
try {
  for await (const _ of query("SELECT * FROM nonexistent", session, config)) {}
} catch (err) {
  // err.message: "Query failed: 404 - Code: 60. DB::Exception: Table ... doesn't exist..."
}
```

Insert errors follow the same pattern:

```ts
try {
  await insert("INSERT INTO t FORMAT JSONEachRow", data, session, config);
} catch (err) {
  // err.message: "Insert failed: 400 - Code: 27. DB::Exception: Cannot parse..."
}
```

### TCP Client

The TCP client throws `ClickHouseException` for server errors, which includes structured details:

```ts
import { TcpClient, ClickHouseException } from "@maxjustus/chttp/tcp";

try {
  for await (const _ of client.query("SELECT * FROM nonexistent")) {}
} catch (err) {
  if (err instanceof ClickHouseException) {
    console.log(err.code);            // 60 (UNKNOWN_TABLE)
    console.log(err.exceptionName);   // "DB::Exception"
    console.log(err.message);         // "Table ... doesn't exist"
    console.log(err.serverStackTrace); // Full server-side stack trace
    console.log(err.nested);          // Nested exception if present
  }
}
```

Connection and protocol errors throw standard `Error`:

```ts
try {
  await client.connect();
} catch (err) {
  // err.message: "Connection timeout after 10000ms"
  // err.message: "Not connected"
  // err.message: "Connection busy - cannot run concurrent operations..."
}
```

## Compression

Set `compression` in options:

- `"lz4"` - fast, uses native bindings when available with WASM fallback (default)
- `"zstd"` - ~2x better compression, uses native bindings when available with WASM fallback
- `false` - no compression

ZSTD and LZ4 use native bindings in Node.js/Bun when available, falling back to WASM in browsers and Deno.

## Performance

Benchmarks on Apple M4 Max, 10k rows. Native format is ClickHouse's columnar wire format.

### Format Comparison (encode + compress)

| Scenario | JSON+LZ4 | Native+LZ4 | JSON+ZSTD | Native+ZSTD | JSON+gzip | Native+gzip |
|----------|----------|------------|-----------|-------------|-----------|-------------|
| Simple (6 cols) | 11.4ms | 2.0ms | 12.1ms | 2.2ms | 18.1ms | 7.5ms |
| Escape-heavy strings | 3.3ms | 2.6ms | 3.2ms | 2.7ms | 5.7ms | 5.9ms |
| Arrays (50 floats/row) | 29ms | 7.7ms | 65ms | 11ms | 269ms | 106ms |
| Variant | 1.1ms | 0.8ms | 1.2ms | 0.9ms | 5.8ms | 4.6ms |
| Dynamic | 1.0ms | 0.8ms | 1.1ms | 0.8ms | 3.9ms | 4.3ms |
| JSON column | 2.5ms | 3.4ms | 3.0ms | 3.9ms | 9.9ms | 10.1ms |

### Compressed Size (Native vs JSON)

| Scenario | LZ4 | ZSTD | gzip |
|----------|-----|------|------|
| Simple (6 cols) | 65% | 68% | 71% |
| Escape-heavy strings | 92% | 140%* | 84% |
| Arrays (50 floats/row) | 48% | 82% | 85% |
| Variant | 70% | 96% | 73% |
| Dynamic | 72% | 98% | 65% |
| JSON column | 56% | 67% | 67% |

*Escape-heavy strings: JSON's escaping creates repetitive patterns that ZSTD compresses exceptionally well.

**Summary**: LZ4 is fastest, ZSTD compresses best. Native format wins on both speed and size for most data shapes. Exception: highly repetitive escaped strings where JSON's redundancy helps ZSTD.

Run `node --experimental-strip-types bench/formats.ts` to reproduce.

## Development

```bash
npm test       # runs integration tests against ClickHouse via testcontainers
make test-tcp  # TCP client tests (requires local ClickHouse on port 9000)
make fuzz-tcp  # TCP fuzz tests (FUZZ_ITERATIONS=10 FUZZ_ROWS=20000)
```

Requires Node.js 20+, Bun, or Deno.

## CLI

Run queries directly from the command line via the bundled TCP client:

```bash
# Single query (outputs NDJSON packets)
npx @maxjustus/chttp 'SELECT version()'
bunx @maxjustus/chttp 'SELECT 1 + 1'

# Interactive REPL with history
npx @maxjustus/chttp

# Deno (with Node compatibility)
deno run -A npm:@maxjustus/chttp 'SELECT now()'
```

Configure via environment variables:

```bash
CH_HOST=clickhouse.example.com CH_PORT=9000 npx @maxjustus/chttp 'SELECT 1'
```

| Variable | Default | Description |
|----------|---------|-------------|
| CH_HOST | localhost | ClickHouse host |
| CH_PORT | 9000 | TCP native port |
| CH_USER | default | Username |
| CH_PASSWORD | "" | Password |

The REPL supports `\load file.jsonl INTO table` for bulk inserts from NDJSON files.
