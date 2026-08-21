# chwire

ClickHouse HTTP/TCP client and Native wire format toolkit for TypeScript.

## Install

```bash
npm install @maxjustus/chwire
```

## HTTP vs TCP

TCP works better for long-running queries and gives you streaming telemetry (progress, logs, profile events). HTTP works anywhere (including browsers) and supports all ClickHouse input/output formats.

## Quick Start

### HTTP Client

The HTTP client is stateless — `query()` and `insert()` are standalone functions that each make a single HTTP request.

```ts
import { insert, query, streamEncodeJsonEachRow, collectText } from "@maxjustus/chwire";

const connectionConfig = {
  url: "http://localhost:8123/",
  auth: { username: "default", password: "" },
};

// Insert
const { summary } = await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  streamEncodeJsonEachRow([{ id: 1, name: "test" }]),
  connectionConfig,
);
console.log(`Wrote ${summary.written_rows} rows`);

// Query
const json = await collectText(query("SELECT * FROM table FORMAT JSON", connectionConfig));

// DDL
await query("CREATE TABLE ...", connectionConfig);
```

### TCP Client

```ts
import { TcpClient } from "@maxjustus/chwire/tcp";

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

## HTTP Client

`query()` returns a `CollectableAsyncGenerator` that yields `Data` packets (raw `Uint8Array` chunks), `Progress` packets, and a final `Summary`. Use `await` to collect all packets into an array, or pipe through helpers like `collectText` and `streamDecodeJsonEachRow` which consume the Data chunks for you.

`insert()` returns `Promise<InsertResult>` — an object with `summary` (containing `written_rows`, `written_bytes`, `elapsed_ns`, etc.) and `queryId`.

### Query Parameters

```ts
const result = await collectText(
  query("SELECT {id: UInt64} as id, {name: String} as name FORMAT JSON", {
    ...connectionConfig,
    params: { id: 42, name: "Alice" },
  }),
);
```

Parameters are type-safe and prevent SQL injection. The type annotation (e.g., `{name: String}`) tells ClickHouse how to parse the value.

Placeholders without a supplied value are sent as-is and resolved by the server. This makes parameterized-view DDL and session-level `SET param_x = ...` bindings work; a genuinely unbound parameter fails server-side with `UNKNOWN_QUERY_PARAMETER`.

Unknown root-level option keys are forwarded as raw ClickHouse URL params:

```ts
const result = await collectText(query("SELECT 42 as value", {
  ...connectionConfig,
  default_format: "TSV",
  wait_end_of_query: 1,
}));
```

The transport keys `url`, `auth`, `compression`, `compressQuery`, `signal`, `timeout`,
`clientVersion`, `settings`, `params`, `externalTables`, `queryId`, and `sessionId` are reserved
and are not forwarded as raw URL params.

### Parsing Query Results

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
} from "@maxjustus/chwire";

// JSONEachRow - streaming parsed objects
for await (const row of streamDecodeJsonEachRow(
  query("SELECT * FROM t FORMAT JSONEachRow", connectionConfig),
)) {
  console.log(row.id, row.name);
}

const res = await collectJsonEachRow(
  query("SELECT * FROM t FORMAT JSONEachRow", connectionConfig),
);

// CSV/TSV - streaming raw lines
for await (const line of streamLines(
  query("SELECT * FROM t FORMAT CSV", connectionConfig),
)) {
  const [id, name] = line.split(",");
}

// JSON format - buffer entire response
const json = await collectText(
  query("SELECT * FROM t FORMAT JSON", connectionConfig),
);
const data = JSON.parse(json);
```

### Streaming Large Inserts

The HTTP `insert` function accepts `Uint8Array`, `Uint8Array[]`, or `AsyncIterable<Uint8Array>`. Use `streamEncodeJsonEachRow` for JSON data:

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
  {
    ...connectionConfig,
    compression: { method: "zstd", level: 6 },
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
  {
    ...connectionConfig,
    compression: "lz4" 
  },
);
```

### External Tables

Send temporary in-memory tables with your query. Schema is auto-extracted from RecordBatch (see [Native Format](#native-format) for construction):

```ts
import { batchFromCols, getCodec, query, collectText } from "@maxjustus/chwire";

const users = batchFromCols({
  id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
  name: getCodec("String").fromValues(["Alice", "Bob", "Charlie"]),
});

const result = await collectText(query(
  "SELECT * FROM users WHERE id > 1 FORMAT JSON",
  { url, auth, externalTables: { users } }
));
```

For raw TSV/CSV/JSON data, use the explicit structure form:

```ts
const result = await collectText(query(
  "SELECT * FROM mydata ORDER BY id FORMAT JSON",
  {
    url, auth,
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

### Timeout and Cancellation

Configure with `timeout` (ms) or provide an `AbortSignal` for manual cancellation:

```ts
// Custom timeout
await insert(sql, data, { ...connectionConfig, timeout: 60_000 });

// Manual cancellation
const controller = new AbortController();
setTimeout(() => controller.abort(), 5000);
await insert(sql, data, { ...connectionConfig, signal: controller.signal });

// Both (whichever triggers first)
await insert(sql, data, {
  ...connectionConfig,
  signal: controller.signal,
  timeout: 60_000,
});
```

### Error Handling

The HTTP client throws `ClickHouseException` for server errors:

```ts
import { ClickHouseException } from "@maxjustus/chwire";

try {
  for await (const _ of query("SELECT * FROM nonexistent", config)) {}
} catch (err) {
  if (err instanceof ClickHouseException) {
    console.log(err.code);          // 60 (UNKNOWN_TABLE)
    console.log(err.exceptionName); // "DB::Exception"
    console.log(err.message);       // "Table ... doesn't exist"
  }
}
```

Insert errors follow the same pattern:

```ts
try {
  await insert("INSERT INTO t FORMAT JSONEachRow", data, config);
} catch (err) {
  if (err instanceof ClickHouseException) {
    console.log(err.code);
    console.log(err.message);
  }
}
```

## TCP Client

Uses ClickHouse's native TCP protocol. TCP Single connection per client; use separate clients for concurrent operations.
Note that the TCP protocol only sends/recieves data in Native format.

### Basic Usage

```ts
import { TcpClient } from "@maxjustus/chwire/tcp";

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

// DDL
await client.query("CREATE TABLE ...");

// Insert (await collects and discards packets; for progress tracking see "Insert Progress Tracking" below)
await client.insert("INSERT INTO table", [{ id: 1, name: "alice" }]);

client.close();
```

TCP connections hold a persistent socket — always call `client.close()` when done, or use [`await using`](#auto-close-on-scope-exit) for automatic cleanup.

### Connection Options

```ts
const client = new TcpClient({
  host: "localhost",
  port: 9000,
  database: "default",
  user: "default",
  password: "",
  compression: "lz4", // 'lz4' | 'zstd' | false | { method: 'zstd', level: 6 }
  connectTimeout: 10000, // ms
  queryTimeout: 30000, // ms
  tls: true, // or Node.js tls.ConnectionOptions for custom CA/certs. IE: tls: { ca: fs.readFileSync("/path/to/ca.pem"), rejectUnauthorized: true },
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

// Row objects with auto-coercion. Types are inferred from server schema.
// Unknown keys ignored, omitted keys use defaults, incompatible provided types throw.
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
- `await gen` collects all packets into an array
- `for await` streams packets one at a time
- the same generator instance is single-consumer and does not replay once drained

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

### Query Parameters

```ts
for await (const packet of client.query(
  "SELECT * FROM users WHERE age > {min_age: UInt32}",
  { params: { min_age: 18 } }
)) { /* ... */ }
```

Same `{name: Type}` syntax as HTTP. Parameters are type-safe and prevent SQL injection.

### External Tables

Pass RecordBatches directly:

```ts
import { batchFromCols, getCodec } from "@maxjustus/chwire";

const users = batchFromCols({
  id: getCodec("UInt32").fromValues(new Uint32Array([1, 2, 3])),
  name: getCodec("String").fromValues(["Alice", "Bob", "Charlie"]),
});

for await (const packet of client.query(
  "SELECT * FROM users WHERE id > 1",
  { externalTables: { users } }
)) {
  if (packet.type === "Data") {
    for (const row of packet.batch) console.log(row.name);
  }
}
```

Supports streaming via iterables/async iterables of RecordBatch:

```ts
async function* generateBatches() {
  for (let i = 0; i < 10; i++) {
    yield batchFromCols({ id: getCodec("UInt32").fromValues([i]) });
  }
}

await client.query("SELECT sum(id) FROM data", {
  externalTables: { data: generateBatches() }
});
```

### Cancellation

```ts
const controller = new AbortController();
setTimeout(() => controller.abort(), 5000);

await client.connect({ signal: controller.signal });

for await (const p of client.query(sql, { signal: controller.signal })) {
  // ...
}
```

### Auto-Close on Scope Exit

```ts
await using client = await TcpClient.connect(options);
// automatically closed when scope exits
```

### Error Handling

The TCP client throws `ClickHouseException` for server errors:

```ts
import { TcpClient, ClickHouseException } from "@maxjustus/chwire/tcp";

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

Connection and protocol errors throw JavaScript's built-in `Error` (not `ClickHouseException`):

```ts
try {
  await client.connect();
} catch (err) {
  // err.message: "Connection timeout after 10000ms"
  // err.message: "Not connected"
  // err.message: "Connection busy - cannot run concurrent operations..."
}
```

## Native Format

Native is ClickHouse's columnar binary wire format. It's generally faster and smaller to serialize/deserialize vs JSON (see Performance below). Data arrives as RecordBatch objects. RecordBatch wraps typed column arrays you can iterate by row or access by column. Use it when throughput matters; use JSON when you want plain objects and don't need the speed.

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
  DynamicValue,
} from "@maxjustus/chwire";

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

// Encode and insert (HTTP)
await insert(
  "INSERT INTO t FORMAT Native",
  encodeNative(batch),
  connectionConfig,
);

// Query returns columnar data as RecordBatch - stream rows directly
for await (const row of rows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", connectionConfig)),
)) {
  console.log(row.id, row.name);
}

// Or collect all rows at once (materialized to plain objects)
const allRows = await collectRows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", connectionConfig)),
);

// Work with batches directly for columnar access
for await (const batch of streamDecodeNative(
  query("SELECT * FROM t FORMAT Native", connectionConfig),
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

// Dynamic with explicit per-value types via DynamicValue — use when you need
// a specific type that inference wouldn't pick (e.g. Int8 instead of Int64)
batchFromCols({
  dyn: getCodec("Dynamic").fromValues([new DynamicValue("Int8", 5), new DynamicValue("Float64", 3)]),
});

// JSON - plain objects (fromValues shreds row objects into columnar paths)
batchFromCols({
  data: getCodec("JSON").fromValues([{ a: 1, b: "x" }, { a: 2, c: true }]),
});

// JSON - columnar construction (fromCols skips row-object shredding, ~7x faster
// for column construction — see Performance section)
const jsonCodec = getCodec("JSON(id UInt32, score Float64)");
batchFromCols({
  data: jsonCodec.fromCols({
    id: new Uint32Array([1, 2, 3]),         // typed path: TypedArray, array, or Column
    score: new Float64Array([0.5, 1.0, 2.5]),
    tag: ["a", "b", null],                  // dynamic path: plain array (nulls OK)
  }),
});

// Nested(a UInt32, b String) - encoded as Array(Tuple(a UInt32, b String)).
// A top-level Nested column only round-trips when the target table was created
// with flatten_nested=0 (see the note below).
batchFromCols({
  n: getCodec("Nested(a UInt32, b String)").fromValues([
    [{ a: 1, b: "x" }, { a: 2, b: "y" }],
    [],
  ]),
});
```

### Streaming Native Insert

```ts
import { insert, streamEncodeNative, batchFromCols, getCodec } from "@maxjustus/chwire";

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
  connectionConfig,
);
```

Supports all ClickHouse types, with the two caveats below.

**Limitation**: `Dynamic` and `JSON` types require V3 flattened format. On ClickHouse 25.6+, set `output_format_native_use_flattened_dynamic_and_json_serialization=1`.

**Limitation**: A top-level `Nested` column is encoded as a single `Array(Tuple(...))` column. It only round-trips when the target table was created with `flatten_nested=0`. Under the default `flatten_nested=1`, ClickHouse stores the group as separate `<name>.<field>` Array columns; inserting the single Nested column then matches no physical column and the rows are silently stored as empty arrays (no error). `Nested` used inside another type is unaffected.

### BigInt Handling

ClickHouse 64-bit+ integers (Int64, UInt64, Int128, etc.) are returned as JavaScript BigInt. Pass `{ bigIntAsString: true }` to convert to strings for consumer code / JSON serialization:

```ts
const row = batch.get(0, { bigIntAsString: true });
const obj = row.toObject({ bigIntAsString: true });
const allRows = batch.toArray({ bigIntAsString: true });
```

> **Global alternative**: Add `BigInt.prototype.toJSON = function() { return this.toString(); };` at startup. See [MDN](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt#use_within_json).

## Compression

Set `compression` in HTTP or TCP options:

- `"lz4"` - default. Fast.
- `"zstd"` - ~2x better compression. Default level is 3.
- `false` - no compression for query results or insert data
- `{ method: "zstd", level: number }` - ZSTD with an explicit level (1-22, default: 3)

LZ4 and ZSTD use native Node addons (`lz4-napi`, `zstd-napi`) installed automatically as optional dependencies. In browsers and Deno, where native addons aren't supported, a bundled WASM implementation is used instead.

`compressQuery` compresses the HTTP request body (your SQL and any external table data) using HTTP `Content-Encoding`. This is independent of `compression`, which controls ClickHouse block compression on responses — they apply to different directions and don't double-compress. Set `compressQuery` to `"zstd"`, `"lz4"`, or `{ method: "zstd", level }`; requires the server setting `enable_http_compression=1`.

### Connection control (`dispatcher`)

`timeout` sets an overall deadline per request, and `signal` cancels one manually. For connection-level behavior — per-phase timeouts, pooling, proxies, retries — pass an undici `Dispatcher`. Node's built-in `fetch` honors it, so the client needs no extra dependency:

```ts
import { Agent } from "undici";

const dispatcher = new Agent({ headersTimeout: 3_000, bodyTimeout: 30_000 });

for await (const packet of query(sql, { dispatcher })) {
  // Fails fast if the server sends no headers within 3 seconds.
}
```

Any undici dispatcher works, including `ProxyAgent`, `RetryAgent`, and `MockAgent` for tests. `httpCompression` requests go over `node:http` and ignore it.

For long queries, prefer ClickHouse's own limits over transport timeouts. `settings: { max_execution_time: 60 }` bounds the query on the server, while an aborted socket does not necessarily stop it.

### Incremental delivery (`httpCompression`)

With the default `compression`, ClickHouse buffers the whole response and sends it once the query finishes. Rows and in-band progress only arrive at the end. Set `httpCompression` to get compression that the server flushes per block instead:

```ts
for await (const packet of query(sql, { httpCompression: "zstd" })) {
  // Data packets arrive as the server produces them.
}
```

Accepts `"zstd"` or `"lz4"`. It replaces `compression` for that request, rather than stacking with it. Measured on a query with 200 ms per row, the default returns all 15 packets at 1679 ms, while `httpCompression` returns the first at 225 ms.

Only `zstd` and `lz4` behave this way. `gzip` and `br` buffer inside the codec and deliver the whole body at the end, so they are not offered.

This path uses `node:http`, for a reason worth knowing: `fetch` decompresses inside its own pipeline, and a mid-stream server error tears that pipeline down before the buffered tail is delivered. The tail holds ClickHouse's error trailer, so the failure surfaces as `TypeError: terminated` instead of the real message, between 23% and 100% of the time. Decoding in-process avoids this. Two consequences:

- Not available in browsers or Deno, which cannot set `Accept-Encoding` in any case.
- `"zstd"` needs the optional `zstd-napi` dependency. `"lz4"` needs nothing extra.

## Performance

Benchmarks on Apple M4 Max / Node v25.9.0, 1M rows.

### Encode (raw, no compression)

| Scenario | JSONEachRow | Native | Speedup |
|----------|------------|--------|---------|
| Simple (6 cols) | 2267ms | 862ms | 2.6x |
| Escape-heavy strings | 1510ms | 630ms | 2.4x |
| Arrays (50 floats/row) | 7585ms | 1245ms | 6.1x |
| Arrays typed (50 floats/row) | 7691ms | 1501ms | 5.1x |
| Variant | 323ms | 633ms | 0.5x |
| Dynamic | 290ms | 337ms | 0.9x |
| JSON column | 762ms | 916ms | 0.8x |

### Decode (raw)

| Scenario | JSONEachRow | Native | Speedup |
|----------|------------|--------|---------|
| Simple (6 cols) | 984ms | 521ms | 1.9x |
| Escape-heavy strings | 833ms | 1011ms | 0.8x |
| Arrays (50 floats/row) | 6368ms | 804ms | 7.9x |
| Arrays typed (50 floats/row) | 6251ms | 1054ms | 5.9x |
| Variant | 488ms | 109ms | 4.5x |
| Dynamic | 488ms | 181ms | 2.7x |
| JSON column | 1109ms | 352ms | 3.2x |

### Encode + Compress (full path)

| Scenario | JSONEachRow+LZ4 | Native+LZ4 | JSONEachRow+ZSTD | Native+ZSTD | JSONEachRow+gzip | Native+gzip |
|----------|----------------|------------|-----------------|-------------|-----------------|-------------|
| Simple (6 cols) | 2755ms | 1459ms | 2340ms | 1410ms | 3196ms | 1435ms |
| Escape-heavy strings | 1271ms | 627ms | 1356ms | 623ms | 1552ms | 939ms |
| Arrays (50 floats/row) | 8768ms | 1933ms | 12601ms | 2231ms | 38306ms | 15073ms |
| Arrays typed (50 floats/row) | 9077ms | 2049ms | 12868ms | 2442ms | 38675ms | 15122ms |
| Variant | 711ms | 660ms | 718ms | 659ms | 866ms | 1104ms |
| Dynamic | 881ms | 653ms | 734ms | 611ms | 711ms | 965ms |
| JSON column | 906ms | 845ms | 1145ms | 847ms | 3491ms | 3155ms |

### Compressed Size (Native as % of JSONEachRow compressed with same codec, lower = smaller)

| Scenario | LZ4 | ZSTD | gzip |
|----------|-----|------|------|
| Simple (6 cols) | 64% | 65% | 66% |
| Escape-heavy strings | 93% | 164%* | 80% |
| Arrays (50 floats/row) | 48% | 81% | 85% |
| Arrays typed (50 floats/row) | 48% | 81% | 85% |
| Variant | 70% | 78% | 67% |
| Dynamic | 72% | 93% | 60% |
| JSON column | 56% | 62% | 66% |

*Escape-heavy strings with ZSTD: JSON's escaping creates repetitive byte patterns that ZSTD exploits.

### JSON fromCols vs fromValues

Column construction for `JSON(id UInt32, score Float64)` with one dynamic string path, 1M rows:

| | fromValues | fromCols | Speedup |
|--|-----------|----------|---------|
| Column construction | 249ms | 35ms | 7.1x |
| Full encode path | 977ms | 718ms | 1.4x |

`fromCols` skips row-object shredding: typed paths forward TypedArrays directly, dynamic paths bypass per-row key enumeration.

Run `make bench` (or `npm run bench`) to reproduce.

## Development

Requires Node.js 22+. The default test suite includes browser coverage; after `npm ci`, install the Playwright browser once with `npx playwright install chromium` (CI uses `--with-deps`).

```bash
make test              # build + run full test matrix across ClickHouse versions
make test-tcp          # TCP client tests only
make fuzz              # generated/native fuzz suite
make format            # run Biome formatter
make bench                  # Native vs JSON encode/compress benchmark
make bench-formats          # Same benchmark via direct node runner
make bench-concurrent       # HTTP vs TCP connect+query throughput under concurrency
make profile-json-caching   # JSON codec schema caching across batches
npm run bench:tcp            # TCP bulk-read throughput + wall-time ratio vs official clickhouse-client
make bench-profile ARGS="-f native -o encode -d complex"  # CPU profile a specific scenario
make update-settings   # regenerate ClickHouseSettings types from latest CH source
```

For a quick single-version test run: `npm test` (or `CH_VERSION=26.4 npm test`).

## CLI test client

Run queries directly from the command line via the bundled TCP client:

```bash
# Single query (outputs NDJSON packets)
npx @maxjustus/chwire 'SELECT version()'
bunx @maxjustus/chwire 'SELECT 1 + 1'

# Interactive REPL with history
npx @maxjustus/chwire

# Deno (with Node compatibility)
deno run -A npm:@maxjustus/chwire 'SELECT now()'
```

Configure via environment variables:

```bash
CH_HOST=clickhouse.example.com CH_PORT=9000 npx @maxjustus/chwire 'SELECT 1'
```

| Variable | Default | Description |
|----------|---------|-------------|
| CH_HOST | localhost | ClickHouse host |
| CH_PORT | 9000 | TCP native port |
| CH_USER | default | Username |
| CH_PASSWORD | "" | Password |

The REPL supports `\load file.jsonl INTO table` for bulk inserts from NDJSON files.
