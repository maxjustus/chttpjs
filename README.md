# chttp

ClickHouse HTTP client with native compression (LZ4/ZSTD).

## Install

```bash
npm install @maxjustus/chttp
```

## Quick Start

```ts
import { insert, query, streamEncodeJsonEachRow, collectText } from "@maxjustus/chttp";

const config = {
  baseUrl: "http://localhost:8123/",
  auth: { username: "default", password: "" },
};

// Insert - returns { summary, queryId } (HTTP is request/response, no streaming progress)
const { summary } = await insert(
  "INSERT INTO table FORMAT JSONEachRow",
  streamEncodeJsonEachRow([{ id: 1, name: "test" }]),
  "session123",
  config, // compression defaults to "lz4"
);
console.log(`Wrote ${summary.written_rows} rows`);

// Insert raw bytes (any format)
const encoder = new TextEncoder();
const csvData = encoder.encode("1,test\n2,other\n");
await insert("INSERT INTO table FORMAT CSV", csvData, "session123", config);

// Query - yields packets: Progress, Data, Summary (mirrors TCP client API)
// Helper functions filter for Data packets automatically:
const json = await collectText(query("SELECT * FROM table FORMAT JSON", "session123", config));

// Or iterate packets directly for progress/summary access:
for await (const packet of query("SELECT * FROM table FORMAT JSON", "session123", config)) {
  switch (packet.type) {
    case "Progress":
      console.log(`Progress: ${packet.progress.read_rows} rows`);
      break;
    case "Data":
      processChunk(packet.chunk);
      break;
    case "Summary":
      console.log(`Done: ${packet.summary.read_rows} rows in ${packet.summary.elapsed_ns}ns`);
      break;
  }
}

// DDL statements
for await (const _ of query("CREATE TABLE ...", "session123", config)) {}
```

## Query Parameters

Use ClickHouse's native query parameters to safely inject values:

```ts
import { query, collectText } from "@maxjustus/chttp";

// Single parameter
const result = await collectText(
  query("SELECT {id: UInt64} as id, {name: String} as name FORMAT JSON", sessionId, {
    ...config,
    params: { id: 42, name: "Alice" },
  }),
);

// Multiple parameters with different types
const filtered = await collectText(
  query(
    "SELECT * FROM users WHERE age > {min_age: UInt32} AND status = {status: String} FORMAT JSON",
    sessionId,
    { ...config, params: { min_age: 18, status: "active" } },
  ),
);

// BigInt for large integers
const big = await collectText(
  query("SELECT {value: UInt64} FORMAT JSON", sessionId, {
    ...config,
    params: { value: 9007199254740993n },
  }),
);
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
  query,
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

### BigInt

ClickHouse integer types (Int64, UInt64, Int128, etc.) are returned as JavaScript BigInt values via the Native format to preserve full precision.
By default, `JSON.stringify()` throws when trying to serialize BigInts.
Add this code so it runs once at startup to enable serialization of BigInts to strings as a global default (matching ClickHouse's default behavior for JSON encoding):

```typescript
BigInt.prototype.toJSON = function() { return this.toString(); };
```

as awful as it might seem to monkeypatch built-ins this is actually a "blessed"/suggested approach:
https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt#use_within_json

Alternatively you can pass `{ bigIntAsString: true }` to convert bigints to strings when materializing rows:

```ts
// On row access
const row = batch.get(0, { bigIntAsString: true });
console.log(row.largeId); // "9223372036854775807" (string)

// On toObject/toArray
const obj = row.toObject({ bigIntAsString: true });
const arr = row.toArray({ bigIntAsString: true });

// On batch materialization
const allRows = batch.toArray({ bigIntAsString: true });
```

## TCP Client (Experimental)

Direct TCP protocol for lower latency. Preferable for long-running queries and large inserts where you want real-time progress streaming. Single connection per client - use separate clients for concurrent operations.

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

### Query Parameters

Use parameterized queries to safely inject values:

```ts
// UInt64 parameter
for await (const packet of client.query(
  "SELECT {value: UInt64} as v",
  { params: { value: 42 } }
)) {
  if (packet.type === "Data") {
    console.log(packet.batch.getColumn("v")?.get(0)); // 42n
  }
}

// String parameter
for await (const packet of client.query(
  "SELECT {name: String} as s",
  { params: { name: "hello world" } }
)) {
  // ...
}

// Multiple parameters
for await (const packet of client.query(
  "SELECT * FROM users WHERE age > {min_age: UInt32} AND status = {status: String}",
  { params: { min_age: 18, status: "active" } }
)) {
  // ...
}
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

The percentage calculation uses `max(readRows, totalRowsToRead)` as denominator to handle cases where the server's estimate is low.

### ProfileEvents and Resource Metrics

ProfileEvents packets provide detailed execution metrics (memory usage, CPU time, I/O stats). Memory and CPU stats are automatically merged into the accumulated progress:

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

`memoryUsage` reflects current memory (latest value), while `peakMemoryUsage` tracks the highest seen (max). CPU time is summed. The `cpuUsage` field shows equivalent CPUs utilized (1.0 = one full CPU, 4.0 = four CPUs busy).

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

### External Tables (TCP)

Send data as temporary in-memory tables available during query execution:

```ts
import { batchFromArrays } from "@maxjustus/chttp";

const users = batchFromArrays(
  [{ name: "id", type: "UInt32" }, { name: "name", type: "String" }],
  { id: new Uint32Array([1, 2, 3]), name: ["Alice", "Bob", "Charlie"] }
);

for await (const packet of client.query(
  "SELECT * FROM users WHERE id > 1",
  { externalTables: { users } }
)) {
  if (packet.type === "Data") {
    for (const row of packet.batch) {
      console.log(row.name);
    }
  }
}

// Multiple tables for JOINs
const orders = batchFromArrays(
  [{ name: "user_id", type: "UInt32" }, { name: "amount", type: "Float64" }],
  { user_id: new Uint32Array([1, 2, 1]), amount: new Float64Array([10.5, 20.0, 15.5]) }
);

client.query(
  "SELECT u.name, sum(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
  { externalTables: { users, orders } }
);

// Stream large external tables with async generators
async function* generateBatches() {
  for (let i = 0; i < 100; i++) {
    yield batchFromArrays(schema, { id: new Uint32Array([i]) });
  }
}
client.query("SELECT count() FROM data", { externalTables: { data: generateBatches() } });
```

## External Tables (HTTP)

Send temporary tables via multipart/form-data. Schema must be specified explicitly.

```ts
import { query, collectText, encodeNative, batchFromArrays } from "@maxjustus/chttp";

// Native format (recommended - compact binary encoding)
const batch = batchFromArrays(
  [{ name: "id", type: "UInt32" }, { name: "name", type: "String" }],
  { id: new Uint32Array([1, 2, 3]), name: ["Alice", "Bob", "Charlie"] }
);

const result = await collectText(query(
  "SELECT * FROM mydata ORDER BY id FORMAT JSON",
  sessionId,
  {
    baseUrl, auth,
    externalTables: {
      mydata: {
        structure: "id UInt32, name String",
        format: "Native",
        data: encodeNative(batch)
      }
    }
  }
));

// Text formats work too (TabSeparated is default)
query("SELECT sum(value) FROM numbers", sessionId, {
  externalTables: {
    numbers: {
      structure: "value Int64",
      data: "100\n200\n300\n"
    }
  }
});
```

Data can be `string`, `Uint8Array`, or `AsyncIterable<Uint8Array>`. Supports any ClickHouse input format via the `format` option.

**Note**: HTTP external tables do not support request body compression. Use Native or Parquet format for efficient binary encoding.

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

### Common Error Codes

| Code | Name | Description |
|------|------|-------------|
| 60 | UNKNOWN_TABLE | Table does not exist |
| 62 | SYNTAX_ERROR | SQL syntax error |
| 27 | CANNOT_PARSE_INPUT_ASSERTION_FAILED | Data type mismatch |
| 117 | UNKNOWN_COLUMN | Column does not exist |
| 164 | READONLY | Cannot execute in readonly mode |

## Compression

Set `compression` in options:

- `"lz4"` - fast, native in Node.js with WASM fallback (default)
- `"zstd"` - ~2x better compression, native in Node.js with WASM fallback
- `false` - no compression

ZSTD and LZ4 use native bindings in Node.js when available, falling back to WASM in browsers. Run `npm run bench` to see compression ratios and speeds for your data.

## Development

```bash
npm test       # runs integration tests against ClickHouse via testcontainers
make test-tcp  # TCP client tests (requires local ClickHouse on port 9000)
make fuzz-tcp  # TCP fuzz tests (FUZZ_ITERATIONS=10 FUZZ_ROWS=20000)
```

Requires Node.js 24+ (uses `--experimental-strip-types` for direct TS execution).
