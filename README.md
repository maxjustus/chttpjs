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

// Binary formats (RowBinary, etc.)
const bytes = await collectBytes(
  query("SELECT * FROM t FORMAT RowBinaryWithNamesAndTypes", session, config),
);
```

## RowBinary Format

Binary format that's ~7x faster to encode than JSON for simple data, with ~3x smaller payloads. Uses `RowBinaryWithNamesAndTypes` format (self-describing with column names and types in header).

```ts
import { insert, encodeRowBinary, type ColumnDef } from "@maxjustus/chttp";

const columns: ColumnDef[] = [
  { name: "id", type: "UInt32" },
  { name: "name", type: "String" },
  { name: "value", type: "Float64" },
];

const rows = [
  [1, "alice", 1.5],
  [2, "bob", 2.5],
];

const data = encodeRowBinary(columns, rows);

await insert(
  "INSERT INTO table FORMAT RowBinaryWithNamesAndTypes",
  data,
  "session123",
  config,
);
```

Supported types:

- Integers: `Int8`-`Int64`, `UInt8`-`UInt64`, `Int128`, `UInt128`, `Int256`, `UInt256`
- Floats: `Float32`, `Float64`
- Decimals: `Decimal32(P,S)`, `Decimal64(P,S)`, `Decimal128(P,S)`, `Decimal256(P,S)`
- Strings: `String`, `FixedString(N)`
- Date/Time: `Date`, `Date32`, `DateTime`, `DateTime64(precision)`
- Other: `Bool`, `UUID`, `IPv4`, `IPv6`, `Enum8(...)`, `Enum16(...)`
- Geo: `Point`, `Ring`, `Polygon`, `MultiPolygon`
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
  [42], // -> Int64
  [3.14], // -> Float64
  ["hello"], // -> String
  [true], // -> Bool
  [new Date()], // -> DateTime64(3)
  [[1, 2, 3]], // -> Array(Int64)
];

// Explicit types (for anything not auto-inferred)
const rows = [
  [{ type: "UInt8", value: 255 }],
  [{ type: "Decimal64(18, 4)", value: "123.4567" }],
];
```

### Streaming Insert

For large inserts, use `streamEncodeRowBinary` to generate chunks on demand:

```ts
import {
  insert,
  streamEncodeRowBinary,
  type ColumnDef,
} from "@maxjustus/chttp";

const columns: ColumnDef[] = [
  { name: "id", type: "UInt32" },
  { name: "value", type: "Float64" },
];

async function* generateRows() {
  for (let i = 0; i < 1000000; i++) {
    yield [i, Math.random()];
  }
}

await insert(
  "INSERT INTO table FORMAT RowBinaryWithNamesAndTypes",
  streamEncodeRowBinary(columns, generateRows()),
  "session123",
  config,
);
```

Options:

- `chunkSize`: Target bytes per yielded chunk (default 64KB). Larger = fewer chunks, smaller = lower memory.
- `includeHeader`: Emit column names/types in first chunk (default true). Set false if appending to existing stream.

### Streaming Select

Use `streamDecodeRowBinary` to decode rows as they arrive (yields batches per chunk):

```ts
import { query, streamDecodeRowBinary } from "@maxjustus/chttp";

const stream = query(
  "SELECT * FROM table FORMAT RowBinaryWithNamesAndTypes",
  "session123",
  config,
);

for await (const { columns, rows } of streamDecodeRowBinary(stream)) {
  for (const row of rows) {
    console.log(row);
  }
}
```

### Buffered Decode

For smaller results, buffer the entire response:

```ts
import { query, collectBytes, decodeRowBinary } from "@maxjustus/chttp";

const data = await collectBytes(
  query(
    "SELECT * FROM table FORMAT RowBinaryWithNamesAndTypes",
    "session123",
    config,
  ),
);

const { columns, rows } = decodeRowBinary(data);
```

## Native Format

ClickHouse's internal wire format. Returns columnar data (virtual columns) rather than materializing all rows upfront.

### Table Construction

```ts
import {
  insert,
  query,
  collectBytes,
  encodeNative,
  decodeNative,
  tableFromArrays,
  tableFromRows,
  tableFromCols,
  tableBuilder,
  makeBuilder,
} from "@maxjustus/chttp";

const schema = [
  { name: "id", type: "UInt32" },
  { name: "name", type: "String" },
];

// From columnar data (named columns)
const table = tableFromArrays(schema, {
  id: new Uint32Array([1, 2, 3]),
  name: ["alice", "bob", "charlie"],
});

// From row arrays
const table2 = tableFromRows(schema, [
  [1, "alice"],
  [2, "bob"],
  [3, "charlie"],
]);

// Row-by-row builder
const builder = tableBuilder(schema);
builder.appendRow([1, "alice"]);
builder.appendRow([2, "bob"]);
builder.appendRow([3, "charlie"]);
const table3 = builder.finish();

// Encode and insert
await insert(
  "INSERT INTO t FORMAT Native",
  encodeNative(table),
  "session",
  config,
);

// Query returns columnar data wrapped in a Table
const bytes = await collectBytes(
  query("SELECT * FROM t FORMAT Native", "session", config),
);
const result = await decodeNative(bytes);

for (const row of result) {
  console.log(row.id, row.name);
}

// Access columns directly
const ids = result.getColumn("id")!;
for (let i = 0; i < ids.length; i++) {
  console.log(ids.get(i));
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
const table = tableFromCols({ id: idCol, name: nameCol });
// table.schema = [{ name: "id", type: "UInt32" }, { name: "name", type: "String" }]
```

### Complex Types

```ts
// Array(Int32)
tableFromArrays([{ name: "tags", type: "Array(Int32)" }], {
  tags: [[1, 2], [3, 4, 5], [6]],
});

// Tuple(Float64, Float64) - positional
tableFromArrays([{ name: "point", type: "Tuple(Float64, Float64)" }], {
  point: [
    [1.0, 2.0],
    [3.0, 4.0],
  ],
});

// Tuple(x Float64, y Float64) - named tuples use objects
tableFromArrays([{ name: "point", type: "Tuple(x Float64, y Float64)" }], {
  point: [
    { x: 1.0, y: 2.0 },
    { x: 3.0, y: 4.0 },
  ],
});

// Map(String, Int32)
tableFromArrays([{ name: "meta", type: "Map(String, Int32)" }], {
  meta: [{ a: 1, b: 2 }, new Map([["c", 3]])],
});

// Nullable(String)
tableFromArrays([{ name: "note", type: "Nullable(String)" }], {
  note: ["hello", null, "world"],
});

// Variant(String, Int64, Bool) - type inferred from values
tableFromArrays([{ name: "val", type: "Variant(String, Int64, Bool)" }], {
  val: ["hello", 42n, true, null],
});

// Variant with explicit discriminators (for ambiguous cases)
tableFromArrays(
  [{ name: "val", type: "Variant(String, Int64, Bool)" }],
  { val: [[0, "hello"], [1, 42n], [2, true], null] }, // [discriminator, value]
);

// Dynamic - types inferred automatically
tableFromArrays([{ name: "dyn", type: "Dynamic" }], {
  dyn: ["hello", 42, true, [1, 2, 3], null],
});

// JSON - plain objects
tableFromArrays([{ name: "data", type: "JSON" }], {
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
  streamNativeRows,
  tableFromArrays,
  asRows,
} from "@maxjustus/chttp";

// Streaming decode - rows as objects (lazy)
for await (const row of streamNativeRows(
  streamDecodeNative(query("SELECT * FROM t FORMAT Native", "session", config)),
)) {
  console.log(row.id, row.name);
}

// Or work with Table blocks directly
for await (const table of streamDecodeNative(
  query("SELECT * FROM t FORMAT Native", "session", config),
)) {
  // Iterate rows from a Table block
  for (const row of asRows(table)) {
    console.log(row.id, row.name);
  }
}

// Streaming insert - generate Tables
async function* generateTables() {
  const schema = [
    { name: "id", type: "UInt32" },
    { name: "value", type: "Float64" },
  ];
  const batchSize = 10000;
  for (let batch = 0; batch < 100; batch++) {
    const ids = new Uint32Array(batchSize);
    const values = new Float64Array(batchSize);
    for (let i = 0; i < batchSize; i++) {
      ids[i] = batch * batchSize + i;
      values[i] = Math.random();
    }
    yield tableFromArrays(schema, { id: ids, value: values });
  }
}

await insert(
  "INSERT INTO t FORMAT Native",
  streamEncodeNative(generateTables()),
  "session",
  config,
);
```

Supports the same types as RowBinary.

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

// Query - streams packets as they arrive
for await (const packet of client.query("SELECT * FROM table")) {
  if (packet.type === "Data") {
    for (const row of packet.table) {
      console.log(row.id, row.name);
    }
  }
}

// Execute DDL
await client.execute("CREATE TABLE ...");

// Insert
await client.insert("INSERT INTO table VALUES", table);

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
      console.log(`${packet.table.rowCount} rows`);
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

### Streaming Insert

Use separate connections for read and write when streaming:

```ts
const readClient = new TcpClient(options);
const writeClient = new TcpClient(options);
await readClient.connect();
await writeClient.connect();

// Stream from one table to another
const tables = (async function* () {
  for await (const packet of readClient.query("SELECT * FROM src")) {
    if (packet.type === "Data") yield packet.table;
  }
})();

await writeClient.insert("INSERT INTO dst VALUES", tables);
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

### Auto-Close

```ts
await using client = await TcpClient.connect(options);
// automatically closed when scope exits
```

## JSONCompactEachRowWithNames Format

Compact JSON format where the first row contains column names and subsequent rows are value arrays:

```ts
import {
  insert,
  query,
  streamJsonCompactEachRowWithNames,
  parseJsonCompactEachRowWithNames,
} from "@maxjustus/chttp";

// Insert - objects are automatically converted to compact arrays
const rows = [
  { id: 1, name: "alice", value: 1.5 },
  { id: 2, name: "bob", value: 2.5 },
];

await insert(
  "INSERT INTO table FORMAT JSONCompactEachRowWithNames",
  streamJsonCompactEachRowWithNames(rows), // columns extracted from first object
  "session123",
  config,
);

// Query - parse compact format back to objects
for await (const row of parseJsonCompactEachRowWithNames(
  query(
    "SELECT * FROM table FORMAT JSONCompactEachRowWithNames",
    "session123",
    config,
  ),
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
  timeout: 60_000,
});
```

Requires Node.js 20+ or modern browsers (Chrome 116+, Firefox 124+, Safari 17.4+) for `AbortSignal.any()`.

## Compression

Set `compression` in options:

- `"lz4"` - fast, WASM (default)
- `"zstd"` - ~2x better compression, native in Node.js with WASM fallback
- `"none"` - no compression

ZSTD uses native bindings in Node.js when available, falling back to WASM in browsers. Run `npm run bench` to see compression ratios and speeds for your data.

## Development

```bash
npm test       # runs integration tests against ClickHouse via testcontainers
make test-tcp  # TCP client tests (requires local ClickHouse on port 9000)
make fuzz-tcp  # TCP fuzz tests (FUZZ_ITERATIONS=10 FUZZ_ROWS=20000)
```

Requires Node.js 24+ (uses `--experimental-strip-types` for direct TS execution).
