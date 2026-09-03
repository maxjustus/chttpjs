# Agent Guide for chttp

This repository implements a ClickHouse HTTP and TCP client in TypeScript, focused on efficient Native (columnar binary) format serialization/deserialization.

## Essential Commands

### Build & Type Check
- `npm run build`: Full build (bundles with esbuild).
- `tsc --noEmit`: **CRITICAL** - Always run this to verify type correctness before submitting.

### Testing
Node is the target runtime — run tests and benches under `node` (or `tsx`), not another runtime.
- `npm test`: Runs all tests (integration tests start a ClickHouse 25.8 container; override with `CH_VERSION=26.4 npm test`).
- `npm run test:matrix`: Runs the suite against multiple ClickHouse versions (`CH_VERSIONS="25.8 26.4"` to customize). Run it when touching protocol parsing, error framing, or anything wire-adjacent.
- `npm run test:fuzz` / `make fuzz`: Fuzz the Native codec against a live ClickHouse server using `generateRandom` / `generateRandomStructure`.
- `npm run test:tcp`: Runs TCP client tests.
- `FUZZ_ITERATIONS=5 FUZZ_ROWS=10000 npx tsx fuzz/parallel.ts --http`: Run one fuzz suite with parameters (`--unit`, `--tcp`, `--generated`, `--all`).
- `FUZZ_STRUCTURE='c1 UInt32, ...'`: Replay a specific failing structure. Data is unseeded, so structure replay alone may not reproduce data-dependent failures; on http roundtrip decode failures the raw decoder input is saved to `.tmp/fuzz-artifacts/` for offline analysis.

### Benchmarking & Profiling
- `node --experimental-strip-types bench/formats.ts`: Benchmark comparing Native vs JSONEachRow encode/decode. Keep it current whenever the Native codec changes.
- `npm run bench:tcp`: TCP bulk-read profile (throughput, kernel readStop churn, decode time) plus a wall-time ratio against the official clickhouse-client. Run it when touching the TCP reader, codecs, or compression; the regression signal is drift in the chwire/cli ratio, not its absolute value.
- `make bench-profile ARGS="-f native -o encode -d complex"`: Profile with CPU sampling.

### Changelog
- Update `CHANGELOG.md` before committing any change to user-facing behavior: new or changed public APIs, bug fixes, behavior changes, or packaging changes. Add a terse bullet under the current unreleased version's Added/Fixed/Changed/Package heading.
- Internal-only work (refactors, tests, fuzz harness, build/tooling, type-strictness) does not need an entry.

## Project Structure

- `client.ts`: HTTP client — query/insert, streaming, compression, server-error framing.
- `compression.ts`: LZ4 / ZSTD / None block framing shared by HTTP and TCP.
- `iter.ts`: Async iteration utilities (`toAsyncIterable`, `mapAsync`, `prepend`, `readChunks`).
- `native/`: Native (columnar) format implementation.
  - `index.ts`: Public Native API (`RecordBatch`, encode/decode).
  - `io.ts`: Buffer readers/writers and block buffering.
  - `columns.ts`, `table.ts`: Columnar data structures and the `RecordBatch` table abstraction.
  - `codecs/`: Per-type encode/decode, split into `scalar`, `composite`, `dynamic`, `base`, `registry`.
- `tcp_client/`: TCP protocol implementation and client.
- `bench/`: Performance benchmarks.
- `fuzz/`: Fuzz harness (parallel runner and generators).
- `test/`: Unit, integration, and fuzz tests.

## Key Patterns & Conventions

### Performance
- **JS Array Allocation**: Profiling on Node v25.9.0 / Apple M4 Max showed `new Array(n)` + full index assignment beats `.fill(...)` and `push()` for hot column extraction, string decode/copy, nullable inner arrays, and array flattening. Use `.fill(default)` only when the array is semantically default-filled/sparse (e.g. sparse decode fallback), not as a blanket packed-array optimization.
- **TypedArrays**: Use TypedArrays (Int32Array, Float64Array, etc.) whenever possible for numeric columns.
- **Buffer Management**: Use `BufferWriter` and `BufferReader` (or `StreamingReader`/`StreamingWriter`) for efficient byte operations.
- **NaN Handling**: Floats are plain JS `number`s and JavaScript canonicalizes NaN, so a specific NaN bit pattern from the server is not preserved on re-encode. Fuzz comparisons rely on `cityHash64(*)` instead of byte equality (see Gotchas).

### Native Format
- Native is columnar. Data is processed column by column rather than row by row.
- **Serialization Nodes**: The Native format uses a serialization tree to track dense/sparse encoding for nested types.

### Handshake & Revision
- The TCP client protocol depends on the server's revision. Always check `this.serverHello.revision` before reading/writing fields added in newer versions.

## Gotchas

- **NaN Equality**: In fuzz tests, `NaN != NaN`. Use `cityHash64(*)` in ClickHouse queries to compare rows containing NaNs.
- **Columnar vs Row**: Native is columnar. Inserting row objects or row arrays transposes them into columns before encoding, which has a cost.
- **Buffer Underflow**: Streaming decoders must handle `BufferUnderflowError` or `RangeError` by pulling more data from the source.
- **Server-version wire behavior**: Tests pin ClickHouse 25.8 (`test/setup.ts`), but wire details change across versions — e.g. 26.x frames mid-stream HTTP exception trailers with the random tag from `X-ClickHouse-Exception-Tag`, which 25.8 omits. When touching protocol parsing or error framing, probe the actual bytes against a newer server too (`docker run clickhouse/clickhouse-server:<ver>` + curl).
- **TextDecoder per-call cost**: `TextDecoder.decode` has a high fixed cost per call; short ASCII strings decode several times faster char-by-char (see `BufferReader.readString`). Don't add per-row `TextDecoder` calls to hot decode paths.
- **LZ4 match copy**: keep the byte-by-byte loop in `lz4_frame.ts` `decodeSequences`. `dst.copyWithin` for non-overlapping matches measured 10-25% slower (Node v26, M4 Max): LZ4 match lengths are mostly short, so the call overhead beats the byte loop.
- **Response buffering**: `compress=1` (the `compression` option) makes the server buffer the whole response, so data and in-band progress rows land only at the end. `IOutputFormat::flushImpl` forwards `next()` only through `WriteBufferWithOwnMemoryDecorator`, which `CompressedWriteBuffer` is not. Of the content codings that do get forwarded, only `zstd` and `lz4` flush per call — `gzip`/`deflate` use `Z_NO_FLUSH` until finalize, and `br` buffers too. `httpCompression` uses the two that work.
- **Never decompress inside `fetch` for this**: a mid-stream ClickHouse error closes the connection without the terminating chunk, and undici's `Fetch.onAborted` errors the response stream at once. Output still inside the decompression transform is discarded, which loses the framed `__exception__` trailer — measured between 23% and 100% of the body across identical runs. `http_transport.ts` reads raw bytes over `node:http` and decodes in-process, so the trailer always lands. Note `ReadableStream.controller.error()` clears the queue: any body that must deliver bytes before failing has to be an async generator, not an enqueue-then-error stream.
- **Mid-stream error shape varies by version**: with `JSONEachRowWithProgress`, 25.8 reports a mid-stream failure as an `{"exception": ...}` row in the body, while 26.x sends the framed `__exception__` trailer and throws. Assert on parity with an uncompressed response, not on a specific mechanism.
- **Node.js Flags**: Use `--experimental-strip-types` to run `.ts` files directly with Node.js.
