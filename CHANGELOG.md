# Changelog

## 1.1.2

### Added

- `httpCompression: "zstd" | "lz4"` requests the response with an HTTP content coding that ClickHouse flushes per block, so rows and in-band progress arrive during the query instead of after it finishes. Replaces `compression` for that request. Decodes over `node:http`, so it is unavailable in browsers, and `"zstd"` needs the optional `zstd-napi` dependency.

### Fixed

- `{name: Identifier}` query parameters (table/column/database names) no longer throw `Unknown type: Identifier`. The value is sent verbatim so the server escapes it into a quoted identifier; pre-quoting is not applied.

## 1.1.1

### Fixed

- 1.1.0 bundles were unusable: the main bundle imported `isAsciiWhitespace` from the `/native` subpath, which does not export it. The ESM bundle failed at import; the CJS bundle threw on every `query()`/`insert()`. The helper is local to `params.ts` again.

### Package

- `npm publish` now runs the dist smoke test (`test:dist`) via `prepublishOnly` instead of only building, so a bundle that cannot be imported cannot be published.

## 1.1.0

### Added

- Added `JsonCodec.fromCols()` for constructing JSON columns path-by-path from columnar data, skipping row-object shredding. `getCodec("JSON(...)")` returns a narrowed type with the method available. Accepts `Dynamic` columns on dynamic paths without re-shredding, and rejects non-arraylike inputs (a bare string would otherwise become one row per character).

### Changed

- **Breaking**: `Codec.readPrefix` takes the block's `DeserializerState` as a second parameter; Dynamic/JSON wire metadata (type list, dynamic path list) now lives on that per-column, per-block state instead of the codec instance. Custom codecs implementing `readPrefix` must add the parameter. Codecs are now fully stateless, so `getCodec` caches every type — including `Dynamic`, `JSON`, and composites containing them, which previously rebuilt their codec tree per column per block.
- **Breaking**: Explicit Variant arm selection now uses the exported `VariantValue` class instead of `[discriminator, value]` arrays. Any `[number, x]` array was previously treated as an explicit discriminator pair, so `[1, 5]` into `Variant(Array(Int64), Int64)` silently encoded as `Int64 5` instead of an array. Plain arrays now always encode as values; `VariantColumn.get` returns `VariantValue` symmetrically.
- Credentials are sent via `X-ClickHouse-User`/`X-ClickHouse-Key` headers instead of URL parameters, keeping them out of server logs, proxies, and URL caches.
- `insert()` streams its body with pull-based backpressure, so a fast producer no longer buffers the entire payload in memory.

### Fixed

- Queries with unbound `{name: Type}` placeholders are now sent as-is instead of throwing `Missing parameter` client-side. Parameterized-view DDL (`CREATE VIEW v AS SELECT ... {x: String}`) and session-level `SET param_x = ...` bindings work; a genuinely unbound parameter fails server-side with `UNKNOWN_QUERY_PARAMETER`. Redeclaring a parameter at a conflicting type also no longer throws — the first declaration determines serialization and the server casts per use site.
- `Dynamic` index columns scale UInt8/UInt16/UInt32 with the flattened type count (shared-variant overflow makes the list unbounded by `max_types`); a 300-type Native insert now round-trips.
- Bare JS numbers into `Variant` route to `Int64`/`UInt64` arms; previously they skipped those arms and hit a narrower one (`Variant(Int64, UInt8)`: 300 threw out-of-range, 5 encoded on the wrong wire arm).
- JSON type parsing handles ClickHouse's canonical identifier spellings: per-segment backtick-quoted dotted paths (`` JSON(`sp ace`.s0 Int64) ``), backslash-escaped characters in quoted identifiers, a quoted `` `SKIP` `` typed path no longer treated as a SKIP directive, and only exact `SKIP` directives dropped (typed paths named `skipped`/`skip_*` were silently discarded).
- IPv4-mapped IPv6 addresses (`::ffff:192.168.1.1`) encode instead of throwing; zone IDs (`fe80::1%eth0`) are rejected at validation since 16 wire bytes cannot carry them.
- Decimal values with trailing zeros beyond the scale (`"1.50"` into `Decimal(9, 1)`) no longer throw a false precision-loss error; real precision loss still throws.
- `insert()` rejects non-positive `bufferSize` up front and clamps the flush threshold to `bufferSize`; both previously caused infinite flush loops. Server exceptions delivered in the body of a 200 insert response are now detected, and draining the body returns the connection to the pool.
- `decodeBlocks` throws on undecodable trailing bytes instead of silently dropping them; plain-text error bodies previously decoded to an empty string and surfaced as `ClickHouseException(0, "Unknown", "")`.
- Varint reads/writes guard against overflow and negative lengths (a negative string length previously moved the read cursor backward), and `readUInt32LE` stays unsigned when the high bit is set.

### Performance

- `Variant` `fromValues` shredding is 2.3x faster and `Dynamic` 1.17x (precomputed `typeof`-to-discriminator dispatch instead of a linear arm scan); JSON typed-path `fromValues` gains 1.38x indirectly.
- JSON `fromValues` no longer builds dense null-filled arrays per dynamic path (which made it O(paths x rows) even for keys present in few rows); sparse paths collect (row, value) pairs instead, ~38% faster encode on sparse-keyed objects. Row-key iteration also avoids allocating a key array per row (~9% dense, ~18% sparse).

## 1.0.0

`@maxjustus/chwire` is the renamed successor to `@maxjustus/chttp`, continuing from `1.15.0`.

### Added

- Added configurable ZSTD compression level via `compression: { method: "zstd", level }` (replaces the separate `zstdLevel` option).
- Added `DynamicValue` for inserting `Dynamic`-column values with an explicit ClickHouse type, bypassing runtime type inference.
- Added exported `ClickHouseException` support for structured ClickHouse server errors.
- Added `RecordBatch.isRecordBatch()` for identity-safe batch detection across module copies (ESM/CJS, source vs. bundle); insert and external-table dispatch use it instead of `instanceof`.
- Added a clear error when the server mandates the chunked TCP protocol, instead of desyncing after the handshake.
- Added TCP TLS coverage in the test suite.
- Added adaptive underflow retry backoff to `streamDecodeNative()` (configurable via `underflowRetryMinBytes` / `underflowRetryMaxBytes`) to avoid retrying partial Native blocks after every small chunk.

### Fixed

- `JsonColumn.type` now preserves the declared type string (e.g. `JSON(id UInt32)`) instead of always reporting `JSON`, so `batchFromCols` schema inference picks up typed-path declarations.
- HTTP requests now send `Accept-Encoding: identity`: the client already does its own block compression, and against ClickHouse 26.x a non-identity Accept-Encoding combined with `compress=1` made the server return empty error bodies (errors surfaced as "Unknown" with no message).
- Fixed mid-stream HTTP exception detection against ClickHouse 26.x, which frames the `__exception__` trailer with the random tag announced in `X-ClickHouse-Exception-Tag`. The old parser missed tagged trailers entirely: text formats returned silently truncated results, and Native streams surfaced decoder garbage (e.g. "Need 4 bytes at offset N") instead of the server exception.
- Native stream decoding now reports "stream ended mid-block" with byte counts when the source ends inside a block, instead of a bare buffer-underflow error.
- The TCP client now defaults to LZ4 compression. ZSTD choices are mirrored to the server as `network_compression_method` (plus `network_zstd_compression_level` when a level is set), merged below client and per-call settings. Previously the server always replied with its own default (LZ4) regardless of the configured codec, which made bulk reads over slow links carry 2-4x more wire bytes than zstd allows.
- Fixed TCP bulk-read throughput collapse on high-latency links: the reader now consumes the socket eagerly via `'data'` events (flowing mode) instead of the stream async iterator, which stopped kernel reads every ~64KB and kept the TCP receive window from growing. Backpressure still applies above a 16MB buffered threshold.
- Enabled `TCP_NODELAY` on the TCP connection; Nagle's algorithm delayed the query delimiter packet by one round trip per query.
- Fixed `RecordBatch.decodeTimeMs` reporting an absolute timestamp instead of decode duration when `debug` was off.
- Hardened TCP cancellation, abort, timeout, connection reuse, and reconnect-race behavior.
- Fixed HTTP query parameter passthrough for unmodeled ClickHouse URL parameters.
- Fixed Native and TCP partial block decoding across fragmented streams.
- Fixed TCP settings serialization revision gating so ZSTD compression mirroring works against ClickHouse 23.8/24.x instead of causing the server to close the connection mid-query.
- Fixed Native codec edge cases around decimals, tuple field parsing, sparse data, JSON, Dynamic, IPv6 (including non-ASCII rejection), and negative sub-second DateTime64 sign handling.
- Surfaced mid-stream server exceptions consistently across text and binary (Native) HTTP responses.
- Cancelled the HTTP response reader on early generator exit so the connection returns to the pool instead of being held until GC.
- Copied `Method.None` decode output so in-place HTTP buffer compaction cannot corrupt uncompressed blocks.
- Fixed TCP packet buffer ownership so queued writes cannot alias later `StreamingWriter` reuse, and large uncompressed Data packets no longer bloat the reusable writer buffer.
- Reduced retained memory for small Native/TCP result blocks by lowering stream decode buffer defaults.
- Hardened TCP lifecycle and draining: repeated `connect()` now refuses to replace an active socket, and compressed query drains can discard Data packets without materializing ignored batches.
- Hardened Native compression decode with decompressed-size caps and size verification.
- Fixed stateful `Dynamic(...)` codecs bypassing the Native codec cache, matching `Dynamic`/`JSON` cache behavior.
- Native JSON inserts now reject top-level arrays with a clear error instead of silently encoding them as empty objects.

### Changed

- **Breaking**: HTTP `query()` and `insert()` signatures changed — `sessionId` is no longer a positional parameter. It moves into the options object as `options.sessionId` and is now optional; omitting it produces a stateless request (no `session_id` URL param sent). Old: `query(sql, sessionId, options)` / `insert(sql, data, sessionId, options)`. New: `query(sql, options?)` / `insert(sql, data, options?)`.
- **Breaking**: Renamed HTTP option `baseUrl` to `url`.
- Decode short ASCII strings (<= 64 bytes) without TextDecoder, whose per-call overhead dominated string-heavy reads; cuts Native block decode time roughly in half on short-string workloads.
- Optimized `concat()` with 0/1 chunk fast paths.
- Improved fragmented Native stream decoding performance.
- Applied resumable Native block decoding more consistently across compressed and uncompressed TCP reads.
- Split Native codec internals into smaller scalar, composite, dynamic, base, and registry modules.
- Made default TCP fuzz coverage bounded and consolidated heavier generated fuzzing behind the shared `fuzz/` runner (`npm run test:fuzz`).
- Clarified collectable async generator behavior: one generator instance is single-consumer and does not replay after it is drained.
- Unexpected-packet errors now include human-readable packet names.
- Changed exported `encodeBlock()` to accept the `Compression` union (`"lz4"`, `"zstd"`, `false`, or `{ method: "zstd", level }`) instead of a method code plus separate ZSTD level.

### Package

- Renamed the published package and CLI from `@maxjustus/chttp` / `chttp` to `@maxjustus/chwire` / `chwire`.
- Declared `engines.node >= 22` (the package uses Node 22 APIs such as `Array.fromAsync`); CI tests Node 22 and 24.
- Cleaned package contents with an explicit publish allowlist.
- Removed `development` export conditions; source remains available through the `./source` export.
- Added `clean` before `build` so stale `dist` files cannot ship.
- Restored the main Native vs JSON format benchmark.
- Added `bench/tcp-read-profile.ts` (`bench:tcp`) for profiling TCP bulk-read throughput, kernel read stop/start churn, an HTTP transport comparison, and a wall-time ratio against the official clickhouse-client for regression tracking.
- Added `bench:check` to type-check benchmark code.
- Added `test:matrix` for running the suite across multiple ClickHouse versions.
- Repaired `test:integration` and removed broken benchmark script targets.
