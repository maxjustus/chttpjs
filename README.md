# ClickHouse LZ4 Compression for Node.js

Implementation of ClickHouse's native LZ4 compression protocol for Node.js.

## Key Findings

ClickHouse uses a custom LZ4 compression format with the following structure:

- 16-byte CityHash128 checksum (v1.0.2)
- 1-byte magic number (0x82 for LZ4)
- 4-byte compressed size (little-endian, includes 9-byte header)
- 4-byte uncompressed size (little-endian)
- Raw LZ4 compressed data

## Important Notes

1. **CityHash Version**: ClickHouse uses CityHash v1.0.2, which is incompatible with most JavaScript implementations. Use `http_native_compression_disable_checksumming_on_decompress=1` to bypass checksum verification.

2. **Compressed Size**: The `compressed_size` field includes the 9-byte header size, not just the compressed data.

3. **Decompression**: Use `decompress=1` parameter to tell ClickHouse to decompress incoming data.

## Usage

### Sending Compressed Data

```javascript
const { encodeBlock, Method } = require("./compression-node");

// Compress data
const data = Buffer.from("your data here");
const compressed = encodeBlock(data, Method.LZ4);

// Send to ClickHouse with proper parameters:
// - decompress=1
// - http_native_compression_disable_checksumming_on_decompress=1
```

### Receiving Compressed Responses

```javascript
const { decodeBlock } = require("./compression-node");

// Request compressed response with compress=1 parameter
// Decompress the response (skip checksum verification due to CityHash version)
const decompressed = decodeBlock(compressedData, true);
```

## Files

- `compression-node.js` - Core compression implementation
- `client-node.js` - Example client showing how to use compression with ClickHouse

## Features

- **LZ4 compression** - Fast compression with good ratios
- **ZSTD compression** - Better compression ratios (often 5-10x better than LZ4)
- **Multi-block support** - Handles multiple compressed blocks in responses
- **Bidirectional** - Both sending and receiving compressed data

## Dependencies

```bash
npm install lz4 bling-hashes zstd-napi
```

## Compression Comparison

For repetitive data:

- LZ4: ~4x compression ratio, fastest
- ZSTD: ~37x compression ratio, slightly slower but much better compression
