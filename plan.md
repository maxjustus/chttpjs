# Streaming Insert Implementation Plan

## Overview
Implement a streaming insert function that accepts an async generator, buffers rows, compresses them in chunks, and sends them progressively to ClickHouse using HTTP chunked transfer encoding.

## Background
Based on analysis of clickhouse-rs implementation:
- Uses 256KB buffer size
- Sends chunks when buffer reaches ~254KB
- Each chunk is compressed independently
- Uses HTTP chunked transfer encoding
- Allows progressive processing by server

## Implementation Strategy

### 1. Create `insertCompressedStream` function
**Location**: `client-node.js`

**Features**:
- Accept async generator/iterator that yields data items
- Buffer items until reaching size threshold (default 256KB like clickhouse-rs)
- Compress each buffer as a separate block
- Use HTTP chunked transfer encoding to send blocks progressively
- Yield progress updates for monitoring

### 2. Key Design Decisions

**Buffering Strategy**:
- Buffer size: 256KB (matches clickhouse-rs)
- Threshold to send: ~254KB (leaves room for last row)
- Buffer by bytes, not row count (more predictable memory usage)

**HTTP Approach**:
- Use Node.js native chunked transfer encoding (omit Content-Length header)
- Keep connection open while streaming chunks
- Each chunk is a complete compressed block

**Progress Reporting**:
- Yield objects with: `{ blocksSent, bytesCompressed, bytesUncompressed, rowsProcessed }`
- Allow monitoring of compression ratios and throughput

### 3. Implementation Steps

1. **Create streaming function signature**:
   ```javascript
   async function* insertCompressedStream(
     query,           // INSERT query
     dataGenerator,   // Async generator yielding rows
     sessionId,       
     options = {}     // method, bufferSize, etc.
   )
   ```

2. **Implement buffering logic**:
   - Accumulate rows in array
   - Track buffer size in bytes
   - When threshold reached, compress and send

3. **Handle HTTP streaming**:
   - Start request without Content-Length
   - Use Transfer-Encoding: chunked
   - Write compressed blocks as they're ready
   - Keep connection alive between chunks

4. **Add error handling**:
   - Abort on compression errors
   - Handle network failures
   - Clean up resources on error

5. **Create test/example**:
   - Generator that yields large dataset
   - Compare memory usage vs non-streaming
   - Verify all rows inserted correctly

### 4. Example Implementation Structure

```javascript
async function* insertCompressedStream(query, dataGenerator, sessionId, options = {}) {
  const {
    method = Method.LZ4,
    bufferSize = 256 * 1024,  // 256KB
    threshold = bufferSize - 2048  // Leave room for last row
  } = options;

  // Start HTTP request with chunked encoding
  const url = buildReqUrl('http://localhost:8123/', {
    session_id: sessionId,
    query: query,
    decompress: '1',
    http_native_compression_disable_checksumming_on_decompress: '1',
  });

  const req = http.request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/octet-stream',
      'Transfer-Encoding': 'chunked'
    }
  });

  let buffer = [];
  let bufferBytes = 0;
  let totalRows = 0;
  let blocksSent = 0;

  try {
    for await (const row of dataGenerator) {
      const line = JSON.stringify(row) + '\n';
      buffer.push(line);
      bufferBytes += Buffer.byteLength(line);
      totalRows++;

      if (bufferBytes >= threshold) {
        // Compress and send this chunk
        const dataBytes = Buffer.from(buffer.join(''), 'utf8');
        const compressed = encodeBlock(dataBytes, method);
        
        req.write(compressed);
        blocksSent++;

        yield {
          blocksSent,
          bytesCompressed: compressed.length,
          bytesUncompressed: bufferBytes,
          rowsProcessed: totalRows
        };

        // Reset buffer
        buffer = [];
        bufferBytes = 0;
      }
    }

    // Send remaining data
    if (buffer.length > 0) {
      const dataBytes = Buffer.from(buffer.join(''), 'utf8');
      const compressed = encodeBlock(dataBytes, method);
      req.write(compressed);
      blocksSent++;

      yield {
        blocksSent,
        bytesCompressed: compressed.length,
        bytesUncompressed: bufferBytes,
        rowsProcessed: totalRows,
        complete: true
      };
    }

    // End request and wait for response
    await new Promise((resolve, reject) => {
      req.on('response', (res) => {
        if (res.statusCode === 200) {
          resolve();
        } else {
          reject(new Error(`Insert failed: ${res.statusCode}`));
        }
      });
      req.on('error', reject);
      req.end();
    });

  } catch (error) {
    req.destroy();
    throw error;
  }
}
```

### 5. Files to Modify/Create

- **Modify** `client-node.js`: Add `insertCompressedStream` function
- **Create** `test-streaming.js`: Test streaming with large dataset
- **Update** `README.md`: Document streaming API

### 6. Testing Plan

1. **Correctness Tests**:
   - Small dataset (100 rows)
   - Verify all rows inserted
   - Compare with non-streaming insert

2. **Performance Tests**:
   - Large dataset (1M+ rows)
   - Monitor memory usage
   - Measure throughput

3. **Error Handling**:
   - Network failure simulation
   - Invalid data
   - Server errors

4. **Compression Comparison**:
   - Test with LZ4
   - Test with ZSTD
   - Compare compression ratios and speed

### 7. Usage Example

```javascript
// Generator that yields rows
async function* generateData() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, value: `data_${i}` };
  }
}

// Stream insert with progress monitoring
const stream = insertCompressedStream(
  'INSERT INTO test FORMAT JSONEachRow',
  generateData(),
  sessionId,
  { method: Method.ZSTD }
);

for await (const progress of stream) {
  console.log(`Sent ${progress.blocksSent} blocks, ${progress.rowsProcessed} rows`);
  console.log(`Compression ratio: ${(progress.bytesUncompressed / progress.bytesCompressed).toFixed(2)}x`);
}
```

## Benefits

- **Memory Efficient**: Only one buffer in memory at a time
- **Progressive Processing**: ClickHouse can start processing early chunks
- **Backpressure**: Natural flow control via async iteration
- **Monitoring**: Real-time progress updates
- **Scalable**: Can handle datasets larger than memory

## Notes

- HTTP/1.1 chunked transfer encoding is well-supported by Node.js
- Each compressed block must be complete and valid
- ClickHouse processes blocks as they arrive
- Consider implementing connection pooling for multiple concurrent streams