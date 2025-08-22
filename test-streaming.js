const { Method } = require('./compression-node');
const { insertCompressed, execQuery } = require('./client-node');

// Generator that yields batches of rows
async function* generateBatchedData(totalRows = 100000, batchSize = 1000) {
  for (let i = 0; i < totalRows; i += batchSize) {
    const batch = [];
    for (let j = 0; j < batchSize && i + j < totalRows; j++) {
      batch.push({
        id: i + j,
        value: `data_${i + j}`,
        timestamp: new Date().toISOString(),
        random: Math.random()
      });
    }
    yield batch;
    
    // Simulate some async work between batches
    if (i % 10000 === 0 && i > 0) {
      console.log(`Generated ${i} rows...`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  }
}

// Generator that yields individual rows
async function* generateSingleRows(totalRows = 100000) {
  for (let i = 0; i < totalRows; i++) {
    yield {
      id: i,
      value: `data_${i}`,
      timestamp: new Date().toISOString(),
      random: Math.random()
    };
    
    // Simulate some async work periodically
    if (i % 10000 === 0 && i > 0) {
      console.log(`Generated ${i} rows...`);
      await new Promise(resolve => setTimeout(resolve, 1));
    }
  }
}

async function testStreaming() {
  const sessionId = Date.now().toString();
  
  try {
    // Create table
    console.log('Creating test table...');
    for await (const chunk of execQuery('DROP TABLE IF EXISTS streaming_test', sessionId)) {
      // Just consume the stream
    }
    for await (const chunk of execQuery(`
      CREATE TABLE streaming_test (
        id UInt32,
        value String,
        timestamp String,
        random Float64
      ) ENGINE = Memory
    `, sessionId)) {
      // Just consume the stream
    }
    
    // Test 1: Streaming with batched generator and LZ4
    console.log('\n=== Test 1: Streaming with batched generator (LZ4) ===');
    const insertQuery = 'INSERT INTO streaming_test FORMAT JSONEachRow';
    
    const startTime = Date.now();
    const memBefore = process.memoryUsage();
    
    await insertCompressed(
      insertQuery,
      generateBatchedData(100000, 1000),
      sessionId,
      Method.LZ4,
      {
        onProgress: (progress) => {
          if (progress.complete) {
            console.log('Streaming complete!');
          } else if (progress.blocksSent % 5 === 0) {
            console.log(`Progress: ${progress.blocksSent} blocks, ${progress.rowsProcessed} rows`);
          }
        }
      }
    );
    
    const memAfter = process.memoryUsage();
    const elapsed = Date.now() - startTime;
    
    console.log(`Time: ${elapsed}ms`);
    console.log(`Memory delta: ${((memAfter.heapUsed - memBefore.heapUsed) / 1024 / 1024).toFixed(2)} MB`);
    
    // Verify count
    let result1 = '';
    for await (const chunk of execQuery('SELECT count(*) as cnt FROM streaming_test FORMAT JSON', sessionId)) {
      result1 += chunk;
    }
    const data1 = JSON.parse(result1);
    console.log('Rows inserted:', data1.data[0].cnt);
    
    // Test 2: Streaming with ZSTD
    console.log('\n=== Test 2: Streaming with single-row generator (ZSTD) ===');
    for await (const chunk of execQuery('TRUNCATE TABLE streaming_test', sessionId)) {
      // Just consume the stream
    }
    
    await insertCompressed(
      insertQuery,
      generateSingleRows(50000),
      sessionId,
      Method.ZSTD,
      {
        bufferSize: 128 * 1024,  // Smaller buffer for testing
        onProgress: (progress) => {
          if (progress.complete) {
            console.log('Streaming complete!');
            console.log(`Final compression ratio: ${(progress.bytesUncompressed / progress.bytesCompressed).toFixed(2)}x`);
          }
        }
      }
    );
    
    let result2 = '';
    for await (const chunk of execQuery('SELECT count(*) as cnt FROM streaming_test FORMAT JSON', sessionId)) {
      result2 += chunk;
    }
    const data2 = JSON.parse(result2);
    console.log('Rows inserted:', data2.data[0].cnt);
    
    // Test 3: Compare with non-streaming (array) insert
    console.log('\n=== Test 3: Non-streaming array insert (for comparison) ===');
    for await (const chunk of execQuery('TRUNCATE TABLE streaming_test', sessionId)) {
      // Just consume the stream
    }
    
    // Generate all data in memory
    const allData = [];
    for (let i = 0; i < 50000; i++) {
      allData.push({
        id: i,
        value: `data_${i}`,
        timestamp: new Date().toISOString(),
        random: Math.random()
      });
    }
    
    const memBefore2 = process.memoryUsage();
    console.log('Inserting 50000 rows as a single array...');
    await insertCompressed(insertQuery, allData, sessionId, Method.LZ4);
    const memAfter2 = process.memoryUsage();
    
    console.log(`Memory delta for array insert: ${((memAfter2.heapUsed - memBefore2.heapUsed) / 1024 / 1024).toFixed(2)} MB`);
    
    let result3 = '';
    for await (const chunk of execQuery('SELECT count(*) as cnt FROM streaming_test FORMAT JSON', sessionId)) {
      result3 += chunk;
    }
    const data3 = JSON.parse(result3);
    console.log('Rows inserted:', data3.data[0].cnt);
    
    // Test 4: Streaming SELECT with compressed response
    console.log('\n=== Test 4: Streaming SELECT with compressed response ===');
    console.log('Querying 50000 rows with compression...');
    let streamedRows = 0;
    let chunks = 0;
    for await (const chunk of execQuery('SELECT * FROM streaming_test FORMAT JSONEachRow', sessionId, true)) {
      chunks++;
      // Count newlines to estimate rows (JSONEachRow has one row per line)
      streamedRows += (chunk.match(/\n/g) || []).length;
      if (chunks % 10 === 0) {
        console.log(`Received ${chunks} chunks, ~${streamedRows} rows so far...`);
      }
    }
    console.log(`Total: ${chunks} chunks received, ${streamedRows} rows`);
    
    // Clean up
    for await (const chunk of execQuery('DROP TABLE streaming_test', sessionId)) {
      // Just consume the stream
    }
    console.log('\nAll tests completed successfully!');
    
  } catch (error) {
    console.error('Test failed:', error.message);
    process.exit(1);
  }
}

testStreaming();