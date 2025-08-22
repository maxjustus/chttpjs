const { Method } = require('./compression-node');
const { insertCompressed, execQuery } = require('./client-node');

async function testMultiBlockResponse() {
  const sessionId = Date.now().toString();
  
  try {
    // Create table
    console.log('Creating test table...');
    for await (const chunk of execQuery('DROP TABLE IF EXISTS multi_block_test', sessionId)) {
      // consume
    }
    for await (const chunk of execQuery(`
      CREATE TABLE multi_block_test (
        id UInt32,
        value String
      ) ENGINE = Memory
    `, sessionId)) {
      // consume
    }
    
    // Insert more test data to trigger multiple blocks
    const data = [];
    for (let i = 0; i < 10000; i++) {
      data.push({ id: i, value: `value_${i}_with_some_longer_text_to_make_blocks_bigger` });
    }
    await insertCompressed(
      'INSERT INTO multi_block_test FORMAT JSONEachRow',
      data,
      sessionId,
      Method.LZ4
    );
    
    // Test 1: Force multiple blocks with max_block_size=1
    console.log('\n=== Test 1: max_block_size=1 (each row = separate block) ===');
    let chunks = 0;
    let totalData = '';
    
    // Build query with max_block_size option
    const url = require('./client-node').buildReqUrl('http://localhost:8123/', {
      session_id: sessionId,
      query: 'SELECT * FROM multi_block_test FORMAT JSONEachRow',
      compress: '1',
      max_block_size: '1'  // Force each row into separate block
    });
    
    const http = require('http');
    const { decodeBlock } = require('./compression-node');
    
    await new Promise((resolve, reject) => {
      const req = http.request(url, { method: 'POST' }, (res) => {
        let buffer = Buffer.alloc(0);
        let blocksReceived = 0;
        
        res.on('data', chunk => {
          buffer = Buffer.concat([buffer, chunk]);
          
          // Process complete blocks from buffer
          while (buffer.length >= 25) {
            if (buffer.length < 17) break;
            
            const compressedSize = buffer.readUInt32LE(17);
            const blockSize = 16 + compressedSize;
            
            if (buffer.length < blockSize) break;
            
            const block = buffer.slice(0, blockSize);
            buffer = buffer.slice(blockSize);
            
            const decompressed = decodeBlock(block, true);
            blocksReceived++;
            console.log(`Block ${blocksReceived}: ${decompressed.length} bytes decompressed`);
            totalData += decompressed.toString();
          }
        });
        
        res.on('end', () => {
          console.log(`Total blocks received: ${blocksReceived}`);
          console.log(`Rows in response: ${totalData.split('\n').filter(l => l).length}`);
          resolve();
        });
        
        res.on('error', reject);
      });
      
      req.on('error', reject);
      req.end();
    });
    
    // Test 2: Default block size (should be fewer blocks)
    console.log('\n=== Test 2: Default block size ===');
    
    const url2 = require('./client-node').buildReqUrl('http://localhost:8123/', {
      session_id: sessionId,
      query: 'SELECT * FROM multi_block_test FORMAT JSONEachRow',
      compress: '1'
      // No max_block_size - use default
    });
    
    await new Promise((resolve, reject) => {
      const req = http.request(url2, { method: 'POST' }, (res) => {
        let buffer = Buffer.alloc(0);
        let blocksReceived = 0;
        
        res.on('data', chunk => {
          buffer = Buffer.concat([buffer, chunk]);
          
          while (buffer.length >= 25) {
            if (buffer.length < 17) break;
            
            const compressedSize = buffer.readUInt32LE(17);
            const blockSize = 16 + compressedSize;
            
            if (buffer.length < blockSize) break;
            
            const block = buffer.slice(0, blockSize);
            buffer = buffer.slice(blockSize);
            
            const decompressed = decodeBlock(block, true);
            blocksReceived++;
            console.log(`Block ${blocksReceived}: ${decompressed.length} bytes decompressed`);
          }
        });
        
        res.on('end', () => {
          console.log(`Total blocks received: ${blocksReceived}`);
          resolve();
        });
        
        res.on('error', reject);
      });
      
      req.on('error', reject);
      req.end();
    });
    
    // Test 3: Using our streaming execQuery with max_block_size
    console.log('\n=== Test 3: Using execQuery generator with max_block_size=1 ===');
    
    // We need to modify execQuery to accept options, or do it manually
    const http2 = require('http');
    const url3 = require('./client-node').buildReqUrl('http://localhost:8123/', {
      session_id: sessionId,
      query: 'SELECT * FROM multi_block_test FORMAT JSONEachRow',
      compress: '1',
      max_block_size: '1'
    });
    
    let streamChunks = 0;
    const stream = await new Promise((resolve, reject) => {
      const req = http2.request(url3, { method: 'POST' }, resolve);
      req.on('error', reject);
      req.end();
    });
    
    const { decodeBlock: decode } = require('./compression-node');
    let buf = Buffer.alloc(0);
    
    for await (const chunk of stream) {
      buf = Buffer.concat([buf, chunk]);
      
      while (buf.length >= 25) {
        if (buf.length < 17) break;
        
        const compressedSize = buf.readUInt32LE(17);
        const blockSize = 16 + compressedSize;
        
        if (buf.length < blockSize) break;
        
        buf = buf.slice(blockSize);
        streamChunks++;
      }
    }
    
    console.log(`Streamed ${streamChunks} compressed blocks`);
    
    // Clean up
    for await (const chunk of execQuery('DROP TABLE multi_block_test', sessionId)) {
      // consume
    }
    
    console.log('\nAll tests completed!');
    
  } catch (error) {
    console.error('Test failed:', error.message);
    process.exit(1);
  }
}

testMultiBlockResponse();