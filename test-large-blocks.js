const http = require('http');
const { decodeBlock } = require('./compression-node');

async function testLargeQuery() {
  console.log('Testing large query to force multiple compressed blocks...\n');
  
  const sessionId = Date.now().toString();
  
  // Test different scenarios
  const queries = [
    {
      name: 'Small query (100 rows)',
      query: 'SELECT * FROM system.numbers LIMIT 100',
      max_block_size: null
    },
    {
      name: 'Medium query (100K rows)',
      query: 'SELECT * FROM system.numbers LIMIT 100000',
      max_block_size: null
    },
    {
      name: 'Large query (1M rows)',
      query: 'SELECT * FROM system.numbers LIMIT 1000000',
      max_block_size: null
    },
    {
      name: 'Large query with max_block_size=10000',
      query: 'SELECT * FROM system.numbers LIMIT 1000000',
      max_block_size: '10000'
    },
    {
      name: 'Large query with max_block_size=65505',
      query: 'SELECT * FROM system.numbers LIMIT 1000000', 
      max_block_size: '65505'  // Default value
    },
    {
      name: 'Huge query (10M rows) - CAREFUL!',
      query: 'SELECT * FROM system.numbers LIMIT 10000000',
      max_block_size: null
    }
  ];
  
  for (const test of queries) {
    console.log(`\n=== ${test.name} ===`);
    console.log(`Query: ${test.query}`);
    if (test.max_block_size) {
      console.log(`max_block_size: ${test.max_block_size}`);
    }
    
    try {
      await runCompressedQuery(test.query, sessionId, test.max_block_size);
    } catch (error) {
      console.error(`Error: ${error.message}`);
    }
    
    // Don't run the huge query by default
    if (test.name.includes('CAREFUL')) {
      console.log('Skipping huge query - uncomment to run');
      break;
    }
  }
}

async function runCompressedQuery(query, sessionId, maxBlockSize) {
  const url = new URL('http://localhost:8123/');
  url.searchParams.append('session_id', sessionId);
  url.searchParams.append('query', query);
  url.searchParams.append('compress', '1');  // Request compressed response
  url.searchParams.append('http_native_compression_disable_checksumming_on_decompress', '1');
  
  if (maxBlockSize) {
    url.searchParams.append('max_block_size', maxBlockSize);
  }
  
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    let blocksReceived = 0;
    let totalCompressedBytes = 0;
    let totalDecompressedBytes = 0;
    let rowsReceived = 0;
    let buffer = Buffer.alloc(0);
    let blockSizes = [];
    
    const req = http.request(url, { method: 'POST' }, (res) => {
      if (res.statusCode !== 200) {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
          reject(new Error(`Query failed: ${res.statusCode} - ${Buffer.concat(chunks).toString()}`));
        });
        return;
      }
      
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
          
          try {
            const decompressed = decodeBlock(block, true);
            blocksReceived++;
            totalCompressedBytes += blockSize;
            totalDecompressedBytes += decompressed.length;
            
            // Count rows (each number is on its own line)
            const blockRows = (decompressed.toString().match(/\n/g) || []).length;
            rowsReceived += blockRows;
            blockSizes.push({
              compressed: blockSize,
              decompressed: decompressed.length,
              rows: blockRows
            });
            
            // Log progress for large queries
            if (blocksReceived % 10 === 0) {
              console.log(`  Received ${blocksReceived} blocks, ${rowsReceived} rows...`);
            }
          } catch (err) {
            console.error(`Failed to decompress block ${blocksReceived + 1}: ${err.message}`);
          }
        }
      });
      
      res.on('end', () => {
        const elapsed = Date.now() - startTime;
        
        console.log(`\nResults:`);
        console.log(`  Blocks received: ${blocksReceived}`);
        console.log(`  Total rows: ${rowsReceived}`);
        console.log(`  Compressed size: ${(totalCompressedBytes / 1024).toFixed(2)} KB`);
        console.log(`  Decompressed size: ${(totalDecompressedBytes / 1024).toFixed(2)} KB`);
        console.log(`  Compression ratio: ${(totalDecompressedBytes / totalCompressedBytes).toFixed(2)}x`);
        console.log(`  Time: ${elapsed}ms`);
        
        if (blocksReceived > 1) {
          console.log(`\n  Block size distribution:`);
          const first5 = blockSizes.slice(0, 5);
          const last5 = blockSizes.slice(-5);
          
          first5.forEach((b, i) => {
            console.log(`    Block ${i + 1}: ${b.compressed} bytes compressed, ${b.decompressed} bytes decompressed, ${b.rows} rows`);
          });
          
          if (blockSizes.length > 10) {
            console.log(`    ... ${blockSizes.length - 10} more blocks ...`);
          }
          
          if (blockSizes.length > 5) {
            last5.forEach((b, i) => {
              const blockNum = blocksReceived - (5 - i);
              console.log(`    Block ${blockNum}: ${b.compressed} bytes compressed, ${b.decompressed} bytes decompressed, ${b.rows} rows`);
            });
          }
        }
        
        if (buffer.length > 0) {
          console.log(`  WARNING: ${buffer.length} bytes remaining in buffer`);
        }
        
        resolve();
      });
      
      res.on('error', reject);
    });
    
    req.on('error', reject);
    req.end();
  });
}

testLargeQuery().catch(console.error);