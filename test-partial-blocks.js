const { encodeBlock, decodeBlock, Method } = require('./compression-node');
const { Readable } = require('stream');

// Test that our decompression handles partial blocks split across chunks
async function testPartialBlocks() {
  console.log('Testing partial block handling...\n');
  
  // Create test data
  const testData1 = Buffer.from('Hello, World! This is test block 1.\n');
  const testData2 = Buffer.from('This is test block 2 with more data.\n');
  const testData3 = Buffer.from('And finally, test block 3!\n');
  
  // Compress into blocks
  const block1 = encodeBlock(testData1, Method.LZ4);
  const block2 = encodeBlock(testData2, Method.LZ4);
  const block3 = encodeBlock(testData3, Method.LZ4);
  
  console.log(`Block 1 size: ${block1.length} bytes`);
  console.log(`Block 2 size: ${block2.length} bytes`);
  console.log(`Block 3 size: ${block3.length} bytes`);
  
  // Concatenate all blocks
  const allBlocks = Buffer.concat([block1, block2, block3]);
  console.log(`Total size: ${allBlocks.length} bytes\n`);
  
  // Test different split scenarios
  const scenarios = [
    { name: 'Single chunk', splits: [allBlocks.length] },
    { name: 'Split in block 1 header', splits: [10] },
    { name: 'Split in block 1 data', splits: [30] },
    { name: 'Split between blocks', splits: [block1.length] },
    { name: 'Multiple splits', splits: [10, 20, 15, 25, 30] },
    { name: 'Byte by byte', splits: Array(20).fill(1) },
    { name: 'Split at checksum boundary', splits: [16] },
    { name: 'Split at size field', splits: [20] },
  ];
  
  for (const scenario of scenarios) {
    console.log(`Testing: ${scenario.name}`);
    await testScenario(allBlocks, scenario.splits, [testData1, testData2, testData3]);
  }
  
  console.log('\nAll partial block tests passed!');
}

async function testScenario(data, splits, expectedBlocks) {
  // Create chunks based on split points
  const chunks = [];
  let offset = 0;
  
  for (const size of splits) {
    if (offset >= data.length) break;
    const chunkSize = Math.min(size, data.length - offset);
    chunks.push(data.slice(offset, offset + chunkSize));
    offset += chunkSize;
  }
  
  // Add remaining data if any
  if (offset < data.length) {
    chunks.push(data.slice(offset));
  }
  
  console.log(`  Chunks: ${chunks.map(c => c.length).join(', ')} bytes`);
  
  // Simulate our streaming decompression logic
  const decompressed = await processChunks(chunks);
  
  // Verify results
  const expected = Buffer.concat(expectedBlocks).toString();
  if (decompressed !== expected) {
    console.error(`  FAILED: Expected ${expected.length} bytes, got ${decompressed.length}`);
    console.error(`  Expected: ${expected}`);
    console.error(`  Got: ${decompressed}`);
    process.exit(1);
  }
  
  console.log(`  ✓ Correctly decompressed ${decompressed.length} bytes\n`);
}

async function processChunks(chunks) {
  // This simulates our execQuery decompression logic
  let buffer = Buffer.alloc(0);
  let result = '';
  
  for (const chunk of chunks) {
    buffer = Buffer.concat([buffer, chunk]);
    
    // Process complete blocks from buffer
    while (buffer.length >= 25) {
      // Check if we have enough data for a complete block
      if (buffer.length < 17) break;
      
      // Read compressed size to know block size
      const compressedSize = buffer.readUInt32LE(17);
      const blockSize = 16 + compressedSize;
      
      if (buffer.length < blockSize) break;
      
      // Extract and decompress block
      const block = buffer.slice(0, blockSize);
      buffer = buffer.slice(blockSize);
      
      try {
        const decompressed = decodeBlock(block, true);
        result += decompressed.toString();
      } catch (err) {
        throw new Error(`Block decompression failed: ${err.message}`);
      }
    }
  }
  
  // Process any remaining complete blocks
  while (buffer.length >= 25) {
    if (buffer.length < 17) break;
    const compressedSize = buffer.readUInt32LE(17);
    const blockSize = 16 + compressedSize;
    if (buffer.length < blockSize) break;
    
    const block = buffer.slice(0, blockSize);
    buffer = buffer.slice(blockSize);
    
    const decompressed = decodeBlock(block, true);
    result += decompressed.toString();
  }
  
  // Check for leftover incomplete data
  if (buffer.length > 0) {
    console.error(`  WARNING: ${buffer.length} bytes remaining in buffer (incomplete block)`);
  }
  
  return result;
}

// Also test with async generator (like our real implementation)
async function testWithGenerator() {
  console.log('\nTesting with async generator (like execQuery)...\n');
  
  const testData = Buffer.from('Testing async generator decompression!');
  const block = encodeBlock(testData, Method.LZ4);
  
  // Split block into small chunks
  const chunkSize = 5;
  const chunks = [];
  for (let i = 0; i < block.length; i += chunkSize) {
    chunks.push(block.slice(i, Math.min(i + chunkSize, block.length)));
  }
  
  // Create async generator that yields chunks
  async function* chunkGenerator() {
    for (const chunk of chunks) {
      yield chunk;
      // Simulate network delay
      await new Promise(resolve => setTimeout(resolve, 1));
    }
  }
  
  // Process using generator
  let buffer = Buffer.alloc(0);
  let result = '';
  
  for await (const chunk of chunkGenerator()) {
    buffer = Buffer.concat([buffer, chunk]);
    
    while (buffer.length >= 25) {
      if (buffer.length < 17) break;
      
      const compressedSize = buffer.readUInt32LE(17);
      const blockSize = 16 + compressedSize;
      
      if (buffer.length < blockSize) break;
      
      const block = buffer.slice(0, blockSize);
      buffer = buffer.slice(blockSize);
      
      const decompressed = decodeBlock(block, true);
      result += decompressed.toString();
    }
  }
  
  console.log(`Decompressed: "${result}"`);
  if (result === testData.toString()) {
    console.log('✓ Async generator test passed!');
  } else {
    console.error('FAILED: Async generator test');
    process.exit(1);
  }
}

async function main() {
  try {
    await testPartialBlocks();
    await testWithGenerator();
    console.log('\n✅ All tests passed! Our implementation correctly handles partial blocks.');
  } catch (error) {
    console.error('Test failed:', error);
    process.exit(1);
  }
}

main();