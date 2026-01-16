// Test importing from the bundled dist directly
// This simulates what happens when someone npm installs the package

console.log("=== Testing full build (dist/chttp.js) ===\n");

const full = await import("../dist/chttp.js");

try {
  console.log("Calling init()...");
  await full.init();
  console.log("init() succeeded!\n");

  // Test compression by encoding some data
  console.log("Testing compression via encodeBlock...");
  const testData = new TextEncoder().encode("Hello, World! This is a test string for compression.");

  // LZ4 compression
  const lz4Compressed = full.encodeBlock(testData, 0x82); // Method.LZ4
  console.log(`LZ4: ${testData.length} bytes -> ${lz4Compressed.length} bytes`);

  // ZSTD compression
  const zstdCompressed = full.encodeBlock(testData, 0x90); // Method.ZSTD
  console.log(`ZSTD: ${testData.length} bytes -> ${zstdCompressed.length} bytes`);

  console.log("\nFull build PASSED!\n");
} catch (err) {
  console.error("ERROR:", err.message);
  console.error("Stack:", err.stack);
  process.exit(1);
}

console.log("=== Testing LZ4-only build (dist/chttp-lz4.js) ===\n");

const lz4Only = await import("../dist/chttp-lz4.js");

try {
  console.log("Calling init()...");
  await lz4Only.init();
  console.log("init() succeeded!\n");

  console.log("Testing LZ4 compression...");
  const testData = new TextEncoder().encode("Hello, World! This is a test string for compression.");
  const compressed = lz4Only.encodeBlock(testData, 0x82);
  console.log(`LZ4: ${testData.length} bytes -> ${compressed.length} bytes`);

  console.log("\nLZ4-only build PASSED!\n");
} catch (err) {
  console.error("ERROR:", err.message);
  console.error("Stack:", err.stack);
  process.exit(1);
}

console.log("=== Testing RecordBatch instanceof consistency ===\n");

// This test catches dual-package hazards where different import paths
// would create separate class instances, causing instanceof to fail
const native = await import("../dist/native.js");

try {
  // Create a batch using the dist native module
  const batch = native.batchFromCols({
    id: native.getCodec("Int64").fromValues([10n, 20n]),
    value: native.getCodec("String").fromValues(["val_1", "val_2"]),
  });

  // Verify instanceof works
  if (!(batch instanceof native.RecordBatch)) {
    throw new Error("batch should be instanceof RecordBatch");
  }

  // Verify data is correct
  if (batch.rowCount !== 2) {
    throw new Error(`Expected rowCount=2, got ${batch.rowCount}`);
  }

  // Test encode/decode roundtrip
  const encoded = native.encodeNative(batch);
  const decoded = native.decodeNativeBlock(encoded, 0);

  if (decoded.rowCount !== 2) {
    throw new Error(`Decoded rowCount should be 2, got ${decoded.rowCount}`);
  }

  console.log("RecordBatch instanceof: PASSED");
  console.log(`  Created batch with ${batch.rowCount} rows`);
  console.log(`  Encoded to ${encoded.length} bytes`);
  console.log(`  Decoded back to ${decoded.rowCount} rows`);
  console.log("\nRecordBatch consistency PASSED!\n");
} catch (err) {
  console.error("ERROR:", err.message);
  console.error("Stack:", err.stack);
  process.exit(1);
}

console.log("=== All bundle tests PASSED! ===");
