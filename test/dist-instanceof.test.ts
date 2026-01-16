/**
 * Test that RecordBatch from dist imports works with instanceof checks.
 * This catches dual-package hazard issues where different import paths
 * could create separate class instances.
 */
import { describe, test } from "node:test";
import assert from "node:assert/strict";

describe("RecordBatch instanceof across import paths", () => {
  test("dist RecordBatch works with tcp_client instanceof check", async () => {
    // Import from the package path (as users would)
    const { batchFromCols, getCodec, RecordBatch } = await import("@maxjustus/chttp/native");

    // Create a batch the way a user would
    const batch = batchFromCols({
      id: getCodec("Int64").fromValues([10n, 20n]),
      value: getCodec("String").fromValues(["val_1", "val_2"]),
    });

    // Verify it's recognized as a RecordBatch
    assert.ok(batch instanceof RecordBatch, "batch should be instanceof RecordBatch");
    assert.equal(batch.rowCount, 2, "batch should have 2 rows");
  });

  test("RecordBatch class identity is consistent", async () => {
    // Import RecordBatch from both the package path and source path
    const { RecordBatch: PackageRecordBatch } = await import("@maxjustus/chttp/native");
    const { RecordBatch: SourceRecordBatch } = await import("../native/index.ts");

    // They should be the exact same class (same identity)
    // This will fail if there's a dual-package hazard
    assert.strictEqual(
      PackageRecordBatch,
      SourceRecordBatch,
      "RecordBatch from package path and source path should be the same class",
    );
  });

  test("batchFromCols result passes instanceof from different import", async () => {
    // Simulate what happens when user creates batch with package import
    // but it's checked by tcp_client which also imports from package
    const { batchFromCols, getCodec } = await import("@maxjustus/chttp/native");
    const { RecordBatch } = await import("@maxjustus/chttp/native");

    const batch = batchFromCols({
      x: getCodec("UInt32").fromValues([1, 2, 3]),
    });

    // This is the check tcp_client does
    const isRecordBatch = batch instanceof RecordBatch;
    assert.ok(isRecordBatch, "batch should pass instanceof RecordBatch check");
  });
});
