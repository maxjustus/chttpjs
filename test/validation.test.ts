import { describe, it } from "node:test";
import assert from "node:assert";
import {
  tableFromArrays,
  tableFromRows,
  tableFromCols,
  Table,
  makeBuilder,
} from "../formats/native/index.ts";
import {
  encodeRowBinary,
  streamEncodeRowBinary,
} from "../formats/rowbinary.ts";

describe("Input Validation", () => {
  describe("tableFromArrays", () => {
    it("throws on missing column in data", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const data = { id: [1, 2, 3] }; // missing 'name'

      assert.throws(
        () => tableFromArrays(schema, data as any),
        /Missing column 'name' in data/
      );
    });

    it("throws on column length mismatch", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const data = {
        id: [1, 2, 3],
        name: ["a", "b"], // only 2 elements
      };

      assert.throws(
        () => tableFromArrays(schema, data),
        /Column length mismatch.*'id' has 3 rows.*'name' has 2 rows/
      );
    });

    it("accepts valid data", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const data = {
        id: [1, 2, 3],
        name: ["a", "b", "c"],
      };

      const table = tableFromArrays(schema, data);
      assert.strictEqual(table.rowCount, 3);
    });
  });

  describe("tableFromRows", () => {
    it("throws on row length mismatch", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const rows = [
        [1, "a"],
        [2], // missing second value
        [3, "c"],
      ];

      assert.throws(
        () => tableFromRows(schema, rows as any),
        /Row 1 has 1 values but schema expects 2 columns/
      );
    });

    it("throws on non-array row", () => {
      const schema = [{ name: "id", type: "UInt32" }];
      const rows = [[1], "not an array", [3]];

      assert.throws(
        () => tableFromRows(schema, rows as any),
        /Row 1 is not an array/
      );
    });

    it("throws on non-array rows argument", () => {
      const schema = [{ name: "id", type: "UInt32" }];

      assert.throws(
        () => tableFromRows(schema, "not an array" as any),
        /rows must be an array/
      );
    });

    it("accepts valid data", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const rows = [
        [1, "a"],
        [2, "b"],
      ];

      const table = tableFromRows(schema, rows);
      assert.strictEqual(table.rowCount, 2);
    });

    it("accepts empty rows", () => {
      const schema = [{ name: "id", type: "UInt32" }];
      const table = tableFromRows(schema, []);
      assert.strictEqual(table.rowCount, 0);
    });
  });

  describe("tableFromCols", () => {
    it("throws on column length mismatch", () => {
      const idCol = makeBuilder("UInt32").append(1).append(2).append(3).finish();
      const nameCol = makeBuilder("String").append("a").append("b").finish();

      assert.throws(
        () => tableFromCols({ id: idCol, name: nameCol }),
        /Column length mismatch.*'id' has 3 rows.*'name' has 2 rows/
      );
    });

    it("accepts valid columns", () => {
      const idCol = makeBuilder("UInt32").append(1).append(2).finish();
      const nameCol = makeBuilder("String").append("a").append("b").finish();

      const table = tableFromCols({ id: idCol, name: nameCol });
      assert.strictEqual(table.rowCount, 2);
    });
  });

  describe("Table.fromColumnar", () => {
    it("throws on column length mismatch", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];

      assert.throws(
        () => Table.fromColumnar(schema, [[1, 2, 3], ["a", "b"]]),
        /Column length mismatch.*'id' has 3 rows.*'name' has 2 rows/
      );
    });

    it("accepts valid data", () => {
      const schema = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];

      const table = Table.fromColumnar(schema, [[1, 2], ["a", "b"]]);
      assert.strictEqual(table.rowCount, 2);
    });
  });

  describe("encodeRowBinary", () => {
    it("throws on row length mismatch", () => {
      const columns = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const rows = [
        [1, "a"],
        [2], // missing second value
      ];

      assert.throws(
        () => encodeRowBinary(columns, rows as any),
        /Row 1 has 1 values but schema expects 2 columns/
      );
    });

    it("throws on non-array row", () => {
      const columns = [{ name: "id", type: "UInt32" }];
      const rows = [[1], "not an array"];

      assert.throws(
        () => encodeRowBinary(columns, rows as any),
        /Row 1 is not an array/
      );
    });

    it("accepts valid data", () => {
      const columns = [{ name: "id", type: "UInt32" }];
      const rows = [[1], [2], [3]];

      const result = encodeRowBinary(columns, rows);
      assert(result instanceof Uint8Array);
    });
  });

  describe("streamEncodeRowBinary", () => {
    it("throws on non-array columns", async () => {
      const gen = streamEncodeRowBinary("not an array" as any, []);

      await assert.rejects(
        async () => {
          for await (const _ of gen) {}
        },
        /columns must be an array/
      );
    });

    it("throws on row length mismatch", async () => {
      const columns = [
        { name: "id", type: "UInt32" },
        { name: "name", type: "String" },
      ];
      const rows = [[1, "a"], [2]]; // second row missing value

      const gen = streamEncodeRowBinary(columns, rows as any);

      await assert.rejects(
        async () => {
          for await (const _ of gen) {}
        },
        /Row 1 has 1 values but schema expects 2 columns/
      );
    });

    it("accepts valid data", async () => {
      const columns = [{ name: "id", type: "UInt32" }];
      const rows = [[1], [2], [3]];

      const chunks: Uint8Array[] = [];
      for await (const chunk of streamEncodeRowBinary(columns, rows)) {
        chunks.push(chunk);
      }
      assert(chunks.length > 0);
    });
  });
});
