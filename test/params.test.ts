import { describe, it } from "node:test";
import assert from "node:assert";
import { extractParamTypes, serializeParams } from "../params.ts";

describe("extractParamTypes", () => {
  it("extracts simple types", () => {
    const types = extractParamTypes("SELECT {id: UInt64}, {name: String}");
    assert.strictEqual(types.get("id"), "UInt64");
    assert.strictEqual(types.get("name"), "String");
  });

  it("handles array types", () => {
    const types = extractParamTypes("SELECT {ids: Array(UInt64)}");
    assert.strictEqual(types.get("ids"), "Array(UInt64)");
  });

  it("handles nested types", () => {
    const types = extractParamTypes("SELECT {data: Array(Tuple(String, Array(Int32)))}");
    assert.strictEqual(types.get("data"), "Array(Tuple(String, Array(Int32)))");
  });

  it("handles map types", () => {
    const types = extractParamTypes("SELECT {m: Map(String, UInt32)}");
    assert.strictEqual(types.get("m"), "Map(String, UInt32)");
  });

  it("handles tuple types", () => {
    const types = extractParamTypes("SELECT {point: Tuple(Int32, Int32)}");
    assert.strictEqual(types.get("point"), "Tuple(Int32, Int32)");
  });

  it("handles nullable types", () => {
    const types = extractParamTypes("SELECT {val: Nullable(Int32)}");
    assert.strictEqual(types.get("val"), "Nullable(Int32)");
  });

  it("handles whitespace variations", () => {
    const types = extractParamTypes("SELECT { id :  UInt64 }, {name:String}");
    assert.strictEqual(types.get("id"), "UInt64");
    assert.strictEqual(types.get("name"), "String");
  });

  it("deduplicates same param used multiple times", () => {
    const types = extractParamTypes("SELECT {id: UInt64}, {id: UInt64} + 1");
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("id"), "UInt64");
  });

  it("throws on conflicting types for same param", () => {
    assert.throws(
      () => extractParamTypes("SELECT {id: UInt64}, {id: String}"),
      /conflicting types/i,
    );
  });

  it("skips single-quoted string literals", () => {
    const types = extractParamTypes("SELECT '{not: a_param}', {real: UInt64}");
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("skips single-quoted strings with doubled-quote escaping", () => {
    const types = extractParamTypes("SELECT 'it''s {not: a_param}', {real: UInt64}");
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("skips double-quoted string literals", () => {
    const types = extractParamTypes('SELECT "{not: a_param}", {real: UInt64}');
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("skips double-quoted strings with doubled-quote escaping", () => {
    const types = extractParamTypes('SELECT "it""s {not: a_param}", {real: UInt64}');
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("skips backtick-quoted identifiers", () => {
    const types = extractParamTypes("SELECT `{not: a_param}`, {real: UInt64}");
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("skips line comments", () => {
    const types = extractParamTypes(`
      SELECT {real: UInt64}
      -- {fake: String} this is commented out
      WHERE 1=1
    `);
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("skips block comments", () => {
    const types = extractParamTypes(`
      SELECT {real: UInt64}
      /* {fake: String} this is commented out */
      WHERE 1=1
    `);
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("does not treat {a: 1} as a param", () => {
    const types = extractParamTypes("SELECT {a: 1}, {real: UInt64}");
    assert.strictEqual(types.size, 1);
    assert.strictEqual(types.get("real"), "UInt64");
  });

  it("handles Enum types with parens in string values", () => {
    const types = extractParamTypes("SELECT {status: Enum8('active(' = 1, 'inactive)' = 2)}");
    assert.strictEqual(types.get("status"), "Enum8('active(' = 1, 'inactive)' = 2)");
  });

  it("handles empty query", () => {
    const types = extractParamTypes("SELECT 1");
    assert.strictEqual(types.size, 0);
  });
});

describe("serializeParams", () => {
  it("serializes integers", () => {
    const result = serializeParams("SELECT {id: UInt64}", { id: 42 });
    assert.strictEqual(result.id, "42");
  });

  it("serializes strings raw (top-level unquoted)", () => {
    const result = serializeParams("SELECT {name: String}", {
      name: "it's a test",
    });
    // Top-level strings are raw for HTTP params - no escaping
    assert.strictEqual(result.name, "it's a test");
  });

  it("serializes booleans", () => {
    const result = serializeParams("SELECT {flag: Bool}", { flag: true });
    assert.strictEqual(result.flag, "true");
  });

  it("serializes arrays", () => {
    const result = serializeParams("SELECT {ids: Array(UInt64)}", {
      ids: [1, 2, 3],
    });
    assert.strictEqual(result.ids, "[1, 2, 3]");
  });

  it("serializes tuples", () => {
    const result = serializeParams("SELECT {point: Tuple(Int32, String)}", {
      point: [10, "hello"],
    });
    assert.strictEqual(result.point, "(10, 'hello')");
  });

  it("serializes maps from objects", () => {
    const result = serializeParams("SELECT {m: Map(String, UInt32)}", {
      m: { a: 1, b: 2 },
    });
    assert.strictEqual(result.m, "{'a': 1, 'b': 2}");
  });

  it("serializes maps from Map objects", () => {
    const result = serializeParams("SELECT {m: Map(UInt32, String)}", {
      m: new Map([
        [1, "one"],
        [2, "two"],
      ]),
    });
    assert.strictEqual(result.m, "{1: 'one', 2: 'two'}");
  });

  it("serializes nested arrays", () => {
    const result = serializeParams("SELECT {matrix: Array(Array(Int32))}", {
      matrix: [
        [1, 2],
        [3, 4],
      ],
    });
    assert.strictEqual(result.matrix, "[[1, 2], [3, 4]]");
  });

  it("serializes array of tuples", () => {
    const result = serializeParams("SELECT {points: Array(Tuple(Int32, Int32))}", {
      points: [
        [0, 0],
        [1, 1],
      ],
    });
    assert.strictEqual(result.points, "[(0, 0), (1, 1)]");
  });

  it("serializes null values", () => {
    const result = serializeParams("SELECT {val: Nullable(Int32)}", {
      val: null,
    });
    assert.strictEqual(result.val, "NULL");
  });

  it("ignores extra params not in query", () => {
    const result = serializeParams("SELECT {id: UInt64}", {
      id: 42,
      unused: "foo",
    });
    assert.deepStrictEqual(Object.keys(result), ["id"]);
  });

  it("throws on missing required param", () => {
    assert.throws(() => serializeParams("SELECT {id: UInt64}", {}), /Missing parameter: id/);
  });

  it("serializes complex nested structures", () => {
    const result = serializeParams(
      "SELECT {data: Array(Tuple(String, Map(String, Array(Int32))))}",
      {
        data: [
          ["key1", { a: [1, 2], b: [3, 4] }],
          ["key2", { c: [5, 6] }],
        ],
      },
    );
    // Should produce: [('key1', {'a': [1, 2], 'b': [3, 4]}), ('key2', {'c': [5, 6]})]
    assert.ok(result.data.startsWith("[('key1'"));
  });

  it("handles string with special characters (top-level unquoted)", () => {
    const result = serializeParams("SELECT {s: String}", {
      s: "line1\nline2\ttab\\backslash'quote",
    });
    // Control chars escaped, but quotes NOT escaped (for HTTP params)
    assert.strictEqual(result.s, "line1\\nline2\\ttab\\\\backslash'quote");
  });

  it("serializes valid enum string value", () => {
    const result = serializeParams("SELECT {status: Enum8('active' = 1, 'inactive' = 2)}", {
      status: "active",
    });
    // HTTP params: unquoted value
    assert.strictEqual(result.status, "active");
  });

  it("throws on invalid enum string value", () => {
    assert.throws(
      () =>
        serializeParams("SELECT {status: Enum8('active' = 1, 'inactive' = 2)}", {
          status: "bogus",
        }),
      /Invalid enum value "bogus"/,
    );
  });

  it("throws on invalid enum numeric value", () => {
    assert.throws(
      () =>
        serializeParams("SELECT {status: Enum8('active' = 1, 'inactive' = 2)}", {
          status: 3,
        }),
      /Invalid enum value: 3/,
    );
  });
});
