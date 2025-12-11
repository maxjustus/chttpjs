import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  encodeRowBinaryWithNames,
  decodeRowBinaryWithNames,
  type ColumnDef,
} from "../rowbinary.ts";

// Helper to read LEB128
function readLEB128(bytes: Uint8Array, offset: number): [number, number] {
  let value = 0;
  let shift = 0;
  let pos = offset;
  while (true) {
    const byte = bytes[pos++];
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
    shift += 7;
  }
  return [value, pos];
}

// Helper to read string from buffer
function readString(bytes: Uint8Array, offset: number): [string, number] {
  const [len, pos] = readLEB128(bytes, offset);
  const str = new TextDecoder().decode(bytes.slice(pos, pos + len));
  return [str, pos + len];
}

describe("encodeRowBinaryWithNames", () => {
  it("encodes header with column names", () => {
    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "name", type: "String" },
    ];
    const result = encodeRowBinaryWithNames(columns, []);

    // Read column count
    const [count, pos1] = readLEB128(result, 0);
    assert.strictEqual(count, 2);

    // Read column names
    const [name1, pos2] = readString(result, pos1);
    assert.strictEqual(name1, "id");
    const [name2, pos3] = readString(result, pos2);
    assert.strictEqual(name2, "name");

    // Should be end of buffer (no rows)
    assert.strictEqual(pos3, result.length);
  });

  it("encodes Int8/Int16/Int32/Int64", () => {
    const columns: ColumnDef[] = [
      { name: "a", type: "Int8" },
      { name: "b", type: "Int16" },
      { name: "c", type: "Int32" },
      { name: "d", type: "Int64" },
    ];
    const result = encodeRowBinaryWithNames(columns, [
      [-128, -32768, -2147483648, -9223372036854775808n],
    ]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    assert.strictEqual(view.getInt8(offset), -128);
    assert.strictEqual(view.getInt16(offset + 1, true), -32768);
    assert.strictEqual(view.getInt32(offset + 3, true), -2147483648);
    assert.strictEqual(view.getBigInt64(offset + 7, true), -9223372036854775808n);
  });

  it("encodes UInt8/UInt16/UInt32/UInt64", () => {
    const columns: ColumnDef[] = [
      { name: "a", type: "UInt8" },
      { name: "b", type: "UInt16" },
      { name: "c", type: "UInt32" },
      { name: "d", type: "UInt64" },
    ];
    const result = encodeRowBinaryWithNames(columns, [
      [255, 65535, 4294967295, 18446744073709551615n],
    ]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    assert.strictEqual(view.getUint8(offset), 255);
    assert.strictEqual(view.getUint16(offset + 1, true), 65535);
    assert.strictEqual(view.getUint32(offset + 3, true), 4294967295);
    assert.strictEqual(view.getBigUint64(offset + 7, true), 18446744073709551615n);
  });

  it("encodes Float32/Float64", () => {
    const columns: ColumnDef[] = [
      { name: "a", type: "Float32" },
      { name: "b", type: "Float64" },
    ];
    const result = encodeRowBinaryWithNames(columns, [[3.14, 2.718281828]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    assert.ok(Math.abs(view.getFloat32(offset, true) - 3.14) < 0.00001);
    assert.ok(Math.abs(view.getFloat64(offset + 4, true) - 2.718281828) < 0.000000001);
  });

  it("encodes String", () => {
    const columns: ColumnDef[] = [{ name: "s", type: "String" }];
    const result = encodeRowBinaryWithNames(columns, [["hello"], ["world"]]);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const [str1, pos2] = readString(result, offset);
    assert.strictEqual(str1, "hello");
    const [str2, _] = readString(result, pos2);
    assert.strictEqual(str2, "world");
  });

  it("encodes Bool", () => {
    const columns: ColumnDef[] = [{ name: "b", type: "Bool" }];
    const result = encodeRowBinaryWithNames(columns, [[true], [false]]);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    assert.strictEqual(result[offset], 1);
    assert.strictEqual(result[offset + 1], 0);
  });

  it("encodes Date", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Date" }];
    const date = new Date("2024-01-15");
    const result = encodeRowBinaryWithNames(columns, [[date]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const days = view.getUint16(offset, true);
    const expectedDays = Math.floor(date.getTime() / 86400000);
    assert.strictEqual(days, expectedDays);
  });

  it("encodes DateTime", () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime" }];
    const date = new Date("2024-01-15T12:30:45Z");
    const result = encodeRowBinaryWithNames(columns, [[date]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const seconds = view.getUint32(offset, true);
    const expectedSeconds = Math.floor(date.getTime() / 1000);
    assert.strictEqual(seconds, expectedSeconds);
  });

  it("encodes Nullable with null value", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Nullable(Int32)" }];
    const result = encodeRowBinaryWithNames(columns, [[null]]);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    assert.strictEqual(result[offset], 1); // null marker
    assert.strictEqual(result.length, offset + 1); // just the null marker
  });

  it("encodes Nullable with non-null value", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Nullable(Int32)" }];
    const result = encodeRowBinaryWithNames(columns, [[42]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    assert.strictEqual(result[offset], 0); // not null marker
    assert.strictEqual(view.getInt32(offset + 1, true), 42);
  });

  it("encodes Array with typed array (Int32Array)", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    const arr = new Int32Array([1, 2, 3, 4, 5]);
    const result = encodeRowBinaryWithNames(columns, [[arr]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const [len, dataStart] = readLEB128(result, offset);
    assert.strictEqual(len, 5);

    for (let i = 0; i < 5; i++) {
      assert.strictEqual(view.getInt32(dataStart + i * 4, true), i + 1);
    }
  });

  it("encodes Array with JS number array", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Float64)" }];
    const arr = [1.1, 2.2, 3.3];
    const result = encodeRowBinaryWithNames(columns, [[arr]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const [len, dataStart] = readLEB128(result, offset);
    assert.strictEqual(len, 3);

    assert.ok(Math.abs(view.getFloat64(dataStart, true) - 1.1) < 0.0001);
    assert.ok(Math.abs(view.getFloat64(dataStart + 8, true) - 2.2) < 0.0001);
    assert.ok(Math.abs(view.getFloat64(dataStart + 16, true) - 3.3) < 0.0001);
  });

  it("encodes Array with JS string array", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(String)" }];
    const arr = ["foo", "bar"];
    const result = encodeRowBinaryWithNames(columns, [[arr]]);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const [len, dataStart] = readLEB128(result, offset);
    assert.strictEqual(len, 2);

    const [str1, pos2] = readString(result, dataStart);
    assert.strictEqual(str1, "foo");
    const [str2, _] = readString(result, pos2);
    assert.strictEqual(str2, "bar");
  });

  it("encodes multiple rows", () => {
    const columns: ColumnDef[] = [
      { name: "id", type: "Int32" },
      { name: "name", type: "String" },
    ];
    const rows = [
      [1, "alice"],
      [2, "bob"],
      [3, "charlie"],
    ];
    const result = encodeRowBinaryWithNames(columns, rows);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Row 1
    assert.strictEqual(view.getInt32(offset, true), 1);
    offset += 4;
    let [name, pos] = readString(result, offset);
    assert.strictEqual(name, "alice");
    offset = pos;

    // Row 2
    assert.strictEqual(view.getInt32(offset, true), 2);
    offset += 4;
    [name, pos] = readString(result, offset);
    assert.strictEqual(name, "bob");
    offset = pos;

    // Row 3
    assert.strictEqual(view.getInt32(offset, true), 3);
    offset += 4;
    [name, pos] = readString(result, offset);
    assert.strictEqual(name, "charlie");
  });

  it("encodes Uint8Array as String (raw bytes)", () => {
    const columns: ColumnDef[] = [{ name: "data", type: "String" }];
    const bytes = new Uint8Array([0x01, 0x02, 0x03, 0xff]);
    const result = encodeRowBinaryWithNames(columns, [[bytes]]);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    const [len, dataStart] = readLEB128(result, offset);
    assert.strictEqual(len, 4);
    assert.strictEqual(result[dataStart], 0x01);
    assert.strictEqual(result[dataStart + 1], 0x02);
    assert.strictEqual(result[dataStart + 2], 0x03);
    assert.strictEqual(result[dataStart + 3], 0xff);
  });

  it("encodes Tuple", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, String)" }];
    const result = encodeRowBinaryWithNames(columns, [[[42, "hello"]]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Int32 followed by String
    assert.strictEqual(view.getInt32(offset, true), 42);
    offset += 4;
    const [str, _] = readString(result, offset);
    assert.strictEqual(str, "hello");
  });

  it("encodes nested Tuple", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, Tuple(String, Float64))" }];
    const result = encodeRowBinaryWithNames(columns, [[[42, ["hello", 3.14]]]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Int32
    assert.strictEqual(view.getInt32(offset, true), 42);
    offset += 4;
    // Nested tuple: String, Float64
    const [str, pos2] = readString(result, offset);
    assert.strictEqual(str, "hello");
    offset = pos2;
    assert.ok(Math.abs(view.getFloat64(offset, true) - 3.14) < 0.0001);
  });

  it("encodes Tuple with Array element", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(String, Array(Int32))" }];
    const result = encodeRowBinaryWithNames(columns, [[["test", [1, 2, 3]]]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // String
    const [str, pos2] = readString(result, offset);
    assert.strictEqual(str, "test");
    offset = pos2;

    // Array(Int32) - LEB128 count then values
    const [arrLen, dataStart] = readLEB128(result, offset);
    assert.strictEqual(arrLen, 3);
    assert.strictEqual(view.getInt32(dataStart, true), 1);
    assert.strictEqual(view.getInt32(dataStart + 4, true), 2);
    assert.strictEqual(view.getInt32(dataStart + 8, true), 3);
  });

  it("encodes Map from object", () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    const result = encodeRowBinaryWithNames(columns, [[{ foo: 1, bar: 2 }]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Map: LEB128 count, then key-value pairs
    const [mapLen, dataStart] = readLEB128(result, offset);
    assert.strictEqual(mapLen, 2);
    offset = dataStart;

    // First pair: "foo" -> 1
    const [key1, pos2] = readString(result, offset);
    assert.strictEqual(key1, "foo");
    offset = pos2;
    assert.strictEqual(view.getInt32(offset, true), 1);
    offset += 4;

    // Second pair: "bar" -> 2
    const [key2, pos3] = readString(result, offset);
    assert.strictEqual(key2, "bar");
    offset = pos3;
    assert.strictEqual(view.getInt32(offset, true), 2);
  });

  it("encodes Map from JS Map", () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    const map = new Map([["foo", 1], ["bar", 2]]);
    const result = encodeRowBinaryWithNames(columns, [[map]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Map: LEB128 count
    const [mapLen, dataStart] = readLEB128(result, offset);
    assert.strictEqual(mapLen, 2);
    offset = dataStart;

    // First pair
    const [key1, pos2] = readString(result, offset);
    assert.strictEqual(key1, "foo");
    offset = pos2;
    assert.strictEqual(view.getInt32(offset, true), 1);
    offset += 4;

    // Second pair
    const [key2, pos3] = readString(result, offset);
    assert.strictEqual(key2, "bar");
    offset = pos3;
    assert.strictEqual(view.getInt32(offset, true), 2);
  });

  it("encodes Map with Array values", () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Array(Int32))" }];
    const result = encodeRowBinaryWithNames(columns, [[{ a: [1, 2], b: [3, 4, 5] }]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Map: 2 entries
    const [mapLen, dataStart] = readLEB128(result, offset);
    assert.strictEqual(mapLen, 2);
    offset = dataStart;

    // First pair: "a" -> [1, 2]
    const [key1, pos2] = readString(result, offset);
    assert.strictEqual(key1, "a");
    offset = pos2;
    const [arr1Len, arr1Start] = readLEB128(result, offset);
    assert.strictEqual(arr1Len, 2);
    assert.strictEqual(view.getInt32(arr1Start, true), 1);
    assert.strictEqual(view.getInt32(arr1Start + 4, true), 2);
    offset = arr1Start + 8;

    // Second pair: "b" -> [3, 4, 5]
    const [key2, pos3] = readString(result, offset);
    assert.strictEqual(key2, "b");
    offset = pos3;
    const [arr2Len, arr2Start] = readLEB128(result, offset);
    assert.strictEqual(arr2Len, 3);
    assert.strictEqual(view.getInt32(arr2Start, true), 3);
    assert.strictEqual(view.getInt32(arr2Start + 4, true), 4);
    assert.strictEqual(view.getInt32(arr2Start + 8, true), 5);
  });

  it("encodes Array of Tuples", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Tuple(String, Int32))" }];
    const result = encodeRowBinaryWithNames(columns, [[[["a", 1], ["b", 2]]]]);

    const view = new DataView(result.buffer, result.byteOffset, result.byteLength);

    // Skip header
    let offset = 0;
    const [count, pos1] = readLEB128(result, offset);
    offset = pos1;
    for (let i = 0; i < count; i++) {
      const [_, pos] = readString(result, offset);
      offset = pos;
    }

    // Array: 2 elements
    const [arrLen, dataStart] = readLEB128(result, offset);
    assert.strictEqual(arrLen, 2);
    offset = dataStart;

    // First tuple: ("a", 1)
    const [str1, pos2] = readString(result, offset);
    assert.strictEqual(str1, "a");
    offset = pos2;
    assert.strictEqual(view.getInt32(offset, true), 1);
    offset += 4;

    // Second tuple: ("b", 2)
    const [str2, pos3] = readString(result, offset);
    assert.strictEqual(str2, "b");
    offset = pos3;
    assert.strictEqual(view.getInt32(offset, true), 2);
  });
});

describe("decodeRowBinaryWithNames", () => {
  it("decodes scalars (round-trip)", () => {
    const columns: ColumnDef[] = [
      { name: "a", type: "Int32" },
      { name: "b", type: "String" },
      { name: "c", type: "Float64" },
    ];
    const rows = [
      [42, "hello", 3.14],
      [-100, "world", 2.71],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.columns.length, 3);
    assert.strictEqual(decoded.columns[0].name, "a");
    assert.strictEqual(decoded.columns[1].name, "b");
    assert.strictEqual(decoded.columns[2].name, "c");

    assert.strictEqual(decoded.rows.length, 2);
    assert.strictEqual(decoded.rows[0][0], 42);
    assert.strictEqual(decoded.rows[0][1], "hello");
    assert.ok(Math.abs((decoded.rows[0][2] as number) - 3.14) < 0.0001);
    assert.strictEqual(decoded.rows[1][0], -100);
    assert.strictEqual(decoded.rows[1][1], "world");
  });

  it("decodes Int64/UInt64 as BigInt", () => {
    const columns: ColumnDef[] = [
      { name: "signed", type: "Int64" },
      { name: "unsigned", type: "UInt64" },
    ];
    const rows = [[-9223372036854775808n, 18446744073709551615n]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], -9223372036854775808n);
    assert.strictEqual(decoded.rows[0][1], 18446744073709551615n);
  });

  it("decodes Bool", () => {
    const columns: ColumnDef[] = [{ name: "b", type: "Bool" }];
    const rows = [[true], [false]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], true);
    assert.strictEqual(decoded.rows[1][0], false);
  });

  it("decodes Date and DateTime", () => {
    const columns: ColumnDef[] = [
      { name: "d", type: "Date" },
      { name: "dt", type: "DateTime" },
    ];
    const date = new Date("2024-01-15");
    const datetime = new Date("2024-01-15T12:30:45Z");
    const rows = [[date, datetime]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    // Date precision is days
    const decodedDate = decoded.rows[0][0] as Date;
    assert.strictEqual(
      Math.floor(decodedDate.getTime() / 86400000),
      Math.floor(date.getTime() / 86400000)
    );

    // DateTime precision is seconds
    const decodedDateTime = decoded.rows[0][1] as Date;
    assert.strictEqual(
      Math.floor(decodedDateTime.getTime() / 1000),
      Math.floor(datetime.getTime() / 1000)
    );
  });

  it("decodes Nullable", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Nullable(Int32)" }];
    const rows = [[42], [null], [100]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], 42);
    assert.strictEqual(decoded.rows[1][0], null);
    assert.strictEqual(decoded.rows[2][0], 100);
  });

  it("decodes Array", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Int32)" }];
    const rows = [[[1, 2, 3]], [[]], [[100]]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], [1, 2, 3]);
    assert.deepStrictEqual(decoded.rows[1][0], []);
    assert.deepStrictEqual(decoded.rows[2][0], [100]);
  });

  it("decodes Tuple", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, String, Float64)" }];
    const rows = [[[42, "hello", 3.14]], [[100, "world", 2.71]]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const tuple0 = decoded.rows[0][0] as unknown[];
    assert.strictEqual(tuple0[0], 42);
    assert.strictEqual(tuple0[1], "hello");
    assert.ok(Math.abs((tuple0[2] as number) - 3.14) < 0.0001);

    const tuple1 = decoded.rows[1][0] as unknown[];
    assert.strictEqual(tuple1[0], 100);
    assert.strictEqual(tuple1[1], "world");
  });

  it("decodes Map", () => {
    const columns: ColumnDef[] = [{ name: "m", type: "Map(String, Int32)" }];
    const rows = [[{ foo: 1, bar: 2 }], [{}]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { foo: 1, bar: 2 });
    assert.deepStrictEqual(decoded.rows[1][0], {});
  });

  it("decodes nested types", () => {
    const columns: ColumnDef[] = [
      { name: "data", type: "Tuple(String, Array(Int32), Map(String, Float64))" },
    ];
    const rows = [[["outer", [1, 2, 3], { a: 1.5, b: 2.5 }]]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const tuple = decoded.rows[0][0] as unknown[];
    assert.strictEqual(tuple[0], "outer");
    assert.deepStrictEqual(tuple[1], [1, 2, 3]);
    assert.deepStrictEqual(tuple[2], { a: 1.5, b: 2.5 });
  });

  it("decodes Array of Tuples", () => {
    const columns: ColumnDef[] = [{ name: "arr", type: "Array(Tuple(String, Int32))" }];
    const rows = [[[["a", 1], ["b", 2]]]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const arr = decoded.rows[0][0] as unknown[][];
    assert.deepStrictEqual(arr[0], ["a", 1]);
    assert.deepStrictEqual(arr[1], ["b", 2]);
  });

  it("throws on column count mismatch", () => {
    const columns: ColumnDef[] = [
      { name: "a", type: "Int32" },
      { name: "b", type: "String" },
    ];
    const encoded = encodeRowBinaryWithNames(columns, [[1, "test"]]);

    assert.throws(
      () => decodeRowBinaryWithNames(encoded, ["Int32"]),
      /Column count mismatch/
    );
  });

  it("Date32", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Date32" }];
    // Date32 supports negative (pre-1970) dates
    const rows = [
      [new Date("2024-01-15")],
      [new Date("1950-06-20")],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const d1 = decoded.rows[0][0] as Date;
    const d2 = decoded.rows[1][0] as Date;
    assert.strictEqual(
      Math.floor(d1.getTime() / 86400000),
      Math.floor(rows[0][0].getTime() / 86400000)
    );
    assert.strictEqual(
      Math.floor(d2.getTime() / 86400000),
      Math.floor(rows[1][0].getTime() / 86400000)
    );
  });

  it("FixedString(N)", () => {
    const columns: ColumnDef[] = [{ name: "s", type: "FixedString(10)" }];
    const rows = [["hello"], ["world12345"], ["x"]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], "hello");
    assert.strictEqual(decoded.rows[1][0], "world12345");
    assert.strictEqual(decoded.rows[2][0], "x");
  });

  it("Enum8", () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum8('a' = 1, 'b' = 2)" }];
    const rows = [[1], [2], [1]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], 1);
    assert.strictEqual(decoded.rows[1][0], 2);
    assert.strictEqual(decoded.rows[2][0], 1);
  });

  it("Enum16", () => {
    const columns: ColumnDef[] = [{ name: "e", type: "Enum16('big' = 1000)" }];
    const rows = [[1000]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], 1000);
  });

  it("UUID", () => {
    const columns: ColumnDef[] = [{ name: "id", type: "UUID" }];
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    const rows = [[uuid]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], uuid);
  });

  it("IPv4", () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv4" }];
    const rows = [["192.168.1.1"], ["10.0.0.1"], ["255.255.255.255"]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], "192.168.1.1");
    assert.strictEqual(decoded.rows[1][0], "10.0.0.1");
    assert.strictEqual(decoded.rows[2][0], "255.255.255.255");
  });

  it("IPv6", () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv6" }];
    const rows = [["2001:db8:85a3:0:0:8a2e:370:7334"]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    // IPv6 normalizes to lowercase without leading zeros in groups
    assert.strictEqual(decoded.rows[0][0], "2001:db8:85a3:0:0:8a2e:370:7334");
  });

  it("IPv6 with :: expansion", () => {
    const columns: ColumnDef[] = [{ name: "ip", type: "IPv6" }];
    const rows = [["::1"], ["fe80::1"]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    // Decoded form is expanded
    assert.strictEqual(decoded.rows[0][0], "0:0:0:0:0:0:0:1");
    assert.strictEqual(decoded.rows[1][0], "fe80:0:0:0:0:0:0:1");
  });

  it("DateTime64(3)", () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(3)" }];
    const date = new Date("2024-01-15T12:30:45.123Z");
    const rows = [[date]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const d = decoded.rows[0][0] as Date;
    assert.strictEqual(d.getTime(), date.getTime());
  });

  it("DateTime64(6) - microseconds", () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(6)" }];
    const date = new Date("2024-01-15T12:30:45.123Z");
    const rows = [[date]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const d = decoded.rows[0][0] as Date;
    // Precision is microseconds, but JS Date is ms, so should match
    assert.strictEqual(d.getTime(), date.getTime());
  });

  it("DateTime64(0) - seconds only", () => {
    const columns: ColumnDef[] = [{ name: "dt", type: "DateTime64(0)" }];
    const date = new Date("2024-01-15T12:30:45.000Z");
    const rows = [[date]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const d = decoded.rows[0][0] as Date;
    assert.strictEqual(Math.floor(d.getTime() / 1000), Math.floor(date.getTime() / 1000));
  });

  it("Int128", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Int128" }];
    const big = 170141183460469231731687303715884105727n; // max Int128
    const neg = -170141183460469231731687303715884105728n; // min Int128
    const rows = [[big], [neg], [0n]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], big);
    assert.strictEqual(decoded.rows[1][0], neg);
    assert.strictEqual(decoded.rows[2][0], 0n);
  });

  it("UInt128", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "UInt128" }];
    const big = 340282366920938463463374607431768211455n; // max UInt128
    const rows = [[big], [0n], [12345678901234567890n]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], big);
    assert.strictEqual(decoded.rows[1][0], 0n);
    assert.strictEqual(decoded.rows[2][0], 12345678901234567890n);
  });

  it("Int256", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "Int256" }];
    const val = 12345678901234567890123456789012345678901234567890n;
    const neg = -val;
    const rows = [[val], [neg], [0n]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], val);
    assert.strictEqual(decoded.rows[1][0], neg);
    assert.strictEqual(decoded.rows[2][0], 0n);
  });

  it("UInt256", () => {
    const columns: ColumnDef[] = [{ name: "n", type: "UInt256" }];
    const val = 12345678901234567890123456789012345678901234567890n;
    const rows = [[val], [0n]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], val);
    assert.strictEqual(decoded.rows[1][0], 0n);
  });

  it("Decimal32(9, 2)", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal32(9, 2)" }];
    const rows = [[123.45], [-99.99], [0]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], "123.45");
    assert.strictEqual(decoded.rows[1][0], "-99.99");
    assert.strictEqual(decoded.rows[2][0], "0.00");
  });

  it("Decimal64(18, 4)", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal64(18, 4)" }];
    const rows = [[12345.6789], [-0.0001]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], "12345.6789");
    assert.strictEqual(decoded.rows[1][0], "-0.0001");
  });

  it("Decimal128(38, 10)", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal128(38, 10)" }];
    // Use string for precision
    const rows = [["12345678901234567890.1234567890"]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], "12345678901234567890.1234567890");
  });

  it("Decimal256(76, 20)", () => {
    const columns: ColumnDef[] = [{ name: "d", type: "Decimal256(76, 20)" }];
    const rows = [["123456789012345678901234567890.12345678901234567890"]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.strictEqual(decoded.rows[0][0], "123456789012345678901234567890.12345678901234567890");
  });

  it("Variant(String, Int32)", () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(String, Int32)" }];
    const rows = [
      [{ type: 0, value: "hello" }],
      [{ type: 1, value: 42 }],
      [null],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { type: 0, value: "hello" });
    assert.deepStrictEqual(decoded.rows[1][0], { type: 1, value: 42 });
    assert.strictEqual(decoded.rows[2][0], null);
  });

  it("Variant with nested types", () => {
    const columns: ColumnDef[] = [{ name: "v", type: "Variant(Array(Int32), String)" }];
    const rows = [
      [{ type: 0, value: [1, 2, 3] }],
      [{ type: 1, value: "text" }],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { type: 0, value: [1, 2, 3] });
    assert.deepStrictEqual(decoded.rows[1][0], { type: 1, value: "text" });
  });

  it("JSON (string mode)", () => {
    const columns: ColumnDef[] = [{ name: "j", type: "JSON" }];
    const rows = [
      [{ foo: "bar", num: 42, nested: { a: [1, 2, 3] } }],
      [{ empty: {} }],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { foo: "bar", num: 42, nested: { a: [1, 2, 3] } });
    assert.deepStrictEqual(decoded.rows[1][0], { empty: {} });
  });

  it("Object('json') alias", () => {
    const columns: ColumnDef[] = [{ name: "j", type: "Object('json')" }];
    const rows = [[{ key: "value" }]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { key: "value" });
  });

  it("Decimal(P, S) generic form", () => {
    // Test all precision ranges
    const tests = [
      { type: "Decimal(9, 2)", value: "1234567.89", expected: "1234567.89" },
      { type: "Decimal(18, 4)", value: "12345678901234.5678", expected: "12345678901234.5678" },
      { type: "Decimal(38, 10)", value: "1234567890123456789.0123456789", expected: "1234567890123456789.0123456789" },
      { type: "Decimal(76, 20)", value: "12345678901234567890.12345678901234567890", expected: "12345678901234567890.12345678901234567890" },
    ];

    for (const { type, value, expected } of tests) {
      const columns: ColumnDef[] = [{ name: "d", type }];
      const rows = [[value]];
      const encoded = encodeRowBinaryWithNames(columns, rows);
      const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

      assert.strictEqual(decoded.rows[0][0], expected, `Failed for ${type}`);
    }
  });

  it("Named tuple basic", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(id Int32, name String)" }];
    const rows = [
      [{ id: 1, name: "alice" }],
      [{ id: 2, name: "bob" }],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { id: 1, name: "alice" });
    assert.deepStrictEqual(decoded.rows[1][0], { id: 2, name: "bob" });
  });

  it("Named tuple with nested array", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(tags Array(String), count Int32)" }];
    const rows = [
      [{ tags: ["a", "b", "c"], count: 3 }],
      [{ tags: [], count: 0 }],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], { tags: ["a", "b", "c"], count: 3 });
    assert.deepStrictEqual(decoded.rows[1][0], { tags: [], count: 0 });
  });

  it("Deep nesting: Array(Tuple(a Array(String), v Variant(String, Int64)))", () => {
    const columns: ColumnDef[] = [{
      name: "data",
      type: "Array(Tuple(a Array(String), v Variant(String, Int64)))"
    }];
    const rows = [
      [[
        { a: ["hello", "world"], v: { type: 0, value: "string_value" } },
        { a: ["foo"], v: { type: 1, value: 42n } },
        { a: [], v: null },
      ]],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const result = decoded.rows[0][0] as Array<{ a: string[]; v: { type: number; value: unknown } | null }>;
    assert.strictEqual(result.length, 3);
    assert.deepStrictEqual(result[0].a, ["hello", "world"]);
    assert.deepStrictEqual(result[0].v, { type: 0, value: "string_value" });
    assert.deepStrictEqual(result[1].a, ["foo"]);
    assert.deepStrictEqual(result[1].v, { type: 1, value: 42n });
    assert.deepStrictEqual(result[2].a, []);
    assert.strictEqual(result[2].v, null);
  });

  it("Unnamed tuples still work as arrays", () => {
    const columns: ColumnDef[] = [{ name: "t", type: "Tuple(Int32, String, Float64)" }];
    const rows = [[[100, "test", 3.14]]];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    const tuple = decoded.rows[0][0] as unknown[];
    assert.strictEqual(tuple[0], 100);
    assert.strictEqual(tuple[1], "test");
    assert.ok(Math.abs(tuple[2] as number - 3.14) < 0.0001);
  });

  it("Named tuple with Map field", () => {
    const columns: ColumnDef[] = [{
      name: "t",
      type: "Tuple(id UInt32, meta Map(String, String))"
    }];
    const rows = [
      [{ id: 1, meta: { key1: "value1", key2: "value2" } }],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], {
      id: 1,
      meta: { key1: "value1", key2: "value2" }
    });
  });

  it("Nested named tuples", () => {
    const columns: ColumnDef[] = [{
      name: "t",
      type: "Tuple(outer_id Int32, inner Tuple(x Float64, y Float64))"
    }];
    const rows = [
      [{ outer_id: 1, inner: { x: 1.5, y: 2.5 } }],
    ];
    const encoded = encodeRowBinaryWithNames(columns, rows);
    const decoded = decodeRowBinaryWithNames(encoded, columns.map((c) => c.type));

    assert.deepStrictEqual(decoded.rows[0][0], {
      outer_id: 1,
      inner: { x: 1.5, y: 2.5 }
    });
  });
});

