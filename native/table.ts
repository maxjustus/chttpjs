import {
  getCodec,
  toInt8,
  toInt16,
  toInt32,
  toInt64,
  toNumber,
  toUInt8,
  toUInt16,
  toUInt32,
  toUInt64,
} from "./codecs.ts";
import { type Column, DataColumn } from "./columns.ts";
import type { Block } from "./index.ts";
import type { TypedArrayConstructor } from "./io.ts";
import type { ColumnDef, TypedArray } from "./types.ts";

type NumericConverter = (v: unknown) => number | bigint;

/** Map of numeric ClickHouse types to their TypedArray constructors and converters. */
const NUMERIC_TYPES: Record<string, { ctor: TypedArrayConstructor<any>; convert: NumericConverter }> = {
  Int8: { ctor: Int8Array, convert: toInt8 },
  Int16: { ctor: Int16Array, convert: toInt16 },
  Int32: { ctor: Int32Array, convert: toInt32 },
  Int64: { ctor: BigInt64Array, convert: toInt64 },
  UInt8: { ctor: Uint8Array, convert: toUInt8 },
  UInt16: { ctor: Uint16Array, convert: toUInt16 },
  UInt32: { ctor: Uint32Array, convert: toUInt32 },
  UInt64: { ctor: BigUint64Array, convert: toUInt64 },
  Float32: { ctor: Float32Array, convert: toNumber },
  Float64: { ctor: Float64Array, convert: toNumber },
};

function getNumericTypeInfo(type: string): { ctor: TypedArrayConstructor<any>; convert: NumericConverter } | undefined {
  return NUMERIC_TYPES[type];
}

/**
 * Growing TypedArray for efficient numeric accumulation.
 * Doubles capacity when full, returns trimmed subarray at finish.
 * Coerces values using the provided converter function.
 */
class GrowingTypedArray<T extends TypedArray> {
  private arr: T;
  private offset = 0;
  private Ctor: TypedArrayConstructor<T>;
  private convert: NumericConverter;

  constructor(Ctor: TypedArrayConstructor<T>, convert: NumericConverter, initialCapacity = 1024) {
    this.Ctor = Ctor;
    this.convert = convert;
    this.arr = new Ctor(initialCapacity) as T;
  }

  push(value: unknown): void {
    if (this.offset >= this.arr.length) {
      const newArr = new this.Ctor(this.arr.length * 2) as T;
      (newArr as any).set(this.arr);
      this.arr = newArr;
    }
    (this.arr as any)[this.offset++] = this.convert(value);
  }

  finish(): T {
    return this.arr.subarray(0, this.offset) as T;
  }
}

/** Options for materializing row data. */
export interface MaterializeOptions {
  /** Convert bigint values (Int64, UInt64, Int128, etc.) to strings. */
  bigIntAsString?: boolean;
}

function maybeStringify(val: unknown, opts?: MaterializeOptions): unknown {
  if (opts?.bigIntAsString && typeof val === "bigint") return val.toString();
  return val;
}

/**
 * A Row object is a Proxy that lazily accesses column data.
 *
 * Performance note: Each `row.field` access goes through a Proxy trap and
 * Map lookup. For hot loops, prefer:
 * - `batch.toArray()` for full materialization
 * - `batch.getColumn(name)` + column iteration for columnar access
 * - `batch.getAt(rowIndex, colIndex)` for direct value access
 */
export type Row = Record<string, unknown> & {
  /** Materialize row to a plain object. */
  toObject(options?: MaterializeOptions): Record<string, unknown>;
  /** Materialize row to a plain array in column order. */
  toArray(options?: MaterializeOptions): unknown[];
};

/**
 * RecordBatch provides an ergonomic, virtual view over columnar ClickHouse data.
 * Matches Apache Arrow terminology - a single batch of records with shared schema.
 */
export class RecordBatch implements Iterable<Row> {
  readonly columns: ColumnDef[];
  readonly columnData: Column[];
  readonly rowCount: number;
  readonly decodeTimeMs?: number;

  private nameToIndex: Map<string, number>;

  constructor(block: Block) {
    this.columns = block.columns;
    this.columnData = block.columnData;
    this.rowCount = block.rowCount;
    this.decodeTimeMs = block.decodeTimeMs;
    this.nameToIndex = new Map(this.columns.map((c, i) => [c.name, i]));
  }

  static from(block: Block): RecordBatch {
    return new RecordBatch(block);
  }

  get length(): number {
    return this.rowCount;
  }
  get numCols(): number {
    return this.columns.length;
  }
  get schema(): ColumnDef[] {
    return this.columns;
  }
  get columnNames(): string[] {
    return this.columns.map((c) => c.name);
  }

  /** Get column by name. */
  getColumn(name: string): Column | undefined {
    const idx = this.nameToIndex.get(name);
    return idx !== undefined ? this.columnData[idx] : undefined;
  }

  /** Get column by index. */
  getColumnAt(index: number): Column | undefined {
    return this.columnData[index];
  }

  /** Get value at specific row and column index. Allocation-free. */
  getAt(rowIndex: number, colIndex: number): unknown {
    return this.columnData[colIndex].get(rowIndex);
  }

  /** Get row at index (returns a lazy Proxy). */
  get(index: number, options?: MaterializeOptions): Row {
    if (index < 0 || index >= this.rowCount) {
      throw new RangeError(`Index out of bounds: ${index}`);
    }
    return createRowProxy(this, index, options);
  }

  /** Iterate over rows lazily. Default iterator creates new proxies per row (safe to store/collect). */
  *[Symbol.iterator](): Iterator<Row> {
    for (let i = 0; i < this.rowCount; i++) {
      yield this.get(i);
    }
  }

  /** Materialize all rows to plain objects. */
  toArray(options?: MaterializeOptions): Record<string, unknown>[] {
    const result = new Array(this.rowCount);
    const numCols = this.columns.length;
    const names = this.columnNames;

    for (let i = 0; i < this.rowCount; i++) {
      const row: Record<string, unknown> = {};
      for (let j = 0; j < numCols; j++) {
        row[names[j]] = maybeStringify(this.columnData[j].get(i), options);
      }
      result[i] = row;
    }
    return result;
  }

  /** For JSON.stringify(table). */
  toJSON(): Record<string, unknown>[] {
    return this.toArray();
  }
}

/**
 * internal helper to create a lazy row proxy.
 */
function createRowProxy(batch: RecordBatch, rowIndex: number, options?: MaterializeOptions): Row {
  const names = batch.columnNames;
  const materialize = (opts?: MaterializeOptions) => {
    const o = opts ?? options;
    const obj: Record<string, unknown> = {};
    for (let j = 0; j < batch.numCols; j++) {
      obj[names[j]] = maybeStringify(batch.columnData[j].get(rowIndex), o);
    }
    return obj;
  };
  return new Proxy({} as Row, {
    get(_, prop) {
      if (prop === "toObject" || prop === "toJSON") {
        return materialize;
      }
      if (prop === "toArray") {
        return (opts?: MaterializeOptions) => {
          const o = opts ?? options;
          const arr = new Array(batch.numCols);
          for (let j = 0; j < batch.numCols; j++) {
            arr[j] = maybeStringify(batch.columnData[j].get(rowIndex), o);
          }
          return arr;
        };
      }
      if (typeof prop === "string") {
        const col = batch.getColumn(prop);
        if (col) return maybeStringify(col.get(rowIndex), options);
      }
      return undefined;
    },
    ownKeys() {
      return names;
    },
    getOwnPropertyDescriptor(_, prop) {
      if (typeof prop === "string" && names.includes(prop)) {
        const col = batch.getColumn(prop);
        return {
          enumerable: true,
          configurable: true,
          value: col ? maybeStringify(col.get(rowIndex), options) : undefined,
        };
      }
      return undefined;
    },
    has(_, prop) {
      return typeof prop === "string" && names.includes(prop);
    },
  });
}

/**
 * Builder for constructing RecordBatches row-by-row.
 * Grows dynamically - no upfront capacity required.
 */
export class RecordBatchBuilder {
  private schema: ColumnDef[];
  private accumulators: (unknown[] | GrowingTypedArray<any>)[];
  private _rowCount: number = 0;
  private finished: boolean = false;

  constructor(schema: ColumnDef[], expectedRows?: number) {
    this.schema = schema;
    const initialCapacity = expectedRows ?? 1024;
    this.accumulators = schema.map((col) => {
      const info = getNumericTypeInfo(col.type);
      return info ? new GrowingTypedArray(info.ctor, info.convert, initialCapacity) : [];
    });
  }

  get rowCount(): number {
    return this._rowCount;
  }

  /** Append a row (values in column order). Coerces values to correct types. */
  appendRow(values: unknown[]): this {
    if (values.length !== this.schema.length) throw new Error("Row length mismatch");
    for (let i = 0; i < values.length; i++) {
      this.accumulators[i].push(values[i]);
    }
    this._rowCount++;
    return this;
  }

  /** Finalize and return an immutable RecordBatch. */
  finish(): RecordBatch {
    if (this.finished) throw new Error("Builder already finished");
    this.finished = true;
    const columnData = this.accumulators.map((acc, i) => {
      if (acc instanceof GrowingTypedArray) {
        return new DataColumn(this.schema[i].type, acc.finish());
      }
      return getCodec(this.schema[i].type).fromValues(acc);
    });
    return new RecordBatch({
      columns: this.schema,
      columnData,
      rowCount: this._rowCount,
    });
  }
}

/**
 * Create a RecordBatch from row data.
 * Accepts arrays, sync iterables/generators, or async iterables/generators.
 * Returns Promise<RecordBatch> for async iterables.
 *
 * @example
 * // From array
 * batchFromRows(schema, [[1, "a"], [2, "b"]]);
 *
 * // From generator
 * batchFromRows(schema, function*() { yield [1, "a"]; }());
 *
 * // From async generator
 * await batchFromRows(schema, async function*() { yield [1, "a"]; }());
 */
export function batchFromRows(
  schema: ColumnDef[],
  rows: unknown[][] | Iterable<unknown[]>,
  expectedRows?: number,
): RecordBatch;
export function batchFromRows(
  schema: ColumnDef[],
  rows: AsyncIterable<unknown[]>,
  expectedRows?: number,
): Promise<RecordBatch>;
export function batchFromRows(
  schema: ColumnDef[],
  rows: unknown[][] | Iterable<unknown[]> | AsyncIterable<unknown[]>,
  expectedRows?: number,
): RecordBatch | Promise<RecordBatch> {
  // Detect async iterable
  if (typeof (rows as any)[Symbol.asyncIterator] === "function") {
    return (async () => {
      const builder = new RecordBatchBuilder(schema, expectedRows);
      for await (const row of rows as AsyncIterable<unknown[]>) {
        builder.appendRow(row);
      }
      return builder.finish();
    })();
  }
  // Sync path: array or iterable
  const builder = new RecordBatchBuilder(schema, expectedRows);
  for (const row of rows as Iterable<unknown[]>) {
    builder.appendRow(row);
  }
  return builder.finish();
}

/**
 * Create a RecordBatch from pre-built Column objects.
 * Schema is inferred from the columns.
 *
 * @example
 * batchFromCols({
 *   id: getCodec("UInt32").fromValues([1, 2, 3]),
 *   name: getCodec("String").fromValues(["a", "b", "c"]),
 * });
 */
export function batchFromCols(columns: Record<string, Column>): RecordBatch {
  const names = Object.keys(columns);
  const schema = names.map((name) => ({ name, type: columns[name].type }));
  const columnData = names.map((name) => columns[name]);
  const rowCount = columnData[0]?.length ?? 0;
  return new RecordBatch({ columns: schema, columnData, rowCount });
}
