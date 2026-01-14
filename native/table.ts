import { type ColumnBuilder, getCodec, makeBuilder } from "./codecs.ts";
import { type Column, DataColumn } from "./columns.ts";
import type { Block } from "./index.ts";
import type { ColumnDef, TypedArray } from "./types.ts";

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

  /**
   * Create a RecordBatch from columnar data.
   * Accepts TypedArrays, plain arrays, or Column objects.
   */
  static fromColumnar(
    columns: ColumnDef[],
    columnData: (unknown[] | TypedArray | Column)[],
  ): RecordBatch {
    const rowCount = columnData[0]?.length ?? 0;
    const cols: Column[] = columnData.map((data, i) => {
      // Already a Column - use as-is
      if (data && typeof (data as any).get === "function") return data as Column;
      // TypedArray - wrap in DataColumn with type from schema
      if (ArrayBuffer.isView(data) && !(data instanceof DataView))
        return new DataColumn(columns[i].type, data as TypedArray);
      // Array - use codec.fromValues
      return getCodec(columns[i].type).fromValues(data as unknown[]);
    });
    return new RecordBatch({ columns, columnData: cols, rowCount });
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
  private builders: ColumnBuilder[];
  private _rowCount: number = 0;
  private finished: boolean = false;

  constructor(schema: ColumnDef[]) {
    this.schema = schema;
    this.builders = schema.map((col) => makeBuilder(col.type));
  }

  get rowCount(): number {
    return this._rowCount;
  }

  /** Append a row (values in column order). */
  appendRow(values: unknown[]): this {
    if (values.length !== this.schema.length) throw new Error("Row length mismatch");
    for (let i = 0; i < values.length; i++) {
      this.builders[i].append(values[i]);
    }
    this._rowCount++;
    return this;
  }

  /** Finalize and return an immutable RecordBatch. */
  finish(): RecordBatch {
    if (this.finished) throw new Error("Builder already finished");
    this.finished = true;
    return new RecordBatch({
      columns: this.schema,
      columnData: this.builders.map((b) => b.finish()),
      rowCount: this._rowCount,
    });
  }
}

/**
 * Create a RecordBatch from columnar data keyed by column name.
 *
 * @param schema - Column definitions (name and type)
 * @param data - Object with column names as keys, arrays/TypedArrays as values
 *
 * @example
 * const batch = batchFromArrays(
 *   [{ name: 'id', type: 'UInt32' }, { name: 'name', type: 'String' }],
 *   { id: new Uint32Array([1, 2, 3]), name: ['alice', 'bob', 'charlie'] }
 * );
 */
export function batchFromArrays(
  schema: ColumnDef[],
  data: Record<string, unknown[] | TypedArray | Column>,
): RecordBatch {
  const columnData = schema.map((col) => data[col.name]);
  return RecordBatch.fromColumnar(schema, columnData);
}

/**
 * Create a RecordBatch from row arrays.
 *
 * @param schema - Column definitions (name and type)
 * @param rows - Array of rows, each row is an array of values in schema order
 *
 * @example
 * const batch = batchFromRows(
 *   [{ name: 'id', type: 'UInt32' }, { name: 'name', type: 'String' }],
 *   [[1, 'alice'], [2, 'bob'], [3, 'charlie']]
 * );
 */
export function batchFromRows(schema: ColumnDef[], rows: unknown[][]): RecordBatch {
  // Transpose rows to columns
  const numCols = schema.length;
  const columns: unknown[][] = schema.map(() => new Array(rows.length));
  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    for (let c = 0; c < numCols; c++) {
      columns[c][r] = row[c];
    }
  }
  // Use codec.fromValues for each column
  const columnData = columns.map((arr, i) => getCodec(schema[i].type).fromValues(arr));
  return new RecordBatch({ columns: schema, columnData, rowCount: rows.length });
}

/**
 * Create a RecordBatch from pre-built Column objects.
 * Schema is derived from the columns themselves (each Column has a type property).
 *
 * @param columns - Object with column names as keys, Column objects as values
 *
 * @example
 * const idCol = makeBuilder('UInt32').append(1).append(2).finish();
 * const nameCol = makeBuilder('String').append('alice').append('bob').finish();
 * const batch = batchFromCols({ id: idCol, name: nameCol });
 */
export function batchFromCols(columns: Record<string, Column>): RecordBatch {
  const names = Object.keys(columns);
  const schema = names.map((name) => ({ name, type: columns[name].type }));
  const columnData = names.map((name) => columns[name]);
  const rowCount = columnData[0]?.length ?? 0;
  return new RecordBatch({ columns: schema, columnData, rowCount });
}

/**
 * Create a RecordBatchBuilder for incremental row construction.
 *
 * @param schema - Column definitions (name and type)
 *
 * @example
 * const builder = batchBuilder([{ name: 'id', type: 'UInt32' }]);
 * builder.appendRow([1]).appendRow([2]);
 * const batch = builder.finish();
 */
export function batchBuilder(schema: ColumnDef[]): RecordBatchBuilder {
  return new RecordBatchBuilder(schema);
}
