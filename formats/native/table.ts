import { type ColumnDef, type TypedArray } from "../shared.ts";
import { type Column, DataColumn } from "./columns.ts";
import { type Block } from "./index.ts";
import { getCodec, makeBuilder, type ColumnBuilder } from "./codecs.ts";

/**
 * A Row object is a Proxy that lazily accesses column data.
 */
export type Row = Record<string, unknown> & {
  /** Materialize row to a plain object. */
  toObject(): Record<string, unknown>;
  /** Materialize row to a plain array in column order. */
  toArray(): unknown[];
};

/**
 * Table provides an ergonomic, virtual view over columnar ClickHouse data.
 * Inspired by Apache Arrow and dataframe libraries.
 */
export class Table implements Iterable<Row> {
  readonly columns: ColumnDef[];
  readonly columnData: Column[];
  readonly rowCount: number;

  private nameToIndex: Map<string, number>;

  constructor(block: Block) {
    this.columns = block.columns;
    this.columnData = block.columnData;
    this.rowCount = block.rowCount;
    this.nameToIndex = new Map(this.columns.map((c, i) => [c.name, i]));
  }

  static from(block: Block): Table {
    return new Table(block);
  }

  /**
   * Create a Table from columnar data.
   * Accepts TypedArrays, plain arrays, or Column objects.
   */
  static fromColumnar(
    columns: ColumnDef[],
    columnData: (unknown[] | TypedArray | Column)[],
  ): Table {
    const rowCount = columnData[0]?.length ?? 0;
    const cols: Column[] = columnData.map((data, i) => {
      // Already a Column - use as-is
      if (data && typeof (data as any).get === "function")
        return data as Column;
      // TypedArray - wrap in DataColumn with type from schema
      if (ArrayBuffer.isView(data) && !(data instanceof DataView))
        return new DataColumn(columns[i].type, data as TypedArray);
      // Array - use codec.fromValues
      return getCodec(columns[i].type).fromValues(data as unknown[]);
    });
    return new Table({ columns, columnData: cols, rowCount });
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
  get(index: number): Row {
    if (index < 0 || index >= this.rowCount) {
      throw new RangeError(`Index out of bounds: ${index}`);
    }
    return createRowProxy(this, index);
  }

  /**
   * Iterate over rows lazily using a single reused Proxy object.
   * Extremely efficient for large tables as it avoids per-row allocations.
   */
  *rows(): IterableIterator<Row> {
    const names = this.columnNames;
    const numCols = this.numCols;
    let currentRow = 0;

    const proxy = new Proxy({} as Row, {
      get: (_, prop) => {
        if (prop === "toObject") {
          return () => {
            const obj: Record<string, unknown> = {};
            for (let j = 0; j < numCols; j++)
              obj[names[j]] = this.getAt(currentRow, j);
            return obj;
          };
        }
        if (prop === "toArray") {
          return () => {
            const arr = new Array(numCols);
            for (let j = 0; j < numCols; j++)
              arr[j] = this.getAt(currentRow, j);
            return arr;
          };
        }
        if (typeof prop === "string") {
          const idx = this.nameToIndex.get(prop);
          if (idx !== undefined) return this.getAt(currentRow, idx);
        }
        return undefined;
      },
      ownKeys: () => names,
      getOwnPropertyDescriptor: (_, prop) =>
        typeof prop === "string" && names.includes(prop)
          ? { enumerable: true, configurable: true }
          : undefined,
      has: (_, prop) => typeof prop === "string" && names.includes(prop),
    });

    for (; currentRow < this.rowCount; currentRow++) {
      yield proxy;
    }
  }

  /** Iterate over rows lazily. Default iterator still creates new proxies for safety. */
  *[Symbol.iterator](): Iterator<Row> {
    for (let i = 0; i < this.rowCount; i++) {
      yield this.get(i);
    }
  }

  /** Materialize all rows to plain objects. */
  toArray(): Record<string, unknown>[] {
    const result = new Array(this.rowCount);
    const numCols = this.columns.length;
    const names = this.columnNames;

    for (let i = 0; i < this.rowCount; i++) {
      const row: Record<string, unknown> = {};
      for (let j = 0; j < numCols; j++) {
        row[names[j]] = this.columnData[j].get(i);
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
function createRowProxy(table: Table, rowIndex: number): Row {
  const names = table.columnNames;
  return new Proxy({} as Row, {
    get(_, prop) {
      if (prop === "toObject") {
        return () => {
          const obj: Record<string, unknown> = {};
          for (let j = 0; j < table.numCols; j++) {
            obj[names[j]] = table.columnData[j].get(rowIndex);
          }
          return obj;
        };
      }
      if (prop === "toArray") {
        return () => {
          const arr = new Array(table.numCols);
          for (let j = 0; j < table.numCols; j++) {
            arr[j] = table.columnData[j].get(rowIndex);
          }
          return arr;
        };
      }
      if (typeof prop === "string") {
        const col = table.getColumn(prop);
        if (col) return col.get(rowIndex);
      }
      return undefined;
    },
    ownKeys() {
      return names;
    },
    getOwnPropertyDescriptor(_, prop) {
      if (typeof prop === "string" && names.includes(prop)) {
        return { enumerable: true, configurable: true };
      }
      return undefined;
    },
    has(_, prop) {
      return typeof prop === "string" && names.includes(prop);
    },
  });
}

/**
 * Builder for constructing Tables row-by-row.
 * Grows dynamically - no upfront capacity required.
 */
export class TableBuilder {
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
    if (values.length !== this.schema.length)
      throw new Error("Row length mismatch");
    for (let i = 0; i < values.length; i++) {
      this.builders[i].append(values[i]);
    }
    this._rowCount++;
    return this;
  }

  /** Finalize and return an immutable Table. */
  finish(): Table {
    if (this.finished) throw new Error("Builder already finished");
    this.finished = true;
    return new Table({
      columns: this.schema,
      columnData: this.builders.map((b) => b.finish()),
      rowCount: this._rowCount,
    });
  }
}

/**
 * Create a Table from columnar data keyed by column name.
 *
 * @param schema - Column definitions (name and type)
 * @param data - Object with column names as keys, arrays/TypedArrays as values
 *
 * @example
 * const table = tableFromArrays(
 *   [{ name: 'id', type: 'UInt32' }, { name: 'name', type: 'String' }],
 *   { id: new Uint32Array([1, 2, 3]), name: ['alice', 'bob', 'charlie'] }
 * );
 */
export function tableFromArrays(
  schema: ColumnDef[],
  data: Record<string, unknown[] | TypedArray | Column>,
): Table {
  const columnData = schema.map((col) => data[col.name]);
  return Table.fromColumnar(schema, columnData);
}

/**
 * Create a Table from row arrays.
 *
 * @param schema - Column definitions (name and type)
 * @param rows - Array of rows, each row is an array of values in schema order
 *
 * @example
 * const table = tableFromRows(
 *   [{ name: 'id', type: 'UInt32' }, { name: 'name', type: 'String' }],
 *   [[1, 'alice'], [2, 'bob'], [3, 'charlie']]
 * );
 */
export function tableFromRows(schema: ColumnDef[], rows: unknown[][]): Table {
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
  const columnData = columns.map((arr, i) =>
    getCodec(schema[i].type).fromValues(arr),
  );
  return new Table({ columns: schema, columnData, rowCount: rows.length });
}

/**
 * Create a Table from pre-built Column objects.
 * Schema is derived from the columns themselves (each Column has a type property).
 *
 * @param columns - Object with column names as keys, Column objects as values
 *
 * @example
 * const idCol = makeBuilder('UInt32').append(1).append(2).finish();
 * const nameCol = makeBuilder('String').append('alice').append('bob').finish();
 * const table = tableFromCols({ id: idCol, name: nameCol });
 */
export function tableFromCols(columns: Record<string, Column>): Table {
  const names = Object.keys(columns);
  const schema = names.map((name) => ({ name, type: columns[name].type }));
  const columnData = names.map((name) => columns[name]);
  const rowCount = columnData[0]?.length ?? 0;
  return new Table({ columns: schema, columnData, rowCount });
}

/**
 * Create a TableBuilder for incremental row construction.
 *
 * @param schema - Column definitions (name and type)
 *
 * @example
 * const builder = tableBuilder([{ name: 'id', type: 'UInt32' }]);
 * builder.appendRow([1]).appendRow([2]);
 * const table = builder.finish();
 */
export function tableBuilder(schema: ColumnDef[]): TableBuilder {
  return new TableBuilder(schema);
}
