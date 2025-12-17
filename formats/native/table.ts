import { type ColumnDef } from "../shared.ts";
import { type Column } from "./columns.ts";
import { type Block } from "./index.ts";

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

  get numRows(): number { return this.rowCount; }
  get numCols(): number { return this.columns.length; }
  get schema(): ColumnDef[] { return this.columns; }
  get columnNames(): string[] { return this.columns.map(c => c.name); }

  /** Get column by name. */
  getColumn(name: string): Column | undefined {
    const idx = this.nameToIndex.get(name);
    return idx !== undefined ? this.columnData[idx] : undefined;
  }

  /** Get column by index. */
  getColumnAt(index: number): Column | undefined {
    return this.columnData[index];
  }

  /** Get row at index (returns a lazy Proxy). */
  get(index: number): Row {
    if (index < 0 || index >= this.rowCount) {
      throw new RangeError(`Index out of bounds: ${index}`);
    }
    return createRowProxy(this, index);
  }

  /** Get row at index, supporting negative indices. */
  at(index: number): Row | undefined {
    const idx = index < 0 ? this.rowCount + index : index;
    if (idx < 0 || idx >= this.rowCount) return undefined;
    return this.get(idx);
  }

  /** Iterate over rows lazily. */
  *[Symbol.iterator](): Iterator<Row> {
    for (let i = 0; i < this.rowCount; i++) {
      yield this.get(i);
    }
  }

  /** Return a new Table with sliced rows (zero-copy where possible). */
  slice(start = 0, end = this.rowCount): Table {
    // Note: To truly support zero-copy slicing, we would need a SlicedColumn wrapper.
    // For now, we can just pass the slice boundaries to the proxy and iterators.
    // But for simplicity, let's just implement a VirtualTable that offsets indices.
    return new SlicedTable(this, start, end);
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
}

/**
 * internal helper to create a lazy row proxy.
 */
function createRowProxy(table: Table, rowIndex: number): Row {
  const names = table.columnNames;
  return new Proxy({} as Row, {
    get(_, prop) {
      if (prop === 'toObject') {
        return () => {
          const obj: Record<string, unknown> = {};
          for (let j = 0; j < table.numCols; j++) {
            obj[names[j]] = table.columnData[j].get(rowIndex);
          }
          return obj;
        };
      }
      if (prop === 'toArray') {
        return () => {
          const arr = new Array(table.numCols);
          for (let j = 0; j < table.numCols; j++) {
            arr[j] = table.columnData[j].get(rowIndex);
          }
          return arr;
        };
      }
      if (typeof prop === 'string') {
        const col = table.getColumn(prop);
        if (col) return col.get(rowIndex);
      }
      return undefined;
    },
    ownKeys() {
      return names;
    },
    getOwnPropertyDescriptor(_, prop) {
      if (typeof prop === 'string' && names.includes(prop)) {
        return { enumerable: true, configurable: true };
      }
      return undefined;
    },
    has(_, prop) {
      return typeof prop === 'string' && names.includes(prop);
    }
  });
}

/**
 * A view over a subset of a Table's rows.
 */
class SlicedTable extends Table {
  private parent: Table;
  private start: number;
  private end: number;

  constructor(parent: Table, start: number, end: number) {
    const rowCount = Math.max(0, Math.min(end, parent.rowCount) - Math.max(0, start));
    super({
      columns: parent.columns,
      columnData: parent.columnData, // Still using parent's columns, but we'll offset indices
      rowCount,
    });
    this.parent = parent;
    this.start = Math.max(0, start);
    this.end = Math.min(end, parent.rowCount);
  }

  get(index: number): Row {
    if (index < 0 || index >= this.rowCount) {
      throw new RangeError(`Index out of bounds: ${index}`);
    }
    return this.parent.get(this.start + index);
  }

  *[Symbol.iterator](): Iterator<Row> {
    for (let i = this.start; i < this.end; i++) {
      yield this.parent.get(i);
    }
  }

  slice(start = 0, end = this.rowCount): Table {
    return new SlicedTable(this.parent, this.start + start, Math.min(this.start + end, this.end));
  }
}
