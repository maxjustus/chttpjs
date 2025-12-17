/**
 * Column data structures for Native format.
 * Columns provide columnar access to data with lazy materialization.
 */

import { ClickHouseDateTime64 } from "../../native_utils.ts";

type TypedArray = Int8Array | Uint8Array | Int16Array | Uint16Array | Int32Array | Uint32Array | BigInt64Array | BigUint64Array | Float32Array | Float64Array;
export type DiscriminatorArray = Uint8Array | Uint16Array | Uint32Array;

// Variant uses 0xFF (255) as the null discriminator
export const VARIANT_NULL_DISCRIMINATOR = 0xFF;

/**
 * Count discriminators and compute group indices in a single pass.
 * Returns counts (for decoding groups) and indices (for O(1) value access).
 */
export function countAndIndexDiscriminators(
  discriminators: DiscriminatorArray,
  nullValue: number
): { counts: Map<number, number>; indices: Uint32Array } {
  const counts = new Map<number, number>();
  const indices = new Uint32Array(discriminators.length);
  for (let i = 0; i < discriminators.length; i++) {
    const d = discriminators[i];
    if (d !== nullValue) {
      indices[i] = counts.get(d) || 0;
      counts.set(d, (counts.get(d) || 0) + 1);
    }
  }
  return { counts, indices };
}

/**
 * Base interface for all column types.
 */
export interface BaseColumn {
  readonly length: number;
  get(i: number): unknown;
  slice(start: number, end: number): BaseColumn;
  materialize(): unknown[] | TypedArray;
}

/**
 * Recursively materialize a value to plain JS.
 * Called by column.materialize() implementations.
 */
export function materializeValue(value: unknown): unknown {
  if (value === null || value === undefined) return value;

  // If it's a column (has materialize method), materialize it
  if (value && typeof (value as any).materialize === 'function') {
    return (value as BaseColumn).materialize();
  }

  // Map - preserve as Map with materialized entries
  if (value instanceof Map) {
    const result = new Map();
    for (const [k, v] of value) {
      result.set(materializeValue(k), materializeValue(v));
    }
    return result;
  }

  // Array - materialize each element
  if (Array.isArray(value)) {
    return value.map(materializeValue);
  }

  // Object (like tuple result) - materialize values
  if (typeof value === 'object' && !(value instanceof Date) && !ArrayBuffer.isView(value)) {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) {
      result[k] = materializeValue(v);
    }
    return result;
  }

  // Primitives, Date, TypedArray - pass through
  return value;
}

/**
 * Wraps a TypedArray as a column with the BaseColumn interface.
 */
export class TypedColumn<T extends TypedArray> implements BaseColumn {
  readonly data: T;

  constructor(data: T) {
    this.data = data;
  }

  get length() { return this.data.length; }

  get(i: number): number | bigint {
    return this.data[i];
  }

  slice(start: number, end: number): TypedColumn<T> {
    return new TypedColumn(this.data.slice(start, end) as T);
  }

  materialize(): T {
    return this.data;
  }
}

/**
 * Generic simple column - wraps an array of values.
 * Used for String, Bytes, Date, DateTime64, Scalar (Decimal, Int128, etc.).
 */
export class SimpleColumn<T> implements BaseColumn {
  readonly values: T[];

  constructor(values: T[]) {
    this.values = values;
  }

  get length() { return this.values.length; }

  get(i: number): T {
    return this.values[i];
  }

  slice(start: number, end: number): SimpleColumn<T> {
    return new SimpleColumn(this.values.slice(start, end));
  }

  materialize(): T[] {
    return this.values.slice();
  }
}

/**
 * Columnar tuple - stores each element as a separate column.
 */
export class TupleColumn implements BaseColumn {
  readonly elements: { name: string | null }[];
  readonly columns: Column[];
  readonly isNamed: boolean;

  constructor(
    elements: { name: string | null }[],
    columns: Column[],
    isNamed: boolean
  ) {
    this.elements = elements;
    this.columns = columns;
    this.isNamed = isNamed;
  }

  get length(): number {
    return this.columns[0].length;
  }

  get(i: number): unknown {
    if (this.isNamed) {
      const obj: Record<string, unknown> = {};
      for (let j = 0; j < this.elements.length; j++) {
        obj[this.elements[j].name!] = this.columns[j].get(i);
      }
      return obj;
    }
    return this.columns.map(c => c.get(i));
  }

  slice(start: number, end: number): TupleColumn {
    return new TupleColumn(
      this.elements,
      this.columns.map(c => c.slice(start, end)),
      this.isNamed
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Columnar map - stores keys and values as separate columns with offsets.
 */
export class MapColumn implements BaseColumn {
  readonly offsets: BigUint64Array;
  readonly keys: Column;
  readonly values: Column;
  private mapAsArray: boolean;

  constructor(
    offsets: BigUint64Array,
    keys: Column,
    values: Column,
    mapAsArray = false
  ) {
    this.offsets = offsets;
    this.keys = keys;
    this.values = values;
    this.mapAsArray = mapAsArray;
  }

  get length(): number {
    return this.offsets.length;
  }

  get(i: number): Map<unknown, unknown> | [unknown, unknown][] {
    const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
    const end = Number(this.offsets[i]);

    if (this.mapAsArray) {
      const entries: [unknown, unknown][] = [];
      for (let j = start; j < end; j++) {
        entries.push([this.keys.get(j), this.values.get(j)]);
      }
      return entries;
    }

    const map = new Map<unknown, unknown>();
    for (let j = start; j < end; j++) {
      map.set(this.keys.get(j), this.values.get(j));
    }
    return map;
  }

  slice(start: number, end: number): MapColumn {
    const startOffset = start === 0 ? 0 : Number(this.offsets[start - 1]);
    const endOffset = Number(this.offsets[end - 1]);

    // Adjust offsets for the slice
    const newOffsets = new BigUint64Array(end - start);
    for (let i = 0; i < newOffsets.length; i++) {
      newOffsets[i] = this.offsets[start + i] - BigInt(startOffset);
    }

    return new MapColumn(
      newOffsets,
      this.keys.slice(startOffset, endOffset),
      this.values.slice(startOffset, endOffset),
      this.mapAsArray
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Base class for discriminated columns (Variant, Dynamic).
 * Stores discriminators and grouped values, with pre-computed indices for O(1) access.
 */
abstract class DiscriminatedColumn<D extends DiscriminatorArray> implements BaseColumn {
  readonly discriminators: D;
  readonly groups: Map<number, Column>;
  protected readonly groupIndices: Uint32Array;
  protected readonly nullDisc: number;

  constructor(
    discriminators: D,
    groups: Map<number, Column>,
    nullDisc: number,
    groupIndices?: Uint32Array
  ) {
    this.discriminators = discriminators;
    this.groups = groups;
    this.nullDisc = nullDisc;
    // Use provided indices or compute them (for sliced columns)
    this.groupIndices = groupIndices ?? countAndIndexDiscriminators(discriminators, nullDisc).indices;
  }

  get length(): number {
    return this.discriminators.length;
  }

  abstract get(i: number): unknown;
  abstract slice(start: number, end: number): DiscriminatedColumn<D>;

  /** Get the raw value at index i (without discriminator wrapper) */
  protected getValue(i: number): unknown {
    const d = this.discriminators[i];
    if (d === this.nullDisc) return null;
    return this.groups.get(d)!.get(this.groupIndices[i]);
  }

  /** Rebuild groups for a slice range */
  protected sliceGroups(start: number, end: number): Map<number, Column> {
    const groupValues = new Map<number, unknown[]>();
    for (let i = start; i < end; i++) {
      const d = this.discriminators[i];
      if (d !== this.nullDisc) {
        if (!groupValues.has(d)) groupValues.set(d, []);
        groupValues.get(d)!.push(this.groups.get(d)!.get(this.groupIndices[i]));
      }
    }
    const newGroups = new Map<number, Column>();
    for (const [d, values] of groupValues) {
      newGroups.set(d, new SimpleColumn(values));
    }
    return newGroups;
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Variant column - returns [discriminator, value] tuples.
 */
export class VariantColumn extends DiscriminatedColumn<Uint8Array> {
  constructor(
    discriminators: Uint8Array,
    groups: Map<number, Column>,
    groupIndices?: Uint32Array
  ) {
    super(discriminators, groups, VARIANT_NULL_DISCRIMINATOR, groupIndices);
  }

  get(i: number): [number, unknown] | null {
    const d = this.discriminators[i];
    if (d === this.nullDisc) return null;
    return [d, this.getValue(i)];
  }

  slice(start: number, end: number): VariantColumn {
    return new VariantColumn(
      this.discriminators.slice(start, end),
      this.sliceGroups(start, end)
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      const v = this.get(i);
      result[i] = v ? [v[0], materializeValue(v[1])] : null;
    }
    return result;
  }
}

/**
 * Dynamic column - returns unwrapped values directly.
 */
export class DynamicColumn extends DiscriminatedColumn<DiscriminatorArray> {
  readonly types: string[];

  constructor(
    types: string[],
    discriminators: DiscriminatorArray,
    groups: Map<number, Column>,
    groupIndices?: Uint32Array
  ) {
    super(discriminators, groups, types.length, groupIndices);
    this.types = types;
  }

  get(i: number): unknown {
    return this.getValue(i);
  }

  slice(start: number, end: number): DynamicColumn {
    const DiscCtor = this.discriminators.constructor as { new(len: number): DiscriminatorArray };
    const newDiscs = new DiscCtor(end - start);
    for (let i = 0; i < end - start; i++) {
      newDiscs[i] = this.discriminators[start + i];
    }
    return new DynamicColumn(this.types, newDiscs, this.sliceGroups(start, end));
  }
}

/**
 * Columnar JSON - stores paths with dynamic columns for each.
 */
export class JsonColumn implements BaseColumn {
  readonly paths: string[];
  readonly pathColumns: Map<string, DynamicColumn>;
  private _length: number;

  constructor(paths: string[], pathColumns: Map<string, DynamicColumn>, length: number) {
    this.paths = paths;
    this.pathColumns = pathColumns;
    this._length = length;
  }

  get length(): number {
    return this._length;
  }

  get(i: number): Record<string, unknown> {
    const obj: Record<string, unknown> = {};
    for (const path of this.paths) {
      const val = this.pathColumns.get(path)!.get(i);
      if (val !== null) obj[path] = val;
    }
    return obj;
  }

  slice(start: number, end: number): JsonColumn {
    const newPathColumns = new Map<string, DynamicColumn>();
    for (const [path, col] of this.pathColumns) {
      newPathColumns.set(path, col.slice(start, end));
    }
    return new JsonColumn(this.paths, newPathColumns, end - start);
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

/**
 * Nullable column for non-float types.
 * Wraps an inner column with null flags.
 * Inner column has same length as nullFlags (includes placeholder values at null positions).
 */
export class NullableColumn implements BaseColumn {
  readonly nullFlags: Uint8Array;
  readonly inner: Column;

  constructor(nullFlags: Uint8Array, inner: Column) {
    this.nullFlags = nullFlags;
    this.inner = inner;
  }

  get length() { return this.nullFlags.length; }

  get(i: number): unknown {
    if (this.nullFlags[i]) return null;
    return this.inner.get(i);
  }

  slice(start: number, end: number): NullableColumn {
    return new NullableColumn(
      this.nullFlags.slice(start, end),
      this.inner.slice(start, end)
    );
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      const v = this.get(i);
      result[i] = v === null ? null : materializeValue(v);
    }
    return result;
  }
}

/**
 * Array column - stores offsets and inner column.
 */
export class ArrayColumn implements BaseColumn {
  readonly offsets: BigUint64Array;
  readonly inner: Column;

  constructor(offsets: BigUint64Array, inner: Column) {
    this.offsets = offsets;
    this.inner = inner;
  }

  get length() { return this.offsets.length; }

  get(i: number): Column {
    const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
    const end = Number(this.offsets[i]);
    return this.inner.slice(start, end);
  }

  slice(start: number, end: number): ArrayColumn {
    const startOffset = start === 0 ? 0 : Number(this.offsets[start - 1]);
    const endOffset = Number(this.offsets[end - 1]);

    const newOffsets = new BigUint64Array(end - start);
    for (let i = 0; i < newOffsets.length; i++) {
      newOffsets[i] = this.offsets[start + i] - BigInt(startOffset);
    }

    return new ArrayColumn(newOffsets, this.inner.slice(startOffset, endOffset));
  }

  materialize(): unknown[] {
    const result = new Array(this.length);
    for (let i = 0; i < this.length; i++) {
      result[i] = materializeValue(this.get(i));
    }
    return result;
  }
}

// Column type alias
export type Column = BaseColumn;

// Type aliases for backwards compatibility and readability
export type StringColumn = SimpleColumn<string>;
export type BytesColumn = SimpleColumn<Uint8Array>;
export type DateColumn = SimpleColumn<Date>;
export type DateTime64Column = SimpleColumn<ClickHouseDateTime64>;
export type ScalarColumn = SimpleColumn<unknown>;
