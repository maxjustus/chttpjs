/**
 * Column data structures for Native format.
 * Minimal interface: length + get(i) only.
 */

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
export interface Column {
  readonly length: number;
  get(i: number): unknown;
}

export class DataColumn<T extends TypedArray | unknown[]> implements Column {
  readonly data: T;

  constructor(data: T) {
    this.data = data;
  }

  get length() { return this.data.length; }

  get(i: number): unknown {
    return (this.data as any)[i];
  }
}

export class TupleColumn implements Column {
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
    return this.columns[0]?.length ?? 0;
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
}

export class MapColumn implements Column {
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
      const entries: [unknown, unknown][] = new Array(end - start);
      for (let j = start; j < end; j++) {
        entries[j - start] = [this.keys.get(j), this.values.get(j)];
      }
      return entries;
    }

    const map = new Map<unknown, unknown>();
    for (let j = start; j < end; j++) {
      map.set(this.keys.get(j), this.values.get(j));
    }
    return map;
  }
}

export class VariantColumn implements Column {
  readonly discriminators: Uint8Array;
  readonly groups: Map<number, Column>;
  private readonly groupIndices: Uint32Array;

  constructor(
    discriminators: Uint8Array,
    groups: Map<number, Column>,
    groupIndices?: Uint32Array
  ) {
    this.discriminators = discriminators;
    this.groups = groups;
    this.groupIndices = groupIndices ?? countAndIndexDiscriminators(discriminators, VARIANT_NULL_DISCRIMINATOR).indices;
  }

  get length(): number {
    return this.discriminators.length;
  }

  get(i: number): [number, unknown] | null {
    const d = this.discriminators[i];
    if (d === VARIANT_NULL_DISCRIMINATOR) return null;
    return [d, this.groups.get(d)!.get(this.groupIndices[i])];
  }
}

export class DynamicColumn implements Column {
  readonly types: string[];
  readonly discriminators: DiscriminatorArray;
  readonly groups: Map<number, Column>;
  private readonly groupIndices: Uint32Array;
  private readonly nullDisc: number;

  constructor(
    types: string[],
    discriminators: DiscriminatorArray,
    groups: Map<number, Column>,
    groupIndices?: Uint32Array
  ) {
    this.types = types;
    this.discriminators = discriminators;
    this.groups = groups;
    this.nullDisc = types.length;
    this.groupIndices = groupIndices ?? countAndIndexDiscriminators(discriminators, this.nullDisc).indices;
  }

  get length(): number {
    return this.discriminators.length;
  }

  get(i: number): unknown {
    const d = this.discriminators[i];
    if (d === this.nullDisc) return null;
    return this.groups.get(d)!.get(this.groupIndices[i]);
  }
}

export class JsonColumn implements Column {
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
}

export class NullableColumn implements Column {
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
}

export class ArrayColumn implements Column {
  readonly offsets: BigUint64Array;
  readonly inner: Column;

  constructor(offsets: BigUint64Array, inner: Column) {
    this.offsets = offsets;
    this.inner = inner;
  }

  get length() { return this.offsets.length; }

  get(i: number): unknown[] {
    const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
    const end = Number(this.offsets[i]);
    const result = new Array(end - start);
    for (let j = start; j < end; j++) {
      result[j - start] = this.inner.get(j);
    }
    return result;
  }
}

