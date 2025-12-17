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
  /** Materialize all values to a plain array. Faster than calling get() in a loop. */
  toArray(): unknown[];
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

  toArray(): unknown[] {
    // TypedArrays need Array.from; plain arrays can be returned directly
    if (Array.isArray(this.data)) return this.data;
    // Handle BigInt64Array/BigUint64Array and numeric typed arrays
    const arr = this.data as TypedArray;
    const result = new Array(arr.length);
    for (let i = 0; i < arr.length; i++) result[i] = arr[i];
    return result;
  }
}

export class TupleColumn implements Column {
  readonly elements: { name: string | null }[];
  readonly columns: Column[];
  readonly isNamed: boolean;
  private _cached?: unknown[];

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
    return this.toArray()[i];
  }

  toArray(): unknown[] {
    if (this._cached) return this._cached;

    const len = this.length;
    const numElements = this.elements.length;
    const childArrays = this.columns.map(c => c.toArray());
    const result = new Array(len);

    if (this.isNamed) {
      for (let i = 0; i < len; i++) {
        const obj: Record<string, unknown> = {};
        for (let j = 0; j < numElements; j++) {
          obj[this.elements[j].name!] = childArrays[j][i];
        }
        result[i] = obj;
      }
    } else {
      for (let i = 0; i < len; i++) {
        const arr = new Array(numElements);
        for (let j = 0; j < numElements; j++) {
          arr[j] = childArrays[j][i];
        }
        result[i] = arr;
      }
    }
    return this._cached = result;
  }
}

export class MapColumn implements Column {
  readonly offsets: BigUint64Array;
  readonly keys: Column;
  readonly values: Column;
  private mapAsArray: boolean;
  private _cached?: (Map<unknown, unknown> | [unknown, unknown][])[];

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
    return this.toArray()[i];
  }

  toArray(): (Map<unknown, unknown> | [unknown, unknown][])[] {
    if (this._cached) return this._cached;

    const len = this.offsets.length;
    const keysArr = this.keys.toArray();
    const valsArr = this.values.toArray();
    const result = new Array(len);

    for (let i = 0; i < len; i++) {
      const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
      const end = Number(this.offsets[i]);

      if (this.mapAsArray) {
        const entries: [unknown, unknown][] = new Array(end - start);
        for (let j = start; j < end; j++) {
          entries[j - start] = [keysArr[j], valsArr[j]];
        }
        result[i] = entries;
      } else {
        const map = new Map<unknown, unknown>();
        for (let j = start; j < end; j++) {
          map.set(keysArr[j], valsArr[j]);
        }
        result[i] = map;
      }
    }
    return this._cached = result;
  }
}

export class VariantColumn implements Column {
  readonly discriminators: Uint8Array;
  readonly groups: Map<number, Column>;
  private readonly groupIndices: Uint32Array;
  private _cached?: ([number, unknown] | null)[];

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
    return this.toArray()[i];
  }

  toArray(): ([number, unknown] | null)[] {
    if (this._cached) return this._cached;

    const len = this.discriminators.length;
    const groupArrays = new Map<number, unknown[]>();
    for (const [d, col] of this.groups) {
      groupArrays.set(d, col.toArray());
    }

    const result = new Array(len);
    for (let i = 0; i < len; i++) {
      const d = this.discriminators[i];
      if (d === VARIANT_NULL_DISCRIMINATOR) {
        result[i] = null;
      } else {
        result[i] = [d, groupArrays.get(d)![this.groupIndices[i]]];
      }
    }
    return this._cached = result;
  }
}

export class DynamicColumn implements Column {
  readonly types: string[];
  readonly discriminators: DiscriminatorArray;
  readonly groups: Map<number, Column>;
  private readonly groupIndices: Uint32Array;
  private readonly nullDisc: number;
  private _cached?: unknown[];

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
    return this.toArray()[i];
  }

  toArray(): unknown[] {
    if (this._cached) return this._cached;

    const len = this.discriminators.length;
    const groupArrays = new Map<number, unknown[]>();
    for (const [d, col] of this.groups) {
      groupArrays.set(d, col.toArray());
    }

    const result = new Array(len);
    for (let i = 0; i < len; i++) {
      const d = this.discriminators[i];
      if (d === this.nullDisc) {
        result[i] = null;
      } else {
        result[i] = groupArrays.get(d)![this.groupIndices[i]];
      }
    }
    return this._cached = result;
  }
}

export class JsonColumn implements Column {
  readonly paths: string[];
  readonly pathColumns: Map<string, DynamicColumn>;
  private _length: number;
  private _cached?: Record<string, unknown>[];

  constructor(paths: string[], pathColumns: Map<string, DynamicColumn>, length: number) {
    this.paths = paths;
    this.pathColumns = pathColumns;
    this._length = length;
  }

  get length(): number {
    return this._length;
  }

  get(i: number): Record<string, unknown> {
    return this.toArray()[i];
  }

  toArray(): Record<string, unknown>[] {
    if (this._cached) return this._cached;

    const len = this._length;
    const pathArrays = new Map<string, unknown[]>();
    for (const path of this.paths) {
      pathArrays.set(path, this.pathColumns.get(path)!.toArray());
    }

    const result = new Array(len);
    for (let i = 0; i < len; i++) {
      const obj: Record<string, unknown> = {};
      for (const path of this.paths) {
        const val = pathArrays.get(path)![i];
        if (val !== null) obj[path] = val;
      }
      result[i] = obj;
    }
    return this._cached = result;
  }
}

export class NullableColumn implements Column {
  readonly nullFlags: Uint8Array;
  readonly inner: Column;
  private _cached?: unknown[];

  constructor(nullFlags: Uint8Array, inner: Column) {
    this.nullFlags = nullFlags;
    this.inner = inner;
  }

  get length() { return this.nullFlags.length; }

  get(i: number): unknown {
    return this.toArray()[i];
  }

  toArray(): unknown[] {
    if (this._cached) return this._cached;

    const innerArr = this.inner.toArray();
    const len = this.nullFlags.length;
    const result = new Array(len);
    for (let i = 0; i < len; i++) {
      result[i] = this.nullFlags[i] ? null : innerArr[i];
    }
    return this._cached = result;
  }
}

export class ArrayColumn implements Column {
  readonly offsets: BigUint64Array;
  readonly inner: Column;
  private _cached?: unknown[][];

  constructor(offsets: BigUint64Array, inner: Column) {
    this.offsets = offsets;
    this.inner = inner;
  }

  get length() { return this.offsets.length; }

  get(i: number): unknown[] {
    return this.toArray()[i];
  }

  toArray(): unknown[][] {
    if (this._cached) return this._cached;

    const len = this.offsets.length;
    const innerArr = this.inner.toArray();
    const result = new Array(len);

    for (let i = 0; i < len; i++) {
      const start = i === 0 ? 0 : Number(this.offsets[i - 1]);
      const end = Number(this.offsets[i]);
      result[i] = innerArr.slice(start, end);
    }
    return this._cached = result;
  }
}

