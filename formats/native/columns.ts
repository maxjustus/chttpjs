import { type TypedArray } from "../shared.ts";
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
 * Supports virtual random access and iteration.
 */
export interface Column extends Iterable<unknown> {
  readonly length: number;
  /** Get value at index. */
  get(index: number): unknown;
}

/**
 * Common implementation for iteration.
 */
abstract class AbstractColumn implements Column {
  abstract readonly length: number;
  abstract get(index: number): unknown;

  *[Symbol.iterator](): Iterator<unknown> {
    for (let i = 0; i < this.length; i++) {
      yield this.get(i);
    }
  }
}

export class DataColumn<T extends TypedArray | unknown[]> extends AbstractColumn {
  readonly data: T;

  constructor(data: T) {
    super();
    this.data = data;
  }

  get length() { return this.data.length; }

  get(index: number): unknown {
    return this.data[index];
  }
}

export class TupleColumn extends AbstractColumn {
  readonly elements: { name: string | null }[];
  readonly columns: Column[];
  readonly isNamed: boolean;

  constructor(
    elements: { name: string | null }[],
    columns: Column[],
    isNamed: boolean
  ) {
    super();
    this.elements = elements;
    this.columns = columns;
    this.isNamed = isNamed;
  }

  get length(): number {
    return this.columns[0]?.length ?? 0;
  }

  get(index: number): unknown {
    const numElements = this.elements.length;
    if (this.isNamed) {
      const obj: Record<string, unknown> = {};
      for (let j = 0; j < numElements; j++) {
        obj[this.elements[j].name!] = this.columns[j].get(index);
      }
      return obj;
    } else {
      const arr = new Array(numElements);
      for (let j = 0; j < numElements; j++) {
        arr[j] = this.columns[j].get(index);
      }
      return arr;
    }
  }
}

export class MapColumn extends AbstractColumn {
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
    super();
    this.offsets = offsets;
    this.keys = keys;
    this.values = values;
    this.mapAsArray = mapAsArray;
  }

  get length(): number {
    return this.offsets.length;
  }

  get(index: number): Map<unknown, unknown> | [unknown, unknown][] {
    const start = index === 0 ? 0 : Number(this.offsets[index - 1]);
    const end = Number(this.offsets[index]);

    if (this.mapAsArray) {
      const entries: [unknown, unknown][] = new Array(end - start);
      for (let j = start; j < end; j++) {
        entries[j - start] = [this.keys.get(j), this.values.get(j)];
      }
      return entries;
    } else {
      const map = new Map<unknown, unknown>();
      for (let j = start; j < end; j++) {
        map.set(this.keys.get(j), this.values.get(j));
      }
      return map;
    }
  }
}

export class VariantColumn extends AbstractColumn {
  readonly discriminators: Uint8Array;
  readonly groups: Map<number, Column>;
  private readonly groupIndices: Uint32Array;

  constructor(
    discriminators: Uint8Array,
    groups: Map<number, Column>,
    groupIndices?: Uint32Array
  ) {
    super();
    this.discriminators = discriminators;
    this.groups = groups;
    this.groupIndices = groupIndices ?? countAndIndexDiscriminators(discriminators, VARIANT_NULL_DISCRIMINATOR).indices;
  }

  get length(): number {
    return this.discriminators.length;
  }

  get(index: number): [number, unknown] | null {
    const d = this.discriminators[index];
    if (d === VARIANT_NULL_DISCRIMINATOR) return null;
    return [d, this.groups.get(d)!.get(this.groupIndices[index])];
  }
}

export class DynamicColumn extends AbstractColumn {
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
    super();
    this.types = types;
    this.discriminators = discriminators;
    this.groups = groups;
    this.nullDisc = types.length;
    this.groupIndices = groupIndices ?? countAndIndexDiscriminators(discriminators, this.nullDisc).indices;
  }

  get length(): number {
    return this.discriminators.length;
  }

  get(index: number): unknown {
    const d = this.discriminators[index];
    if (d === this.nullDisc) return null;
    return this.groups.get(d)!.get(this.groupIndices[index]);
  }
}

export class JsonColumn extends AbstractColumn {
  readonly paths: string[];
  readonly pathColumns: Map<string, DynamicColumn>;
  private _length: number;

  constructor(
    paths: string[],
    pathColumns: Map<string, DynamicColumn>,
    length: number
  ) {
    super();
    this.paths = paths;
    this.pathColumns = pathColumns;
    this._length = length;
  }

  get length(): number {
    return this._length;
  }

  get(index: number): Record<string, unknown> {
    const obj: Record<string, unknown> = {};
    for (const path of this.paths) {
      const val = this.pathColumns.get(path)!.get(index);
      if (val !== null) obj[path] = val;
    }
    return obj;
  }

  /** Get a specific JSON path as its own virtual column. */
  getPath(path: string): DynamicColumn | undefined {
    return this.pathColumns.get(path);
  }
}

export class NullableColumn extends AbstractColumn {
  readonly nullFlags: Uint8Array;
  readonly inner: Column;

  constructor(
    nullFlags: Uint8Array,
    inner: Column
  ) {
    super();
    this.nullFlags = nullFlags;
    this.inner = inner;
  }

  get length() { return this.nullFlags.length; }

  get(index: number): unknown {
    return this.nullFlags[index] ? null : this.inner.get(index);
  }
}

export class ArrayColumn extends AbstractColumn {
  readonly offsets: BigUint64Array;
  readonly inner: Column;

  constructor(
    offsets: BigUint64Array,
    inner: Column
  ) {
    super();
    this.offsets = offsets;
    this.inner = inner;
  }

  get length() { return this.offsets.length; }

  get(index: number): unknown[] {
    const start = index === 0 ? 0 : Number(this.offsets[index - 1]);
    const end = Number(this.offsets[index]);
    const result = new Array(end - start);
    for (let j = 0; j < end - start; j++) {
      result[j] = this.inner.get(start + j);
    }
    return result;
  }
}
