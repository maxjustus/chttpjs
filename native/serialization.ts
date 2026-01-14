/**
 * Shared serialization state/types used by ClickHouse Native codecs and block decode.
 *
 * This module exists to avoid core implementation files (e.g. `codecs.ts`)
 * depending on the public barrel (`index.ts`), which can create accidental
 * circular dependencies and makes internal refactors harder.
 */

/**
 * Node in the serialization tree. Tracks kind (dense/sparse) for each position
 * in the type tree.
 *
 * Tree structure mirrors the type hierarchy:
 * - Leaf types (UInt32, String, etc.): no children
 * - Array(T): 1 child for inner type
 * - Nullable(T): 1 child for inner type
 * - Map(K, V): 2 children [key, value]
 * - Tuple(T1, T2, ...): N children, one per element
 * - Variant(T1, T2, ...): N children, one per variant
 * - JSON: M children for typed/dynamic paths (dynamic paths use fallback)
 */
export interface SerializationNode {
  /** 0 = Dense, 1 = Sparse (see `SerializationKind` in `constants.ts`) */
  kind: number;
  children: SerializationNode[];
}

/**
 * Default serialization node representing dense (non-sparse) encoding.
 *
 * ClickHouse Native format supports sparse serialization (v54454+) where
 * columns with many default/zero values encode only non-default positions.
 * The server sends a tree of "kind" bytes (0=dense, 1=sparse) matching the
 * type structure before column data.
 *
 * This constant is used when:
 * - Server doesn't use custom serialization (hasCustomSerialization=false)
 * - Accessing child nodes that don't exist in the tree (fallback)
 * - Creating fresh state for inner decoding during sparse materialization
 */
export const DEFAULT_DENSE_NODE: SerializationNode = { kind: 0, children: [] };

/**
 * State maintained during a block deserialization.
 */
export interface DeserializerState {
  serNode: SerializationNode;
  /**
   * Tracks partial sparse groups across granules/blocks.
   *
   * Map key is the `SerializationNode` reference, value is:
   * - `trailing_defaults`: number of default rows that carry over
   * - `has_value_after_defaults`: whether a non-default follows those defaults
   */
  sparseRuntime: Map<SerializationNode, [number, boolean]>;
}
