/**
 * Shared types for query parameter handling.
 */

/** Query parameter value - supports primitives and complex types */
export type QueryParamValue =
  | string
  | number
  | boolean
  | bigint
  | null
  | Date
  | QueryParamValue[]
  | { [key: string]: QueryParamValue };

/** Query parameters for parameterized queries like SELECT {x:UInt64} */
export type QueryParams = Record<string, QueryParamValue>;
