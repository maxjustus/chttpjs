/**
 * Shared types for query parameter handling.
 */

import type { ClickHouseDateTime64 } from "./native/types.ts";

/** Query parameter value - supports primitives and complex types */
export type QueryParamValue =
  | string
  | number
  | boolean
  | bigint
  | null
  | Date
  | Uint8Array
  | ClickHouseDateTime64
  | Map<unknown, QueryParamValue>
  | QueryParamValue[]
  | { [key: string]: QueryParamValue };

/** Query parameters for parameterized queries like SELECT {x:UInt64} */
export type QueryParams = Record<string, QueryParamValue>;
