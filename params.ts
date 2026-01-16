/**
 * Query parameter serialization with type-aware literal formatting.
 *
 * Parses {paramName: Type} patterns from query strings and uses
 * Native format codecs to serialize values to ClickHouse literals.
 */

import { getCodec } from "./native/codecs.ts";

/**
 * Extract parameter types from a query string.
 * Matches {name: Type} patterns, handling nested parentheses.
 *
 * @example
 * extractParamTypes("SELECT {ids: Array(UInt64)}, {name: String}")
 * // => Map { 'ids' => 'Array(UInt64)', 'name' => 'String' }
 */
export function extractParamTypes(query: string): Map<string, string> {
  const result = new Map<string, string>();

  // State machine to track:
  // - Whether we're inside a string literal (skip matching there)
  // - Brace depth for nested types like Array(Tuple(Int32, String))
  let i = 0;
  const len = query.length;

  while (i < len) {
    const ch = query[i];

    // Skip single-quoted string literals
    if (ch === "'") {
      i++;
      while (i < len) {
        if (query[i] === "\\") {
          i += 2; // Skip escaped char
        } else if (query[i] === "'") {
          i++;
          break;
        } else {
          i++;
        }
      }
      continue;
    }

    // Look for opening brace
    if (ch === "{") {
      let start = i + 1;

      // Skip leading whitespace after {
      while (start < len && /\s/.test(query[start])) {
        start++;
      }

      // Find parameter name (alphanumeric + underscore)
      let nameEnd = start;
      while (nameEnd < len && /[\w]/.test(query[nameEnd])) {
        nameEnd++;
      }

      if (nameEnd === start) {
        // No name found, skip this brace
        i++;
        continue;
      }

      const name = query.slice(start, nameEnd);

      // Skip whitespace
      let typeStart = nameEnd;
      while (typeStart < len && /\s/.test(query[typeStart])) {
        typeStart++;
      }

      // Expect colon
      if (query[typeStart] !== ":") {
        i++;
        continue;
      }
      typeStart++;

      // Skip whitespace after colon
      while (typeStart < len && /\s/.test(query[typeStart])) {
        typeStart++;
      }

      // Parse type with nested parentheses
      let typeEnd = typeStart;
      let parenDepth = 0;

      while (typeEnd < len) {
        const c = query[typeEnd];
        if (c === "(") {
          parenDepth++;
          typeEnd++;
        } else if (c === ")") {
          if (parenDepth === 0) break;
          parenDepth--;
          typeEnd++;
        } else if (c === "}" && parenDepth === 0) {
          break;
        } else {
          typeEnd++;
        }
      }

      const type = query.slice(typeStart, typeEnd).trim();

      if (type) {
        // Check for conflicting type declarations
        const existing = result.get(name);
        if (existing !== undefined && existing !== type) {
          throw new Error(
            `Parameter '${name}' declared with conflicting types: ${existing} vs ${type}`,
          );
        }
        result.set(name, type);
      }

      // Move past the closing brace
      i = typeEnd;
      while (i < len && query[i] !== "}") i++;
      i++;
      continue;
    }

    i++;
  }

  return result;
}

/**
 * Serialize query parameters to ClickHouse literal strings.
 *
 * Parses the query to extract parameter types, then uses Native format
 * codecs to serialize each value with proper type-aware formatting.
 *
 * @param query - SQL query with {name: Type} parameter placeholders
 * @param params - Parameter values to serialize
 * @returns Serialized parameters as string values
 *
 * @example
 * serializeParams(
 *   "SELECT {ids: Array(UInt64)}, {point: Tuple(Int32, String)}",
 *   { ids: [1, 2, 3], point: [10, 'hello'] }
 * )
 * // => { ids: '[1, 2, 3]', point: "(10, 'hello')" }
 */
export function serializeParams(
  query: string,
  params: Record<string, unknown>,
): Record<string, string> {
  const types = extractParamTypes(query);
  const result: Record<string, string> = {};

  for (const [name, value] of Object.entries(params)) {
    const type = types.get(name);
    if (type === undefined) {
      // Param not in query - silently ignore (matches clickhouse-js behavior)
      continue;
    }
    const codec = getCodec(type);
    result[name] = codec.toLiteral(value);
  }

  // Check for missing required params
  for (const [name, type] of types) {
    if (!(name in result)) {
      throw new Error(`Missing parameter: ${name} (type: ${type})`);
    }
  }

  return result;
}
