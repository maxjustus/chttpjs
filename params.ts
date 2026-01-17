/**
 * Query parameter serialization with type-aware literal formatting.
 *
 * Parses {paramName: Type} patterns from query strings and uses
 * Native format codecs to serialize values to ClickHouse literals.
 */

import { getCodec } from "./native/codecs.ts";

function skipQuotedString(query: string, i: number, quote: string): number {
  while (i < query.length) {
    if (query[i] === "\\") {
      i += 2;
    } else if (query[i] === quote) {
      // Support doubled-quote escaping: '' or "" or ``
      if (query[i + 1] === quote) {
        i += 2;
        continue;
      }
      return i + 1;
    } else {
      i++;
    }
  }
  return i;
}

function skipLineComment(query: string, i: number): number {
  while (i < query.length && query[i] !== "\n") {
    i++;
  }
  return i + 1;
}

function skipBlockComment(query: string, i: number): number {
  while (i < query.length - 1) {
    if (query[i] === "*" && query[i + 1] === "/") {
      return i + 2;
    }
    i++;
  }
  return query.length;
}

function isWhitespace(ch: string): boolean {
  return ch === " " || ch === "\t" || ch === "\n" || ch === "\r";
}

function isWordChar(ch: string): boolean {
  return (
    (ch >= "a" && ch <= "z") || (ch >= "A" && ch <= "Z") || (ch >= "0" && ch <= "9") || ch === "_"
  );
}

function skipWhitespace(query: string, i: number): number {
  while (i < query.length && isWhitespace(query[i])) {
    i++;
  }
  return i;
}

/** Handles quoted strings inside type (for Enum definitions like Enum8('foo(' = 1)) */
function parseType(query: string, start: number): [string, number] {
  let i = start;
  let depth = 0;

  while (i < query.length) {
    const ch = query[i];
    if (ch === "'" || ch === '"') {
      i = skipQuotedString(query, i + 1, ch);
    } else if (ch === "(") {
      depth++;
      i++;
    } else if (ch === ")") {
      if (depth === 0) break;
      depth--;
      i++;
    } else if (ch === "}" && depth === 0) {
      break;
    } else {
      i++;
    }
  }

  return [query.slice(start, i).trim(), i];
}

/** Returns [name, type, endIndex] or null if not a valid {name: Type} pattern */
function parseParam(query: string, i: number): [string, string, number] | null {
  i = skipWhitespace(query, i);

  const nameStart = i;
  while (i < query.length && isWordChar(query[i])) {
    i++;
  }
  if (i === nameStart) return null;

  const name = query.slice(nameStart, i);
  i = skipWhitespace(query, i);

  if (query[i] !== ":") return null;
  i++;
  i = skipWhitespace(query, i);

  const [type, typeEnd] = parseType(query, i);
  if (!type) return null;
  // Basic sanity check: ClickHouse type names start with a letter or underscore.
  // This avoids accidentally treating unrelated "{a: 1}" patterns as params.
  const firstTypeChar = type[0];
  const isTypeStart =
    firstTypeChar === "_" ||
    (firstTypeChar >= "a" && firstTypeChar <= "z") ||
    (firstTypeChar >= "A" && firstTypeChar <= "Z");
  if (!isTypeStart) return null;

  i = typeEnd;
  while (i < query.length && query[i] !== "}") {
    i++;
  }

  return [name, type, i + 1];
}

/**
 * Extract parameter types from a query string.
 * Matches {name: Type} patterns, skipping strings and comments.
 */
export function extractParamTypes(query: string): Map<string, string> {
  const result = new Map<string, string>();
  let i = 0;

  while (i < query.length) {
    const ch = query[i];

    if (ch === "'" || ch === '"' || ch === "`") {
      i = skipQuotedString(query, i + 1, ch);
    } else if (ch === "-" && query[i + 1] === "-") {
      i = skipLineComment(query, i + 2);
    } else if (ch === "/" && query[i + 1] === "*") {
      i = skipBlockComment(query, i + 2);
    } else if (ch === "{") {
      const parsed = parseParam(query, i + 1);
      if (parsed) {
        const [name, type, end] = parsed;
        const existing = result.get(name);
        if (existing !== undefined && existing !== type) {
          throw new Error(
            `Parameter '${name}' declared with conflicting types: ${existing} vs ${type}`,
          );
        }
        result.set(name, type);
        i = end;
      } else {
        i++;
      }
    } else {
      i++;
    }
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
