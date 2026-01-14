export type ColumnSchemaLike = {
  name: string;
};

/**
 * Transpose row objects into column arrays with JSONEachRow-like behavior:
 * - Unknown keys are ignored
 * - Missing keys are treated as omitted (value becomes `undefined`)
 * - `undefined` values are treated as omitted (value remains `undefined`)
 *
 * Codecs handle omitted values by inserting defaults for non-nullable types and
 * NULL for Nullable types (matching common ClickHouse JSONEachRow defaults).
 */
export function transposeRowObjectsToColumns(
  schema: readonly ColumnSchemaLike[],
  rows: readonly Record<string, unknown>[],
): unknown[][] {
  const rowCount = rows.length;
  const numCols = schema.length;

  const schemaNames = new Array<string>(numCols);
  for (let i = 0; i < numCols; i++) schemaNames[i] = schema[i].name;

  const hasOwn = Object.prototype.hasOwnProperty;

  const columns: unknown[][] = new Array(numCols);
  for (let c = 0; c < numCols; c++) columns[c] = new Array(rowCount);

  for (let r = 0; r < rowCount; r++) {
    const row = rows[r] as Record<string, unknown>;
    if (row === null || typeof row !== "object" || Array.isArray(row)) {
      throw new TypeError(`Row ${r} must be an object, got ${row === null ? "null" : typeof row}`);
    }

    for (let c = 0; c < numCols; c++) {
      const name = schemaNames[c];
      columns[c][r] = hasOwn.call(row, name) ? row[name] : undefined;
    }
  }

  return columns;
}
