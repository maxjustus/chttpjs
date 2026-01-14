export type ColumnSchemaLike = {
  name: string;
};

/**
 * Transpose row objects into column arrays, validating that each row has:
 * - no missing schema keys
 * - no extra keys not present in schema
 *
 * Note: `undefined` values are treated as missing (use `null` for Nullable columns).
 */
export function transposeRowObjectsToColumns(
  schema: readonly ColumnSchemaLike[],
  rows: readonly Record<string, unknown>[],
): unknown[][] {
  const rowCount = rows.length;
  const numCols = schema.length;

  const schemaNames = new Array<string>(numCols);
  for (let i = 0; i < numCols; i++) schemaNames[i] = schema[i].name;

  const schemaNameSet = new Set(schemaNames);
  const hasOwn = Object.prototype.hasOwnProperty;

  const columns: unknown[][] = new Array(numCols);
  for (let c = 0; c < numCols; c++) columns[c] = new Array(rowCount);

  for (let r = 0; r < rowCount; r++) {
    const row = rows[r] as Record<string, unknown>;
    if (row === null || typeof row !== "object" || Array.isArray(row)) {
      throw new TypeError(`Row ${r} must be an object, got ${row === null ? "null" : typeof row}`);
    }

    for (const key in row) {
      if (!hasOwn.call(row, key)) continue;
      if (!schemaNameSet.has(key)) {
        throw new Error(`Row ${r} has unexpected key "${key}"`);
      }
    }

    for (let c = 0; c < numCols; c++) {
      const name = schemaNames[c];
      if (!hasOwn.call(row, name)) {
        throw new Error(`Row ${r} is missing required key "${name}"`);
      }
      const v = row[name];
      if (v === undefined) {
        throw new Error(`Row ${r} key "${name}" is undefined`);
      }
      columns[c][r] = v;
    }
  }

  return columns;
}
