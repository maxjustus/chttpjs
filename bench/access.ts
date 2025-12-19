// Benchmark: Different ways to access columnar data
//
// Compares the performance of:
// 1. toArrayRows() - loops using get(i) for all columns
// 2. hybrid - direct data access for DataColumn, get(i) for others
// 3. asRows() - lazy row generation using get(i)
// 4. Table iteration - using the new Table class with Proxy rows

import {
  decodeNative,
  toArrayRows,
  asRows,
  type Block,
  Table,
  tableFromRows,
  encodeNative,
  type ColumnDef,
} from "../native/index.ts";
import { DataColumn } from "../native/columns.ts";

function encodeNativeRows(columns: ColumnDef[], rows: unknown[][]): Uint8Array {
  return encodeNative(tableFromRows(columns, rows));
}

function bench(
  name: string,
  fn: () => void,
  warmup = 50,
  iterations = 100,
): { name: string; ms: number } {
  for (let i = 0; i < warmup; i++) fn();
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  return { name, ms: (performance.now() - start) / iterations };
}

function collectRows(gen: Iterable<any>): unknown[] {
  const rows: any[] = [];
  for (const row of gen) rows.push(row);
  return rows;
}

function collectRowsTable(table: Table): unknown[] {
  const rows: unknown[] = [];
  for (const row of table) {
    // Access at least one property to ensure proxy overhead is measured
    const _ = row.id;
    rows.push(row);
  }
  return rows;
}

function collectRowsTableFull(table: Table): unknown[] {
  const rows: unknown[] = [];
  for (const row of table) {
    rows.push(row.toObject());
  }
  return rows;
}

// Generate test data
function generateSimpleData(count: number): {
  columns: ColumnDef[];
  rows: unknown[][];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "x", type: "Float64" },
    { name: "y", type: "Float64" },
    { name: "z", type: "Float64" },
  ];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    rows.push([i, Math.random(), Math.random(), Math.random()]);
  }
  return { columns, rows };
}

function generateMixedData(count: number): {
  columns: ColumnDef[];
  rows: unknown[][];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "name", type: "String" },
    { name: "email", type: "String" },
    { name: "active", type: "Bool" },
    { name: "score", type: "Float64" },
  ];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    rows.push([
      i,
      `user_${i}`,
      `user${i}@example.com`,
      i % 2 === 0,
      Math.random() * 100,
    ]);
  }
  return { columns, rows };
}

function generateComplexData(count: number): {
  columns: ColumnDef[];
  rows: unknown[][];
} {
  const columns: ColumnDef[] = [
    { name: "id", type: "UInt32" },
    { name: "tags", type: "Array(String)" },
    { name: "scores", type: "Array(Float64)" },
    { name: "metadata", type: "Nullable(String)" },
  ];
  const rows: unknown[][] = [];
  for (let i = 0; i < count; i++) {
    const tags = [`tag_${i % 5}`, `cat_${i % 3}`, `type_${i % 7}`];
    const scores = Array.from({ length: 10 }, () => Math.random() * 100);
    const metadata = i % 3 === 0 ? null : `meta_${i}`;
    rows.push([i, tags, scores, metadata]);
  }
  return { columns, rows };
}

function toArrayRowsHybrid(result: Block): unknown[][] {
  const { columnData, rowCount } = result;
  const numCols = columnData.length;

  const cols: unknown[][] = columnData.map((col) => {
    if (col instanceof DataColumn) {
      return col.data as unknown[];
    }
    // For non-DataColumn, we have to use get(i) loop since toArray() is gone
    const arr = new Array(col.length);
    for (let i = 0; i < col.length; i++) arr[i] = col.get(i);
    return arr;
  });

  const rows: unknown[][] = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    const row = new Array(numCols);
    for (let j = 0; j < numCols; j++) {
      row[j] = cols[j][i];
    }
    rows[i] = row;
  }
  return rows;
}

async function main() {
  const ROWS = 10_000;
  const ITERATIONS = 100;

  console.log(
    `Benchmarking data access with ${ROWS} rows, ${ITERATIONS} iterations\n`,
  );

  const scenarios = [
    { name: "Simple numeric (4 cols)", ...generateSimpleData(ROWS) },
    { name: "Mixed types (5 cols)", ...generateMixedData(ROWS) },
    { name: "Complex nested (4 cols)", ...generateComplexData(ROWS) },
  ];

  for (const scenario of scenarios) {
    console.log(`=== ${scenario.name} ===\n`);

    const encoded = encodeNativeRows(scenario.columns, scenario.rows);
    const decoded = await decodeNative(encoded);

    console.log("Materializing to rows:");
    const toArrayResult = bench(
      "  toArrayRows() (get loop)",
      () => toArrayRows(decoded),
      50,
      ITERATIONS,
    );
    const hybridToArray = bench(
      "  hybrid (direct + get)",
      () => toArrayRowsHybrid(decoded),
      50,
      ITERATIONS,
    );
    console.log(
      `  ${toArrayResult.name.padEnd(25)} ${toArrayResult.ms.toFixed(3).padStart(8)}ms`,
    );
    console.log(
      `  ${hybridToArray.name.padEnd(25)} ${hybridToArray.ms.toFixed(3).padStart(8)}ms`,
    );
    console.log(
      `  Hybrid speedup: ${(toArrayResult.ms / hybridToArray.ms).toFixed(2)}x\n`,
    );

    console.log("asRows() (collected):");
    const asRowsResult = bench(
      "  asRows()",
      () => collectRows(asRows(decoded)),
      50,
      ITERATIONS,
    );
    console.log(
      `  ${asRowsResult.name.padEnd(25)} ${asRowsResult.ms.toFixed(3).padStart(8)}ms\n`,
    );

    console.log("Table (Proxy rows):");
    const table = Table.from(decoded);
    const tableResult = bench(
      "  Table iter (access 1)",
      () => collectRowsTable(table),
      50,
      ITERATIONS,
    );
    const tableFullResult = bench(
      "  Table iter (toObject)",
      () => collectRowsTableFull(table),
      50,
      ITERATIONS,
    );
    console.log(
      `  ${tableResult.name.padEnd(25)} ${tableResult.ms.toFixed(3).padStart(8)}ms`,
    );
    console.log(
      `  ${tableFullResult.name.padEnd(25)} ${tableFullResult.ms.toFixed(3).padStart(8)}ms\n`,
    );
  }
}

main().catch(console.error);
