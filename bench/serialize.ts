// Benchmark: JSON serialization strategies for ClickHouse formats
//
// Approaches tried and results:
//
// JSONEachRow (objects):
//   - V1: needsEscape check before stringify  -> slower than native (branch misprediction)
//   - V2: always JSON.stringify strings       -> ~same as native
//   - V3: array.join() instead of += concat   -> slower (allocation overhead)
//   - V4: pre-built key strings               -> marginal improvement
//   - V5: codegen via new Function()          -> WINNER: 30-40% faster
//   - V6: columnar (field-by-field batching)  -> slower (cache thrashing)
//   - V7: extract columns then serialize      -> slower (multiple passes)
//   - V8: codegen batch (loop inside fn)      -> slower (JIT prefers small hot fns)
//   - V9: fast-json-stringify style escape    -> slower on escape data (closure overhead)
//   - fast-json-stringify comparison          -> similar to V5, slower on escape data
//
// JSONCompactEachRow (arrays):
//   - simple: JSON.stringify(Object.values()) -> baseline
//   - codegen: '[' + parts.join(',') + ']'    -> 10% faster
//   - template: `[${...}]` template literal   -> WINNER: V8 optimizes templates
//   - values: JSON.stringify([row.a, row.b])  -> ties with template
//   - fastesc: custom escape function         -> slower (closure overhead)
//   - batch: loop inside generated function   -> slower (JIT issue)
//   - buffer: write bytes to Uint8Array       -> 2x slower (per-byte overhead)
//
// JSONColumns (columnar):
//   - simple: build columns object, stringify -> baseline
//   - codegen: manual string building         -> slower (many small allocations)
//   - batch: JSON.stringify per column array  -> WINNER: lets V8 optimize arrays
//
// Key insights:
//   1. Template literals beat string concatenation (V8 internal optimization)
//   2. JSON.stringify for strings beats custom escape (native C++ vs JS)
//   3. Small hot functions beat large batched functions (JIT inlining)
//   4. Direct buffer writes have too much per-byte overhead
//   5. Columnar batching only wins when JSON.stringify handles whole arrays
//   6. flatstr provides no benefit - encodeInto already flattens cons strings

import fastJsonStringify from 'fast-json-stringify';
import { init, encodeBlock, Method } from '../compression.ts';

type SchemaType = 'string' | 'number' | 'boolean' | 'null' | 'object';

interface SchemaEntry {
  key: string;
  type: SchemaType;
}

function inferSchema(row: Record<string, unknown>): SchemaEntry[] {
  return Object.keys(row).map(key => ({
    key,
    type: getType(row[key]),
  }));
}

function getType(value: unknown): SchemaType {
  if (value === null) return 'null';
  const t = typeof value;
  if (t === 'string' || t === 'number' || t === 'boolean') return t;
  return 'object';
}

// === JSONEachRow: codegen via new Function() ===
// Generates: return '{"id":' + row.id + ',"name":' + JSON.stringify(row.name) + ...
function createObjectSerializer(schema: SchemaEntry[]): (row: Record<string, unknown>) => string {
  const parts: string[] = [];

  for (let i = 0; i < schema.length; i++) {
    const entry = schema[i];
    const prefix = i === 0 ? '{"' : ',"';

    if (entry.type === 'number' || entry.type === 'boolean') {
      parts.push(`'${prefix}${entry.key}":' + row.${entry.key}`);
    } else if (entry.type === 'null') {
      parts.push(`'${prefix}${entry.key}":null'`);
    } else {
      parts.push(`'${prefix}${entry.key}":' + JSON.stringify(row.${entry.key})`);
    }
  }

  const body = `return ${parts.join(' + ')} + '}'`;
  return new Function('row', body) as (row: Record<string, unknown>) => string;
}

// === JSONCompactEachRow: template literal ===
// Generates: return `[${row.id},${JSON.stringify(row.name)},...]`
function createCompactSerializer(schema: SchemaEntry[]): (row: Record<string, unknown>) => string {
  const parts: string[] = [];
  for (const entry of schema) {
    if (entry.type === 'number' || entry.type === 'boolean') {
      parts.push(`\${row.${entry.key}}`);
    } else if (entry.type === 'null') {
      parts.push('null');
    } else {
      parts.push(`\${JSON.stringify(row.${entry.key})}`);
    }
  }
  const body = `return \`[${parts.join(',')}]\``;
  return new Function('row', body) as (row: Record<string, unknown>) => string;
}

// === JSONColumns: batch with JSON.stringify per column ===
function serializeColumns(schema: SchemaEntry[], rows: Record<string, unknown>[]): string {
  const n = rows.length;
  const parts: string[] = [];

  for (const entry of schema) {
    const key = entry.key;
    let arrayStr: string;

    if (entry.type === 'number' || entry.type === 'boolean') {
      const col = new Array(n);
      for (let i = 0; i < n; i++) col[i] = rows[i][key];
      arrayStr = '[' + col.join(',') + ']';
    } else if (entry.type === 'null') {
      arrayStr = '[' + new Array(n).fill('null').join(',') + ']';
    } else {
      const col = new Array(n);
      for (let i = 0; i < n; i++) col[i] = rows[i][key];
      arrayStr = JSON.stringify(col);
    }

    parts.push(`"${key}":${arrayStr}`);
  }

  return '{' + parts.join(',') + '}';
}

// --- Benchmark infrastructure ---

function generateCleanData(count: number): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      uuid: `550e8400-e29b-41d4-a716-${String(i).padStart(12, '0')}`,
      name: `user_${i}`,
      email: `user${i}@example.com`,
      active: i % 2 === 0,
      score: Math.random() * 100,
      created_at: '2024-01-15T10:30:00Z',
      tags: null,
    });
  }
  return rows;
}

function generateEscapeData(count: number): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      name: `user "test" ${i}`,
      description: `Line1\nLine2\tTabbed`,
      path: `C:\\Users\\test\\file${i}.txt`,
    });
  }
  return rows;
}

function bench(name: string, fn: () => void, rows: number, iterations: number): void {
  // Warmup
  for (let i = 0; i < 100; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  const elapsed = performance.now() - start;

  const totalRows = rows * iterations;
  const rowsPerSec = (totalRows / elapsed) * 1000;
  console.log(`  ${name.padEnd(20)} ${elapsed.toFixed(0).padStart(5)}ms  ${(rowsPerSec / 1_000_000).toFixed(2)}M rows/sec`);
}

// --- Run benchmarks ---

const ROWS = 100_000;
const ITERATIONS = 10;

const cleanData = generateCleanData(ROWS);
const escapeData = generateEscapeData(ROWS);
const cleanSchema = inferSchema(cleanData[0]);
const escapeSchema = inferSchema(escapeData[0]);

// Build serializers
const objClean = createObjectSerializer(cleanSchema);
const objEsc = createObjectSerializer(escapeSchema);
const compactClean = createCompactSerializer(cleanSchema);
const compactEsc = createCompactSerializer(escapeSchema);

// fast-json-stringify for comparison
const fjsClean = fastJsonStringify({
  type: 'object',
  properties: {
    id: { type: 'integer' },
    uuid: { type: 'string' },
    name: { type: 'string' },
    email: { type: 'string' },
    active: { type: 'boolean' },
    score: { type: 'number' },
    created_at: { type: 'string' },
    tags: { type: 'null', nullable: true },
  }
});
const fjsEsc = fastJsonStringify({
  type: 'object',
  properties: {
    id: { type: 'integer' },
    name: { type: 'string' },
    description: { type: 'string' },
    path: { type: 'string' },
  }
});

// Verify correctness
for (const row of cleanData.slice(0, 10)) {
  const obj = objClean(row);
  const cmp = compactClean(row);
  const fjs = fjsClean(row);
  if (obj !== JSON.stringify(row)) throw new Error('Object serializer mismatch');
  if (cmp !== JSON.stringify(Object.values(row))) throw new Error('Compact serializer mismatch');
  if (fjs !== JSON.stringify(row)) throw new Error('fast-json-stringify mismatch');
}
for (const row of escapeData.slice(0, 10)) {
  const obj = objEsc(row);
  const cmp = compactEsc(row);
  if (obj !== JSON.stringify(row)) throw new Error('Object serializer mismatch (escape)');
  if (cmp !== JSON.stringify(Object.values(row))) throw new Error('Compact serializer mismatch (escape)');
}
console.log('All serializers verified correct.\n');

console.log('=== JSONEachRow (objects) - Clean data ===');
bench('JSON.stringify', () => { for (const row of cleanData) JSON.stringify(row); }, ROWS, ITERATIONS);
bench('codegen', () => { for (const row of cleanData) objClean(row); }, ROWS, ITERATIONS);
bench('fast-json-stringify', () => { for (const row of cleanData) fjsClean(row); }, ROWS, ITERATIONS);

console.log('\n=== JSONEachRow (objects) - Escape data ===');
bench('JSON.stringify', () => { for (const row of escapeData) JSON.stringify(row); }, ROWS, ITERATIONS);
bench('codegen', () => { for (const row of escapeData) objEsc(row); }, ROWS, ITERATIONS);
bench('fast-json-stringify', () => { for (const row of escapeData) fjsEsc(row); }, ROWS, ITERATIONS);

console.log('\n=== JSONCompactEachRow (arrays) - Clean data ===');
bench('JSON.stringify', () => { for (const row of cleanData) JSON.stringify(row); }, ROWS, ITERATIONS);
bench('compact template', () => { for (const row of cleanData) compactClean(row); }, ROWS, ITERATIONS);

console.log('\n=== JSONCompactEachRow (arrays) - Escape data ===');
bench('JSON.stringify', () => { for (const row of escapeData) JSON.stringify(row); }, ROWS, ITERATIONS);
bench('compact template', () => { for (const row of escapeData) compactEsc(row); }, ROWS, ITERATIONS);

console.log('\n=== JSONColumns (columnar) - Clean data ===');
bench('row-by-row JSON', () => { for (const row of cleanData) JSON.stringify(row); }, ROWS, ITERATIONS);
bench('columns batch', () => { serializeColumns(cleanSchema, cleanData); }, ROWS, ITERATIONS);

console.log('\n=== JSONColumns (columnar) - Escape data ===');
bench('row-by-row JSON', () => { for (const row of escapeData) JSON.stringify(row); }, ROWS, ITERATIONS);
bench('columns batch', () => { serializeColumns(escapeSchema, escapeData); }, ROWS, ITERATIONS);

// === Full insert path: serialize -> encode -> LZ4 compress ===
// This exercises the real path including WASM compression
await init();

console.log('\n=== Full Insert Path (serialize + encode + LZ4) ===');
const encoder = new TextEncoder();
const BUFFER_SIZE = 256 * 1024; // 256KB blocks like real insert

// Simulate the streaming insert: accumulate rows, compress when threshold hit
function benchFullPath(name: string, serialize: (row: Record<string, unknown>) => string, data: Record<string, unknown>[]) {
  const threshold = BUFFER_SIZE - 2048;

  bench(name, () => {
    let buffer = new Uint8Array(BUFFER_SIZE);
    let bufferLen = 0;
    let totalCompressed = 0;

    for (const row of data) {
      const line = serialize(row) + '\n';

      // Ensure capacity
      if (bufferLen + line.length * 3 > buffer.length) {
        const newSize = Math.max(buffer.length * 2, bufferLen + line.length * 3);
        const newBuffer = new Uint8Array(newSize);
        newBuffer.set(buffer.subarray(0, bufferLen));
        buffer = newBuffer;
      }

      const { written } = encoder.encodeInto(line, buffer.subarray(bufferLen));
      bufferLen += written;

      if (bufferLen >= threshold) {
        const compressed = encodeBlock(buffer.subarray(0, bufferLen), Method.LZ4);
        totalCompressed += compressed.length;
        bufferLen = 0;
      }
    }

    // Final block
    if (bufferLen > 0) {
      const compressed = encodeBlock(buffer.subarray(0, bufferLen), Method.LZ4);
      totalCompressed += compressed.length;
    }
  }, ROWS, ITERATIONS);
}

benchFullPath('JSON.stringify', JSON.stringify, cleanData);
benchFullPath('codegen (obj)', objClean, cleanData);
benchFullPath('template (arr)', compactClean, cleanData);

// JSONColumns - batch serialize then compress
function benchColumnPath(name: string, schema: SchemaEntry[], data: Record<string, unknown>[]) {
  bench(name, () => {
    const json = serializeColumns(schema, data);
    const bytes = encoder.encode(json);
    const compressed = encodeBlock(bytes, Method.LZ4);
  }, ROWS, ITERATIONS);
}
benchColumnPath('columns batch', cleanSchema, cleanData);

console.log('\n=== Full Insert Path - Escape data ===');
benchFullPath('JSON.stringify', JSON.stringify, escapeData);
benchFullPath('codegen (obj)', objEsc, escapeData);
benchFullPath('template (arr)', compactEsc, escapeData);
benchColumnPath('columns batch', escapeSchema, escapeData);

// Note: flatstr was tested but provides no benefit - encodeInto already flattens,
// and the extra function call overhead makes it slower (1.63M vs 1.73M for template)
