// Benchmark: Array initialization strategies and V8 element kinds
// Tests HOLEY vs PACKED arrays and their impact on performance

import { benchSync, readBenchOptions, reportEnvironment } from "./harness.ts";

const SIZES = [1000, 10_000, 100_000, 1_000_000];
const ITERATIONS = 100;

function runSuite(size: number, benchOptions: { iterations: number; warmup: number }) {
  console.log(`\n=== Size: ${size.toLocaleString()} ===\n`);

  // --- Creation benchmarks ---
  console.log("--- Array Creation ---");

  const createHoley = () => new Array(size);
  const createFillUndefined = () => new Array(size).fill(undefined);
  const createFillNull = () => new Array(size).fill(null);
  const createFillZero = () => new Array(size).fill(0);
  const createFillString = () => new Array(size).fill("");
  const createPush = () => {
    const arr = [];
    for (let i = 0; i < size; i++) arr.push(null);
    return arr;
  };

  const opts = { ...benchOptions, iterations: benchOptions.iterations };
  console.log(
    `new Array(n):              ${benchSync("holey", createHoley, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(undef):  ${benchSync("fill-undef", createFillUndefined, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(null):   ${benchSync("fill-null", createFillNull, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(0):      ${benchSync("fill-zero", createFillZero, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(''):     ${benchSync("fill-string", createFillString, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `[].push() loop:            ${benchSync("push-loop", createPush, opts).meanMs.toFixed(3)}ms`,
  );

  // --- Write benchmarks (simulating decode loop) ---
  console.log("\n--- Sequential Write (decode pattern) ---");

  const writeToHoley = () => {
    const arr = new Array(size);
    for (let i = 0; i < size; i++) arr[i] = i;
    return arr;
  };

  const writeToFillNull = () => {
    const arr = new Array(size).fill(null);
    for (let i = 0; i < size; i++) arr[i] = i;
    return arr;
  };

  const writeToFillZero = () => {
    const arr = new Array(size).fill(0);
    for (let i = 0; i < size; i++) arr[i] = i;
    return arr;
  };

  const writePush = () => {
    const arr: number[] = [];
    for (let i = 0; i < size; i++) arr.push(i);
    return arr;
  };

  console.log(
    `write to holey:            ${benchSync("write-holey", writeToHoley, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `write to fill(null):       ${benchSync("write-fill-null", writeToFillNull, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `write to fill(0):          ${benchSync("write-fill-zero", writeToFillZero, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `push loop:                 ${benchSync("write-push", writePush, opts).meanMs.toFixed(3)}ms`,
  );

  // --- Read benchmarks (after array is populated) ---
  console.log("\n--- Sequential Read (post-decode iteration) ---");

  // Create arrays once for read tests
  const holeyArr = new Array(size);
  for (let i = 0; i < size; i++) holeyArr[i] = i;

  const packedArr = new Array(size).fill(0);
  for (let i = 0; i < size; i++) packedArr[i] = i;

  const readHoley = () => {
    let sum = 0;
    for (let i = 0; i < holeyArr.length; i++) sum += holeyArr[i];
    return sum;
  };

  const readPacked = () => {
    let sum = 0;
    for (let i = 0; i < packedArr.length; i++) sum += packedArr[i];
    return sum;
  };

  console.log(
    `read holey array:          ${benchSync("read-holey", readHoley, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `read packed array:         ${benchSync("read-packed", readPacked, opts).meanMs.toFixed(3)}ms`,
  );

  // --- Reference type benchmarks ---
  console.log("\n--- Reference Types (Date) ---");

  const writeDateToHoley = () => {
    const arr = new Array(size);
    for (let i = 0; i < size; i++) arr[i] = new Date(i);
    return arr;
  };

  const writeDateToFillNull = () => {
    const arr = new Array(size).fill(null);
    for (let i = 0; i < size; i++) arr[i] = new Date(i);
    return arr;
  };

  const writeDateToFillShared = () => {
    const arr = new Array(size).fill(new Date(0)); // shared ref
    for (let i = 0; i < size; i++) arr[i] = new Date(i);
    return arr;
  };

  console.log(
    `Date to holey:             ${benchSync("date-holey", writeDateToHoley, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `Date to fill(null):        ${benchSync("date-fill-null", writeDateToFillNull, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `Date to fill(shared):      ${benchSync("date-fill-shared", writeDateToFillShared, opts).meanMs.toFixed(3)}ms`,
  );

  // --- String type benchmarks (matching StringCodec pattern) ---
  console.log("\n--- String Types ---");

  const writeStringToHoley = () => {
    const arr = new Array(size);
    for (let i = 0; i < size; i++) arr[i] = `str${i}`;
    return arr;
  };

  const writeStringToFillEmpty = () => {
    const arr = new Array(size).fill("");
    for (let i = 0; i < size; i++) arr[i] = `str${i}`;
    return arr;
  };

  const writeStringToFillNull = () => {
    const arr = new Array(size).fill(null);
    for (let i = 0; i < size; i++) arr[i] = `str${i}`;
    return arr;
  };

  console.log(
    `String to holey:           ${benchSync("str-holey", writeStringToHoley, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `String to fill(''):        ${benchSync("str-fill-empty", writeStringToFillEmpty, opts).meanMs.toFixed(3)}ms`,
  );
  console.log(
    `String to fill(null):      ${benchSync("str-fill-null", writeStringToFillNull, opts).meanMs.toFixed(3)}ms`,
  );
}

console.log("Array Initialization Performance Benchmark");
console.log("==========================================");
reportEnvironment();
const benchOptions = readBenchOptions({ iterations: ITERATIONS, warmup: 10 });
console.log(`Iterations per test: ${benchOptions.iterations ?? ITERATIONS}`);

for (const size of SIZES) {
  runSuite(size, { iterations: benchOptions.iterations ?? ITERATIONS, warmup: benchOptions.warmup ?? 10 });
}

console.log("\n\nDone.");
