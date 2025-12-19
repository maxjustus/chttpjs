// Benchmark: Array initialization strategies and V8 element kinds
// Tests HOLEY vs PACKED arrays and their impact on performance

const SIZES = [1000, 10_000, 100_000, 1_000_000];
const ITERATIONS = 100;

function benchmark(
  name: string,
  fn: () => void,
  iterations: number = ITERATIONS,
): number {
  // Warmup
  for (let i = 0; i < 10; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  const end = performance.now();
  return (end - start) / iterations;
}

function runSuite(size: number) {
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

  console.log(
    `new Array(n):              ${benchmark("holey", createHoley).toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(undef):  ${benchmark("fill-undef", createFillUndefined).toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(null):   ${benchmark("fill-null", createFillNull).toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(0):      ${benchmark("fill-zero", createFillZero).toFixed(3)}ms`,
  );
  console.log(
    `new Array(n).fill(''):     ${benchmark("fill-string", createFillString).toFixed(3)}ms`,
  );
  console.log(
    `[].push() loop:            ${benchmark("push-loop", createPush).toFixed(3)}ms`,
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
    `write to holey:            ${benchmark("write-holey", writeToHoley).toFixed(3)}ms`,
  );
  console.log(
    `write to fill(null):       ${benchmark("write-fill-null", writeToFillNull).toFixed(3)}ms`,
  );
  console.log(
    `write to fill(0):          ${benchmark("write-fill-zero", writeToFillZero).toFixed(3)}ms`,
  );
  console.log(
    `push loop:                 ${benchmark("write-push", writePush).toFixed(3)}ms`,
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
    `read holey array:          ${benchmark("read-holey", readHoley).toFixed(3)}ms`,
  );
  console.log(
    `read packed array:         ${benchmark("read-packed", readPacked).toFixed(3)}ms`,
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
    `Date to holey:             ${benchmark("date-holey", writeDateToHoley).toFixed(3)}ms`,
  );
  console.log(
    `Date to fill(null):        ${benchmark("date-fill-null", writeDateToFillNull).toFixed(3)}ms`,
  );
  console.log(
    `Date to fill(shared):      ${benchmark("date-fill-shared", writeDateToFillShared).toFixed(3)}ms`,
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
    `String to holey:           ${benchmark("str-holey", writeStringToHoley).toFixed(3)}ms`,
  );
  console.log(
    `String to fill(''):        ${benchmark("str-fill-empty", writeStringToFillEmpty).toFixed(3)}ms`,
  );
  console.log(
    `String to fill(null):      ${benchmark("str-fill-null", writeStringToFillNull).toFixed(3)}ms`,
  );
}

console.log("Array Initialization Performance Benchmark");
console.log("==========================================");
console.log(`Node version: ${process.version}`);
console.log(`Iterations per test: ${ITERATIONS}`);

for (const size of SIZES) {
  runSuite(size);
}

console.log("\n\nDone.");
