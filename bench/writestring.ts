/**
 * Benchmark comparing writeString implementations in V8
 */

const TEXT_ENCODER = new TextEncoder();

// Current implementation: speculative 1-byte length, copyWithin for overflow
class CurrentImpl {
  private buffer: Uint8Array;
  private offset = 0;

  constructor(initialSize = 1024 * 1024) {
    this.buffer = new Uint8Array(initialSize);
  }

  private ensure(bytes: number) {
    const needed = this.offset + bytes;
    if (needed <= this.buffer.length) return;
    let newSize = this.buffer.length * 2;
    while (newSize < needed) newSize *= 2;
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
  }

  writeString(val: string) {
    const maxLen = val.length * 3;
    this.ensure(maxLen + 5);

    const lenOffset = this.offset++;
    const { written } = TEXT_ENCODER.encodeInto(
      val,
      this.buffer.subarray(this.offset, this.offset + maxLen)
    );

    if (written < 128) {
      this.buffer[lenOffset] = written;
      this.offset += written;
    } else {
      let len = written, varintSize = 1;
      while (len >= 0x80) { varintSize++; len >>>= 7; }

      this.buffer.copyWithin(
        lenOffset + varintSize,
        lenOffset + 1,
        this.offset + written
      );

      len = written;
      let pos = lenOffset;
      while (len >= 0x80) {
        this.buffer[pos++] = (len & 0x7f) | 0x80;
        len >>>= 7;
      }
      this.buffer[pos] = len;
      this.offset = lenOffset + varintSize + written;
    }
  }

  reset() { this.offset = 0; }
}

// Alternative 1: Always use TextEncoder.encode() + separate length write
class EncodeFirst {
  private buffer: Uint8Array;
  private offset = 0;

  constructor(initialSize = 1024 * 1024) {
    this.buffer = new Uint8Array(initialSize);
  }

  private ensure(bytes: number) {
    const needed = this.offset + bytes;
    if (needed <= this.buffer.length) return;
    let newSize = this.buffer.length * 2;
    while (newSize < needed) newSize *= 2;
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
  }

  writeVarint(value: number) {
    this.ensure(10);
    while (value >= 0x80) {
      this.buffer[this.offset++] = (value & 0x7f) | 0x80;
      value >>>= 7;
    }
    this.buffer[this.offset++] = value;
  }

  writeString(val: string) {
    const encoded = TEXT_ENCODER.encode(val);
    this.ensure(encoded.length + 5);
    this.writeVarint(encoded.length);
    this.buffer.set(encoded, this.offset);
    this.offset += encoded.length;
  }

  reset() { this.offset = 0; }
}

// Alternative 2: Check string length to guarantee fast path
class GuaranteedFastPath {
  private buffer: Uint8Array;
  private offset = 0;

  constructor(initialSize = 1024 * 1024) {
    this.buffer = new Uint8Array(initialSize);
  }

  private ensure(bytes: number) {
    const needed = this.offset + bytes;
    if (needed <= this.buffer.length) return;
    let newSize = this.buffer.length * 2;
    while (newSize < needed) newSize *= 2;
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
  }

  writeVarint(value: number) {
    while (value >= 0x80) {
      this.buffer[this.offset++] = (value & 0x7f) | 0x80;
      value >>>= 7;
    }
    this.buffer[this.offset++] = value;
  }

  writeString(val: string) {
    const maxLen = val.length * 3;
    this.ensure(maxLen + 5);

    // If charLength < 43, maxLen < 128, guaranteed single-byte varint
    if (val.length < 43) {
      const lenOffset = this.offset++;
      const { written } = TEXT_ENCODER.encodeInto(
        val,
        this.buffer.subarray(this.offset, this.offset + maxLen)
      );
      this.buffer[lenOffset] = written;
      this.offset += written;
    } else {
      // Longer strings: encode first, then write
      const encoded = TEXT_ENCODER.encode(val);
      this.writeVarint(encoded.length);
      this.buffer.set(encoded, this.offset);
      this.offset += encoded.length;
    }
  }

  reset() { this.offset = 0; }
}

// Alternative 3: encodeInto to temp buffer, then copy with length
class TempBuffer {
  private buffer: Uint8Array;
  private offset = 0;
  private temp: Uint8Array;

  constructor(initialSize = 1024 * 1024) {
    this.buffer = new Uint8Array(initialSize);
    this.temp = new Uint8Array(65536); // 64KB temp buffer
  }

  private ensure(bytes: number) {
    const needed = this.offset + bytes;
    if (needed <= this.buffer.length) return;
    let newSize = this.buffer.length * 2;
    while (newSize < needed) newSize *= 2;
    const newBuffer = new Uint8Array(newSize);
    newBuffer.set(this.buffer.subarray(0, this.offset));
    this.buffer = newBuffer;
  }

  private ensureTemp(bytes: number) {
    if (bytes > this.temp.length) {
      this.temp = new Uint8Array(bytes);
    }
  }

  writeVarint(value: number) {
    while (value >= 0x80) {
      this.buffer[this.offset++] = (value & 0x7f) | 0x80;
      value >>>= 7;
    }
    this.buffer[this.offset++] = value;
  }

  writeString(val: string) {
    const maxLen = val.length * 3;
    this.ensureTemp(maxLen);
    this.ensure(maxLen + 5);

    const { written } = TEXT_ENCODER.encodeInto(val, this.temp);
    this.writeVarint(written);
    this.buffer.set(this.temp.subarray(0, written), this.offset);
    this.offset += written;
  }

  reset() { this.offset = 0; }
}

// Generate test strings
function generateStrings(count: number, avgLen: number, variance: number): string[] {
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ';
  const result: string[] = [];
  for (let i = 0; i < count; i++) {
    const len = Math.max(1, Math.floor(avgLen + (Math.random() - 0.5) * variance * 2));
    let s = '';
    for (let j = 0; j < len; j++) {
      s += chars[Math.floor(Math.random() * chars.length)];
    }
    result.push(s);
  }
  return result;
}

function generateUnicodeStrings(count: number, avgLen: number): string[] {
  // Mix of ASCII and multi-byte UTF-8
  const chars = 'abcdefghijklmnopqrstuvwxyz日本語中文한국어émojis🎉🚀💻';
  const result: string[] = [];
  for (let i = 0; i < count; i++) {
    const len = Math.max(1, Math.floor(avgLen + (Math.random() - 0.5) * avgLen));
    let s = '';
    for (let j = 0; j < len; j++) {
      s += chars[Math.floor(Math.random() * chars.length)];
    }
    result.push(s);
  }
  return result;
}

// Benchmark runner
function bench(name: string, fn: () => void, iterations: number): number {
  // Warmup
  for (let i = 0; i < 100; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn();
  const elapsed = performance.now() - start;
  return elapsed;
}

// Run benchmarks
const ITERATIONS = 100;

console.log('=== writeString Benchmark ===\n');

const scenarios = [
  { name: 'Short ASCII (10 chars avg)', strings: generateStrings(10000, 10, 5) },
  { name: 'Medium ASCII (50 chars avg)', strings: generateStrings(10000, 50, 20) },
  { name: 'Long ASCII (200 chars avg)', strings: generateStrings(10000, 200, 100) },
  { name: 'Very Long ASCII (1000 chars avg)', strings: generateStrings(1000, 1000, 500) },
  { name: 'Unicode mixed (30 chars avg)', strings: generateUnicodeStrings(10000, 30) },
];

const implementations = [
  { name: 'Current (speculative)', create: () => new CurrentImpl() },
  { name: 'Encode first', create: () => new EncodeFirst() },
  { name: 'Guaranteed fast path', create: () => new GuaranteedFastPath() },
  { name: 'Temp buffer', create: () => new TempBuffer() },
];

for (const scenario of scenarios) {
  console.log(`\n${scenario.name} (${scenario.strings.length} strings):`);
  console.log('-'.repeat(60));

  const results: { name: string; time: number }[] = [];

  for (const impl of implementations) {
    const writer = impl.create();
    const time = bench(impl.name, () => {
      writer.reset();
      for (const s of scenario.strings) {
        writer.writeString(s);
      }
    }, ITERATIONS);
    results.push({ name: impl.name, time });
  }

  // Sort by time and display
  results.sort((a, b) => a.time - b.time);
  const baseline = results[0].time;

  for (const r of results) {
    const ratio = r.time / baseline;
    const marker = ratio === 1 ? '(fastest)' : `(${ratio.toFixed(2)}x)`;
    console.log(`  ${r.name.padEnd(25)} ${r.time.toFixed(2).padStart(8)}ms ${marker}`);
  }
}
