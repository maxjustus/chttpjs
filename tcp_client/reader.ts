
import { TEXT_DECODER } from "../formats/shared.ts";
import { decodeBlock } from "../compression.ts";
import { BufferReader } from "../formats/native/io.ts";
import { ClickHouseException } from "./types.ts";

/**
 * A streaming byte reader that handles async buffering and optional ClickHouse compression.
 */
export class StreamingReader {
  private source: AsyncIterator<Uint8Array>;
  private buffer: Uint8Array = new Uint8Array(0);
  private offset: number = 0;
  private done: boolean = false;
  private compressionEnabled: boolean = false;

  constructor(iterable: AsyncIterable<Uint8Array>) {
    this.source = iterable[Symbol.asyncIterator]();
  }

  /**
   * Enable/disable transport-level decompression.
   */
  setCompression(enabled: boolean) {
    this.compressionEnabled = enabled;
  }

  /**
   * Ensures at least 'n' bytes are available in the buffer.
   * Pulls from the source iterator as needed.
   */
  private async ensure(n: number): Promise<void> {
    while (this.buffer.length - this.offset < n) {
      if (this.done) {
        throw new Error(`Unexpected end of stream: needed ${n} bytes, only ${this.buffer.length - this.offset} available`);
      }

      if (this.compressionEnabled) {
        await this.pullCompressedBlock();
      } else {
        await this.pullRawChunk();
      }
    }
  }

  private async pullRawChunk(): Promise<void> {
    const { value, done } = await this.source.next();
    if (done) {
      this.done = true;
      return;
    }
    this.feed(value);
  }

  /**
   * Reads exactly one compressed block from the source, decompresses it,
   * and appends it to our logical buffer.
   */
  private async pullCompressedBlock(): Promise<void> {
    // 1. Read 16-byte checksum
    const checksum = await this.readRaw(16);
    // 2. Read 1-byte method + 4-byte compressed size + 4-byte uncompressed size
    const header = await this.readRaw(9);
    
    const compressedSizeWithHeader = new DataView(header.buffer, header.byteOffset + 1, 4).getUint32(0, true);
    const compressedDataSize = compressedSizeWithHeader - 9;
    
    // 3. Read compressed data
    const compressedData = await this.readRaw(compressedDataSize);

    // Combine into a single block for decodeBlock()
    const fullBlock = new Uint8Array(16 + 9 + compressedData.length);
    fullBlock.set(checksum, 0);
    fullBlock.set(header, 16);
    fullBlock.set(compressedData, 25);

    const decompressed = decodeBlock(fullBlock);
    this.feed(decompressed);
  }

  /**
   * Low-level read directly from the source bypasses logical buffering.
   * Used only during compressed block framing.
   */
  private async readRaw(n: number): Promise<Uint8Array> {
    while (this.buffer.length - this.offset < n) {
      const { value, done } = await this.source.next();
      if (done) throw new Error("EOF while reading compressed block header");
      this.feed(value);
    }
    const res = this.buffer.subarray(this.offset, this.offset + n);
    this.offset += n;
    return res;
  }

  /**
   * Add a new chunk of data to the logical buffer.
   */
  private feed(chunk: Uint8Array) {
    if (this.offset === this.buffer.length) {
      this.buffer = chunk;
      this.offset = 0;
    } else {
      const next = new Uint8Array((this.buffer.length - this.offset) + chunk.length);
      next.set(this.buffer.subarray(this.offset));
      next.set(chunk, this.buffer.length - this.offset);
      this.buffer = next;
      this.offset = 0;
    }
  }

  /**
   * Returns a view of the next 'n' bytes without advancing the offset.
   * Pulls from source if necessary.
   */
  async peek(n: number): Promise<Uint8Array> {
    await this.ensure(n);
    return this.buffer.subarray(this.offset, this.offset + n);
  }

  /**
   * Advances the offset by 'n' bytes.
   */
  consume(n: number): void {
    if (this.offset + n > this.buffer.length) {
      throw new Error(`Cannot consume ${n} bytes, only ${this.buffer.length - this.offset} available`);
    }
    this.offset += n;
  }

  /**
   * Returns all currently buffered bytes.
   */
  peekAll(): Uint8Array {
    return this.buffer.subarray(this.offset);
  }

  /**
   * Pulls the next chunk from the source and appends it to the buffer.
   */
  async nextChunk(): Promise<boolean> {
    if (this.done) return false;
    const { value, done } = await this.source.next();
    if (done) {
      this.done = true;
      return false;
    }
    this.feed(value);
    return true;
  }

  async readVarInt(): Promise<bigint> {
    let result = 0n;
    let shift = 0n;
    while (true) {
      await this.ensure(1);
      const byte = this.buffer[this.offset++];
      result |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) break;
      shift += 7n;
    }
    return result;
  }

  async readString(): Promise<string> {
    const len = Number(await this.readVarInt());
    if (len === 0) return "";
    await this.ensure(len);
    const bytes = this.buffer.subarray(this.offset, this.offset + len);
    const str = TEXT_DECODER.decode(bytes);
    this.offset += len;
    return str;
  }

  async readFixed(n: number): Promise<Uint8Array> {
    await this.ensure(n);
    const bytes = this.buffer.slice(this.offset, this.offset + n);
    this.offset += n;
    return bytes;
  }

  async readU8(): Promise<number> {
    await this.ensure(1);
    return this.buffer[this.offset++];
  }

  async readU32LE(): Promise<number> {
    await this.ensure(4);
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 4);
    const val = view.getUint32(0, true);
    this.offset += 4;
    return val;
  }

  async readInt32LE(): Promise<number> {
    await this.ensure(4);
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 4);
    const val = view.getInt32(0, true);
    this.offset += 4;
    return val;
  }

  async readU64LE(): Promise<bigint> {
    await this.ensure(8);
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 8);
    const val = view.getBigUint64(0, true);
    this.offset += 8;
    return val;
  }

  /**
   * Reads a ClickHouse exception from the stream.
   * Format: code (i32), name (String), message (String), stack_trace (String), has_nested (u8)
   */
  async readException(): Promise<ClickHouseException> {
    const code = await this.readInt32LE();
    const name = await this.readString();
    const message = await this.readString();
    const stackTrace = await this.readString();
    const hasNested = (await this.readU8()) !== 0;
    const nested = hasNested ? await this.readException() : undefined;
    return new ClickHouseException(code, name, message, stackTrace, hasNested, nested);
  }

  /**
   * Reads one compressed block from the stream, decompresses it, and returns
   * the decompressed data. Used for reading compressed Data packets.
   *
   * Format: [16-byte checksum][1-byte method][4-byte compressed size][4-byte uncompressed size][compressed data]
   */
  async readCompressedBlock(): Promise<Uint8Array> {
    // Read checksum (16 bytes)
    await this.ensure(16);
    const checksum = this.buffer.slice(this.offset, this.offset + 16);
    this.offset += 16;

    // Read header (9 bytes): method + compressed size + uncompressed size
    await this.ensure(9);
    const header = this.buffer.slice(this.offset, this.offset + 9);
    this.offset += 9;

    // Parse compressed size (includes header size)
    const compressedSizeWithHeader = new DataView(header.buffer, header.byteOffset + 1, 4).getUint32(0, true);
    const compressedDataSize = compressedSizeWithHeader - 9;

    // Read compressed data
    await this.ensure(compressedDataSize);
    const compressedData = this.buffer.slice(this.offset, this.offset + compressedDataSize);
    this.offset += compressedDataSize;

    // Combine into full block for decodeBlock
    const fullBlock = new Uint8Array(16 + 9 + compressedData.length);
    fullBlock.set(checksum, 0);
    fullBlock.set(header, 16);
    fullBlock.set(compressedData, 25);

    return decodeBlock(fullBlock);
  }
}
