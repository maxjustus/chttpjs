import {
  TEXT_DECODER,
  readVarInt64,
  BufferUnderflowError,
  Compression,
} from "@maxjustus/chttp/native";
import { decodeBlock } from "../compression.ts";
import { ClickHouseException } from "./types.ts";

/**
 * A streaming byte reader that handles async buffering and optional ClickHouse compression.
 * Optimized to avoid O(N^2) copies during buffering.
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

  setCompression(enabled: boolean) {
    this.compressionEnabled = enabled;
  }

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

  private async pullCompressedBlock(): Promise<void> {
    const checksum = await this.readRaw(Compression.CHECKSUM_SIZE);
    const header = await this.readRaw(Compression.HEADER_SIZE);
    
    // Header format: 1 byte method, 4 bytes compressed size, 4 bytes uncompressed size
    const compressedSizeWithHeader = new DataView(header.buffer, header.byteOffset + 1, 4).getUint32(0, true);
    const compressedDataSize = compressedSizeWithHeader - Compression.HEADER_SIZE;
    const compressedData = await this.readRaw(compressedDataSize);

    const fullBlock = new Uint8Array(Compression.FULL_HEADER_SIZE + compressedData.length);
    fullBlock.set(checksum, 0);
    fullBlock.set(header, Compression.CHECKSUM_SIZE);
    fullBlock.set(compressedData, Compression.FULL_HEADER_SIZE);

    this.feed(decodeBlock(fullBlock));
  }

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

  private feed(chunk: Uint8Array) {
    if (this.offset === this.buffer.length) {
      this.buffer = chunk;
      this.offset = 0;
    } else {
      // Consolidate remaining data with new chunk.
      // For very large buffers, we might want a chunk list, but for typical
      // ClickHouse blocks, this is acceptable compared to the previous version.
      const remaining = this.buffer.length - this.offset;
      const next = new Uint8Array(remaining + chunk.length);
      next.set(this.buffer.subarray(this.offset), 0);
      next.set(chunk, remaining);
      this.buffer = next;
      this.offset = 0;
    }
  }

  async peek(n: number): Promise<Uint8Array> {
    await this.ensure(n);
    return this.buffer.subarray(this.offset, this.offset + n);
  }

  consume(n: number): void {
    if (this.offset + n > this.buffer.length) {
      throw new Error(`Cannot consume ${n} bytes, only ${this.buffer.length - this.offset} available`);
    }
    this.offset += n;
  }

  peekAll(): Uint8Array {
    return this.buffer.subarray(this.offset);
  }

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
    // Reuse shared logic by providing a cursor-like object
    while (true) {
      const cursor = { offset: this.offset };
      try {
        const val = readVarInt64(this.buffer, cursor);
        this.offset = cursor.offset;
        return val;
      } catch (err) {
        if (err instanceof BufferUnderflowError) {
          if (this.done) throw err;
          if (this.compressionEnabled) await this.pullCompressedBlock();
          else await this.pullRawChunk();
          continue;
        }
        throw err;
      }
    }
  }

  async readString(): Promise<string> {
    const len = Number(await this.readVarInt());
    if (len === 0) return "";
    await this.ensure(len);
    const str = TEXT_DECODER.decode(this.buffer.subarray(this.offset, this.offset + len));
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

  async readException(): Promise<ClickHouseException> {
    const code = await this.readInt32LE();
    const name = await this.readString();
    const message = await this.readString();
    const stackTrace = await this.readString();
    const hasNested = (await this.readU8()) !== 0;
    const nested = hasNested ? await this.readException() : undefined;
    return new ClickHouseException(code, name, message, stackTrace, hasNested, nested);
  }

  async readCompressedBlock(): Promise<Uint8Array> {
    const checksum = await this.readFixed(Compression.CHECKSUM_SIZE);
    const header = await this.readFixed(Compression.HEADER_SIZE);
    const compressedSizeWithHeader = new DataView(header.buffer, header.byteOffset + 1, 4).getUint32(0, true);
    const compressedDataSize = compressedSizeWithHeader - Compression.HEADER_SIZE;
    const compressedData = await this.readFixed(compressedDataSize);

    const fullBlock = new Uint8Array(Compression.FULL_HEADER_SIZE + compressedData.length);
    fullBlock.set(checksum, 0);
    fullBlock.set(header, Compression.CHECKSUM_SIZE);
    fullBlock.set(compressedData, Compression.FULL_HEADER_SIZE);

    return decodeBlock(fullBlock);
  }
}
