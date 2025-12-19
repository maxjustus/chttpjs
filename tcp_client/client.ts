import * as net from "node:net";
import * as tls from "node:tls";
import { randomUUID } from "node:crypto";
import { StreamingReader } from "./reader.ts";
import { StreamingWriter } from "./writer.ts";
import {
  ServerPacketId,
  type Progress,
  type ServerHello,
  type ProfileInfo,
  type Packet,
  type LogEntry,
  DBMS_TCP_PROTOCOL_VERSION
} from "./types.ts";
import { readBlockInfo } from "./protocol_data.ts";
import { getCodec } from "../formats/native/codecs.ts";
import { BufferReader, BufferWriter, BufferUnderflowError } from "../formats/native/io.ts";
import { Table } from "../formats/native/table.ts";
import { asRows, type DeserializerState, type SerializationNode, DENSE_LEAF } from "../formats/native/index.ts";
import { init as initCompression, Method } from "../compression.ts";

export interface TcpClientOptions {
  host: string;
  port: number;
  database?: string;
  user?: string;
  password?: string;
  debug?: boolean;
  /** Compression: true/'lz4' for LZ4, 'zstd' for ZSTD, false to disable */
  compression?: boolean | 'lz4' | 'zstd';
  /** Connection timeout in ms (default: 10000) */
  connectTimeout?: number;
  /** Query timeout in ms (default: 30000) */
  queryTimeout?: number;
  /** Keep-alive interval in ms. 0 or undefined = disabled. */
  keepAliveIntervalMs?: number;
  /** TLS options. true for defaults, or tls.ConnectionOptions for custom config. */
  tls?: boolean | tls.ConnectionOptions;
  /** Grace period in ms after sending CANCEL before forceful socket close (default: 2000) */
  cancelGracePeriodMs?: number;
}

export interface ColumnSchema {
  name: string;
  type: string;
}

export class TcpClient {
  private socket: net.Socket | null = null;
  private reader: StreamingReader | null = null;
  private writer: StreamingWriter = new StreamingWriter();
  private options: TcpClientOptions;
  private _serverHello: ServerHello | null = null;
  private currentSchema: ColumnSchema[] | null = null;
  private sessionTimezone: string | null = null;
  private busy: boolean = false;

  private log(...args: any[]) {
    if (this.options.debug) {
      console.log("[TcpClient]", ...args);
    }
  }

  /** Write with backpressure - waits for drain if socket buffer is full */
  private async writeWithBackpressure(data: Uint8Array): Promise<void> {
    if (!this.socket!.write(data)) {
      await new Promise<void>(resolve => this.socket!.once('drain', resolve));
    }
  }

  /** Server info from handshake, available after connect() */
  get serverHello() { return this._serverHello; }

  /** Session timezone, updated by server TimezoneUpdate packets */
  get timezone(): string | null { return this.sessionTimezone; }

  constructor(options: TcpClientOptions) {
    this.options = {
      database: "default",
      user: "default",
      password: "",
      debug: false,
      compression: false,
      ...options
    };
  }

  async connect(options: { signal?: AbortSignal } = {}): Promise<void> {
    const signal = options.signal;
    if (signal?.aborted) throw new Error("Connect aborted before start");

    await initCompression();
    const timeout = this.options.connectTimeout ?? 10000;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let settled = false;

    const cleanup = () => {
      settled = true;
      if (timeoutId) clearTimeout(timeoutId);
      signal?.removeEventListener("abort", abortHandler);
    };

    const abortHandler = () => {
      if (!settled) {
        cleanup();
        this.socket?.destroy();
      }
    };
    signal?.addEventListener("abort", abortHandler);

    const connectPromise = new Promise<void>((resolve, reject) => {
      const onConnected = async () => {
        try {
          if (signal?.aborted) {
            reject(new Error("Connect aborted"));
            return;
          }
          this.reader = new StreamingReader(this.socket!);
          await this.handshake();
          this.startKeepAliveTimer();
          resolve();
        } catch (err) {
          reject(err);
        }
      };

      const tlsOpts = this.options.tls;
      if (tlsOpts) {
        const opts: tls.ConnectionOptions = {
          host: this.options.host,
          port: this.options.port,
          ...(typeof tlsOpts === 'object' ? tlsOpts : {})
        };
        this.socket = tls.connect(opts, onConnected);
      } else {
        this.socket = net.connect(this.options.port, this.options.host);
        this.socket.on("connect", onConnected);
      }

      this.socket.on("error", (err) => reject(err));
    });

    const timeoutPromise = new Promise<void>((_, reject) => {
      timeoutId = setTimeout(() => {
        this.socket?.destroy();
        reject(new Error(`Connection timeout after ${timeout}ms`));
      }, timeout);
    });

    const abortPromise = new Promise<void>((_, reject) => {
      if (signal) {
        signal.addEventListener("abort", () => reject(new Error("Connect aborted")), { once: true });
      }
    });

    try {
      await Promise.race(signal ? [connectPromise, timeoutPromise, abortPromise] : [connectPromise, timeoutPromise]);
    } finally {
      cleanup();
    }
  }

  private async handshake() {
    if (!this.socket || !this.reader) throw new Error("Not connected");

    this.log("Handshake: Sending Hello...");
    const hello = this.writer.encodeHello(
      this.options.database!,
      this.options.user!,
      this.options.password!
    );
    this.socket.write(hello);

    const packetId = Number(await this.reader.readVarInt());
    if (packetId === ServerPacketId.Exception) {
      throw await this.reader.readException();
    }

    if (packetId !== ServerPacketId.Hello) {
      throw new Error(`Unexpected packet during handshake: ${packetId}`);
    }

    const serverName = await this.reader.readString();
    const major = await this.reader.readVarInt();
    const minor = await this.reader.readVarInt();
    const revision = await this.reader.readVarInt();

    const effectiveRevision = revision < DBMS_TCP_PROTOCOL_VERSION ? revision : DBMS_TCP_PROTOCOL_VERSION;

    if (effectiveRevision >= 54471n) await this.reader.readVarInt();

    const timezone = effectiveRevision >= 54058n ? await this.reader.readString() : "";
    const displayName = effectiveRevision >= 54372n ? await this.reader.readString() : "";
    const patch = effectiveRevision >= 54401n ? await this.reader.readVarInt() : effectiveRevision;

    if (effectiveRevision >= 54470n) {
      await this.reader.readString();
      await this.reader.readString();
    }

    if (effectiveRevision >= 54461n) {
      const rulesSize = Number(await this.reader.readVarInt());
      for (let i = 0; i < rulesSize; i++) {
        await this.reader.readString();
        await this.reader.readString();
      }
    }

    if (effectiveRevision >= 54462n) await this.reader.readU64LE();

    if (effectiveRevision >= 54474n) {
      while (true) {
        const name = await this.reader.readString();
        if (name === "") break;
        await this.reader.readVarInt();
        await this.reader.readString();
      }
    }

    if (effectiveRevision >= 54477n) await this.reader.readVarInt();
    if (effectiveRevision >= 54479n) await this.reader.readVarInt();

    this._serverHello = { serverName, major, minor, revision: effectiveRevision, timezone, displayName, patch };

    if (effectiveRevision >= 54458n) {
      const addendum = this.writer.encodeAddendum(effectiveRevision);
      this.socket.write(addendum);
    }
    this.log("Handshake: Complete!");
  }

  async execute(sql: string): Promise<void> {
    for await (const _ of this.query(sql)) { }
  }

  async insert(
    sql: string,
    data: Table | AsyncIterable<Table> | Iterable<Table>,
    options: { signal?: AbortSignal } = {}
  ) {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");
    if (this.busy) throw new Error("Connection busy - cannot run concurrent operations on the same TcpClient");
    this.busy = true;

    const signal = options.signal;
    if (signal?.aborted) throw new Error("Insert aborted before start");

    let cancelled = false;
    const abortHandler = () => {
      if (!cancelled && this.socket) {
        cancelled = true;
        this.socket.write(this.writer.encodeCancel());
      }
    };
    signal?.addEventListener("abort", abortHandler);

    try {
      const useCompression = !!this.options.compression;
      const compressionMethod = this.options.compression === 'zstd' ? Method.ZSTD : Method.LZ4;

      const queryPacket = this.writer.encodeQuery(randomUUID(), sql, this.serverHello.revision, {}, useCompression, {});
      this.socket.write(queryPacket);

      const queryDelimiter = this.writer.encodeData("", 0, [], this.serverHello.revision, useCompression, compressionMethod);
      this.socket.write(queryDelimiter);

      let schemaReceived = false;
      while (!schemaReceived) {
        if (cancelled) throw new Error("Insert cancelled");
        const packetId = Number(await this.reader.readVarInt());

        switch (packetId) {
          case ServerPacketId.Data: {
            const block = await this.readBlock(useCompression);
            this.currentSchema = block.columns.map(c => ({ name: c.name, type: c.type }));
            schemaReceived = true;
            break;
          }
          case ServerPacketId.Progress: await this.readProgress(); break;
          case ServerPacketId.Log: await this.readBlock(false); break;
          case 11: // TableColumns
            await this.reader.readString();
            await this.reader.readString();
            break;
          case ServerPacketId.Exception:
            throw await this.reader.readException();
          default:
            throw new Error(`Unexpected packet while waiting for insert header: ${packetId}`);
        }
      }

      const blocks = (data instanceof Table)
        ? [data]
        : (data as AsyncIterable<Table> | Iterable<Table>);

      let totalInserted = 0;
      for await (const table of blocks) {
        if (cancelled) throw new Error("Insert cancelled");

        const encodedColumns = [];
        for (let i = 0; i < table.columns.length; i++) {
          const colDef = table.columns[i];
          const colData = table.columnData[i];
          const codec = getCodec(colDef.type);

          const writer = new BufferWriter();
          codec.writePrefix?.(writer, colData);
          const data = codec.encode(colData);
          writer.write(data);

          encodedColumns.push({
            name: colDef.name,
            type: colDef.type,
            data: writer.finish()
          });
        }

        const dataPacket = this.writer.encodeData("", table.rowCount, encodedColumns, this.serverHello.revision, useCompression, compressionMethod);
        await this.writeWithBackpressure(dataPacket);
        totalInserted += table.rowCount;
      }

      const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision, useCompression, compressionMethod);
      this.socket.write(delimiter);

      while (true) {
        const packetId = Number(await this.reader.readVarInt());
        if (packetId === ServerPacketId.EndOfStream) break;

        switch (packetId) {
          case ServerPacketId.Progress: await this.readProgress(); break;
          case ServerPacketId.ProfileInfo: await this.readProfileInfo(); break;
          case ServerPacketId.Data:
            await this.readBlock(useCompression);
            break;
          case ServerPacketId.Log:
          case ServerPacketId.ProfileEvents:
            await this.readBlock(false);
            break;
          case ServerPacketId.Exception:
            throw await this.reader.readException();
        }
      }
      this.log(`Successfully inserted ${totalInserted} rows.`);
    } finally {
      this.busy = false;
      signal?.removeEventListener("abort", abortHandler);
    }
  }

  private async readProgress(): Promise<Progress> {
    const progress: Progress = {
      readRows: await this.reader!.readVarInt(),
      readBytes: await this.reader!.readVarInt(),
      totalRowsToRead: this.serverHello!.revision >= 54406n ? await this.reader!.readVarInt() : 0n,
    };
    if (this.serverHello!.revision >= 54463n) progress.totalBytesToRead = await this.reader!.readVarInt();
    if (this.serverHello!.revision >= 54420n) {
      progress.writtenRows = await this.reader!.readVarInt();
      progress.writtenBytes = await this.reader!.readVarInt();
    }
    if (this.serverHello!.revision >= 54460n) progress.elapsedNs = await this.reader!.readVarInt();
    return progress;
  }

  private async readProfileInfo(): Promise<ProfileInfo> {
    const info: ProfileInfo = {
      rows: await this.reader!.readVarInt(),
      blocks: await this.reader!.readVarInt(),
      bytes: await this.reader!.readVarInt(),
      appliedLimit: (await this.reader!.readU8()) !== 0,
      rowsBeforeLimit: await this.reader!.readVarInt(),
      calculatedRowsBeforeLimit: (await this.reader!.readU8()) !== 0,
      appliedAggregation: false,
      rowsBeforeAggregation: 0n,
    };
    if (this.serverHello!.revision >= 54469n) {
      info.appliedAggregation = (await this.reader!.readU8()) !== 0;
      info.rowsBeforeAggregation = await this.reader!.readVarInt();
    }
    return info;
  }

  private parseLogBlock(table: Table): LogEntry[] {
    const entries: LogEntry[] = [];
    for (const row of asRows(table)) {
      entries.push({
        time: row.event_time as string,
        timeMicroseconds: row.event_time_microseconds as number,
        hostName: row.host_name as string,
        queryId: row.query_id as string,
        threadId: row.thread_id as bigint,
        priority: row.priority as number,
        source: row.source as string,
        text: row.text as string,
      });
    }
    return entries;
  }

  private async readBlock(compressed: boolean = false): Promise<Table> {
    // Table name is always uncompressed, even when block data is compressed
    await this.reader!.readString(); // tableName - required by protocol but unused

    // For compressed blocks, decompress first then parse from buffer
    if (compressed) {
      const decompressed = await this.reader!.readCompressedBlock();
      return this.parseCompressedBlockFromBuffer(new BufferReader(decompressed));
    }

    // For uncompressed, stream data and parse incrementally
    await readBlockInfo(this.reader!);
    const columnsCount = Number(await this.reader!.readVarInt());
    const rowsCount = Number(await this.reader!.readVarInt());

    const columns: ColumnSchema[] = [];
    const columnData = [];

    for (let i = 0; i < columnsCount; i++) {
      const colName = await this.reader!.readString();
      const colType = await this.reader!.readString();
      columns.push({ name: colName, type: colType });

      const codec = getCodec(colType);

      // Use BufferUnderflowError retry pattern for streaming reads
      while (true) {
        const currentBuffer = this.reader!.peekAll();
        const tempReader = new BufferReader(currentBuffer);
        try {
          // Read serialization kind tree if supported
          let serNode: SerializationNode = DENSE_LEAF;
          if (this.serverHello!.revision >= 54454n) {
            const hasCustom = tempReader.readU8() !== 0;
            if (hasCustom) {
              serNode = codec.readKinds(tempReader);
            }
          }

          const state: DeserializerState = { serNode, sparseRuntime: new Map() };

          if (rowsCount === 0) {
            this.reader!.consume(tempReader.offset);
            columnData.push(codec.decode(new BufferReader(new Uint8Array(0)), 0, state));
            break;
          }

          codec.readPrefix?.(tempReader);
          const data = codec.decode(tempReader, rowsCount, state);
          this.reader!.consume(tempReader.offset);
          columnData.push(data);
          break;
        } catch (err) {
          if (err instanceof BufferUnderflowError) {
            const more = await this.reader!.nextChunk();
            if (!more) throw new Error(`EOF while decoding block column ${colName} (${colType})`);
            continue;
          }
          throw err;
        }
      }
    }

    return new Table({
      columns: columns.map(c => ({ name: c.name, type: c.type })),
      columnData,
      rowCount: rowsCount
    });
  }

  /**
   * Parse a decompressed block from a BufferReader.
   * Note: Table name is read before decompression, not included in the compressed data.
   */
  private parseCompressedBlockFromBuffer(reader: BufferReader): Table {
    // Block info (table name was already read before decompression)
    while (true) {
      const fieldNum = reader.readVarint();
      if (fieldNum === 0) break;
      if (fieldNum === 1) reader.offset += 1;
      else if (fieldNum === 2) reader.offset += 4;
    }

    const columnsCount = reader.readVarint();
    const rowsCount = reader.readVarint();

    const columns: ColumnSchema[] = [];
    const columnData = [];

    for (let i = 0; i < columnsCount; i++) {
      const colName = reader.readString();
      const colType = reader.readString();
      columns.push({ name: colName, type: colType });

      const codec = getCodec(colType);

      let serNode: SerializationNode = DENSE_LEAF;
      if (this.serverHello!.revision >= 54454n) {
        const hasCustom = reader.readU8() !== 0;
        if (hasCustom) {
          serNode = codec.readKinds(reader);
        }
      }

      const state: DeserializerState = { serNode, sparseRuntime: new Map() };

      codec.readPrefix?.(reader);
      columnData.push(codec.decode(reader, rowsCount, state));
    }

    return new Table({
      columns: columns.map(c => ({ name: c.name, type: c.type })),
      columnData,
      rowCount: rowsCount
    });
  }

  async *query(
    sql: string,
    settings: Record<string, string | number | boolean> = {},
    options: { signal?: AbortSignal; params?: Record<string, string | number | boolean> } = {}
  ): AsyncGenerator<Packet> {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");
    if (this.busy) throw new Error("Connection busy - cannot run concurrent operations on the same TcpClient");
    this.busy = true;

    const signal = options?.signal;
    if (signal?.aborted) throw new Error("Query aborted before start");

    const useCompression = !!this.options.compression;
    const compressionMethod = this.options.compression === 'zstd' ? Method.ZSTD : Method.LZ4;
    const queryTimeout = this.options.queryTimeout ?? 30000;
    const cancelGracePeriod = this.options.cancelGracePeriodMs ?? 2000;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let graceTimeoutId: ReturnType<typeof setTimeout> | null = null;
    let timedOut = false;
    let cancelled = false;

    const startTimeout = () => {
      if (queryTimeout > 0) {
        timeoutId = setTimeout(() => {
          timedOut = true;
          // First try graceful cancel
          if (this.socket && !cancelled) {
            cancelled = true;
            this.socket.write(this.writer.encodeCancel());
          }
          // Give server grace period to respond, then force close
          graceTimeoutId = setTimeout(() => {
            if (timedOut) {
              this.socket?.destroy();
            }
          }, cancelGracePeriod);
        }, queryTimeout);
      }
    };

    const clearQueryTimeout = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      if (graceTimeoutId) {
        clearTimeout(graceTimeoutId);
        graceTimeoutId = null;
      }
    };

    const abortHandler = () => {
      if (!cancelled && this.socket) {
        cancelled = true;
        this.socket.write(this.writer.encodeCancel());
      }
    };

    signal?.addEventListener("abort", abortHandler);

    try {
      try {
        startTimeout();

        // The compression flag in the query packet enables bidirectional compression:
        // - When 1: client sends compressed Data blocks, server sends compressed Data blocks
        // - When 0: both sides send uncompressed
        const queryPacket = this.writer.encodeQuery(randomUUID(), sql, this.serverHello.revision, {
          "allow_special_serialization_kinds_in_output_formats": "0",
          ...settings
        }, useCompression, options.params ?? {});
        this.log(`[query] sending query packet (${queryPacket.length} bytes), compression=${useCompression}`);
        this.socket.write(queryPacket);

        // Send delimiter (compressed if compression is enabled)
        const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision, useCompression, compressionMethod);
        this.log(`[query] sending delimiter (${delimiter.length} bytes, compressed=${useCompression})`);
        this.socket.write(delimiter);

        this.currentSchema = null;
        this.log(`[query] waiting for response...`);

        // Accumulate ProfileEvents deltas
        const profileEventsAccumulated = new Map<string, bigint>();

        while (true) {
          this.log(`[query] reading packet id...`);
          const packetId = Number(await this.reader.readVarInt());
          if (timedOut) throw new Error(`Query timeout after ${queryTimeout}ms`);
          this.log(`[query] packetId=${packetId}, useCompression=${useCompression}`);

          switch (packetId) {
            case ServerPacketId.Data: {
              // With compression=1, ALL Data blocks from server are compressed
              this.log(`[query] reading Data block (compressed=${useCompression})...`);
              const table = await this.readBlock(useCompression);
              this.log(`[query] got Data block with ${table.rowCount} rows`);
              if (this.currentSchema === null) {
                this.currentSchema = table.columns.map(c => ({ name: c.name, type: c.type }));
              }
              if (table.rowCount > 0) yield { type: "Data", table };
              break;
            }
            case ServerPacketId.Progress:
              yield { type: "Progress", progress: await this.readProgress() };
              break;
            case ServerPacketId.ProfileInfo:
              yield { type: "ProfileInfo", info: await this.readProfileInfo() };
              break;
            case ServerPacketId.ProfileEvents: {
              // ProfileEvents blocks are always uncompressed (diagnostic metadata)
              // They send deltas, so we accumulate values across packets
              const table = await this.readBlock(false);
              const nameCol = table.getColumn("name");
              const valueCol = table.getColumn("value");
              const typeCol = table.getColumn("type");
              if (nameCol && valueCol && typeCol) {
                for (let i = 0; i < table.rowCount; i++) {
                  const name = nameCol.get(i) as string;
                  const value = valueCol.get(i) as bigint;
                  const eventType = typeCol.get(i) as number; // 1=increment, 2=gauge
                  if (eventType === 1) {
                    // Increment: sum values
                    profileEventsAccumulated.set(name, (profileEventsAccumulated.get(name) ?? 0n) + value);
                  } else {
                    // Gauge: use latest value
                    profileEventsAccumulated.set(name, value);
                  }
                }
              }
              yield { type: "ProfileEvents", table, accumulated: profileEventsAccumulated };
              break;
            }
            case ServerPacketId.Totals:
              yield { type: "Totals", table: await this.readBlock(useCompression) };
              break;
            case ServerPacketId.Extremes:
              yield { type: "Extremes", table: await this.readBlock(useCompression) };
              break;
            case ServerPacketId.Log: {
              // Log blocks are always uncompressed (diagnostic metadata)
              const table = await this.readBlock(false);
              if (table.rowCount > 0) {
                yield { type: "Log", entries: this.parseLogBlock(table) };
              }
              break;
            }
            case ServerPacketId.TimezoneUpdate:
              this.sessionTimezone = await this.reader.readString();
              this.log(`[query] timezone updated to: ${this.sessionTimezone}`);
              break;
            case ServerPacketId.EndOfStream:
              yield { type: "EndOfStream" };
              return;
            case ServerPacketId.Exception:
              throw await this.reader.readException();
            default:
              throw new Error(`Unknown packet ID: ${packetId}. Cannot proceed.`);
          }
        }
      } catch (err: any) {
        if (timedOut && (err.message === "Premature close" || err.code === "ERR_STREAM_PREMATURE_CLOSE")) {
          throw new Error(`Query timeout after ${queryTimeout}ms`);
        }
        throw err;
      }
    } finally {
      this.busy = false;
      clearQueryTimeout();
      signal?.removeEventListener("abort", abortHandler);
    }
  }

  /**
   * Send a ping packet and wait for pong response.
   * Useful for checking connection health.
   */
  async ping(): Promise<void> {
    if (!this.socket || !this.reader) throw new Error("Not connected");

    this.socket.write(this.writer.encodePing());
    const packetId = Number(await this.reader.readVarInt());
    if (packetId !== ServerPacketId.Pong) {
      throw new Error(`Expected Pong (4), got packet ${packetId}`);
    }
  }

  private startKeepAliveTimer(): void {
    const interval = this.options.keepAliveIntervalMs;
    if (interval && interval > 0 && this.socket) {
      // Use TCP-level keep-alive - this is the proper way to maintain connections
      // The interval is in milliseconds, setKeepAlive expects milliseconds for initialDelay
      this.socket.setKeepAlive(true, interval);
    }
  }

  close() {
    this.socket?.destroy();
    this.socket = null;
  }

  /**
   * Async disposable support for "await using" syntax.
   * Automatically closes connection when scope exits.
   */
  async [Symbol.asyncDispose](): Promise<void> {
    this.close();
  }

  /**
   * Static factory that connects and returns a disposable client.
   * Usage: await using client = await TcpClient.connect(options);
   */
  static async connect(options: TcpClientOptions, connectOptions: { signal?: AbortSignal } = {}): Promise<TcpClient> {
    const client = new TcpClient(options);
    await client.connect(connectOptions);
    return client;
  }
}
