import * as net from "node:net";
import * as tls from "node:tls";
import { randomUUID } from "node:crypto";
import { StreamingReader } from "./reader.ts";
import { StreamingWriter } from "./writer.ts";
import {
  ServerPacketId,
  type Progress,
  type AccumulatedProgress,
  type ServerHello,
  type ProfileInfo,
  type Packet,
  type LogEntry,
  DBMS_TCP_PROTOCOL_VERSION,
  REVISIONS,
} from "./types.ts";
import {
  getCodec,
  BufferWriter,
  BufferUnderflowError,
  RecordBatch,
  decodeNativeBlock,
  type ColumnDef,
} from "@maxjustus/chttp/native";
import { init as initCompression, Method, type MethodCode } from "../compression.ts";
import type { ClickHouseSettings } from "../settings.ts";
import { collectable, type CollectableAsyncGenerator } from "../util.ts";

export type { CollectableAsyncGenerator } from "../util.ts";

/**
 * Duck-type check for RecordBatch-like objects.
 * Using instanceof fails across bundle boundaries (each bundle has its own class).
 */
function isRecordBatch(obj: unknown): obj is RecordBatch {
  return (
    obj !== null &&
    typeof obj === "object" &&
    "columns" in obj &&
    "columnData" in obj &&
    "rowCount" in obj
  );
}

export interface TcpClientOptions {
  host: string;
  port: number;
  database?: string;
  user?: string;
  password?: string;
  debug?: boolean;
  /** Compression: 'lz4', 'zstd', or false to disable */
  compression?: 'lz4' | 'zstd' | false;
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
  /** Default settings applied to all queries and inserts (can be overridden per-call) */
  settings?: ClickHouseSettings;
}

export interface ColumnSchema {
  name: string;
  type: string;
}

export interface InsertOptions {
  signal?: AbortSignal;
  /** Batch size for row object mode (default: 10000) */
  batchSize?: number;
  /** Optional schema to validate against server schema */
  schema?: ColumnDef[];
  /** Per-insert settings (merged with client defaults, overrides them) */
  settings?: ClickHouseSettings;
  /** Custom query ID for tracking in system.query_log and KILL QUERY */
  queryId?: string;
}

/** Data that can be sent as an external table */
export type ExternalTableData = RecordBatch | Iterable<RecordBatch> | AsyncIterable<RecordBatch>;

export interface QueryOptions {
  /** Per-query settings (merged with client defaults, overrides them) */
  settings?: ClickHouseSettings;
  /** Query parameters (substitution values) */
  params?: Record<string, string | number | boolean | bigint>;
  signal?: AbortSignal;
  /** External tables to send with the query (available as temporary tables in the SQL) */
  externalTables?: Record<string, ExternalTableData>;
  /** Custom query ID for tracking in system.query_log and KILL QUERY */
  queryId?: string;
}


/** Validates that expected schema matches server schema exactly. */
function validateSchema(expected: ColumnDef[], actual: ColumnSchema[]): void {
  if (expected.length !== actual.length) {
    throw new Error(`Schema mismatch: expected ${expected.length} columns, got ${actual.length}`);
  }
  for (let i = 0; i < expected.length; i++) {
    if (expected[i].name !== actual[i].name) {
      throw new Error(`Schema mismatch: column ${i} expected name '${expected[i].name}', got '${actual[i].name}'`);
    }
    if (expected[i].type !== actual[i].type) {
      throw new Error(`Schema mismatch: column '${expected[i].name}' expected type '${expected[i].type}', got '${actual[i].type}'`);
    }
  }
}

export class TcpClient {
  private socket: net.Socket | null = null;
  private reader: StreamingReader | null = null;
  private writer: StreamingWriter = new StreamingWriter();
  private options: TcpClientOptions;
  private defaultSettings: ClickHouseSettings;
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
    this.defaultSettings = options.settings ?? {};
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

    // Use minimum of our supported version and server version
    const effectiveRevision = revision < DBMS_TCP_PROTOCOL_VERSION ? revision : DBMS_TCP_PROTOCOL_VERSION;

    if (effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL) {
      // Server-side parallel replicas protocol version
      await this.reader.readVarInt();
    }

    const timezone = effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE ? await this.reader.readString() : "";
    const displayName = effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME ? await this.reader.readString() : "";
    const patch = effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_VERSION_PATCH ? await this.reader.readVarInt() : effectiveRevision;

    if (effectiveRevision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS) {
      // Server sends its chunked mode preferences - read and discard
      // We always use notchunked since chunked requires server config
      await this.reader.readString(); // server send preference
      await this.reader.readString(); // server recv preference
    }

    if (effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_EXOTIC_STUFF) {
      // Read rules for parameters or similar exotic metadata
      const rulesSize = Number(await this.reader.readVarInt());
      for (let i = 0; i < rulesSize; i++) {
        await this.reader.readString();
        await this.reader.readString();
      }
    }

    if (effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_EXTRA_U64) {
      // Extra metadata field (currently unused in most drivers)
      await this.reader.readU64LE();
    }

    if (effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_PASSWORD_PARAMS_IN_HELLO) {
      // Server might send parameters for password verification (e.g. Salt)
      while (true) {
        const name = await this.reader.readString();
        if (name === "") break;
        await this.reader.readVarInt(); // value type
        await this.reader.readString(); // value
      }
    }

    if (effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_TCP_PROTOCOL_VERSION) {
      // Server reports its native TCP protocol version
      await this.reader.readVarInt();
    }
    if (effectiveRevision >= REVISIONS.DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS_CUSTOM_KEY) {
      // Additional parallel replicas metadata
      await this.reader.readVarInt();
    }

    this._serverHello = {
      serverName,
      major,
      minor,
      revision: effectiveRevision,
      timezone,
      displayName,
      patch,
    };

    if (effectiveRevision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY) {
      // Send addendum (quota key, etc) - await to ensure it's flushed before returning.
      // Without this, rapid connect() -> query() can fail because Query packet
      // may be written before addendum is actually sent.
      const addendum = this.writer.encodeAddendum(effectiveRevision);
      await new Promise<void>((resolve, reject) => {
        this.socket!.write(addendum, (err) => (err ? reject(err) : resolve()));
      });
    }

    this.log("Handshake: Complete!");
  }

  /** Insert a single RecordBatch. */
  insert(sql: string, data: RecordBatch, options?: InsertOptions): CollectableAsyncGenerator<Packet>;
  /** Insert an iterable of RecordBatches. */
  insert(sql: string, data: Iterable<RecordBatch> | AsyncIterable<RecordBatch>, options?: InsertOptions): CollectableAsyncGenerator<Packet>;
  /** Insert row objects with auto-coercion using server schema. */
  insert(sql: string, data: Iterable<Record<string, unknown>> | AsyncIterable<Record<string, unknown>>, options?: InsertOptions): CollectableAsyncGenerator<Packet>;
  insert(
    sql: string,
    data: RecordBatch | Iterable<RecordBatch | Record<string, unknown>> | AsyncIterable<RecordBatch | Record<string, unknown>>,
    options: InsertOptions = {}
  ): CollectableAsyncGenerator<Packet> {
    return collectable(this.insertImpl(sql, data, options));
  }

  private async *insertImpl(
    sql: string,
    data: RecordBatch | Iterable<RecordBatch | Record<string, unknown>> | AsyncIterable<RecordBatch | Record<string, unknown>>,
    options: InsertOptions = {}
  ): AsyncGenerator<Packet> {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");
    if (this.busy) throw new Error("Connection busy - cannot run concurrent operations on the same TcpClient");
    this.busy = true;

    const signal = options.signal;
    const batchSize = options.batchSize ?? 10000;
    if (signal?.aborted) throw new Error("Insert aborted before start");

    let cancelled = false;
    let reachedEndOfStream = false;
    let receivedException = false;
    let sentDataDelimiter = false;  // Track if we've finished sending data

    const abortHandler = () => {
      if (!cancelled && this.socket) {
        cancelled = true;
        this.socket!.write(this.writer.encodeCancel());
      }
    };
    signal?.addEventListener("abort", abortHandler);

    const useCompression = !!this.options.compression;
    const compressionMethod = this.options.compression === 'zstd' ? Method.ZSTD : Method.LZ4;

    try {
      // Merge settings: client defaults < per-insert overrides
      const mergedSettings = { ...this.defaultSettings, ...options.settings };

      const serverSchema = await this.sendInsertQueryAndGetSchema(sql, useCompression, compressionMethod, mergedSettings, () => cancelled, options.queryId);

      // Validate schema if provided
      if (options.schema) {
        validateSchema(options.schema, serverSchema);
      }

      let totalInserted = 0;

      const sendBatch = async (batch: RecordBatch) => {
        if (cancelled) throw new Error("Insert cancelled");
        const encodedColumns = [];
        for (let i = 0; i < batch.columns.length; i++) {
          const colDef = batch.columns[i];
          const colData = batch.columnData[i];
          const codec = getCodec(colDef.type);

          const writer = new BufferWriter();
          codec.writePrefix?.(writer, colData);
          const encoded = codec.encode(colData);
          writer.write(encoded);

          encodedColumns.push({
            name: colDef.name,
            type: colDef.type,
            data: writer.finish()
          });
        }

        const dataPacket = this.writer.encodeData("", batch.rowCount, encodedColumns, this.serverHello!.revision, useCompression, compressionMethod);
        await this.writeWithBackpressure(dataPacket);
        totalInserted += batch.rowCount;
      };

      const sendRowBatch = async (rows: Record<string, unknown>[]) => {
        if (rows.length === 0) return;
        const numCols = serverSchema.length;
        const codecs = serverSchema.map(c => getCodec(c.type));

        // Transpose row objects to columns
        const columns: unknown[][] = serverSchema.map(() => new Array(rows.length));
        for (let r = 0; r < rows.length; r++) {
          const row = rows[r];
          for (let c = 0; c < numCols; c++) {
            columns[c][r] = row[serverSchema[c].name];
          }
        }

        // Build Column objects via codecs (coercion happens in fromValues)
        const encodedColumns = [];
        for (let i = 0; i < numCols; i++) {
          const col = codecs[i].fromValues(columns[i]);
          const writer = new BufferWriter();
          codecs[i].writePrefix?.(writer, col);
          const encoded = codecs[i].encode(col);
          writer.write(encoded);
          encodedColumns.push({
            name: serverSchema[i].name,
            type: serverSchema[i].type,
            data: writer.finish()
          });
        }

        const dataPacket = this.writer.encodeData("", rows.length, encodedColumns, this.serverHello!.revision, useCompression, compressionMethod);
        await this.writeWithBackpressure(dataPacket);
        totalInserted += rows.length;
      };

      // Single RecordBatch - fast path
      if (isRecordBatch(data)) {
        await sendBatch(data);
      } else {
        // Get iterator (sync or async)
        const isAsync = Symbol.asyncIterator in data;
        const iterator = isAsync
          ? (data as AsyncIterable<any>)[Symbol.asyncIterator]()
          : (data as Iterable<any>)[Symbol.iterator]();

        const firstResult = await Promise.resolve(iterator.next());
        if (!firstResult.done) {
          const first = firstResult.value;

          if (isRecordBatch(first)) {
            // RecordBatch mode
            await sendBatch(first);
            while (true) {
              if (cancelled) throw new Error("Insert cancelled");
              const result = await Promise.resolve(iterator.next());
              if (result.done) break;
              await sendBatch(result.value as RecordBatch);
            }
          } else {
            // Row object mode with batching
            let buffer: Record<string, unknown>[] = [first as Record<string, unknown>];
            while (true) {
              if (cancelled) throw new Error("Insert cancelled");
              const result = await Promise.resolve(iterator.next());
              if (result.done) break;
              buffer.push(result.value as Record<string, unknown>);
              if (buffer.length >= batchSize) {
                await sendRowBatch(buffer);
                buffer = [];
              }
            }
            if (buffer.length > 0) {
              await sendRowBatch(buffer);
            }
          }
        }
      }

      const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision, useCompression, compressionMethod);
      this.socket!.write(delimiter);
      sentDataDelimiter = true;

      const { progress: progressAccumulated, profileEvents: profileEventsAccumulated } = this.createAccumulators();

      // Read response packets until EndOfStream
      while (true) {
        const packetId = Number(await this.reader!.readVarInt());

        switch (packetId) {
          case ServerPacketId.Progress: {
            const progress = await this.readProgress();
            this.accumulateProgress(progress, progressAccumulated);
            yield { type: "Progress", progress, accumulated: progressAccumulated };
            break;
          }
          case ServerPacketId.ProfileInfo:
            yield { type: "ProfileInfo", info: await this.readProfileInfo() };
            break;
          case ServerPacketId.ProfileEvents: {
            const batch = await this.readBlock(false);
            this.processProfileEventsBlock(batch, profileEventsAccumulated, progressAccumulated);
            yield { type: "ProfileEvents", batch, accumulated: profileEventsAccumulated };
            break;
          }
          case ServerPacketId.Data:
            await this.readBlock(useCompression);
            break;
          case ServerPacketId.Log: {
            const batch = await this.readBlock(false);
            if (batch.rowCount > 0) {
              yield { type: "Log", entries: this.parseLogBlock(batch) };
            }
            break;
          }
          case ServerPacketId.EndOfStream:
            reachedEndOfStream = true;
            this.log(`Successfully inserted ${totalInserted} rows.`);
            yield { type: "EndOfStream" };
            return;
          case ServerPacketId.Exception:
            receivedException = true;
            throw await this.reader!.readException();
        }
      }
    } finally {
      // If generator was abandoned early, drain remaining packets
      // But only if we've sent the delimiter - otherwise server is waiting for data, not sending responses
      if (!reachedEndOfStream && !receivedException && this.socket && this.reader) {
        if (sentDataDelimiter) {
          try {
            await this.drainInsertResponses(useCompression);
          } catch (err) {
            this.log(`[insert] drain failed, closing connection: ${err instanceof Error ? err.message : err}`);
            this.close();
          }
        } else {
          // We're in the middle of sending data - can't drain, must close
          this.log(`[insert] error before data sent, closing connection`);
          this.close();
        }
      }
      this.busy = false;
      signal?.removeEventListener("abort", abortHandler);
    }
  }

  /**
   * Send INSERT query and wait for schema response from server.
   * Returns the schema (column definitions) for the target table.
   */
  private async sendInsertQueryAndGetSchema(
    sql: string,
    useCompression: boolean,
    compressionMethod: MethodCode,
    settings: Record<string, unknown>,
    isCancelled: () => boolean,
    queryId?: string
  ): Promise<ColumnSchema[]> {
    const queryPacket = this.writer.encodeQuery(queryId ?? randomUUID(), sql, this.serverHello!.revision, settings, useCompression, {});
    this.socket!.write(queryPacket);

    const delimiter = this.writer.encodeData("", 0, [], this.serverHello!.revision, useCompression, compressionMethod);
    this.socket!.write(delimiter);

    while (true) {
      if (isCancelled()) throw new Error("Insert cancelled");
      const packetId = Number(await this.reader!.readVarInt());

      switch (packetId) {
        case ServerPacketId.Data: {
          const block = await this.readBlock(useCompression);
          this.currentSchema = block.columns.map(c => ({ name: c.name, type: c.type }));
          return this.currentSchema;
        }
        case ServerPacketId.Progress: await this.readProgress(); break;
        case ServerPacketId.Log: await this.readBlock(false); break;
        case ServerPacketId.TableColumns:
          await this.reader!.readString();
          await this.reader!.readString();
          break;
        case ServerPacketId.Exception:
          throw await this.reader!.readException();
        default:
          throw new Error(`Unexpected packet while waiting for insert header: ${packetId}`);
      }
    }
  }

  /**
   * Drain response packets after insert data has been sent.
   * Waits until EndOfStream, handling intermediate packets.
   */
  private async drainInsertResponses(useCompression: boolean): Promise<void> {
    while (true) {
      const packetId = Number(await this.reader!.readVarInt());
      if (packetId === ServerPacketId.EndOfStream) break;

      switch (packetId) {
        case ServerPacketId.Progress: await this.readProgress(); break;
        case ServerPacketId.ProfileInfo: await this.readProfileInfo(); break;
        case ServerPacketId.Data: await this.readBlock(useCompression); break;
        case ServerPacketId.Log:
        case ServerPacketId.ProfileEvents:
          await this.readBlock(false);
          break;
        case ServerPacketId.Exception:
          throw await this.reader!.readException();
      }
    }
  }

  private async readProgress(): Promise<Progress> {
    const rev = this.serverHello!.revision;
    const progress: Progress = {
      readRows: await this.reader!.readVarInt(),
      readBytes: await this.reader!.readVarInt(),
      totalRowsToRead: rev >= REVISIONS.DBMS_MIN_REVISION_WITH_SERVER_LOGS ? await this.reader!.readVarInt() : 0n,
    };
    if (rev >= REVISIONS.DBMS_MIN_REVISION_WITH_TOTAL_BYTES_TO_READ) {
      progress.totalBytesToRead = await this.reader!.readVarInt();
    }
    // writtenRows/writtenBytes added between DBMS_MIN_REVISION_WITH_SERVER_LOGS and DBMS_MIN_REVISION_WITH_TOTAL_BYTES_TO_READ
    // The exact revision is 54420, which isn't in our named constants (falls in the 54401-54441 gap)
    if (rev >= 54420n) {
      progress.writtenRows = await this.reader!.readVarInt();
      progress.writtenBytes = await this.reader!.readVarInt();
    }
    if (rev >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_ELAPSED_NS_IN_PROGRESS) {
      progress.elapsedNs = await this.reader!.readVarInt();
    }
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
    if (this.serverHello!.revision >= REVISIONS.DBMS_MIN_REVISION_WITH_APPLIED_AGGREGATION) {
      info.appliedAggregation = (await this.reader!.readU8()) !== 0;
      info.rowsBeforeAggregation = await this.reader!.readVarInt();
    }
    return info;
  }

  private parseLogBlock(batch: RecordBatch): LogEntry[] {
    const entries: LogEntry[] = [];
    for (const row of batch) {
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

  private createAccumulators() {
    return {
      progress: {
        readRows: 0n,
        readBytes: 0n,
        totalRowsToRead: 0n,
        totalBytesToRead: 0n,
        writtenRows: 0n,
        writtenBytes: 0n,
        elapsedNs: 0n,
        percent: 0,
        memoryUsage: 0n,
        peakMemoryUsage: 0n,
        cpuTimeMicroseconds: 0n,
        cpuUsage: 0,
      } as AccumulatedProgress,
      profileEvents: new Map<string, bigint>(),
    };
  }

  private accumulateProgress(progress: Progress, accumulated: AccumulatedProgress): void {
    accumulated.readRows += progress.readRows;
    accumulated.readBytes += progress.readBytes;
    accumulated.totalRowsToRead += progress.totalRowsToRead;
    accumulated.totalBytesToRead += progress.totalBytesToRead ?? 0n;
    accumulated.writtenRows += progress.writtenRows ?? 0n;
    accumulated.writtenBytes += progress.writtenBytes ?? 0n;
    accumulated.elapsedNs += progress.elapsedNs ?? 0n;
  }

  private processProfileEventsBlock(
    batch: RecordBatch,
    profileEventsAccumulated: Map<string, bigint>,
    progressAccumulated: AccumulatedProgress
  ): void {
    const nameCol = batch.getColumn("name");
    const valueCol = batch.getColumn("value");
    const typeCol = batch.getColumn("type");
    const threadIdCol = batch.getColumn("thread_id");
    if (!nameCol || !valueCol || !typeCol) return;

    for (let i = 0; i < batch.rowCount; i++) {
      const name = nameCol.get(i) as string;
      const value = valueCol.get(i) as bigint;
      const eventType = typeCol.get(i) as string;
      const threadId = threadIdCol ? threadIdCol.get(i) as bigint : 0n;

      if (eventType === "increment") {
        profileEventsAccumulated.set(name, (profileEventsAccumulated.get(name) ?? 0n) + value);
      } else {
        profileEventsAccumulated.set(name, value);
      }

      // Extract memory/CPU metrics for query-level aggregates only
      if (threadId === 0n) {
        switch (name) {
          case "MemoryTrackerUsage":
            progressAccumulated.memoryUsage = value;
            break;
          case "MemoryTrackerPeakUsage":
            if (value > progressAccumulated.peakMemoryUsage) {
              progressAccumulated.peakMemoryUsage = value;
            }
            break;
          case "UserTimeMicroseconds":
          case "SystemTimeMicroseconds":
            if (eventType === "increment") {
              progressAccumulated.cpuTimeMicroseconds += value;
            }
            break;
        }
      }
    }

    // Recalculate CPU usage
    if (progressAccumulated.elapsedNs > 0n) {
      const elapsedMicros = progressAccumulated.elapsedNs / 1000n;
      progressAccumulated.cpuUsage = elapsedMicros > 0n
        ? Number(progressAccumulated.cpuTimeMicroseconds) / Number(elapsedMicros)
        : 0;
    }
  }

  private async readBlock(compressed: boolean = false): Promise<RecordBatch> {
    // Block name is always uncompressed, even when block data is compressed
    await this.reader!.readString();

    const options = { clientVersion: Number(this.serverHello!.revision) };

    if (compressed) {
      const decompressed = await this.reader!.readCompressedBlock();
      const start = performance.now();
      const result = decodeNativeBlock(decompressed, 0, options);
      result.decodeTimeMs = performance.now() - start;
      return RecordBatch.from(result);
    }

    // For uncompressed, we need to handle streaming reads which might span multiple chunks.
    while (true) {
      const currentBuffer = this.reader!.peekAll();
      try {
        const start = performance.now();
        const result = decodeNativeBlock(currentBuffer, 0, options);
        result.decodeTimeMs = performance.now() - start;
        this.reader!.consume(result.bytesConsumed);
        return RecordBatch.from(result);
      } catch (err) {
        if (err instanceof BufferUnderflowError) {
          const more = await this.reader!.nextChunk();
          if (!more) throw new Error("EOF while decoding block");
          continue;
        }
        throw err;
      }
    }
  }

  // TODO: we should make the use flattened v3 setting automatically enabled until we support the other dynamic encodings
  query(sql: string, options: QueryOptions = {}): CollectableAsyncGenerator<Packet> {
    return collectable(this.queryImpl(sql, options));
  }

  private async *queryImpl(
    sql: string,
    options: QueryOptions = {},
  ): AsyncGenerator<Packet> {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");
    if (this.busy) throw new Error("Connection busy - cannot run concurrent operations on the same TcpClient");
    this.busy = true;

    const { settings = {}, signal } = options;
    if (signal?.aborted) throw new Error("Query aborted before start");

    const useCompression = !!this.options.compression;
    const compressionMethod = this.options.compression === 'zstd' ? Method.ZSTD : Method.LZ4;
    const queryTimeout = this.options.queryTimeout ?? 30000;
    const cancelGracePeriod = this.options.cancelGracePeriodMs ?? 2000;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;
    let graceTimeoutId: ReturnType<typeof setTimeout> | null = null;
    let timedOut = false;
    let cancelled = false;

    let reachedEndOfStream = false;
    let receivedException = false;

    const startTimeout = () => {
      if (queryTimeout > 0) {
        timeoutId = setTimeout(() => {
          timedOut = true;
          // First try graceful cancel
          if (this.socket && !cancelled) {
            cancelled = true;
            this.socket!.write(this.writer.encodeCancel());
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
        this.socket!.write(this.writer.encodeCancel());
      }
    };

    signal?.addEventListener("abort", abortHandler);

    try {
      try {
        startTimeout();

        // The compression flag in the query packet enables bidirectional compression:
        // - When 1: client sends compressed Data blocks, server sends compressed Data blocks
        // - When 0: both sides send uncompressed
        // Settings merge order: hardcoded < client defaults < per-call overrides
        const baseSettings: ClickHouseSettings = {
          ...this.defaultSettings,
          ...settings,
        };
        const queryPacket = this.writer.encodeQuery(
          options.queryId ?? randomUUID(),
          sql,
          this.serverHello.revision,
          baseSettings,
          useCompression,
          options.params ?? {},
        );
        this.log(`[query] sending query packet (${queryPacket.length} bytes), compression=${useCompression}`);
        this.socket!.write(queryPacket);

        // Send external tables if provided
        if (options.externalTables) {
          await this.sendExternalTables(options.externalTables, useCompression, compressionMethod);
        }

        // Send delimiter (compressed if compression is enabled)
        const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision, useCompression, compressionMethod);
        this.log(`[query] sending delimiter (${delimiter.length} bytes, compressed=${useCompression})`);
        this.socket!.write(delimiter);

        this.currentSchema = null;
        this.log(`[query] waiting for response...`);

        const { progress: progressAccumulated, profileEvents: profileEventsAccumulated } = this.createAccumulators();

        while (true) {
          this.log(`[query] reading packet id...`);
          const packetId = Number(await this.reader.readVarInt());
          if (timedOut) throw new Error(`Query timeout after ${queryTimeout}ms`);
          this.log(`[query] packetId=${packetId}, useCompression=${useCompression}`);

          switch (packetId) {
            case ServerPacketId.Data: {
              // With compression=1, ALL Data blocks from server are compressed
              this.log(`[query] reading Data block (compressed=${useCompression})...`);
              const batch = await this.readBlock(useCompression);
              this.log(`[query] got Data block with ${batch.rowCount} rows`);
              if (this.currentSchema === null) {
                this.currentSchema = batch.columns.map(c => ({ name: c.name, type: c.type }));
              }
              if (batch.rowCount > 0) yield { type: "Data", batch };
              break;
            }
            case ServerPacketId.Progress: {
              const progress = await this.readProgress();
              this.accumulateProgress(progress, progressAccumulated);
              // Calculate percent for queries (based on read progress)
              const progressDenom = progressAccumulated.readRows > progressAccumulated.totalRowsToRead
                ? progressAccumulated.readRows
                : progressAccumulated.totalRowsToRead;
              progressAccumulated.percent = progressDenom > 0n
                ? Number(progressAccumulated.readRows * 100n / progressDenom)
                : 0;
              yield { type: "Progress", progress, accumulated: progressAccumulated };
              break;
            }
            case ServerPacketId.ProfileInfo:
              yield { type: "ProfileInfo", info: await this.readProfileInfo() };
              break;
            case ServerPacketId.ProfileEvents: {
              const batch = await this.readBlock(false);
              this.processProfileEventsBlock(batch, profileEventsAccumulated, progressAccumulated);
              yield { type: "ProfileEvents", batch, accumulated: profileEventsAccumulated };
              break;
            }
            case ServerPacketId.Totals:
              yield { type: "Totals", batch: await this.readBlock(useCompression) };
              break;
            case ServerPacketId.Extremes:
              yield { type: "Extremes", batch: await this.readBlock(useCompression) };
              break;
            case ServerPacketId.Log: {
              // Log blocks are always uncompressed (diagnostic metadata)
              const batch = await this.readBlock(false);
              if (batch.rowCount > 0) {
                yield { type: "Log", entries: this.parseLogBlock(batch) };
              }
              break;
            }
            case ServerPacketId.TimezoneUpdate:
              this.sessionTimezone = await this.reader.readString();
              this.log(`[query] timezone updated to: ${this.sessionTimezone}`);
              break;
            case ServerPacketId.EndOfStream:
              reachedEndOfStream = true;
              yield { type: "EndOfStream" };
              return;
            case ServerPacketId.Exception:
              receivedException = true;
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
      // If generator was abandoned early (before EndOfStream), drain remaining packets
      // to keep the connection in a clean state for subsequent queries.
      // Skip draining if we received an exception - server sends nothing after exception.
      if (!reachedEndOfStream && !receivedException && this.socket && this.reader) {
        try {
          await this.drainPackets(useCompression);
        } catch (err) {
          // Drain failed - connection is in unknown state, close it to prevent corruption
          this.log(`[query] drain failed, closing connection: ${err instanceof Error ? err.message : err}`);
          this.close();
        }
      }
      this.busy = false;
      clearQueryTimeout();
      signal?.removeEventListener("abort", abortHandler);
    }
  }

  /** Encode a RecordBatch as a Data packet with the given table name. */
  private encodeBatchAsDataPacket(
    tableName: string,
    batch: RecordBatch,
    compress: boolean,
    method: MethodCode
  ): Uint8Array {
    const encodedColumns = [];
    for (let i = 0; i < batch.columns.length; i++) {
      const colDef = batch.columns[i];
      const colData = batch.columnData[i];
      const codec = getCodec(colDef.type);
      const writer = new BufferWriter();
      codec.writePrefix?.(writer, colData);
      writer.write(codec.encode(colData));
      encodedColumns.push({ name: colDef.name, type: colDef.type, data: writer.finish() });
    }
    return this.writer.encodeData(tableName, batch.rowCount, encodedColumns,
      this.serverHello!.revision, compress, method);
  }

  /** Send external tables as Data packets before the query delimiter. */
  private async sendExternalTables(
    tables: Record<string, ExternalTableData>,
    compress: boolean,
    method: MethodCode
  ): Promise<void> {
    for (const [name, data] of Object.entries(tables)) {
      if (isRecordBatch(data)) {
        const packet = this.encodeBatchAsDataPacket(name, data, compress, method);
        await this.writeWithBackpressure(packet);
      } else if (Symbol.asyncIterator in data) {
        for await (const batch of data as AsyncIterable<RecordBatch>) {
          const packet = this.encodeBatchAsDataPacket(name, batch, compress, method);
          await this.writeWithBackpressure(packet);
        }
      } else {
        for (const batch of data as Iterable<RecordBatch>) {
          const packet = this.encodeBatchAsDataPacket(name, batch, compress, method);
          await this.writeWithBackpressure(packet);
        }
      }
    }
  }

  /** Drain remaining packets until EndOfStream or Exception. Used when query is abandoned early. */
  private async drainPackets(useCompression: boolean): Promise<void> {
    if (!this.reader) return;
    while (true) {
      const packetId = Number(await this.reader.readVarInt());
      switch (packetId) {
        case ServerPacketId.Data:
          await this.readBlock(useCompression);
          break;
        case ServerPacketId.Progress:
          await this.readProgress();
          break;
        case ServerPacketId.ProfileInfo:
          await this.readProfileInfo();
          break;
        case ServerPacketId.ProfileEvents:
          await this.readBlock(false);
          break;
        case ServerPacketId.Totals:
        case ServerPacketId.Extremes:
          await this.readBlock(useCompression);
          break;
        case ServerPacketId.Log:
          await this.readBlock(false);
          break;
        case ServerPacketId.TimezoneUpdate:
          await this.reader.readString();
          break;
        case ServerPacketId.EndOfStream:
          return;
        case ServerPacketId.Exception:
          // Read and discard the exception
          await this.reader.readException();
          return;
        default:
          // Unknown packet - can't continue safely
          return;
      }
    }
  }

  /**
   * Send a ping packet and wait for pong response.
   * Useful for checking connection health.
   */
  async ping(): Promise<void> {
    if (!this.socket || !this.reader) throw new Error("Not connected");

    this.socket!.write(this.writer.encodePing());
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
    this.busy = false;
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
