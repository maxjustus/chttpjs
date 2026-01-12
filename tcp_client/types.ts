import { RecordBatch } from "@maxjustus/chttp/native";

// Modern revision. We rely on the default server settings for serialization.
export const DBMS_TCP_PROTOCOL_VERSION = 54479n;

/** Client version sent in Hello and Query packets (ClickHouse version we're mimicking) */
export const CLIENT_VERSION = {
  MAJOR: 24,
  MINOR: 8,
  PATCH: 0,
} as const;

/** Protocol version for parallel replicas feature negotiation */
export const DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION = 4;

export const ClientPacketId = {
  Hello: 0,
  Query: 1,
  Data: 2,
  Cancel: 3,
  Ping: 4,
} as const;

export const ServerPacketId = {
  Hello: 0,
  Data: 1,
  Exception: 2,
  Progress: 3,
  Pong: 4,
  EndOfStream: 5,
  ProfileInfo: 6,
  Totals: 7,
  Extremes: 8,
  // 9 = TablesStatusResponse (not used in query flow)
  Log: 10,
  TableColumns: 11,
  // 12 = PartUUIDs, 13 = ReadTaskRequest (internal)
  ProfileEvents: 14,
  // 15 = MergeTreeAllRangesAnnouncement, 16 = MergeTreeReadTaskRequest (internal)
  TimezoneUpdate: 17,
} as const;

export const QueryProcessingStage = {
  FetchColumns: 0,
  WithMergeableState: 1,
  Complete: 2,
  WithMergableStateAfterAggregation: 3,
} as const;

export const QueryKind = {
  None: 0,
  InitialQuery: 1,
  SecondaryQuery: 2,
} as const;

export const Interface = {
  TCP: 1,
  HTTP: 2,
  GRPC: 3,
} as const;

export interface ServerHello {
  serverName: string;
  major: bigint;
  minor: bigint;
  revision: bigint;
  timezone?: string;
  displayName?: string;
  patch: bigint;
}

/**
 * Raw progress delta from a single Progress packet (server packet ID 3).
 *
 * ClickHouse sends Progress packets periodically during query execution. Each packet
 * contains **delta values** (increments since the last Progress packet), not absolute
 * totals. Clients must accumulate these deltas to track overall progress.
 *
 * The server sends Progress packets based on `send_progress_in_http_headers` and
 * internal thresholds - expect multiple packets for queries that process significant data.
 *
 * @see AccumulatedProgress for the client-side accumulated totals
 * @see https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/Progress.h
 */
export interface Progress {
  /** Delta: rows read since last Progress packet */
  readRows: bigint;
  /** Delta: bytes read since last Progress packet */
  readBytes: bigint;
  /** Delta: estimated total rows remaining to read (server's estimate, may increase) */
  totalRowsToRead: bigint;
  /** Delta: estimated total bytes remaining to read (revision >= 54463) */
  totalBytesToRead?: bigint;
  /** Delta: rows written since last Progress packet (revision >= 54420, for INSERT queries) */
  writtenRows?: bigint;
  /** Delta: bytes written since last Progress packet (revision >= 54420, for INSERT queries) */
  writtenBytes?: bigint;
  /** Delta: elapsed nanoseconds since last Progress packet (revision >= 54460) */
  elapsedNs?: bigint;
}

/**
 * Accumulated progress totals across all Progress packets and ProfileEvents.
 *
 * This interface represents the client-side running totals, computed by summing
 * all Progress deltas and extracting metrics from ProfileEvents. The `query()` method
 * yields this alongside each Progress packet for convenient progress tracking.
 *
 * **Accumulation semantics:**
 * - Progress fields (readRows, readBytes, etc.): summed across all Progress packets
 * - Memory metrics: use **max()** semantics (highest value seen, not sum)
 * - CPU time: summed from UserTimeMicroseconds + SystemTimeMicroseconds ProfileEvents
 * - cpuUsage: derived as cpuTimeMicroseconds / (elapsedNs / 1000)
 *
 * **Progress percentage calculation:**
 * - `percent = readRows * 100 / max(readRows, totalRowsToRead)`
 * - The max() prevents > 100% when readRows exceeds the server's estimate
 *
 * @example
 * ```ts
 * for await (const packet of client.query(sql)) {
 *   if (packet.type === "Progress") {
 *     console.log(`${packet.accumulated.percent}% complete`);
 *     console.log(`Memory: ${packet.accumulated.memoryUsage} bytes`);
 *     console.log(`CPU: ${packet.accumulated.cpuUsage.toFixed(1)} cores`);
 *   }
 * }
 * ```
 */
export interface AccumulatedProgress {
  /** Total rows read across all Progress packets */
  readRows: bigint;
  /** Total bytes read across all Progress packets */
  readBytes: bigint;
  /** Server's estimate of total rows to read (may increase as query runs) */
  totalRowsToRead: bigint;
  /** Server's estimate of total bytes to read */
  totalBytesToRead: bigint;
  /** Total rows written (for INSERT queries) */
  writtenRows: bigint;
  /** Total bytes written (for INSERT queries) */
  writtenBytes: bigint;
  /** Total elapsed nanoseconds */
  elapsedNs: bigint;
  /** Percentage complete (0-100), capped using max(readRows, totalRowsToRead) as denominator */
  percent: number;
  /**
   * Current memory usage in bytes from MemoryTrackerUsage ProfileEvent.
   * Uses latest value - reflects memory at the most recent ProfileEvents packet.
   */
  memoryUsage: bigint;
  /**
   * Peak memory usage in bytes from MemoryTrackerPeakUsage ProfileEvent.
   * Uses max() semantics across all hosts/threads that report this metric.
   */
  peakMemoryUsage: bigint;
  /**
   * Total CPU time in microseconds (UserTimeMicroseconds + SystemTimeMicroseconds).
   * Accumulated from ProfileEvents with type="increment".
   */
  cpuTimeMicroseconds: bigint;
  /**
   * Equivalent CPUs utilized, calculated as: cpuTimeMicroseconds / (elapsedNs / 1000).
   * A value of 1.0 means one CPU fully utilized, 4.0 means four CPUs, etc.
   * Useful for understanding query parallelism and CPU-boundedness.
   */
  cpuUsage: number;
}

/**
 * Query execution profile information (server packet ID 6).
 *
 * ProfileInfo is sent once per query after data blocks, providing summary
 * statistics about query execution. Unlike Progress (which is incremental),
 * ProfileInfo contains absolute final values.
 */
export interface ProfileInfo {
  /** Total rows in the result set */
  rows: bigint;
  /** Number of data blocks sent */
  blocks: bigint;
  /** Total bytes in the result set */
  bytes: bigint;
  /** Whether a LIMIT clause was applied */
  appliedLimit: boolean;
  /** Rows that would have been returned without LIMIT */
  rowsBeforeLimit: bigint;
  /** Whether rowsBeforeLimit was computed (vs estimated) */
  calculatedRowsBeforeLimit: boolean;
  /** Whether aggregation was applied (revision >= 54469) */
  appliedAggregation: boolean;
  /** Rows before aggregation (revision >= 54469) */
  rowsBeforeAggregation: bigint;
}

/**
 * Server log entry from Log packets (server packet ID 10).
 *
 * Log packets are sent when `send_logs_level` setting is enabled. Each entry
 * represents a single log line from the server during query execution.
 */
export interface LogEntry {
  /** Timestamp as DateTime string */
  time: string;
  /** Microsecond component of the timestamp */
  timeMicroseconds: number;
  /** Server hostname that generated the log */
  hostName: string;
  /** Query ID this log belongs to */
  queryId: string;
  /** Thread ID that generated the log */
  threadId: bigint;
  /** Log severity: 1=Fatal, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Info, 7=Debug, 8=Trace */
  priority: number;
  /** Source component/module within ClickHouse */
  source: string;
  /** Log message text */
  text: string;
}

/**
 * Union type representing all packets yielded by `query()`.
 *
 * The TCP protocol sends various packet types during query execution. This union
 * captures the relevant ones for client consumption. The `query()` generator yields
 * these packets as they arrive from the server.
 *
 * **Packet ordering:**
 * 1. Progress packets may arrive at any point during execution
 * 2. Data/Totals/Extremes arrive in order after the header block
 * 3. ProfileInfo arrives once after all data
 * 4. ProfileEvents may arrive periodically or at end (depends on server settings)
 * 5. EndOfStream always arrives last
 *
 * @example
 * ```ts
 * for await (const packet of client.query(sql)) {
 *   switch (packet.type) {
 *     case "Data": processRows(packet.batch); break;
 *     case "Progress": updateProgressBar(packet.accumulated); break;
 *     case "ProfileEvents": logMetrics(packet.accumulated); break;
 *     case "EndOfStream": console.log("Query complete"); break;
 *   }
 * }
 * ```
 */
export type Packet =
  /** Query result data block containing rows */
  | { type: "Data"; batch: RecordBatch }
  /** Totals row for GROUP BY WITH TOTALS queries */
  | { type: "Totals"; batch: RecordBatch }
  /** Min/max values for each column (when extremes are enabled) */
  | { type: "Extremes"; batch: RecordBatch }
  /** Server log entries (when send_logs_level is set) */
  | { type: "Log"; entries: LogEntry[] }
  /**
   * Query progress update with both the raw delta and accumulated totals.
   * - `progress`: Raw delta values from this single Progress packet
   * - `accumulated`: Running totals across all Progress packets + ProfileEvents metrics
   */
  | { type: "Progress"; progress: Progress; accumulated: AccumulatedProgress }
  /** Query execution statistics (sent once after data) */
  | { type: "ProfileInfo"; info: ProfileInfo }
  /**
   * ProfileEvents packet containing detailed execution metrics.
   *
   * **Batch schema:** The RecordBatch contains columns:
   * - `name` (String): Event name (e.g., "SelectedRows", "MemoryTrackerUsage")
   * - `value` (Int64/UInt64): Event value (delta or absolute depending on type)
   * - `type` (String): "increment" for counters (sum deltas) or gauge (use latest)
   * - `thread_id` (UInt64): 0 for query-level aggregates, >0 for per-thread stats
   *
   * **Accumulation:** The `accumulated` map sums increment-type events and
   * uses latest value for gauge-type events. Common useful events:
   * - `SelectedRows/SelectedBytes`: Total data selected
   * - `MemoryTrackerUsage`: Current memory (gauge)
   * - `MemoryTrackerPeakUsage`: Peak memory (gauge)
   * - `UserTimeMicroseconds/SystemTimeMicroseconds`: CPU time (increment)
   * - `ReadCompressedBytes/WriteCompressedBytes`: I/O stats
   */
  | { type: "ProfileEvents"; batch: RecordBatch; accumulated: Map<string, bigint> }
  /** End of query - no more packets will be sent */
  | { type: "EndOfStream" };

export const REVISIONS = {
  DBMS_MIN_REVISION_WITH_CLIENT_INFO: 54032n,
  DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: 54058n,
  DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO: 54060n,
  DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: 54372n,
  DBMS_MIN_REVISION_WITH_VERSION_PATCH: 54401n,
  DBMS_MIN_REVISION_WITH_SERVER_LOGS: 54406n,
  DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET: 54441n,
  DBMS_MIN_REVISION_WITH_OPENTELEMETRY: 54442n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH: 54448n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME: 54449n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS: 54453n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION: 54454n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT: 54456n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY: 54458n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS: 54459n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_ELAPSED_NS_IN_PROGRESS: 54460n,
  DBMS_MIN_REVISION_WITH_EXOTIC_STUFF: 54461n,
  DBMS_MIN_REVISION_WITH_EXTRA_U64: 54462n,
  DBMS_MIN_REVISION_WITH_TOTAL_BYTES_TO_READ: 54463n,
  DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS: 54466n,
  DBMS_MIN_REVISION_WITH_APPLIED_AGGREGATION: 54469n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS: 54470n,
  DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL: 54471n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES: 54472n,
  DBMS_MIN_REVISION_WITH_PASSWORD_PARAMS_IN_HELLO: 54474n,
  DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS: 54475n,
  DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER: 54476n,
  DBMS_MIN_REVISION_WITH_TCP_PROTOCOL_VERSION: 54477n,
  DBMS_MIN_REVISION_WITH_PARALLEL_REPLICAS_CUSTOM_KEY: 54479n,
};

/**
 * ClickHouse server exception with full error details.
 */
export class ClickHouseException extends Error {
  readonly code: number;
  readonly exceptionName: string;
  readonly serverStackTrace: string;
  readonly hasNested: boolean;
  readonly nested?: ClickHouseException;

  constructor(
    code: number,
    exceptionName: string,
    message: string,
    serverStackTrace: string,
    hasNested: boolean,
    nested?: ClickHouseException
  ) {
    super(`${exceptionName}: ${message}`);
    this.name = 'ClickHouseException';
    this.code = code;
    this.exceptionName = exceptionName;
    this.serverStackTrace = serverStackTrace;
    this.hasNested = hasNested;
    this.nested = nested;
  }
}
