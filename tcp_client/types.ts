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

/** Raw progress delta from a single Progress packet */
export interface Progress {
  readRows: bigint;
  readBytes: bigint;
  totalRowsToRead: bigint;
  totalBytesToRead?: bigint;
  writtenRows?: bigint;
  writtenBytes?: bigint;
  elapsedNs?: bigint;
}

/** Accumulated progress across all Progress packets */
export interface AccumulatedProgress {
  readRows: bigint;
  readBytes: bigint;
  totalRowsToRead: bigint;
  totalBytesToRead: bigint;
  writtenRows: bigint;
  writtenBytes: bigint;
  elapsedNs: bigint;
  /** Percentage complete (0-100) based on rows read vs total rows */
  percent: number;
}

export interface ProfileInfo {
  rows: bigint;
  blocks: bigint;
  bytes: bigint;
  appliedLimit: boolean;
  rowsBeforeLimit: bigint;
  calculatedRowsBeforeLimit: boolean;
  appliedAggregation: boolean;
  rowsBeforeAggregation: bigint;
}

export interface LogEntry {
  time: string;
  timeMicroseconds: number;
  hostName: string;
  queryId: string;
  threadId: bigint;
  priority: number;  // 1=Fatal, 2=Critical, 3=Error, 4=Warning, 5=Notice, 6=Info, 7=Debug, 8=Trace
  source: string;
  text: string;
}

export type Packet =
  | { type: "Data", batch: RecordBatch }
  | { type: "Totals", batch: RecordBatch }
  | { type: "Extremes", batch: RecordBatch }
  | { type: "Log", entries: LogEntry[] }
  | { type: "Progress", progress: Progress, accumulated: AccumulatedProgress }
  | { type: "ProfileInfo", info: ProfileInfo }
  | { type: "ProfileEvents", batch: RecordBatch, accumulated: Map<string, bigint> }
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
