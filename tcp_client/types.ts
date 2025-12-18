import { Table } from "../formats/native/table.ts";

// Modern revision. We disable sparse/custom serialization via settings
// (allow_special_serialization_kinds_in_output_formats=0) to keep the protocol simple.
export const DBMS_TCP_PROTOCOL_VERSION = 54479n;

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
  Log: 10,
  ProfileEvents: 14,
} as const;

export const QueryProcessingStage = {
  FetchColumns: 0,
  WithMergeableState: 1,
  Complete: 2,
  WithMergableStateAfterAggregation: 3,
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

export interface Progress {
  readRows: bigint;
  readBytes: bigint;
  totalRowsToRead: bigint;
  totalBytesToRead?: bigint;
  writtenRows?: bigint;
  writtenBytes?: bigint;
  elapsedNs?: bigint;
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

export type Packet = 
  | { type: "Data", table: Table }
  | { type: "Progress", progress: Progress }
  | { type: "ProfileInfo", info: ProfileInfo }
  | { type: "ProfileEvents", table: Table }
  | { type: "EndOfStream" };

export const REVISIONS = {
  DBMS_MIN_REVISION_WITH_CLIENT_INFO: 54032n,
  DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE: 54058n,
  DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME: 54372n,
  DBMS_MIN_REVISION_WITH_VERSION_PATCH: 54401n,
  DBMS_MIN_REVISION_WITH_SERVER_LOGS: 54406n,
  DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT: 54456n,
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