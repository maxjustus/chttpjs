
import { TEXT_ENCODER } from "../formats/shared.ts";
import { ClientPacketId, DBMS_TCP_PROTOCOL_VERSION, QueryProcessingStage } from "./types.ts";
import { encodeBlock, Method } from "../compression.ts";

/**
 * Handles encoding and writing ClickHouse protocol packets.
 */
export class StreamingWriter {
  private buffer: Uint8Array;
  private offset: number = 0;

  constructor(initialCapacity = 64 * 1024) {
    this.buffer = new Uint8Array(initialCapacity);
  }

  private ensure(n: number) {
    if (this.offset + n <= this.buffer.length) return;
    const next = new Uint8Array(Math.max(this.buffer.length * 2, this.offset + n));
    next.set(this.buffer);
    this.buffer = next;
  }

  writeVarInt(value: bigint | number) {
    this.ensure(10);
    let v = BigInt(value);
    while (v >= 0x80n) {
      this.buffer[this.offset++] = Number((v & 0x7fn) | 0x80n);
      v >>= 7n;
    }
    this.buffer[this.offset++] = Number(v);
  }

  writeString(str: string) {
    const bytes = TEXT_ENCODER.encode(str);
    this.writeVarInt(bytes.length);
    this.ensure(bytes.length);
    this.buffer.set(bytes, this.offset);
    this.offset += bytes.length;
  }

  writeU8(v: number) {
    this.ensure(1);
    this.buffer[this.offset++] = v;
  }

  writeU32LE(v: number) {
    this.ensure(4);
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 4);
    view.setUint32(0, v, true);
    this.offset += 4;
  }

  writeU64LE(v: bigint) {
    this.ensure(8);
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 8);
    view.setBigUint64(0, v, true);
    this.offset += 8;
  }

  flush(): Uint8Array {
    const data = this.buffer.slice(0, this.offset);
    this.offset = 0;
    return data;
  }

  // --- High Level Packet Helpers ---

  encodeHello(database: string, user: string, pass: string): Uint8Array {
    this.writeVarInt(ClientPacketId.Hello);
    this.writeString("chttp-client 0.1.0");
    this.writeVarInt(24); // Major
    this.writeVarInt(8);  // Minor
    this.writeVarInt(DBMS_TCP_PROTOCOL_VERSION);
    this.writeString(database);
    this.writeString(user);
    this.writeString(pass);
    return this.flush();
  }

  encodeAddendum(revision: bigint): Uint8Array {
    if (revision >= 54458n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY
      this.writeString(""); // quota_key
    }
    if (revision >= 54470n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
      this.writeString("notchunked");
      this.writeString("notchunked");
    }
    if (revision >= 54471n) { // DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
      this.writeVarInt(4); // DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION
    }
    return this.flush();
  }

  encodeQuery(qid: string, query: string, revision: bigint, settings: Record<string, string | number | boolean> = {}, compression: boolean = false, params: Record<string, string | number | boolean> = {}): Uint8Array {
    this.writeVarInt(ClientPacketId.Query);
    this.writeString(qid);

    // ClientInfo
    this.writeU8(1); // query_kind: INITIAL_QUERY
    this.writeString(""); // initial_user
    this.writeString(""); // initial_query_id
    this.writeString("0.0.0.0:0"); // initial_address

    if (revision >= 54449n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME
      this.writeU64LE(BigInt(Date.now()) * 1000n);
    }

    this.writeU8(1); // interface: TCP
    this.writeString("chttp-client"); // os_user
    this.writeString("localhost"); // client_hostname
    this.writeString("ClickHouse"); // client_name
    this.writeVarInt(24); // client_version_major
    this.writeVarInt(8);  // client_version_minor
    this.writeVarInt(DBMS_TCP_PROTOCOL_VERSION);

    if (revision >= 54060n) { // DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
      this.writeString(""); // quota_key
    }
    if (revision >= 54448n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH
      this.writeVarInt(1); // distributed_depth
    }
    if (revision >= 54401n) { // DBMS_MIN_REVISION_WITH_VERSION_PATCH
      this.writeVarInt(0); // client_version_patch
    }
    if (revision >= 54442n) { // DBMS_MIN_REVISION_WITH_OPENTELEMETRY
      this.writeU8(0); // No OpenTelemetry
    }
    if (revision >= 54453n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS
      this.writeVarInt(0);
      this.writeVarInt(0);
      this.writeVarInt(0);
    }
    if (revision >= 54475n) { // DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS
      this.writeVarInt(0);
      this.writeVarInt(0);
    }
    if (revision >= 54476n) { // DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER
      this.writeU8(0);
    }

    // Settings
    for (const [key, val] of Object.entries(settings)) {
      this.writeString(key);
      // Modern settings format: Flags -> Value
      this.writeVarInt(0);
      this.writeString(String(val));  // Convert number/boolean to string
    }
    this.writeString(""); // End of settings

    if (revision >= 54472n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES
      this.writeString("");
    }

    if (revision >= 54441n) { // DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET
      this.writeString(""); 
    }

    this.writeVarInt(QueryProcessingStage.Complete);
    // Compression: 0 = disabled, 1 = enabled (as UVarInt per ClickHouse native protocol docs)
    // When enabled, server will compress Data blocks using LZ4
    this.writeVarInt(compression ? 1 : 0);
    this.writeString(query);

    if (revision >= 54459n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
      // Encode parameters with CUSTOM flag - values must be quoted strings
      const SETTING_FLAG_CUSTOM = 2;
      for (const [key, val] of Object.entries(params)) {
        this.writeString(key);
        this.writeVarInt(SETTING_FLAG_CUSTOM);
        // Format value as quoted string for server-side parsing
        const escaped = String(val).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        this.writeString(`'${escaped}'`);
      }
      this.writeString(""); // end of params
    }

    return this.flush();
  }

  encodeData(tableName: string, rowsCount: number, columns: { name: string, type: string, data: Uint8Array }[], revision: bigint, compress: boolean = false, method: number = Method.LZ4): Uint8Array {
    if (compress) {
      // Packet ID and table name are always uncompressed
      this.writeVarInt(ClientPacketId.Data);
      this.writeString(tableName);
      const headerBytes = this.flush();

      // Encode block info + columns (without table name) then compress
      const payload = this.encodeDataBlockContent(rowsCount, columns, revision);
      const compressed = encodeBlock(payload, method);

      // Combine: header (packet ID + table name) + compressed block
      const result = new Uint8Array(headerBytes.length + compressed.length);
      result.set(headerBytes, 0);
      result.set(compressed, headerBytes.length);
      return result;
    }

    // Uncompressed: write everything inline
    this.writeVarInt(ClientPacketId.Data);
    this.writeString(tableName);

    // BlockInfo
    if (revision > 0n) {
      this.writeVarInt(1); // info version
      this.writeU8(2); // is_overflows (2 = false in CH TCP?) -> Actually 0/1 usually, but Rust uses 1/2
      // Let's stick to what Rust does for safe defaults
      this.writeVarInt(2); // bucket_num field
      this.writeI32LE(-1); // bucket_num
      this.writeVarInt(0); // end of BlockInfo
    }

    this.writeVarInt(columns.length);
    this.writeVarInt(rowsCount);

    for (const col of columns) {
      this.writeString(col.name);
      this.writeString(col.type);
      // Custom serialization flag
      if (revision >= 54454n) { // DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION
        this.writeU8(0);
      }
      // Note: This sketch doesn't handle Prefixes (LowCardinality etc) in the writer yet
      this.ensure(col.data.length);
      this.buffer.set(col.data, this.offset);
      this.offset += col.data.length;
    }

    return this.flush();
  }

  /**
   * Encode Data block content (block info + columns) for compression.
   * Does NOT include table name - that's written uncompressed before the compressed block.
   */
  private encodeDataBlockContent(rowsCount: number, columns: { name: string, type: string, data: Uint8Array }[], revision: bigint): Uint8Array {
    // BlockInfo
    if (revision > 0n) {
      this.writeVarInt(1);
      this.writeU8(2);
      this.writeVarInt(2);
      this.writeI32LE(-1);
      this.writeVarInt(0);
    }

    this.writeVarInt(columns.length);
    this.writeVarInt(rowsCount);

    for (const col of columns) {
      this.writeString(col.name);
      this.writeString(col.type);
      if (revision >= 54454n) {
        this.writeU8(0);
      }
      this.ensure(col.data.length);
      this.buffer.set(col.data, this.offset);
      this.offset += col.data.length;
    }

    return this.flush();
  }

  writeI32LE(v: number) {
    this.ensure(4);
    const view = new DataView(this.buffer.buffer, this.buffer.byteOffset + this.offset, 4);
    view.setInt32(0, v, true);
    this.offset += 4;
  }

  encodeCancel(): Uint8Array {
    this.writeVarInt(ClientPacketId.Cancel);
    return this.flush();
  }

  encodePing(): Uint8Array {
    this.writeVarInt(ClientPacketId.Ping);
    return this.flush();
  }
}
