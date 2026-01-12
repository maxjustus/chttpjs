
import {
  ClientPacketId,
  DBMS_TCP_PROTOCOL_VERSION,
  QueryProcessingStage,
  REVISIONS,
  QueryKind,
  Interface,
  CLIENT_VERSION,
  DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION,
} from "./types.ts";
import { BlockInfoField, BufferWriter } from "@maxjustus/chttp/native";
import { encodeBlock, Method, type MethodCode } from "../compression.ts";

/**
 * Handles encoding and writing ClickHouse protocol packets.
 * Uses optimized BufferWriter internally.
 */
export class StreamingWriter {
  private writer: BufferWriter;

  constructor(initialCapacity = 64 * 1024) {
    this.writer = new BufferWriter(initialCapacity);
  }

  writeVarInt(value: bigint | number) {
    this.writer.writeVarint(value);
  }

  writeString(str: string) {
    this.writer.writeString(str);
  }

  writeU8(v: number) {
    this.writer.writeU8(v);
  }

  writeU32LE(v: number) {
    this.writer.writeU32LE(v);
  }

  writeU64LE(v: bigint) {
    this.writer.writeU64LE(v);
  }

  writeI32LE(v: number) {
    this.writer.writeI32LE(v);
  }

  flush(): Uint8Array {
    const data = this.writer.finish();
    this.writer.reset();
    return data;
  }

  // --- High Level Packet Helpers ---

  encodeHello(database: string, user: string, pass: string): Uint8Array {
    this.writeVarInt(ClientPacketId.Hello);
    this.writeString("chttp-client 0.1.0");
    this.writeVarInt(CLIENT_VERSION.MAJOR);
    this.writeVarInt(CLIENT_VERSION.MINOR);
    this.writeVarInt(DBMS_TCP_PROTOCOL_VERSION);
    this.writeString(database);
    this.writeString(user);
    this.writeString(pass);
    return this.flush();
  }

  /**
   * Encode the addendum packet sent after receiving ServerHello.
   * @param revision - Negotiated protocol revision
   */
  encodeAddendum(revision: bigint): Uint8Array {
    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY) {
      this.writeString(""); // quota_key
    }
    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS) {
      // Always use notchunked - chunked requires server config that users can't control
      this.writeString("notchunked");
      this.writeString("notchunked");
    }
    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL) {
      this.writeVarInt(DBMS_PARALLEL_REPLICAS_PROTOCOL_VERSION);
    }
    return this.flush();
  }

  encodeQuery(qid: string, query: string, revision: bigint, settings: Record<string, unknown> = {}, compression: boolean = false, params: Record<string, unknown> = {}): Uint8Array {
    this.writeVarInt(ClientPacketId.Query);
    this.writeString(qid);

    // --- ClientInfo ---
    this.writeU8(QueryKind.InitialQuery);
    this.writeString(""); // initial_user
    this.writeString(""); // initial_query_id
    this.writeString("0.0.0.0:0"); // initial_address

    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_QUERY_START_TIME) {
      this.writeU64LE(BigInt(Date.now()) * 1000n);
    }

    this.writeU8(Interface.TCP);
    this.writeString("chttp-client"); // os_user
    this.writeString("localhost");    // client_hostname
    this.writeString("ClickHouse");   // client_name
    this.writeVarInt(CLIENT_VERSION.MAJOR);
    this.writeVarInt(CLIENT_VERSION.MINOR);
    this.writeVarInt(DBMS_TCP_PROTOCOL_VERSION);

    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO) {
      this.writeString(""); // quota_key
    }
    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_DEPTH) {
      this.writeVarInt(1); // distributed_depth
    }
    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_VERSION_PATCH) {
      this.writeVarInt(0); // client_version_patch
    }
    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_OPENTELEMETRY) {
      this.writeU8(0); // opentelemetry trace context
    }
    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_PARALLEL_REPLICAS) {
      this.writeVarInt(0); // parallel_replicas_count
      this.writeVarInt(0); // parallel_replica_offset
      this.writeVarInt(0); // parallel_replicas_mode
    }
    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_QUERY_AND_LINE_NUMBERS) {
      this.writeVarInt(0); // query_id (secondary)
      this.writeVarInt(0); // query_line_number
    }
    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_JWT_IN_INTERSERVER) {
      this.writeU8(0); // jwt
    }

    // --- Settings ---
    for (const [key, val] of Object.entries(settings)) {
      this.writeString(key);
      if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS) {
        this.writeVarInt(0); // Flags: 0 = IMPORTANT
      }
      this.writeString(String(val));
    }
    this.writeString(""); // End of settings

    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_INTERSERVER_EXTERNALLY_GRANTED_ROLES) {
      this.writeString(""); // interserver_externally_granted_roles
    }

    if (revision >= REVISIONS.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET) {
      this.writeString(""); // interserver_secret
    }

    this.writeVarInt(QueryProcessingStage.Complete);
    
    // Compression: 0 = disabled, 1 = enabled
    this.writeVarInt(compression ? 1 : 0);
    this.writeString(query);

    if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS) {
      const SETTING_FLAG_CUSTOM = 2;
      for (const [key, val] of Object.entries(params)) {
        this.writeString(key);
        this.writeVarInt(SETTING_FLAG_CUSTOM);
        // Parameters are sent as quoted strings for server-side parsing
        const escaped = String(val).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        this.writeString(`'${escaped}'`);
      }
      this.writeString(""); // end of params
    }

    return this.flush();
  }

  encodeData(tableName: string, rowsCount: number, columns: { name: string, type: string, data: Uint8Array }[], revision: bigint, compress: boolean = false, method: MethodCode = Method.LZ4): Uint8Array {
    if (compress) {
      this.writeVarInt(ClientPacketId.Data);
      this.writeString(tableName);
      const headerBytes = this.flush();

      const payload = this.encodeDataBlockContent(rowsCount, columns, revision);
      const compressed = encodeBlock(payload, method);

      const result = new Uint8Array(headerBytes.length + compressed.length);
      result.set(headerBytes, 0);
      result.set(compressed, headerBytes.length);
      return result;
    }

    this.writeVarInt(ClientPacketId.Data);
    this.writeString(tableName);
    this.writer.write(this.encodeDataBlockContent(rowsCount, columns, revision));
    return this.flush();
  }

  private encodeDataBlockContent(rowsCount: number, columns: { name: string, type: string, data: Uint8Array }[], revision: bigint): Uint8Array {
    const contentWriter = new BufferWriter();

    if (revision > 0n) {
      // BlockInfo: field-based encoding where field 0 marks end
      contentWriter.writeVarint(BlockInfoField.IsOverflows);
      contentWriter.writeU8(0); // is_overflows = false

      contentWriter.writeVarint(BlockInfoField.BucketNum);
      contentWriter.writeI32LE(-1); // bucket_num = -1 (no bucket)

      contentWriter.writeVarint(BlockInfoField.End);
    }

    contentWriter.writeVarint(columns.length);
    contentWriter.writeVarint(rowsCount);

    for (const col of columns) {
      contentWriter.writeString(col.name);
      contentWriter.writeString(col.type);
      
      if (revision >= REVISIONS.DBMS_MIN_PROTOCOL_VERSION_WITH_CUSTOM_SERIALIZATION) {
        // has_custom_serialization (bool) - always 0 (dense) for now
        contentWriter.writeU8(0);
      }
      contentWriter.write(col.data);
    }

    return contentWriter.finish();
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
