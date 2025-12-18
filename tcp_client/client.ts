import * as net from "node:net";
import { randomUUID } from "node:crypto";
import { StreamingReader } from "./reader.ts";
import { StreamingWriter } from "./writer.ts";
import { 
  ServerPacketId, 
  type Progress, 
  type ServerHello, 
  type ProfileInfo,
  type Packet,
  DBMS_TCP_PROTOCOL_VERSION 
} from "./types.ts";
import { readBlockInfo } from "./protocol_data.ts";
import { getCodec, defaultDeserializerState } from "../formats/native/codecs.ts";
import { BufferReader, BufferWriter } from "../formats/native/io.ts";
import { Table } from "../formats/native/table.ts";
import { asRows, type DeserializerState, type KindPlan } from "../formats/native/index.ts";
import { init as initCompression } from "../compression.ts";
import { parseTypeList, parseTupleElements } from "../formats/shared.ts";

export interface TcpClientOptions {
  host: string;
  port: number;
  database?: string;
  user?: string;
  password?: string;
  debug?: boolean;
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
  private deserializerState: DeserializerState = defaultDeserializerState();

  private log(...args: any[]) {
    if (this.options.debug) {
      console.log("[TcpClient]", ...args);
    }
  }

  /** Server info from handshake, available after connect() */
  get serverHello() { return this._serverHello; }

  constructor(options: TcpClientOptions) {
    this.options = {
      database: "default",
      user: "default",
      password: "",
      debug: true,
      ...options
    };
  }

  async connect(): Promise<void> {
    await initCompression();
    return new Promise((resolve, reject) => {
      this.socket = net.connect(this.options.port, this.options.host);
      this.socket.on("connect", async () => {
        try {
          this.reader = new StreamingReader(this.socket!);
          await this.handshake();
          resolve();
        } catch (err) {
          reject(err);
        }
      });
      this.socket.on("error", (err) => reject(err));
    });
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
      throw new Error(`Server Exception: ${await this.reader.readString()}`);
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
    for await (const _ of this.query(sql)) {}
  }

  async insert(sql: string, data: Table | AsyncIterable<Table> | Iterable<Table>) {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");

    const queryPacket = this.writer.encodeQuery(randomUUID(), sql, this.serverHello.revision, {
      "compress": "0",
      "allow_special_serialization_kinds_in_output_formats": "0"
    });
    this.socket.write(queryPacket);

    const queryDelimiter = this.writer.encodeData("", 0, [], this.serverHello.revision);
    this.socket.write(queryDelimiter);

    let schemaReceived = false;
    while (!schemaReceived) {
      const packetId = Number(await this.reader.readVarInt());
      
      switch (packetId) {
        case ServerPacketId.Data: {
          const block = await this.readBlock();
          this.currentSchema = block.columns.map(c => ({ name: c.name, type: c.type }));
          schemaReceived = true;
          break;
        }
        case ServerPacketId.Progress: await this.readProgress(); break;
        case ServerPacketId.Log: await this.readBlock(); break;
        case 11: // TableColumns
          await this.reader.readString();
          await this.reader.readString();
          break;
        case ServerPacketId.Exception:
          throw new Error(`Insert Init Error: ${await this.reader.readString()}`);
        default:
          throw new Error(`Unexpected packet while waiting for insert header: ${packetId}`);
      }
    }

    const blocks = (data instanceof Table) 
      ? [data] 
      : (data as AsyncIterable<Table> | Iterable<Table>);

    let totalInserted = 0;
    for await (const table of blocks) {
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

      const dataPacket = this.writer.encodeData("", table.rowCount, encodedColumns, this.serverHello.revision);
      this.socket.write(dataPacket);
      totalInserted += table.rowCount;
    }

    const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision);
    this.socket.write(delimiter);

    while (true) {
      const packetId = Number(await this.reader.readVarInt());
      if (packetId === ServerPacketId.EndOfStream) break;
      
      switch (packetId) {
        case ServerPacketId.Progress: await this.readProgress(); break;
        case ServerPacketId.ProfileInfo: await this.readProfileInfo(); break;
        case ServerPacketId.Data:
        case ServerPacketId.Log:
        case ServerPacketId.ProfileEvents:
          await this.readBlock();
          break;
        case ServerPacketId.Exception:
          throw new Error(`Insert Commit Error: ${await this.reader.readString()}`);
      }
    }
    this.log(`Successfully inserted ${totalInserted} rows.`);
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

  private async parseKindPlanStreaming(typeStr: string, plan: KindPlan, path: number[]) {
    const kind = Number(await this.reader!.readU8());
    plan.set(path.join(","), kind);

    if (typeStr.startsWith("Tuple")) {
      const elements = parseTupleElements(typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")")));
      for (let i = 0; i < elements.length; i++) {
        await this.parseKindPlanStreaming(elements[i].type, plan, [...path, i]);
      }
    } else if (typeStr.startsWith("Array")) {
      const innerType = typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")"));
      await this.parseKindPlanStreaming(innerType, plan, [...path, 0]);
    } else if (typeStr.startsWith("Map")) {
      const args = parseTypeList(typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")")));
      await this.parseKindPlanStreaming(args[0], plan, [...path, 0]);
      await this.parseKindPlanStreaming(args[1], plan, [...path, 1]);
    } else if (typeStr.startsWith("Nullable")) {
      const innerType = typeStr.substring(typeStr.indexOf("(") + 1, typeStr.lastIndexOf(")"));
      await this.parseKindPlanStreaming(innerType, plan, [...path, 0]);
    }
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

  private async readBlock(): Promise<Table> {
    const tableName = await this.reader!.readString();
    await readBlockInfo(this.reader!);
    const columnsCount = Number(await this.reader!.readVarInt());
    const rowsCount = Number(await this.reader!.readVarInt());

    const columns: ColumnSchema[] = [];
    const columnData = [];

    for (let i = 0; i < columnsCount; i++) {
      const colName = await this.reader!.readString();
      const colType = await this.reader!.readString();

      let kindPlan: KindPlan | undefined = undefined;
      if (this.serverHello!.revision >= 54454n) {
        const hasCustom = await this.reader!.readU8() !== 0;
        if (hasCustom) {
          kindPlan = new Map();
          await this.parseKindPlanStreaming(colType, kindPlan, []);
        }
      }

      columns.push({ name: colName, type: colType });

      const codec = getCodec(colType);
      const state = { ...this.deserializerState, kindPlan };

      if (rowsCount === 0) {
        // For empty blocks, some codecs might still need to read prefixes, 
        // but generally we can just get an empty column.
        // Let's use a dummy BufferReader to see if it wants to read anything.
        const tempReader = new BufferReader(new Uint8Array(0));
        columnData.push(codec.decode(tempReader, 0, state, []));
        continue;
      }

      while (true) {
        const currentBuffer = this.reader!.peekAll();
        const tempReader = new BufferReader(currentBuffer);
        try {
          codec.readPrefix?.(tempReader);
          const data = codec.decode(tempReader, rowsCount, state, []);
          this.reader!.consume(tempReader.offset);
          columnData.push(data);
          break;
        } catch (err: any) {
          if (err.message && err.message.includes("Unexpected end of buffer")) {
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

  async *query(sql: string, settings: Record<string, string> = {}): AsyncGenerator<Packet> {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");

    const queryPacket = this.writer.encodeQuery(randomUUID(), sql, this.serverHello.revision, {
      "compress": "0",
      "allow_special_serialization_kinds_in_output_formats": "0",
      ...settings
    });
    this.socket.write(queryPacket);

    const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision);
    this.socket.write(delimiter); 

    this.currentSchema = null;
    this.reader.setCompression(false);

    while (true) {
      const packetId = Number(await this.reader.readVarInt());
      // console.log(`[TcpClient] packetId=${packetId}`);
      
      switch (packetId) {
        case ServerPacketId.Data: {
          const table = await this.readBlock();
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
        case ServerPacketId.ProfileEvents:
          yield { type: "ProfileEvents", table: await this.readBlock() };
          break;
        case ServerPacketId.EndOfStream:
          yield { type: "EndOfStream" };
          return;
        case ServerPacketId.Exception:
          throw new Error(`Query Error: ${await this.reader.readString()}`);
        default:
          throw new Error(`Unknown packet ID: ${packetId}. Cannot proceed.`);
      }
    }
  }

  close() {
    this.socket?.destroy();
    this.socket = null;
  }
}