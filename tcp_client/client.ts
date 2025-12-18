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
import { getCodec } from "../formats/native/codecs.ts";
import { BufferReader, BufferWriter } from "../formats/native/io.ts";
import { Table } from "../formats/native/table.ts";
import { asRows } from "../formats/native/index.ts";
import { init as initCompression } from "../compression.ts";

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
    this.log(`Handshake: Got PacketID ${packetId}`);
    
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

    if (revision >= 54471n) await this.reader.readVarInt(); 

    const timezone = revision >= 54058n ? await this.reader.readString() : "";
    const displayName = revision >= 54372n ? await this.reader.readString() : "";
    const patch = revision >= 54401n ? await this.reader.readVarInt() : revision;

    if (revision >= 54470n) {
      await this.reader.readString(); 
      await this.reader.readString(); 
    }

    if (revision >= 54461n) {
      const rulesSize = Number(await this.reader.readVarInt());
      for (let i = 0; i < rulesSize; i++) {
        await this.reader.readString();
        await this.reader.readString();
      }
    }

    if (revision >= 54462n) await this.reader.readU64LE();

    if (revision >= 54474n) { // DBMS_MIN_REVISION_WITH_SERVER_SETTINGS
      while (true) {
        const name = await this.reader.readString();
        if (name === "") break;
        await this.reader.readVarInt(); // flags
        await this.reader.readString(); // value
      }
    }

    if (revision >= 54477n) await this.reader.readVarInt();
    if (revision >= 54479n) await this.reader.readVarInt();

    this._serverHello = { serverName, major, minor, revision, timezone, displayName, patch };
    
    if (revision >= 54458n) {
      this.log("Handshake: Sending Addendum...");
      const addendum = this.writer.encodeAddendum(revision);
      this.socket.write(addendum);
    }
    this.log("Handshake: Complete!");
  }

  async execute(sql: string): Promise<void> {
    for await (const _ of this.query(sql)) {
      // Just drain
    }
  }

  async insert(sql: string, data: Table | AsyncIterable<Table> | Iterable<Table>) {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");

    this.log("Insert: Sending query...");
    const queryPacket = this.writer.encodeQuery(randomUUID(), sql, this.serverHello.revision, {
      "compress": "0"
    });
    this.socket.write(queryPacket);

    const queryDelimiter = this.writer.encodeData("", 0, [], this.serverHello.revision);
    this.socket.write(queryDelimiter);

    this.log("Insert: Waiting for Header...");
    let schemaReceived = false;
    while (!schemaReceived) {
      const packetId = Number(await this.reader.readVarInt());
      this.log(`Insert: Packet ${packetId} while waiting for header`);
      
      switch (packetId) {
        case ServerPacketId.Data: {
          const block = await this.readBlock();
          this.currentSchema = block.columns.map(c => ({ name: c.name, type: c.type }));
          schemaReceived = true;
          this.log(`Insert: Header received with ${block.columns.length} columns`);
          break;
        }
        case ServerPacketId.Progress:
          await this.readProgress();
          break;
        case ServerPacketId.Log:
          await this.readBlock();
          break;
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
      this.log(`Insert: Sending Data Block (${table.rowCount} rows)...`);
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

    this.log("Insert: Sending End-of-Data Delimiter...");
    // Delimiter MUST be 0 columns and 0 rows to signal end of stream
    const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision);
    this.socket.write(delimiter);

    this.log("Insert: Draining responses...");
    while (true) {
      const packetId = Number(await this.reader.readVarInt());
      this.log(`Insert: Packet ${packetId} in drain loop`);
      if (packetId === ServerPacketId.EndOfStream) break;
      
      switch (packetId) {
        case ServerPacketId.Progress: await this.readProgress(); break;
        case ServerPacketId.ProfileInfo: await this.readProfileInfo(); break;
        case ServerPacketId.Data:
        case ServerPacketId.Log:
        case ServerPacketId.ProfileEvents:
          const block = await this.readBlock();
          this.log(`  Read side-band block: ${block.rowCount} rows`);
          break;
        case ServerPacketId.Exception:
          throw new Error(`Insert Commit Error: ${await this.reader.readString()}`);
        default:
          this.log(`Insert: Skipping unknown packet ${packetId}`);
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
      if (this.serverHello!.revision >= 54454n) {
        const hasCustom = await this.reader!.readU8();
        if (hasCustom) await this.skipKinds(colType);
      }
      columns.push({ name: colName, type: colType });

      const codec = getCodec(colType);
      while (true) {
        const currentBuffer = this.reader!.peekAll();
        const tempReader = new BufferReader(currentBuffer);
        try {
          codec.readPrefix?.(tempReader);
          const data = codec.decode(tempReader, rowsCount);
          this.reader!.consume(tempReader.offset);
          columnData.push(data);
          break;
        } catch (err: any) {
          if (err.message && err.message.includes("Unexpected end of buffer")) {
            const more = await this.reader!.nextChunk();
            if (!more) throw new Error("EOF while decoding block");
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

  async *query(sql: string): AsyncGenerator<Packet> {
    if (!this.socket || !this.reader || !this.serverHello) throw new Error("Not connected");

    const queryPacket = this.writer.encodeQuery(randomUUID(), sql, this.serverHello.revision, {
      "compress": "0"
    });
    this.socket.write(queryPacket);

    const delimiter = this.writer.encodeData("", 0, [], this.serverHello.revision);
    this.socket.write(delimiter); 

    this.currentSchema = null;
    this.reader.setCompression(false);

    while (true) {
      const packetId = Number(await this.reader.readVarInt());
      this.log(`Query: PacketID ${packetId}`);
      
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
          this.log("Query: EndOfStream");
          yield { type: "EndOfStream" };
          return;
        case ServerPacketId.Exception:
          throw new Error(`Query Error: ${await this.reader.readString()}`);
        default:
          throw new Error(`Unknown packet ID: ${packetId}. Cannot proceed.`);
      }
    }
  }

  private async skipKinds(typeStr: string) {
    const kind = await this.reader!.readU8();
    if (typeStr.startsWith("Array(")) {
      const inner = typeStr.slice(6, -1);
      await this.skipKinds(inner);
    } else if (typeStr.startsWith("Map(")) {
      const args = typeStr.slice(4, -1);
      const [k, v] = args.split(", ");
      await this.skipKinds(k);
      await this.skipKinds(v);
    } else if (typeStr.startsWith("Nullable(")) {
      const inner = typeStr.slice(9, -1);
      await this.skipKinds(inner);
    }
  }

  close() {
    this.socket?.destroy();
    this.socket = null;
  }
}