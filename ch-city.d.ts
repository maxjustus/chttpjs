interface ChCity {
  cityhash64(data: string | Uint8Array | ArrayBuffer): Uint8Array;
  cityhash64Hex(data: string | Uint8Array | ArrayBuffer): string;
  cityhash102(data: string | Uint8Array | ArrayBuffer): Uint8Array;
  cityhash102Hex(data: string | Uint8Array | ArrayBuffer): string;
  digest64Length(): number;
  digestLength(): number;
  version(): string;
}

export function createChCity(): Promise<ChCity>;
