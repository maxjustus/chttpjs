// Type declarations for third-party modules

declare module "bling-hashes" {
  interface City128Value {
    toBuffers(): [Buffer, Buffer];
  }
  
  export function city128(data: Buffer): City128Value;
}

declare module "lz4" {
  export function encodeBound(size: number): number;
  export function encodeBlock(src: Buffer, dst: Buffer): number;
  export function decodeBlock(src: Buffer, dst: Buffer): number;
}

declare module "zstd-napi" {
  export function compress(data: Buffer, level?: number): Buffer;
  export function decompress(data: Buffer): Buffer;
}