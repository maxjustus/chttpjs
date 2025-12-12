// Type declarations for third-party modules

// Build-time constant set by esbuild --define
// May be undefined when running unbundled in development
declare const BUILD_WITH_ZSTD: boolean | undefined;

// Augment @dweb-browser/zstd-wasm with missing exports
declare module "@dweb-browser/zstd-wasm" {
  export function initSync(options: {
    module: ArrayBuffer | WebAssembly.Module;
  }): void;
  export function compress(source: Uint8Array, level: number): Uint8Array;
  export function decompress(source: Uint8Array): Uint8Array;
}

declare module "@dweb-browser/zstd-wasm/zstd_wasm_bg_wasm" {
  export default function getZstdWasm(): ArrayBuffer;
}
