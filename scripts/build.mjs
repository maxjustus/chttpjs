import path from "node:path";
import { fileURLToPath } from "node:url";
import * as esbuild from "esbuild";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const nativeDir = path.join(root, "native");

/**
 * Rewrite any import that resolves into native/ to the published subpath, so
 * non-native bundles reference dist/native.js at runtime instead of inlining
 * a second copy of the codec layer. Source uses plain relative imports and
 * stays runnable with bare node; the package boundary exists only here.
 */
const externalizeNative = {
  name: "externalize-native",
  setup(build) {
    build.onResolve({ filter: /^\.\.?\// }, (args) => {
      const resolved = path.resolve(args.resolveDir, args.path);
      if (resolved === nativeDir || resolved.startsWith(nativeDir + path.sep)) {
        return { path: "@maxjustus/chwire/native", external: true };
      }
      return null;
    });
  },
};

const base = {
  bundle: true,
  target: "es2022",
  minify: true,
  // http_transport.ts reaches node:http through a dynamic import that esbuild
  // cannot statically resolve, so the browser builds fail without this. Only
  // the Node-only httpCompression path ever evaluates it.
  external: ["lz4-napi", "zstd-napi", "node:http", "node:https"],
  absWorkingDir: root,
};

const builds = [
  {
    entryPoints: ["native/index.ts"],
    format: "esm",
    platform: "neutral",
    outfile: "dist/native.js",
  },
  { entryPoints: ["native/index.ts"], format: "cjs", platform: "node", outfile: "dist/native.cjs" },
  {
    entryPoints: ["index.ts"],
    format: "esm",
    platform: "browser",
    outfile: "dist/chwire.js",
    define: { BUILD_WITH_ZSTD: "true" },
    plugins: [externalizeNative],
  },
  {
    entryPoints: ["index.ts"],
    format: "cjs",
    platform: "node",
    outfile: "dist/chwire.cjs",
    define: { BUILD_WITH_ZSTD: "true" },
    plugins: [externalizeNative],
  },
  {
    entryPoints: ["index.ts"],
    format: "esm",
    platform: "browser",
    outfile: "dist/chwire-lz4.js",
    define: { BUILD_WITH_ZSTD: "false" },
    plugins: [externalizeNative],
  },
  {
    entryPoints: ["index.ts"],
    format: "cjs",
    platform: "node",
    outfile: "dist/chwire-lz4.cjs",
    define: { BUILD_WITH_ZSTD: "false" },
    plugins: [externalizeNative],
  },
  {
    entryPoints: ["tcp_client/index.ts"],
    format: "esm",
    platform: "node",
    outfile: "dist/tcp.js",
    plugins: [externalizeNative],
  },
  {
    entryPoints: ["tcp_client/index.ts"],
    format: "cjs",
    platform: "node",
    outfile: "dist/tcp.cjs",
    plugins: [externalizeNative],
  },
  // Standalone CLI: bundles the native layer inline, no package boundary.
  {
    entryPoints: ["tcp_client/cli.ts"],
    format: "esm",
    platform: "node",
    outfile: "dist/cli.js",
    minify: false,
  },
];

await Promise.all(builds.map((b) => esbuild.build({ ...base, ...b })));
console.log(`built ${builds.length} bundles`);
