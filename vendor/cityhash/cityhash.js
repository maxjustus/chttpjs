// Wrapper for ch-city-wasm that works both bundled (esbuild) and unbundled (Node.js)
// When bundled: esbuild picks the default export from ch-city-wasm package (ch_city_wasm.js)
//               which exports initSync, so we initialize with inline wasm
// When unbundled: Node.js picks the node export (node.js) which auto-initializes
//                 and initSync may not be exported, but that's fine - already initialized

import * as cityWasm from "ch-city-wasm";
import getWasm from "ch-city-wasm/wasm";

// Try to initialize if initSync is available (bundled case)
// In unbundled Node.js, the module auto-initializes via the node.js entry
if (typeof cityWasm.initSync === "function") {
  cityWasm.initSync({ module: getWasm() });
}

export const cityhash_102_128 = cityWasm.cityhash_102_128;
