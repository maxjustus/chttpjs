/**
 * `dispatcher`: an undici Dispatcher handed straight to fetch, so a consumer
 * controls connection timeouts, proxies, and retries without a client option
 * per knob. Node's built-in fetch honors `init.dispatcher`, so this needs no
 * undici dependency in the client itself.
 */

import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { after, before, describe, it } from "node:test";
import { Agent } from "undici";
import { insert, query } from "../client.ts";

let server: Server;
let url: string;
let delayMs = 0;
let lastPath: string | undefined;

function isHeadersTimeout(error: unknown): boolean {
  const cause = (error as { cause?: { code?: string } }).cause;
  return cause?.code === "UND_ERR_HEADERS_TIMEOUT";
}

before(async () => {
  server = createServer((request, response) => {
    lastPath = request.url;
    const timer = setTimeout(() => response.end('{"x":1}\n'), delayMs);
    response.on("close", () => clearTimeout(timer));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  url = `http://127.0.0.1:${address.port}/`;
});

after(async () => {
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
});

describe("dispatcher option", () => {
  it("applies the dispatcher's timeouts to a query", async () => {
    delayMs = 2_000;
    const dispatcher = new Agent({ headersTimeout: 500 });
    try {
      await assert.rejects(
        Promise.resolve(query("SELECT 1", { url, compression: false, dispatcher })),
        isHeadersTimeout,
      );
    } finally {
      await dispatcher.close();
    }
  });

  it("applies the dispatcher's timeouts to an insert", async () => {
    delayMs = 2_000;
    const dispatcher = new Agent({ headersTimeout: 500 });
    try {
      await assert.rejects(
        insert("INSERT INTO t FORMAT JSONEachRow", [new TextEncoder().encode('{"a":1}\n')], {
          url,
          dispatcher,
        }),
        isHeadersTimeout,
      );
    } finally {
      await dispatcher.close();
    }
  });

  it("does not send the dispatcher as a ClickHouse setting", async () => {
    delayMs = 0;
    const dispatcher = new Agent();
    try {
      for await (const _ of query("SELECT 1", { url, compression: false, dispatcher })) {
        // drain
      }
    } finally {
      await dispatcher.close();
    }
    assert.ok(lastPath, "server saw no request");
    assert.ok(!lastPath.includes("dispatcher"), `dispatcher leaked into the URL: ${lastPath}`);
  });
});
