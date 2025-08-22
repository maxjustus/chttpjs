// TODO: this won't work unless we port CH's custom compression framing
// https://raw.githubusercontent.com/ClickHouse/clickhouse-rs/refs/heads/main/src/compression/lz4.rs
//
//  1. Compression format: ClickHouse uses a custom wrapper around LZ4 block compression with:
//    - 16-byte CityHash128 checksum
//    - 1-byte magic number (0x82 for LZ4)
//    - 4-byte compressed size (little-endian)
//    - 4-byte uncompressed size (little-endian)
//    - Raw LZ4 compressed data
//  2. Checksum calculation: CityHash128 with 64-bit rotation (swapping high and low halves)
//  3. Key parameter: Use decompress=1 in query parameters when sending compressed data


// import * as lz4 from "https://esm.sh/jsr/@nick/lz4";
import { encodeBlock, decodeBlock, Method } from "./compression2.ts";


async function* readResponse<T = any>(response: Promise<Response>): AsyncGenerator<T> {
  console.log("Response status:", response.status);
  const reader = (await response).body.getReader();
  const decoder = new TextDecoder();

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const chunk = decoder.decode(value, { stream: true });
    const lines = chunk.split('\n').filter(line => line);

    for (const line of lines) {
      const data = JSON.parse(line);
      yield data
    }
  }
}

const buildReqUrl = (baseUrl: string, params: Record<string, string>) => {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, encodeURIComponent(value));
  });
  return url.toString();
}

function selectFetch<T = any>(query: string, sessionId: string) {
  const url = buildReqUrl('http://localhost:8123/', {
    session_id: sessionId,
    decompress: '1',
    default_format: 'JSONEachRowWithProgress',
  });

  return readResponse<T>(fetch(url, {
    method: 'POST',
    body: query
  }))
};

async function execFetch(query: string, sessionId: string) {
  for await (const res of selectFetch(query, sessionId)) {
    console.log(res);
  }
}

const insertFetch = async (query: string, data: any[], sessionId: string) => {
  const dataStr = data.map(d => JSON.stringify(d)).join('\n') + '\n';

  const dataBytes = new TextEncoder().encode(dataStr);
  const compressed = await encodeBlock(dataBytes, Method.LZ4);

  console.log('Compressed size:', compressed.length);
  const url = buildReqUrl('http://localhost:8123/', {
    session_id: sessionId,
    query: query,
  });

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Encoding': 'lz4',
      'Content-Type': 'application/octet-stream',
    },
    body: compressed
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Insert failed: ${res.status} - ${body}`);
  }

  return res;
};

(async () => {
  const sessionId = '12345';

  const data = [
    { hello: 'world' },
    { hello: 'steve' },
  ];

  const query = 'SELECT number FROM numbers(5)';

  for await (const res of selectFetch(query, sessionId)) {
    console.log(res);
  };

  const createQuery = 'CREATE TABLE IF NOT EXISTS test (hello String) ENGINE = Memory';

  await execFetch(createQuery, sessionId);

  const insertQuery = 'INSERT INTO test (hello) VALUES';

  const res = await insertFetch(insertQuery, data, sessionId);

  console.log('Insert response status:', res);
})();

