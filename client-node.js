const { encodeBlock, decodeBlock, Method } = require('./compression-node');
const http = require('http');

function buildReqUrl(baseUrl, params) {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    url.searchParams.append(key, value); // Don't double-encode
  });
  return url;
}

async function insertCompressed(query, data, sessionId) {
  // Convert data to JSON lines format
  const dataStr = data.map(d => JSON.stringify(d)).join('\n') + '\n';
  const dataBytes = Buffer.from(dataStr, 'utf8');
  
  // Compress using ClickHouse format
  const compressed = encodeBlock(dataBytes, Method.LZ4);
  
  console.log('Original size:', dataBytes.length);
  console.log('Compressed size:', compressed.length);
  console.log('First 30 bytes (hex):', compressed.slice(0, 30).toString('hex'));
  
  const url = buildReqUrl('http://localhost:8123/', {
    session_id: sessionId,
    query: query,
    decompress: '1',  // Tell ClickHouse to decompress the incoming data
    http_native_compression_disable_checksumming_on_decompress: '1', // Bypass checksum verification
  });

  return new Promise((resolve, reject) => {
    const req = http.request(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Length': compressed.length
      }
    }, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`Insert failed: ${res.statusCode} - ${body}`));
        } else {
          resolve(body);
        }
      });
    });

    req.on('error', reject);
    req.write(compressed);
    req.end();
  });
}

async function execQuery(query, sessionId, compressed = false) {
  const params = {
    session_id: sessionId,
    default_format: 'JSONEachRowWithProgress',
  };
  
  if (compressed) {
    params.compress = '1';  // Request compressed response
  }
  
  const url = buildReqUrl('http://localhost:8123/', params);

  return new Promise((resolve, reject) => {
    const req = http.request(url, {
      method: 'POST',
    }, (res) => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const body = Buffer.concat(chunks);
        if (res.statusCode !== 200) {
          reject(new Error(`Query failed: ${res.statusCode} - ${body.toString()}`));
        } else {
          if (compressed) {
            // Decompress the response (skip checksum verification due to CityHash version mismatch)
            try {
              const decompressed = decodeBlock(body, true);
              resolve(decompressed.toString());
            } catch (err) {
              reject(new Error(`Decompression failed: ${err.message}`));
            }
          } else {
            resolve(body.toString());
          }
        }
      });
    });

    req.on('error', reject);
    req.write(query);
    req.end();
  });
}

async function main() {
  const sessionId = '12345';
  
  try {
    // Create table (drop first to ensure clean state)
    console.log('Creating table...');
    await execQuery('DROP TABLE IF EXISTS test', sessionId);
    await execQuery('CREATE TABLE test (hello String) ENGINE = Memory', sessionId);
    
    // Insert data with compression
    console.log('\nInserting compressed data...');
    // const data = [
    //   { hello: 'world' },
    //   { hello: 'steve' },
    // ];

    // big data to test
    let data = [];
    for (let i = 0; i < 10000; i++) {
      data.push({ hello: 'world' + i });
    }
    
    
    const insertQuery = 'INSERT INTO test (hello) FORMAT JSONEachRow';
    await insertCompressed(insertQuery, data, sessionId);
    console.log('Insert successful!');
    
    // Query to verify (uncompressed)
    console.log('\nQuerying data (uncompressed)...');
    const result = await execQuery('SELECT * FROM test FORMAT JSONEachRow', sessionId);
    console.log('Result:', result);
    
    // Query with compressed response
    console.log('\nQuerying data (compressed response)...');
    const compressedResult = await execQuery('SELECT * FROM test FORMAT JSONEachRow', sessionId, true);
    console.log('Result:', compressedResult);
    
    // Verify they match
    console.log('\nResults match:', result === compressedResult);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

main();
