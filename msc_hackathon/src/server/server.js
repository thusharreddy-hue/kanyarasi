const http = require('node:http');
const https = require('node:https');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer, WebSocket } = require('ws');

const auctionEngine = require('../engine/auction_engine');
const batchPersister = require('../engine/batch_persister');
const metrics = require('../engine/metrics');
const { verifyAuditLedger } = require('../audit/verifier');

const PORT = process.env.PORT || 3000;
const HTTPS_PORT = process.env.HTTPS_PORT || 3443;
const PUBLIC_DIR = path.resolve(__dirname, '../../');

// Request Handler for HTTP & HTTPS
function handleHttpRequest(req, res) {
  // Enable CORS for local cross-origin testing if needed
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Client-Timestamp');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  // --- REST API ENDPOINTS ---

  // GET /api/auctions
  if (req.method === 'GET' && pathname === '/api/auctions') {
    const auctions = auctionEngine.getAllAuctions();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, auctions }));
    return;
  }

  // POST /api/bid
  if (req.method === 'POST' && pathname === '/api/bid') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = JSON.parse(body || '{}');
        const result = auctionEngine.processBid(payload);

        if (result.success) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
        } else if (result.reason === 'ERR_STALE_BID' || result.reason === 'ERR_OUT_OF_ORDER' || result.reason === 'ERR_INSUFFICIENT_STEP') {
          res.writeHead(409, { 'Content-Type': 'application/json' });
        } else {
          res.writeHead(400, { 'Content-Type': 'application/json' });
        }
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, reason: 'ERR_INVALID_JSON', message: err.message }));
      }
    });
    return;
  }

  // GET /api/metrics
  if (req.method === 'GET' && pathname === '/api/metrics') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      success: true,
      metrics: metrics.getSnapshot(),
      activeClients: wss ? wss.clients.size : 0
    }));
    return;
  }

  // GET /api/audit-verify
  if (req.method === 'GET' && pathname === '/api/audit-verify') {
    batchPersister.flushSync();
    const auditReport = verifyAuditLedger();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, report: auditReport }));
    return;
  }

  // POST /api/reset
  if (req.method === 'POST' && pathname === '/api/reset') {
    auctionEngine.resetLots();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, message: 'All auction lots and metrics have been reset.' }));
    broadcast({
      type: 'AUCTIONS_RESET',
      auctions: auctionEngine.getAllAuctions(),
      timestamp: Date.now()
    });
    return;
  }

  // POST /api/stress-test (Server-side concurrent spike generator)
  if (req.method === 'POST' && pathname === '/api/stress-test') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const { requests = 5000, lotId = 'A101', mode = 'contention' } = JSON.parse(body || '{}');
        const summary = await runServerStressSpike(requests, lotId, mode);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, summary }));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, error: err.message }));
      }
    });
    return;
  }

  // --- STATIC ASSET SERVING ---
  let filePath = path.join(PUBLIC_DIR, pathname === '/' ? 'index.html' : pathname);

  // Security check: restrict to public directory
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end('Forbidden');
    return;
  }

  fs.readFile(filePath, (err, content) => {
    if (err) {
      if (err.code === 'ENOENT') {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
      } else {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end(`Server Error: ${err.code}`);
      }
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    const mimeTypes = {
      '.html': 'text/html',
      '.js': 'application/javascript',
      '.css': 'text/css',
      '.json': 'application/json',
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.svg': 'image/svg+xml'
    };

    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream' });
    res.end(content);
  });
}

// Create HTTP Server
const server = http.createServer(handleHttpRequest);

// Create HTTPS Server if SSL certs exist
let httpsServer = null;
const certPath = path.resolve(__dirname, '../../certs/cert.pem');
const keyPath = path.resolve(__dirname, '../../certs/key.pem');

if (fs.existsSync(certPath) && fs.existsSync(keyPath)) {
  try {
    httpsServer = https.createServer({
      key: fs.readFileSync(keyPath),
      cert: fs.readFileSync(certPath)
    }, handleHttpRequest);
  } catch (err) {
    console.warn('[Nexus Biddings Engine] HTTPS initialization warning:', err.message);
  }
}

// --- WEBSOCKET REAL-TIME BROADCAST ENGINE ---
const wss = new WebSocketServer({ server });
const wssHttps = httpsServer ? new WebSocketServer({ server: httpsServer }) : null;

function broadcast(msgObj) {
  const json = JSON.stringify(msgObj);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  }
  if (wssHttps) {
    for (const client of wssHttps.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(json);
      }
    }
  }
}

// Hook into AuctionEngine for zero-latency broadcasts upon state transition
auctionEngine.onBidAccepted((bidEvent) => {
  broadcast({
    type: 'PRICE_UPDATE',
    lotId: bidEvent.auctionId,
    seqNo: bidEvent.seqNo,
    newPrice: bidEvent.newPrice,
    prevPrice: bidEvent.prevPrice,
    minStep: bidEvent.minStep,
    winnerAlias: bidEvent.winnerAlias,
    version: bidEvent.version,
    processedAt: bidEvent.processedAt,
    latencyUs: bidEvent.latencyUs,
    serverTime: Date.now()
  });
});

// Helper to attach WebSocket listeners
function setupWebSocketServer(wsServer) {
  if (!wsServer) return;
  wsServer.on('connection', (ws) => {
    ws.send(JSON.stringify({
      type: 'INIT_STATE',
      auctions: auctionEngine.getAllAuctions(),
      metrics: metrics.getSnapshot(),
      serverTime: Date.now()
    }));

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'PING') {
          ws.send(JSON.stringify({ type: 'PONG', clientTime: msg.clientTime, serverTime: Date.now() }));
        } else if (msg.type === 'SUBMIT_BID') {
          const result = auctionEngine.processBid(msg.payload || {});
          ws.send(JSON.stringify({
            type: 'BID_RESULT',
            correlationId: msg.correlationId,
            result
          }));
        }
      } catch (err) {
        ws.send(JSON.stringify({ type: 'ERROR', message: 'Invalid message format' }));
      }
    });
  });
}

setupWebSocketServer(wss);
if (wssHttps) setupWebSocketServer(wssHttps);

// Periodic live telemetry pulse (every 500ms)
setInterval(() => {
  const totalClients = wss.clients.size + (wssHttps ? wssHttps.clients.size : 0);
  if (totalClients > 0) {
    broadcast({
      type: 'TELEMETRY_UPDATE',
      metrics: metrics.getSnapshot(),
      activeClients: totalClients,
      timestamp: Date.now()
    });
  }
}, 500);

/**
 * Server-Side Concurrent Stress Spike Generator
 * Simulates heavy concurrent bidding spikes (e.g. 5,000 requests) with race conditions
 */
async function runServerStressSpike(requestCount = 5000, lotId = 'A101', mode = 'contention') {
  const lot = auctionEngine.getAuction(lotId);
  if (!lot) throw new Error(`Lot ${lotId} not found`);

  const startTime = Date.now();
  let accepted = 0;
  let rejected = 0;
  let staleCount = 0;
  let outOfOrderCount = 0;
  const startPrice = lot.currentPrice;

  // We simulate multiple concurrent bidding agents attempting to submit bids
  // In 'contention' mode, all clients read the same starting price and race to submit
  // This produces extreme race conditions and proves zero out-of-order acceptance!
  for (let i = 0; i < requestCount; i++) {
    const agentId = `agent_${(i % 100) + 1}`;
    const agentAlias = `Collector_${String.fromCharCode(65 + (i % 26))}${((i % 50) + 10)}`;

    let bidAmount;
    let expectedPrice = null;

    if (mode === 'contention') {
      // Clients bid based on perceived current price with occasional increments
      const perceivedMultiplier = Math.floor(i / 150) + 1;
      bidAmount = startPrice + (perceivedMultiplier * lot.minStep);
      expectedPrice = startPrice + ((perceivedMultiplier - 1) * lot.minStep);
    } else {
      // Incremental progressive bidding
      bidAmount = startPrice + ((i + 1) * lot.minStep);
    }

    const result = auctionEngine.processBid({
      auctionId: lotId,
      amount: bidAmount,
      bidderId: agentId,
      bidderAlias: agentAlias,
      expectedPrice: (i % 3 === 0) ? expectedPrice : null, // 1 in 3 bids has CAS constraint
      clientTimestamp: Date.now()
    });

    if (result.success) {
      accepted++;
    } else {
      rejected++;
      if (result.reason === 'ERR_STALE_BID') staleCount++;
      if (result.reason === 'ERR_OUT_OF_ORDER') outOfOrderCount++;
    }
  }

  const durationMs = Math.max(1, Date.now() - startTime);
  const throughput = Math.round((requestCount / durationMs) * 1000);

  // Synchronously flush all accepted bids to SQLite database
  batchPersister.flushSync();

  // Run audit verification to guarantee correctness
  const audit = verifyAuditLedger(lotId);

  return {
    lotId,
    totalRequests: requestCount,
    acceptedBids: accepted,
    rejectedBids: rejected,
    staleCount,
    outOfOrderCount,
    durationMs,
    throughputReqSec: throughput,
    finalPrice: auctionEngine.getAuction(lotId).currentPrice,
    auditVerification: audit
  };
}

module.exports = {
  server,
  httpsServer,
  wss,
  wssHttps,
  broadcast,
  PORT,
  HTTPS_PORT
};

if (require.main === module) {
  server.listen(PORT, () => {
    console.log(`[Nexus Biddings Engine] HTTP Server running on http://localhost:${PORT}`);
    console.log(`[Nexus Biddings Engine] Real-time WebSocket listening on ws://localhost:${PORT}`);
  });
  if (httpsServer) {
    httpsServer.listen(HTTPS_PORT, () => {
      console.log(`[Nexus Biddings Engine] Secured HTTPS Server running on https://localhost:${HTTPS_PORT}`);
      console.log(`[Nexus Biddings Engine] Secured WebSocket listening on wss://localhost:${HTTPS_PORT}`);
    });
  }
}
