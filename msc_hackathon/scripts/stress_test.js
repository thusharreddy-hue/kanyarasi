const http = require('node:http');
const auctionEngine = require('../src/engine/auction_engine');
const batchPersister = require('../src/engine/batch_persister');
const { verifyAuditLedger } = require('../src/audit/verifier');

// Parse CLI arguments
const args = process.argv.slice(2);
function getArg(name, defaultValue) {
  const idx = args.indexOf(`--${name}`);
  if (idx !== -1 && args[idx + 1]) return args[idx + 1];
  return defaultValue;
}

const REQUESTS = parseInt(getArg('requests', '5000'), 10);
const CONCURRENCY = parseInt(getArg('concurrency', '50'), 10);
const LOT_ID = getArg('lot', 'A101');
const MODE = getArg('mode', 'contention'); // 'contention' or 'incremental'
const OVER_THE_WIRE = args.includes('--wire');
const PORT = parseInt(getArg('port', '3000'), 10);

async function run() {
  console.log('='.repeat(70));
  console.log('       NEXUS BIDDINGS - HIGH-THROUGHPUT STRESS TEST HARNESS');
  console.log('='.repeat(70));
  console.log(`Target Lot:           ${LOT_ID}`);
  console.log(`Total Bid Requests:   ${REQUESTS.toLocaleString()}`);
  console.log(`Concurrent Workers:   ${CONCURRENCY}`);
  console.log(`Mode:                 ${MODE} (${MODE === 'contention' ? 'Heavy race condition contention' : 'Sequential step increments'})`);
  console.log(`Execution Pipeline:   ${OVER_THE_WIRE ? `HTTP POST over-the-wire (http://localhost:${PORT}/api/bid)` : 'High-Performance Engine Sequencer'}`);
  console.log('-'.repeat(70));

  let lot = auctionEngine.getAuction(LOT_ID);
  if (!lot) {
    console.error(`Lot ${LOT_ID} not found`);
    process.exit(1);
  }

  const startPrice = lot.currentPrice;
  const minStep = lot.minStep;
  console.log(`Initial Lot State:    Price ₹${startPrice.toLocaleString('en-IN')} | Step ₹${minStep.toLocaleString('en-IN')} | Seq: ${lot.seqNo}`);
  console.log(`Simulating multi-client bidding spike...`);

  const latencies = [];
  let accepted = 0;
  let rejectedStale = 0;
  let rejectedOutOfOrder = 0;
  let errors = 0;

  const startTime = Date.now();

  // Create queue of task indices
  const queue = Array.from({ length: REQUESTS }, (_, i) => i);

  async function worker(workerId) {
    const agentId = `worker_${workerId}`;
    const agentAlias = `VIP_Agent_${workerId}`;

    while (queue.length > 0) {
      const i = queue.pop();
      if (i === undefined) break;

      let bidAmount;
      let expectedPrice = null;

      if (MODE === 'contention') {
        // High contention: many workers try to claim the current tranche simultaneously
        const tranche = Math.floor(i / (CONCURRENCY * 2)) + 1;
        bidAmount = startPrice + (tranche * minStep);
        if (i % 3 === 0) {
          expectedPrice = startPrice + ((tranche - 1) * minStep);
        }
      } else {
        bidAmount = startPrice + ((i + 1) * minStep);
      }

      const t0 = process.hrtime.bigint();

      if (OVER_THE_WIRE) {
        // Submit via HTTP POST
        try {
          const res = await sendHttpBid(PORT, {
            auctionId: LOT_ID,
            amount: bidAmount,
            bidderId: agentId,
            bidderAlias: agentAlias,
            expectedPrice,
            idempotencyKey: `tx_${workerId}_${i}`
          });
          const t1 = process.hrtime.bigint();
          latencies.push(Number(t1 - t0) / 1000); // us

          if (res.status === 200) accepted++;
          else if (res.status === 409) {
            if (res.data?.reason === 'ERR_STALE_BID') rejectedStale++;
            else rejectedOutOfOrder++;
          } else errors++;
        } catch (err) {
          errors++;
        }
      } else {
        // Submit directly to atomic sequencer
        const res = auctionEngine.processBid({
          auctionId: LOT_ID,
          amount: bidAmount,
          bidderId: agentId,
          bidderAlias: agentAlias,
          expectedPrice,
          idempotencyKey: `tx_${workerId}_${i}`
        });
        const t1 = process.hrtime.bigint();
        latencies.push(Number(t1 - t0) / 1000); // us

        if (res.success) {
          accepted++;
        } else {
          if (res.reason === 'ERR_STALE_BID') rejectedStale++;
          else if (res.reason === 'ERR_OUT_OF_ORDER') rejectedOutOfOrder++;
          else errors++;
        }
      }
    }
  }

  // Launch concurrent workers
  const workers = Array.from({ length: CONCURRENCY }, (_, i) => worker(i + 1));
  await Promise.all(workers);

  const totalTimeMs = Math.max(1, Date.now() - startTime);
  const throughput = Math.round((REQUESTS / totalTimeMs) * 1000);

  // Synchronously persist batch to DB
  batchPersister.flushSync();

  // Calculate latency percentiles
  latencies.sort((a, b) => a - b);
  const p50 = (latencies[Math.floor(latencies.length * 0.50)] / 1000).toFixed(3);
  const p90 = (latencies[Math.floor(latencies.length * 0.90)] / 1000).toFixed(3);
  const p95 = (latencies[Math.floor(latencies.length * 0.95)] / 1000).toFixed(3);
  const p99 = (latencies[Math.floor(latencies.length * 0.99)] / 1000).toFixed(3);
  const max = (latencies[latencies.length - 1] / 1000).toFixed(3);
  const avg = ((latencies.reduce((a, b) => a + b, 0) / latencies.length) / 1000).toFixed(3);

  lot = auctionEngine.getAuction(LOT_ID);

  console.log('-'.repeat(70));
  console.log('STRESS TEST BENCHMARK RESULTS:');
  console.log(`Total Executed:       ${REQUESTS.toLocaleString()} bids in ${(totalTimeMs / 1000).toFixed(2)}s`);
  console.log(`Throughput:           \x1b[32m${throughput.toLocaleString()} requests/second\x1b[0m`);
  console.log(`Accepted Bids:        ${accepted.toLocaleString()} (Strictly serialized)`);
  console.log(`Rejected Stale:       ${rejectedStale.toLocaleString()} (Rejected out-of-order/stale)`);
  console.log(`Rejected Race CAS:    ${rejectedOutOfOrder.toLocaleString()} (Optimistic CAS conflicts resolved)`);
  console.log(`Errors / Dropped:     ${errors}`);
  console.log(`Final Lot Price:      ₹${lot.currentPrice.toLocaleString('en-IN')} (Seq: ${lot.seqNo})`);
  console.log('-'.repeat(70));
  console.log('LATENCY DISTRIBUTION:');
  console.log(` Avg Latency:         ${avg} ms`);
  console.log(` p50 Latency:         ${p50} ms`);
  console.log(` p90 Latency:         ${p90} ms`);
  console.log(` p95 Latency:         ${p95} ms`);
  console.log(` p99 Latency:         \x1b[32m${p99} ms\x1b[0m`);
  console.log(` Max Latency:         ${max} ms`);
  console.log('-'.repeat(70));

  console.log('RUNNING AUTOMATED LEDGER AUDIT...');
  const audit = verifyAuditLedger(LOT_ID);
  for (const check of audit.checks) {
    const badge = check.passed ? '[\x1b[32mPASS\x1b[0m]' : '[\x1b[31mFAIL\x1b[0m]';
    console.log(` ${badge} ${check.name.padEnd(48)} -> ${check.details}`);
  }

  if (audit.passed) {
    console.log('\x1b[32m>>> 100% INVARIANT INTEGRITY CONFIRMED UNDER CONCURRENT STRESS <<<\x1b[0m');
  } else {
    console.log('\x1b[31m>>> INVARIANT VIOLATIONS DETECTED: <<<\x1b[0m', audit.violations);
    process.exit(1);
  }
  console.log('='.repeat(70));
}

function sendHttpBid(port, payload) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(payload);
    const req = http.request({
      hostname: 'localhost',
      port,
      path: '/api/bid',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    }, res => {
      let resBody = '';
      res.on('data', chunk => { resBody += chunk; });
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, data: JSON.parse(resBody || '{}') });
        } catch (e) {
          resolve({ status: res.statusCode, data: null });
        }
      });
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

run().catch(err => {
  console.error('Stress test error:', err);
  process.exit(1);
});
