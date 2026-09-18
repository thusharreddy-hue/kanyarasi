const test = require('node:test');
const assert = require('node:assert/strict');

const auctionEngine = require('../src/engine/auction_engine');
const batchPersister = require('../src/engine/batch_persister');
const { verifyAuditLedger } = require('../src/audit/verifier');

test('Auction Engine & Distributed Bidding Test Suite', async (t) => {

  t.beforeEach(() => {
    auctionEngine.resetLots();
  });

  t.after(() => {
    batchPersister.stop();
  });

  await t.test('1. Valid bid serialization increases current price and seq_no', () => {
    const lot = auctionEngine.getAuction('A101');
    const startPrice = lot.currentPrice;
    const bidAmount = startPrice + lot.minStep;

    const res = auctionEngine.processBid({
      auctionId: 'A101',
      amount: bidAmount,
      bidderId: 'user_1',
      bidderAlias: 'CollectorAlpha'
    });

    assert.equal(res.success, true);
    assert.equal(res.status, 'ACCEPTED');
    assert.equal(res.newPrice, bidAmount);
    assert.equal(res.seqNo, 1);
    assert.equal(res.winnerAlias, 'CollectorAlpha');

    const updatedLot = auctionEngine.getAuction('A101');
    assert.equal(updatedLot.currentPrice, bidAmount);
    assert.equal(updatedLot.seqNo, 1);
  });

  await t.test('2. Rejects out-of-order or stale bids below current highest + step', () => {
    const lot = auctionEngine.getAuction('A101');
    const validBidAmount = lot.currentPrice + lot.minStep;

    // Place valid first bid
    auctionEngine.processBid({
      auctionId: 'A101',
      amount: validBidAmount,
      bidderId: 'user_1',
      bidderAlias: 'CollectorAlpha'
    });

    // Attempt to submit lower bid (original starting price)
    const staleRes = auctionEngine.processBid({
      auctionId: 'A101',
      amount: lot.currentPrice,
      bidderId: 'user_2',
      bidderAlias: 'CollectorBeta'
    });

    assert.equal(staleRes.success, false);
    assert.equal(staleRes.reason, 'ERR_STALE_BID');

    // Attempt to submit bid equal to current highest (missing min_step)
    const insufficientRes = auctionEngine.processBid({
      auctionId: 'A101',
      amount: validBidAmount,
      bidderId: 'user_3',
      bidderAlias: 'CollectorGamma'
    });

    assert.equal(insufficientRes.success, false);
    assert.equal(insufficientRes.reason, 'ERR_STALE_BID');
  });

  await t.test('3. Optimistic Concurrency Control (CAS) rejects out-of-order bids during race condition', () => {
    const lot = auctionEngine.getAuction('A101');
    const startPrice = lot.currentPrice;

    // Collector 1 places bid at startPrice + minStep
    const res1 = auctionEngine.processBid({
      auctionId: 'A101',
      amount: startPrice + lot.minStep,
      bidderId: 'user_1',
      bidderAlias: 'CollectorAlpha'
    });
    assert.equal(res1.success, true);

    // Collector 2 saw startPrice and sends bid with expectedPrice = startPrice, but new price is higher
    const res2 = auctionEngine.processBid({
      auctionId: 'A101',
      amount: startPrice + (lot.minStep * 2),
      bidderId: 'user_2',
      bidderAlias: 'CollectorBeta',
      expectedPrice: startPrice // Mismatch! Active price is startPrice + minStep
    });

    assert.equal(res2.success, false);
    assert.equal(res2.reason, 'ERR_OUT_OF_ORDER');
    assert.match(res2.message, /Race condition detected/);
  });

  await t.test('4. Idempotency guarantees: Duplicate submission returns cached result with zero sequence drift', () => {
    const lot = auctionEngine.getAuction('A101');
    const bidAmount = lot.currentPrice + lot.minStep;
    const idempotencyKey = 'tx_unique_uuid_999';

    const res1 = auctionEngine.processBid({
      auctionId: 'A101',
      amount: bidAmount,
      bidderId: 'user_1',
      bidderAlias: 'CollectorAlpha',
      idempotencyKey
    });
    assert.equal(res1.success, true);
    assert.equal(res1.seqNo, 1);

    // Replay attack / network retry with same idempotency key
    const res2 = auctionEngine.processBid({
      auctionId: 'A101',
      amount: bidAmount,
      bidderId: 'user_1',
      bidderAlias: 'CollectorAlpha',
      idempotencyKey
    });

    assert.equal(res2.success, true);
    assert.equal(res2.bidId, res1.bidId); // Exact same bid
    assert.equal(res2.seqNo, 1);          // Sequence did not advance

    const currentLot = auctionEngine.getAuction('A101');
    assert.equal(currentLot.seqNo, 1);    // No phantom sequence increments
  });

  await t.test('5. State guard: Rejects bids on closed auctions', () => {
    // A105 is initialized as closed (past duration)
    const closedLot = auctionEngine.getAuction('A105');
    assert.equal(closedLot.status, 'closed');

    const res = auctionEngine.processBid({
      auctionId: 'A105',
      amount: closedLot.currentPrice + closedLot.minStep,
      bidderId: 'user_late',
      bidderAlias: 'LateCollector'
    });

    assert.equal(res.success, false);
    assert.equal(res.reason, 'ERR_AUCTION_CLOSED');
  });

  await t.test('6. Concurrent race conditions: 100 simultaneous bids at identical price point yield exactly 1 accepted and 99 rejected', async () => {
    const lot = auctionEngine.getAuction('A101');
    const targetPrice = lot.currentPrice + lot.minStep;
    const concurrentCount = 100;

    // Launch 100 concurrent bid attempts in parallel
    const promises = Array.from({ length: concurrentCount }, (_, i) => {
      return Promise.resolve().then(() => {
        return auctionEngine.processBid({
          auctionId: 'A101',
          amount: targetPrice,
          bidderId: `concurrent_user_${i}`,
          bidderAlias: `Agent_${i}`
        });
      });
    });

    const results = await Promise.all(promises);
    const accepted = results.filter(r => r.success);
    const rejected = results.filter(r => !r.success);

    assert.equal(accepted.length, 1, 'Exactly 1 bid must be accepted at targetPrice');
    assert.equal(rejected.length, 99, 'All other 99 bids must be rejected due to serialization');

    for (const rej of rejected) {
      assert.equal(rej.reason, 'ERR_STALE_BID');
    }
  });

  await t.test('7. Database WAL persistence and audit verification passes 100% mathematical checks', () => {
    const lot = auctionEngine.getAuction('A101');
    
    // Execute a series of monotonic bids
    for (let i = 1; i <= 25; i++) {
      auctionEngine.processBid({
        auctionId: 'A101',
        amount: lot.currentPrice + (i * lot.minStep),
        bidderId: `seq_user_${i}`,
        bidderAlias: `Collector_${i}`
      });
    }

    // Flush batch persister to SQLite
    batchPersister.flushSync();

    // Verify audit ledger
    const report = verifyAuditLedger('A101');
    assert.equal(report.passed, true);
    assert.equal(report.violationsCount, 0);
    assert.equal(report.totalAcceptedBids, 25);
  });
});
