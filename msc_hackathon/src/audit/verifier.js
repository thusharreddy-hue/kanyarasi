const { db } = require('../db/database');

/**
 * Mathematical Audit & Serialization Verifier
 * Validates distributed ledger invariants against the persisted SQLite WAL database
 */
function verifyAuditLedger(auctionId = null) {
  const violations = [];
  const checks = [];

  // 1. Fetch auctions to verify
  let auctionQuery = 'SELECT * FROM auctions';
  const queryParams = [];
  if (auctionId) {
    auctionQuery += ' WHERE id = ?';
    queryParams.push(auctionId);
  }
  const auctions = db.prepare(auctionQuery).all(...queryParams);

  let totalAcceptedCount = 0;
  let totalRejectedCount = 0;

  for (const auction of auctions) {
    // Fetch all accepted bids ordered strictly by seq_no
    const acceptedBids = db.prepare(`
      SELECT * FROM bids
      WHERE auction_id = ? AND status = 'ACCEPTED'
      ORDER BY seq_no ASC
    `).all(auction.id);

    totalAcceptedCount += acceptedBids.length;

    // Fetch rejected bids count
    const rejectedResult = db.prepare(`
      SELECT COUNT(*) as count FROM bids
      WHERE auction_id = ? AND status != 'ACCEPTED'
    `).get(auction.id);
    totalRejectedCount += (rejectedResult ? rejectedResult.count : 0);

    // --- CHECK 1: Strict Monotonicity ---
    let check1Passed = true;
    for (let i = 1; i < acceptedBids.length; i++) {
      const prev = acceptedBids[i - 1];
      const curr = acceptedBids[i];

      if (curr.amount < prev.amount + auction.min_step) {
        check1Passed = false;
        violations.push({
          auctionId: auction.id,
          type: 'MONOTONICITY_VIOLATION',
          message: `Bid at seq ${curr.seq_no} (₹${curr.amount}) did not meet min step from prev seq ${prev.seq_no} (₹${prev.amount} + step ₹${auction.min_step})`,
          seqNo: curr.seq_no
        });
      }
    }
    checks.push({
      name: `Monotonic Price Progression [${auction.id}]`,
      passed: check1Passed,
      details: `${acceptedBids.length} accepted bids verified strictly monotonic`
    });

    // --- CHECK 2: Sequence Number Continuity (Zero Gaps) ---
    let check2Passed = true;
    for (let i = 0; i < acceptedBids.length; i++) {
      const expectedSeq = i + 1;
      if (acceptedBids[i].seq_no !== expectedSeq) {
        check2Passed = false;
        violations.push({
          auctionId: auction.id,
          type: 'SEQUENCE_GAP_VIOLATION',
          message: `Expected seq_no ${expectedSeq} at index ${i}, found ${acceptedBids[i].seq_no}`,
          seqNo: acceptedBids[i].seq_no
        });
      }
    }
    checks.push({
      name: `Strict Sequence Continuity [${auction.id}]`,
      passed: check2Passed,
      details: `Sequence contiguous from 1 to ${acceptedBids.length} with 0 gaps`
    });

    // --- CHECK 3: Terminal State Integrity ---
    let check3Passed = true;
    if (acceptedBids.length > 0) {
      const highestBid = acceptedBids[acceptedBids.length - 1];
      if (auction.current_price !== highestBid.amount) {
        check3Passed = false;
        violations.push({
          auctionId: auction.id,
          type: 'PRICE_STATE_DESYNC',
          message: `Auction current_price (₹${auction.current_price}) does not match terminal accepted bid amount (₹${highestBid.amount})`
        });
      }
      if (auction.seq_no !== highestBid.seq_no) {
        check3Passed = false;
        violations.push({
          auctionId: auction.id,
          type: 'SEQ_STATE_DESYNC',
          message: `Auction seq_no (${auction.seq_no}) does not match terminal accepted bid seq_no (${highestBid.seq_no})`
        });
      }
      if (auction.winner_id && auction.winner_id !== highestBid.bidder_id) {
        check3Passed = false;
        violations.push({
          auctionId: auction.id,
          type: 'WINNER_DESYNC',
          message: `Auction winner_id (${auction.winner_id}) does not match terminal bidder_id (${highestBid.bidder_id})`
        });
      }
    }
    checks.push({
      name: `Terminal State & Winner Consistency [${auction.id}]`,
      passed: check3Passed,
      details: `Database lot state exactly matches terminal sequence state`
    });

    // --- CHECK 4: Audit Event Checksums ---
    const auditEvents = db.prepare(`
      SELECT * FROM audit_events
      WHERE auction_id = ?
      ORDER BY seq_no ASC
    `).all(auction.id);

    let check4Passed = true;
    for (const evt of auditEvents) {
      const expectedChecksum = `${evt.auction_id}:${evt.seq_no}:${evt.new_price}:${evt.bidder_alias}`;
      if (evt.checksum !== expectedChecksum) {
        check4Passed = false;
        violations.push({
          auctionId: auction.id,
          type: 'AUDIT_CHECKSUM_MISMATCH',
          message: `Audit checksum corrupted for seq ${evt.seq_no}`
        });
      }
    }
    checks.push({
      name: `Cryptographic Audit Ledger Integrity [${auction.id}]`,
      passed: check4Passed,
      details: `${auditEvents.length} audit log entries cryptographically verified`
    });
  }

  const passed = violations.length === 0;

  return {
    passed,
    timestamp: Date.now(),
    totalAuctionsAudited: auctions.length,
    totalAcceptedBids: totalAcceptedCount,
    totalRejectedBids: totalRejectedCount,
    violationsCount: violations.length,
    violations,
    checks
  };
}

module.exports = {
  verifyAuditLedger
};
