const { db } = require('../db/database');
const batchPersister = require('./batch_persister');
const metrics = require('./metrics');
const crypto = require('node:crypto');

class AuctionEngine {
  constructor() {
    // In-memory cache of auctions for microsecond lock-free lookups and state transitions
    this.auctions = new Map();

    // Idempotency cache to prevent double-submissions or replays (Key -> Result)
    this.idempotencyCache = new Map();
    this.maxIdempotencySize = 100000;

    // Listeners for real-time broadcasts
    this.bidListeners = new Set();
    this.rejectionListeners = new Set();

    this.initFromDatabase();
  }

  initFromDatabase() {
    const rows = db.prepare('SELECT * FROM auctions').all();
    for (const row of rows) {
      this.auctions.set(row.id, {
        id: row.id,
        title: row.title,
        category: row.category,
        image: row.image,
        provenance: row.provenance,
        auth: row.auth,
        description: row.description,
        startTime: row.start_time,
        duration: row.duration,
        currentPrice: row.current_price,
        minStep: row.min_step,
        winnerId: row.winner_id,
        winnerAlias: row.winner_alias,
        version: row.version,
        seqNo: row.seq_no,
        status: row.status,
        createdAt: row.created_at
      });
    }
  }

  getAuction(id) {
    const auction = this.auctions.get(id);
    if (!auction) return null;
    this._refreshAuctionStatus(auction);
    return { ...auction };
  }

  getAllAuctions() {
    const list = [];
    for (const auction of this.auctions.values()) {
      this._refreshAuctionStatus(auction);
      list.push({ ...auction });
    }
    return list;
  }

  _refreshAuctionStatus(auction) {
    const now = Date.now();
    if (auction.status === 'upcoming' && now >= auction.startTime) {
      auction.status = 'live';
      db.prepare('UPDATE auctions SET status = ? WHERE id = ?').run('live', auction.id);
    }
    if (auction.status === 'live' && now >= auction.startTime + auction.duration) {
      auction.status = 'closed';
      db.prepare('UPDATE auctions SET status = ? WHERE id = ?').run('closed', auction.id);
    }
  }

  onBidAccepted(listener) {
    this.bidListeners.add(listener);
    return () => this.bidListeners.delete(listener);
  }

  onBidRejected(listener) {
    this.rejectionListeners.add(listener);
    return () => this.rejectionListeners.delete(listener);
  }

  /**
   * High-Throughput Atomic Bid Serialization Method
   * 
   * @param {Object} bidPayload
   * @param {string} bidPayload.auctionId
   * @param {number} bidPayload.amount
   * @param {string} bidPayload.bidderId
   * @param {string} bidPayload.bidderAlias
   * @param {string} [bidPayload.idempotencyKey]
   * @param {number} [bidPayload.expectedPrice] - For Optimistic Concurrency Control (CAS)
   * @param {number} [bidPayload.expectedVersion] - For version-based serialization
   * @param {number} [bidPayload.clientTimestamp]
   */
  processBid(bidPayload) {
    const startTimeHr = process.hrtime.bigint();
    const receivedAt = Date.now();
    const {
      auctionId,
      amount,
      bidderId,
      bidderAlias,
      idempotencyKey,
      expectedPrice,
      expectedVersion,
      clientTimestamp
    } = bidPayload;

    // 1. Validate payload
    if (!auctionId || typeof amount !== 'number' || isNaN(amount) || amount <= 0) {
      return this._recordAndReturnRejection({
        auctionId,
        amount: amount || 0,
        bidderId: bidderId || 'anonymous',
        bidderAlias: bidderAlias || 'Anonymous',
        reason: 'ERR_INVALID_PAYLOAD',
        message: 'Invalid bid payload or amount',
        receivedAt,
        startTimeHr
      });
    }

    // 2. Idempotency check: Return existing result if duplicate key encountered
    if (idempotencyKey) {
      const cached = this.idempotencyCache.get(idempotencyKey);
      if (cached) {
        return cached;
      }
    }

    // 3. Retrieve auction state
    const auction = this.auctions.get(auctionId);
    if (!auction) {
      return this._recordAndReturnRejection({
        auctionId,
        amount,
        bidderId,
        bidderAlias,
        reason: 'ERR_AUCTION_NOT_FOUND',
        message: `Auction lot ${auctionId} does not exist`,
        receivedAt,
        startTimeHr
      });
    }

    // 4. Status checks (Must be currently live)
    this._refreshAuctionStatus(auction);
    const now = Date.now();
    if (now < auction.startTime) {
      return this._recordAndReturnRejection({
        auctionId,
        amount,
        bidderId,
        bidderAlias,
        reason: 'ERR_NOT_STARTED',
        message: 'Auction lot has not started yet',
        receivedAt,
        startTimeHr,
        currentPrice: auction.currentPrice,
        seqNo: auction.seqNo
      });
    }

    if (now >= auction.startTime + auction.duration || auction.status === 'closed') {
      return this._recordAndReturnRejection({
        auctionId,
        amount,
        bidderId,
        bidderAlias,
        reason: 'ERR_AUCTION_CLOSED',
        message: 'Auction lot has closed. No further bids accepted.',
        receivedAt,
        startTimeHr,
        currentPrice: auction.currentPrice,
        seqNo: auction.seqNo
      });
    }

    // 5. CAS / Optimistic Concurrency Control Check:
    // If the client specified expectedPrice or expectedVersion, reject if state has shifted
    if (expectedPrice !== undefined && expectedPrice !== null && expectedPrice !== auction.currentPrice) {
      return this._recordAndReturnRejection({
        auctionId,
        amount,
        bidderId,
        bidderAlias,
        reason: 'ERR_OUT_OF_ORDER',
        message: `Race condition detected: expected price ₹${expectedPrice.toLocaleString('en-IN')} but active price is ₹${auction.currentPrice.toLocaleString('en-IN')}`,
        receivedAt,
        startTimeHr,
        currentPrice: auction.currentPrice,
        seqNo: auction.seqNo
      });
    }

    if (expectedVersion !== undefined && expectedVersion !== null && expectedVersion !== auction.version) {
      return this._recordAndReturnRejection({
        auctionId,
        amount,
        bidderId,
        bidderAlias,
        reason: 'ERR_STALE_BID',
        message: `Stale bid version ${expectedVersion}; current version is ${auction.version}`,
        receivedAt,
        startTimeHr,
        currentPrice: auction.currentPrice,
        seqNo: auction.seqNo
      });
    }

    // 6. Strict Monotonicity and Minimum Increment Check:
    // Every accepted bid MUST be at least currentPrice + minStep
    const minRequired = auction.currentPrice + auction.minStep;
    if (amount < minRequired) {
      const reason = amount <= auction.currentPrice ? 'ERR_STALE_BID' : 'ERR_INSUFFICIENT_STEP';
      return this._recordAndReturnRejection({
        auctionId,
        amount,
        bidderId,
        bidderAlias,
        reason,
        message: `Bid ₹${amount.toLocaleString('en-IN')} must be at least ₹${minRequired.toLocaleString('en-IN')} (increment ₹${auction.minStep.toLocaleString('en-IN')})`,
        receivedAt,
        startTimeHr,
        currentPrice: auction.currentPrice,
        seqNo: auction.seqNo
      });
    }

    // --- ATOMIC STATE TRANSITION ---
    // All checks passed! We advance seq_no and version strictly monotonically.
    const prevPrice = auction.currentPrice;
    auction.currentPrice = amount;
    auction.winnerId = bidderId;
    auction.winnerAlias = bidderAlias;
    auction.version += 1;
    auction.seqNo += 1;

    const processedAt = Date.now();
    const endTimeHr = process.hrtime.bigint();
    const latencyUs = Number((endTimeHr - startTimeHr) / 1000n);

    const bidId = 'B_' + Date.now().toString(36) + '_' + crypto.randomBytes(3).toString('hex');

    const acceptedResult = {
      success: true,
      status: 'ACCEPTED',
      bidId,
      auctionId,
      seqNo: auction.seqNo,
      prevPrice,
      newPrice: amount,
      minStep: auction.minStep,
      winnerId: bidderId,
      winnerAlias: bidderAlias,
      version: auction.version,
      receivedAt,
      processedAt,
      latencyUs,
      latencyMs: (latencyUs / 1000).toFixed(3)
    };

    // Cache for idempotency if key provided
    if (idempotencyKey) {
      if (this.idempotencyCache.size >= this.maxIdempotencySize) {
        // Drop oldest key
        const firstKey = this.idempotencyCache.keys().next().value;
        this.idempotencyCache.delete(firstKey);
      }
      this.idempotencyCache.set(idempotencyKey, acceptedResult);
    }

    // Record metrics
    metrics.recordRequest(latencyUs, true);

    // Queue for asynchronous WAL database commit
    batchPersister.enqueue({
      id: bidId,
      auctionId,
      seqNo: auction.seqNo,
      bidderId,
      bidderAlias,
      amount,
      prevPrice,
      version: auction.version,
      status: 'ACCEPTED',
      reason: null,
      receivedAt,
      processedAt,
      latencyUs
    });

    // Notify real-time WebSocket broadcast engine immediately
    for (const listener of this.bidListeners) {
      try {
        listener(acceptedResult);
      } catch (err) {
        console.error('[AuctionEngine] Listener error on bid accept:', err);
      }
    }

    return acceptedResult;
  }

  _recordAndReturnRejection({
    auctionId,
    amount,
    bidderId,
    bidderAlias,
    reason,
    message,
    receivedAt,
    startTimeHr,
    currentPrice = 0,
    seqNo = 0
  }) {
    const processedAt = Date.now();
    const endTimeHr = process.hrtime.bigint();
    const latencyUs = Number((endTimeHr - startTimeHr) / 1000n);
    const bidId = 'REJ_' + Date.now().toString(36) + '_' + crypto.randomBytes(2).toString('hex');

    metrics.recordRequest(latencyUs, false, reason);

    const rejectionResult = {
      success: false,
      status: reason,
      bidId,
      auctionId,
      amount,
      currentPrice,
      seqNo,
      reason,
      message,
      receivedAt,
      processedAt,
      latencyUs,
      latencyMs: (latencyUs / 1000).toFixed(3)
    };

    // Notify rejection listeners (e.g., live rejection telemetry)
    for (const listener of this.rejectionListeners) {
      try {
        listener(rejectionResult);
      } catch (err) {
        console.error('[AuctionEngine] Listener error on rejection:', err);
      }
    }

    return rejectionResult;
  }

  resetLots() {
    batchPersister.clear();
    db.exec('DELETE FROM audit_events; DELETE FROM bids; DELETE FROM auctions;');
    const { seedDatabase } = require('../db/database');
    seedDatabase(true);
    this.auctions.clear();
    this.idempotencyCache.clear();
    metrics.reset();
    this.initFromDatabase();
  }
}

const auctionEngine = new AuctionEngine();
module.exports = auctionEngine;
