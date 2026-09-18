const { db } = require('../db/database');
const metrics = require('./metrics');

class BatchPersister {
  constructor(options = {}) {
    this.batchThreshold = options.batchThreshold || 100;
    this.flushIntervalMs = options.flushIntervalMs || 10;
    this.queue = [];
    this.timer = null;
    this.isFlushing = false;

    // Prepared statements for high performance batch commits
    this.insertBidStmt = db.prepare(`
      INSERT INTO bids (
        id, auction_id, seq_no, bidder_id, bidder_alias,
        amount, status, reason, received_at, processed_at, latency_us
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.updateAuctionStmt = db.prepare(`
      UPDATE auctions
      SET current_price = ?, winner_id = ?, winner_alias = ?, version = ?, seq_no = ?
      WHERE id = ?
    `);

    this.insertAuditStmt = db.prepare(`
      INSERT INTO audit_events (
        auction_id, event_type, prev_price, new_price, seq_no, bidder_alias, timestamp, checksum
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.startPeriodicFlush();
  }

  enqueue(item) {
    this.queue.push(item);
    if (this.queue.length >= this.batchThreshold) {
      this.flush();
    }
  }

  startPeriodicFlush() {
    if (this.timer) clearInterval(this.timer);
    this.timer = setInterval(() => {
      if (this.queue.length > 0) {
        this.flush();
      }
    }, this.flushIntervalMs);
    if (this.timer && typeof this.timer.unref === 'function') {
      this.timer.unref();
    }
  }

  clear() {
    this.queue = [];
  }

  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.flushSync();
  }

  flush() {
    if (this.isFlushing || this.queue.length === 0) return;
    this.isFlushing = true;

    // Drain current queue
    const batch = this.queue.splice(0, this.queue.length);
    if (batch.length === 0) {
      this.isFlushing = false;
      return;
    }

    try {
      this._persistBatch(batch);
    } catch (err) {
      console.error('[BatchPersister] Flush error:', err);
    } finally {
      this.isFlushing = false;
      // If items arrived while flushing and exceed threshold, trigger another flush
      if (this.queue.length >= this.batchThreshold) {
        setImmediate(() => this.flush());
      }
    }
  }

  flushSync() {
    if (this.queue.length === 0) return;
    const batch = this.queue.splice(0, this.queue.length);
    this._persistBatch(batch);
  }

  _persistBatch(batch) {
    // Execute inside a single ACID SQLite transaction with immediate locking
    db.exec('BEGIN IMMEDIATE;');
    try {
      for (const item of batch) {
        // Record bid
        this.insertBidStmt.run(
          item.id,
          item.auctionId,
          item.seqNo,
          item.bidderId,
          item.bidderAlias,
          item.amount,
          item.status,
          item.reason || null,
          item.receivedAt,
          item.processedAt,
          item.latencyUs
        );

        if (item.status === 'ACCEPTED') {
          // Update auction summary state
          this.updateAuctionStmt.run(
            item.amount,
            item.bidderId,
            item.bidderAlias,
            item.version,
            item.seqNo,
            item.auctionId
          );

          // Append audit ledger record
          const checksum = `${item.auctionId}:${item.seqNo}:${item.amount}:${item.bidderAlias}`;
          this.insertAuditStmt.run(
            item.auctionId,
            'BID_ACCEPTED',
            item.prevPrice,
            item.amount,
            item.seqNo,
            item.bidderAlias,
            item.processedAt,
            checksum
          );
        }
      }
      db.exec('COMMIT;');
      metrics.recordDbCommit(batch.length);
    } catch (err) {
      db.exec('ROLLBACK;');
      console.error('[BatchPersister] Transaction rollback in batch persist:', err);
      throw err;
    }
  }
}

const batchPersister = new BatchPersister();
module.exports = batchPersister;
