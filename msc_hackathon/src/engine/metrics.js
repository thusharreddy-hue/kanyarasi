class MetricsCollector {
  constructor() {
    this.totalRequests = 0;
    this.acceptedBids = 0;
    this.rejectedBids = 0;
    this.rejectedReasons = {
      ERR_STALE_BID: 0,
      ERR_OUT_OF_ORDER: 0,
      ERR_INSUFFICIENT_STEP: 0,
      ERR_AUCTION_CLOSED: 0,
      ERR_NOT_STARTED: 0,
      ERR_DUPLICATE_IDEMPOTENCY: 0,
      ERR_INVALID_PAYLOAD: 0
    };

    // Latency histograms (recorded in microseconds)
    this.latenciesUs = [];
    this.maxLatencySamples = 50000;

    // Rate calculation over sliding windows
    this.reqTimestamps = [];
    this.startTime = Date.now();

    // DB metrics
    this.dbBatchesCommitted = 0;
    this.dbRowsWritten = 0;
  }

  recordRequest(latencyUs, accepted, reason = null) {
    this.totalRequests++;
    const now = Date.now();
    this.reqTimestamps.push(now);

    if (accepted) {
      this.acceptedBids++;
    } else {
      this.rejectedBids++;
      if (reason && this.rejectedReasons[reason] !== undefined) {
        this.rejectedReasons[reason]++;
      } else if (reason) {
        this.rejectedReasons[reason] = (this.rejectedReasons[reason] || 0) + 1;
      }
    }

    if (this.latenciesUs.length < this.maxLatencySamples) {
      this.latenciesUs.push(latencyUs);
    } else {
      // Reservoir sampling or periodic overwrite to keep memory constant
      const idx = Math.floor(Math.random() * this.totalRequests);
      if (idx < this.maxLatencySamples) {
        this.latenciesUs[idx] = latencyUs;
      }
    }
  }

  recordDbCommit(rowCount) {
    this.dbBatchesCommitted++;
    this.dbRowsWritten += rowCount;
  }

  getCurrentThroughput() {
    const now = Date.now();
    const cutoff = now - 1000; // Last 1 second
    while (this.reqTimestamps.length > 0 && this.reqTimestamps[0] < cutoff) {
      this.reqTimestamps.shift();
    }
    return this.reqTimestamps.length;
  }

  getPercentiles() {
    if (this.latenciesUs.length === 0) {
      return { p50Us: 0, p90Us: 0, p95Us: 0, p99Us: 0, maxUs: 0, avgUs: 0 };
    }

    // Sort a slice to calculate percentiles
    const sorted = [...this.latenciesUs].sort((a, b) => a - b);
    const n = sorted.length;
    const p50Us = sorted[Math.floor(n * 0.50)];
    const p90Us = sorted[Math.floor(n * 0.90)];
    const p95Us = sorted[Math.floor(n * 0.95)];
    const p99Us = sorted[Math.floor(n * 0.99)];
    const maxUs = sorted[n - 1];
    const sum = sorted.reduce((acc, v) => acc + v, 0);
    const avgUs = Math.round(sum / n);

    return {
      p50Ms: (p50Us / 1000).toFixed(3),
      p90Ms: (p90Us / 1000).toFixed(3),
      p95Ms: (p95Us / 1000).toFixed(3),
      p99Ms: (p99Us / 1000).toFixed(3),
      maxMs: (maxUs / 1000).toFixed(3),
      avgMs: (avgUs / 1000).toFixed(3),
      p50Us,
      p99Us
    };
  }

  getSnapshot() {
    const throughput = this.getCurrentThroughput();
    const percentiles = this.getPercentiles();
    const uptimeSec = Math.max(1, Math.floor((Date.now() - this.startTime) / 1000));
    const avgThroughput = Math.round(this.totalRequests / uptimeSec);

    return {
      totalRequests: this.totalRequests,
      acceptedBids: this.acceptedBids,
      rejectedBids: this.rejectedBids,
      rejectedReasons: this.rejectedReasons,
      throughputReqSec: throughput,
      avgThroughputReqSec: avgThroughput,
      percentiles,
      dbBatchesCommitted: this.dbBatchesCommitted,
      dbRowsWritten: this.dbRowsWritten
    };
  }

  reset() {
    this.totalRequests = 0;
    this.acceptedBids = 0;
    this.rejectedBids = 0;
    for (const key of Object.keys(this.rejectedReasons)) {
      this.rejectedReasons[key] = 0;
    }
    this.latenciesUs = [];
    this.reqTimestamps = [];
    this.startTime = Date.now();
  }
}

const metrics = new MetricsCollector();
module.exports = metrics;
