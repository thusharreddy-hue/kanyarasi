const { verifyAuditLedger } = require('../src/audit/verifier');
const { batchPersister } = require('../src/engine/batch_persister');

console.log('='.repeat(70));
console.log('       NEXUS BIDDINGS - DISTRIBUTED LEDGER INTEGRITY AUDIT');
console.log('='.repeat(70));

const report = verifyAuditLedger();

console.log(`Audited Auctions:      ${report.totalAuctionsAudited}`);
console.log(`Total Accepted Bids:   ${report.totalAcceptedBids}`);
console.log(`Total Rejected Bids:   ${report.totalRejectedBids}`);
console.log(`Total Violations:      ${report.violationsCount}`);
console.log('-'.repeat(70));
console.log('INVARIANT VERIFICATION CHECKS:');

for (const check of report.checks) {
  const badge = check.passed ? '[\x1b[32mPASS\x1b[0m]' : '[\x1b[31mFAIL\x1b[0m]';
  console.log(` ${badge} ${check.name.padEnd(48)} -> ${check.details}`);
}

console.log('-'.repeat(70));
if (report.passed) {
  console.log('\x1b[32m>>> 100% INVARIANT INTEGRITY CONFIRMED: ZERO GAPS, ZERO OUT-OF-ORDER ACCEPTANCES <<<\x1b[0m');
} else {
  console.log('\x1b[31m>>> AUDIT FAILED! VIOLATIONS DETECTED: <<<\x1b[0m');
  console.log(JSON.stringify(report.violations, null, 2));
  process.exit(1);
}
console.log('='.repeat(70));
