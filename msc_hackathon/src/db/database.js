const { DatabaseSync } = require('node:sqlite');
const path = require('node:path');
const fs = require('node:fs');

// Ensure data directory exists
const dataDir = path.resolve(__dirname, '../../data');
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const dbPath = path.join(dataDir, 'auction.db');
const db = new DatabaseSync(dbPath);

// Enable WAL mode (Write-Ahead Logging) and normal synchronous mode for microsecond commits
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA foreign_keys = ON;
  PRAGMA busy_timeout = 5000;
`);

// Initialize database schema
db.exec(`
  CREATE TABLE IF NOT EXISTS auctions (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    image TEXT NOT NULL,
    provenance TEXT,
    auth TEXT,
    description TEXT,
    start_time INTEGER NOT NULL,
    duration INTEGER NOT NULL,
    current_price INTEGER NOT NULL,
    min_step INTEGER NOT NULL,
    winner_id TEXT,
    winner_alias TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    seq_no INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'upcoming',
    created_at INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS bids (
    id TEXT PRIMARY KEY,
    auction_id TEXT NOT NULL,
    seq_no INTEGER NOT NULL,
    bidder_id TEXT NOT NULL,
    bidder_alias TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL, -- 'ACCEPTED', 'REJECTED_STALE', 'REJECTED_LOW', 'REJECTED_CLOSED'
    reason TEXT,
    received_at INTEGER NOT NULL,
    processed_at INTEGER NOT NULL,
    latency_us INTEGER NOT NULL,
    FOREIGN KEY(auction_id) REFERENCES auctions(id)
  );

  CREATE INDEX IF NOT EXISTS idx_bids_auction_seq ON bids(auction_id, seq_no);
  CREATE INDEX IF NOT EXISTS idx_bids_status ON bids(status);

  CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    prev_price INTEGER,
    new_price INTEGER,
    seq_no INTEGER NOT NULL,
    bidder_alias TEXT,
    timestamp INTEGER NOT NULL,
    checksum TEXT
  );
`);

// Initial lot catalogue
const initialLots = [
  {
    id: 'A101',
    title: 'Mughal Emerald Dagger',
    category: 'Imperial Arms',
    image: 'https://images.unsplash.com/photo-1610375461369-d613b564943d?auto=format&fit=crop&w=900&q=70',
    provenance: 'Private Jaipur lineage collection, 1890 archive seal.',
    auth: 'Thermoluminescence + metallurgical certificate NB-7781',
    description: 'A ceremonial jade-hilt dagger inspired by late Mughal ateliers with emerald-toned ornamentation.',
    start_offset: -10000, // Started 10s ago (Active for stress test)
    duration: 3600000,    // 1 hour
    price: 825000,
    step: 25000
  },
  {
    id: 'A102',
    title: 'Chola Bronze Devi',
    category: 'Sacred Sculpture',
    image: 'https://images.unsplash.com/photo-1599033153041-e88627ca70bb?auto=format&fit=crop&w=900&q=70',
    provenance: 'Documented South Indian estate, export papers verified.',
    auth: '3D surface scan + patina analysis NB-4410',
    description: 'A refined bronze devotional figure with graceful tribhanga posture and archival documentation.',
    start_offset: 60000, // Upcoming in 60s
    duration: 3600000,
    price: 1460000,
    step: 50000
  },
  {
    id: 'A103',
    title: 'Royal Sapphire Necklace',
    category: 'Jewels',
    image: 'https://images.unsplash.com/photo-1515562141207-7a88fb7ce338?auto=format&fit=crop&w=900&q=70',
    provenance: 'Former princely family treasury, 1932 inventory.',
    auth: 'Gemological lab report + provenance dossier NB-2220',
    description: 'A sapphire and diamond suite mounted in a heritage-inspired platinum setting.',
    start_offset: -50000, // Active
    duration: 3600000,
    price: 2200000,
    step: 75000
  },
  {
    id: 'A104',
    title: 'Pichwai Lotus Panel',
    category: 'Fine Art',
    image: 'https://images.unsplash.com/photo-1549887534-1541e9326642?auto=format&fit=crop&w=900&q=70',
    provenance: 'Nathdwara atelier collection, collector-acquired 1978.',
    auth: 'Pigment analysis + textile microscopy NB-6172',
    description: 'A devotional lotus composition with mineral pigments and fine hand detailing.',
    start_offset: 120000, // Upcoming
    duration: 3600000,
    price: 375000,
    step: 15000
  },
  {
    id: 'A105',
    title: 'Colonial Observatory Clock',
    category: 'Horology',
    image: 'https://images.unsplash.com/photo-1509048191080-d2984bad6ae5?auto=format&fit=crop&w=900&q=70',
    provenance: 'Calcutta scientific estate, service marks intact.',
    auth: 'Movement inspection + archive registry NB-1098',
    description: 'A precision table clock with brass astronomical dial and original escapement.',
    start_offset: -7200000, // Closed 2h ago
    duration: 3600000,
    price: 690000,
    step: 20000
  }
];

function seedDatabase(reset = false) {
  const countStmt = db.prepare('SELECT COUNT(*) as count FROM auctions');
  const result = countStmt.get();
  
  if (result.count === 0 || reset) {
    if (reset) {
      db.exec('DELETE FROM audit_events; DELETE FROM bids; DELETE FROM auctions;');
    }
    const now = Date.now();
    const insertStmt = db.prepare(`
      INSERT INTO auctions (
        id, title, category, image, provenance, auth, description,
        start_time, duration, current_price, min_step, winner_id, winner_alias,
        version, seq_no, status, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (const lot of initialLots) {
      const startTime = now + lot.start_offset;
      let status = 'upcoming';
      if (now >= startTime && now < startTime + lot.duration) {
        status = 'live';
      } else if (now >= startTime + lot.duration) {
        status = 'closed';
      }

      insertStmt.run(
        lot.id,
        lot.title,
        lot.category,
        lot.image,
        lot.provenance,
        lot.auth,
        lot.description,
        startTime,
        lot.duration,
        lot.price,
        lot.step,
        null,
        status === 'closed' ? 'Heritage Foundation' : null,
        1,
        0,
        status,
        now
      );
    }
  }
}

seedDatabase();

module.exports = {
  db,
  dbPath,
  seedDatabase
};
