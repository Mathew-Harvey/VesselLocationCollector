require('dotenv').config();
const WebSocket = require('ws');
const { Pool } = require('pg');
const { initDatabase, upsertVessels, insertPositions, purgeOldPositions } = require('./db');

// ── Config ──────────────────────────────────────────────────────────────
const CONFIG = {
  apiKey: process.env.AISSTREAM_API_KEY,
  databaseUrl: process.env.DATABASE_URL,
  minLength: parseFloat(process.env.MIN_VESSEL_LENGTH_M || 30),
  positionIntervalMin: parseInt(process.env.POSITION_INTERVAL_MINUTES || 60),
  batchFlushMs: parseInt(process.env.BATCH_FLUSH_INTERVAL_MS || 5000),
  statsIntervalMs: parseInt(process.env.STATS_INTERVAL_MS || 60000),
  wsUrl: 'wss://stream.aisstream.io/v0/stream',
  maxReconnectDelay: 60000,
  purgeIntervalHours: 24,
  retentionDays: parseInt(process.env.RETENTION_DAYS || 365),
};

if (!CONFIG.apiKey) {
  console.error('[FATAL] AISSTREAM_API_KEY not set');
  process.exit(1);
}
if (!CONFIG.databaseUrl) {
  console.error('[FATAL] DATABASE_URL not set');
  process.exit(1);
}

// ── State ───────────────────────────────────────────────────────────────

// Vessels we know are >= 30m. Key = MMSI, value = { length, name }
const knownVessels = new Map();

// Dedup: MMSI -> "YYYY-MM-DD-HH" of last stored position
const lastStoredHour = new Map();

// Write buffers
let vesselBatch = [];
let positionBatch = [];

// Stats
const stats = {
  messagesReceived: 0,
  positionsProcessed: 0,
  positionsStored: 0,
  positionsSkippedSmall: 0,
  positionsSkippedDedup: 0,
  vesselsDiscovered: 0,
  vesselsSkippedSmall: 0,
  dbWriteErrors: 0,
  wsReconnects: 0,
  startedAt: new Date(),
};

// ── Helpers ─────────────────────────────────────────────────────────────

function hourKey(date) {
  const d = date || new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}-${String(d.getUTCHours()).padStart(2, '0')}`;
}

function cleanStr(s) {
  if (!s) return null;
  return s.replace(/@+$/g, '').trim() || null;
}

// ── Message handlers ────────────────────────────────────────────────────

function handleShipStaticData(msg) {
  const data = msg.Message.ShipStaticData;
  if (!data || !data.Valid) return;

  const mmsi = data.UserID;
  const dim = data.Dimension || {};
  const length = (dim.A || 0) + (dim.B || 0);
  const beam = (dim.C || 0) + (dim.D || 0);

  if (length < CONFIG.minLength) {
    stats.vesselsSkippedSmall++;
    return;
  }

  const isNew = !knownVessels.has(mmsi);
  knownVessels.set(mmsi, { length, name: cleanStr(data.Name) });

  if (isNew) stats.vesselsDiscovered++;

  vesselBatch.push({
    mmsi,
    imo_number: data.ImoNumber || null,
    name: cleanStr(data.Name),
    call_sign: cleanStr(data.CallSign),
    ship_type: data.Type || null,
    length_m: length,
    beam_m: beam || null,
    draught_m: data.MaximumStaticDraught || null,
    destination: cleanStr(data.Destination),
  });
}

function handlePositionReport(msg) {
  // Works for PositionReport, StandardClassBPositionReport, ExtendedClassBPositionReport
  const type = msg.MessageType;
  const data = msg.Message[type];
  if (!data || !data.Valid) return;

  const mmsi = data.UserID;
  stats.positionsProcessed++;

  // Only store positions for vessels we know are 30m+
  if (!knownVessels.has(mmsi)) {
    stats.positionsSkippedSmall++;
    return;
  }

  // Skip invalid coordinates
  const lat = data.Latitude;
  const lon = data.Longitude;
  if (lat === 91 || lon === 181 || (lat === 0 && lon === 0)) return;

  // Hourly dedup
  const now = new Date();
  const hk = hourKey(now);
  const lastHk = lastStoredHour.get(mmsi);
  if (lastHk === hk) {
    stats.positionsSkippedDedup++;
    return;
  }

  lastStoredHour.set(mmsi, hk);
  stats.positionsStored++;

  positionBatch.push({
    mmsi,
    latitude: lat,
    longitude: lon,
    sog: data.Sog != null ? data.Sog : null,
    cog: data.Cog != null ? data.Cog : null,
    heading: data.TrueHeading != null && data.TrueHeading !== 511 ? data.TrueHeading : null,
    nav_status: data.NavigationalStatus != null ? data.NavigationalStatus : null,
    recorded_at: now.toISOString(),
  });
}

function handleMessage(raw) {
  stats.messagesReceived++;

  let msg;
  try {
    msg = JSON.parse(raw);
  } catch {
    return;
  }

  if (msg.error) {
    console.error('[AIS] Server error:', msg.error);
    return;
  }

  const type = msg.MessageType;

  if (type === 'ShipStaticData') {
    handleShipStaticData(msg);
  } else if (
    type === 'PositionReport' ||
    type === 'StandardClassBPositionReport' ||
    type === 'ExtendedClassBPositionReport'
  ) {
    handlePositionReport(msg);
  }
}

// ── Database flush ──────────────────────────────────────────────────────

async function flushBatches(pool) {
  const vBatch = vesselBatch;
  const pBatch = positionBatch;
  vesselBatch = [];
  positionBatch = [];

  if (!vBatch.length && !pBatch.length) return;

  try {
    // Chunk large batches to stay within PG param limits (max ~65535 params)
    const VESSEL_CHUNK = 500;  // 9 params each = 4500 params
    const POS_CHUNK = 800;     // 8 params each = 6400 params

    for (let i = 0; i < vBatch.length; i += VESSEL_CHUNK) {
      await upsertVessels(pool, vBatch.slice(i, i + VESSEL_CHUNK));
    }
    for (let i = 0; i < pBatch.length; i += POS_CHUNK) {
      await insertPositions(pool, pBatch.slice(i, i + POS_CHUNK));
    }
  } catch (err) {
    stats.dbWriteErrors++;
    console.error('[DB] Write error:', err.message);
    // Put failed items back (but cap to prevent memory leak)
    if (vesselBatch.length < 10000) vesselBatch.push(...vBatch);
    if (positionBatch.length < 50000) positionBatch.push(...pBatch);
  }
}

// ── WebSocket connection ────────────────────────────────────────────────

let ws = null;
let reconnectDelay = 1000;
let shouldRun = true;

function connect(pool) {
  console.log('[WS] Connecting to aisstream.io...');

  ws = new WebSocket(CONFIG.wsUrl);

  ws.on('open', () => {
    console.log('[WS] Connected. Subscribing to global feed...');
    reconnectDelay = 1000;

    const subscription = {
      APIKey: CONFIG.apiKey,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FilterMessageTypes: [
        'PositionReport',
        'StandardClassBPositionReport',
        'ExtendedClassBPositionReport',
        'ShipStaticData',
      ],
    };

    ws.send(JSON.stringify(subscription));
    console.log('[WS] Subscription sent. Listening for vessels >= ' + CONFIG.minLength + 'm...');
  });

  ws.on('message', (data) => {
    handleMessage(data.toString());
  });

  ws.on('close', (code, reason) => {
    console.log(`[WS] Disconnected (code=${code}, reason=${reason || 'none'})`);
    if (shouldRun) scheduleReconnect(pool);
  });

  ws.on('error', (err) => {
    console.error('[WS] Error:', err.message);
    // close event will fire after this, triggering reconnect
  });
}

function scheduleReconnect(pool) {
  stats.wsReconnects++;
  console.log(`[WS] Reconnecting in ${reconnectDelay / 1000}s...`);
  setTimeout(() => {
    if (shouldRun) connect(pool);
  }, reconnectDelay);
  reconnectDelay = Math.min(reconnectDelay * 2, CONFIG.maxReconnectDelay);
}

// ── Stats logging ───────────────────────────────────────────────────────

function logStats() {
  const uptime = ((Date.now() - stats.startedAt.getTime()) / 3600000).toFixed(1);
  const msgRate = (stats.messagesReceived / ((Date.now() - stats.startedAt.getTime()) / 1000)).toFixed(0);
  console.log([
    `[STATS] uptime=${uptime}h`,
    `msg/s=${msgRate}`,
    `vessels_known=${knownVessels.size}`,
    `vessels_discovered=${stats.vesselsDiscovered}`,
    `vessels_skipped_small=${stats.vesselsSkippedSmall}`,
    `pos_stored=${stats.positionsStored}`,
    `pos_skipped_dedup=${stats.positionsSkippedDedup}`,
    `pos_skipped_small=${stats.positionsSkippedSmall}`,
    `db_errors=${stats.dbWriteErrors}`,
    `ws_reconnects=${stats.wsReconnects}`,
    `vessel_batch=${vesselBatch.length}`,
    `pos_batch=${positionBatch.length}`,
    `dedup_map=${lastStoredHour.size}`,
  ].join(' | '));
}

// Clean up the dedup map every hour (remove entries from >2 hours ago)
function cleanDedupMap() {
  const currentHk = hourKey();
  // Simple: just clear the whole map once per hour
  // Worst case we store one extra position per vessel right after clearing
  lastStoredHour.clear();
  console.log('[DEDUP] Map cleared for new hour');
}

// ── Main ────────────────────────────────────────────────────────────────

async function main() {
  console.log('=== AIS Biofouling Collector ===');
  console.log(`Min vessel length: ${CONFIG.minLength}m`);
  console.log(`Position interval: ${CONFIG.positionIntervalMin} min`);
  console.log(`Retention: ${CONFIG.retentionDays} days`);
  console.log(`Batch flush: every ${CONFIG.batchFlushMs}ms`);

  const pool = new Pool({
    connectionString: CONFIG.databaseUrl,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
    ssl: CONFIG.databaseUrl.includes('render.com') ? { rejectUnauthorized: false } : false,
  });

  // Test DB connection
  try {
    await pool.query('SELECT 1');
    console.log('[DB] Connected');
  } catch (err) {
    console.error('[DB] Connection failed:', err.message);
    process.exit(1);
  }

  await initDatabase(pool);

  // Load existing known vessels from DB into memory
  try {
    const existing = await pool.query('SELECT mmsi, length_m, name FROM vessels');
    for (const row of existing.rows) {
      knownVessels.set(row.mmsi, { length: row.length_m, name: row.name });
    }
    console.log(`[DB] Loaded ${knownVessels.size} known vessels from database`);
  } catch (err) {
    console.log('[DB] No existing vessels loaded:', err.message);
  }

  // Start websocket
  connect(pool);

  // Periodic flush
  const flushInterval = setInterval(() => flushBatches(pool), CONFIG.batchFlushMs);

  // Periodic stats
  const statsInterval = setInterval(logStats, CONFIG.statsIntervalMs);

  // Periodic dedup cleanup (every hour on the hour-ish)
  const dedupInterval = setInterval(cleanDedupMap, CONFIG.positionIntervalMin * 60 * 1000);

  // Periodic old data purge (daily)
  const purgeInterval = setInterval(async () => {
    try {
      const purged = await purgeOldPositions(pool, CONFIG.retentionDays);
      if (purged > 0) console.log(`[DB] Purged ${purged} positions older than ${CONFIG.retentionDays} days`);
    } catch (err) {
      console.error('[DB] Purge error:', err.message);
    }
  }, CONFIG.purgeIntervalHours * 3600000);

  // Graceful shutdown
  async function shutdown(signal) {
    console.log(`\n[SHUTDOWN] Received ${signal}. Flushing final batch...`);
    shouldRun = false;

    clearInterval(flushInterval);
    clearInterval(statsInterval);
    clearInterval(dedupInterval);
    clearInterval(purgeInterval);

    if (ws) ws.close();

    await flushBatches(pool);
    logStats();

    await pool.end();
    console.log('[SHUTDOWN] Clean exit.');
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  console.error('[FATAL]', err);
  process.exit(1);
});
