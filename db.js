const { Pool } = require('pg');

async function initDatabase(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS vessels (
      mmsi INTEGER PRIMARY KEY,
      imo_number INTEGER,
      name TEXT,
      call_sign TEXT,
      ship_type INTEGER,
      length_m REAL,
      beam_m REAL,
      draught_m REAL,
      destination TEXT,
      first_seen_at TIMESTAMPTZ DEFAULT NOW(),
      last_seen_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS positions (
      id BIGSERIAL PRIMARY KEY,
      mmsi INTEGER NOT NULL,
      latitude DOUBLE PRECISION NOT NULL,
      longitude DOUBLE PRECISION NOT NULL,
      sog REAL,
      cog REAL,
      heading INTEGER,
      nav_status INTEGER,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_positions_mmsi ON positions (mmsi);
    CREATE INDEX IF NOT EXISTS idx_positions_recorded_at ON positions (recorded_at);
    CREATE INDEX IF NOT EXISTS idx_positions_mmsi_recorded ON positions (mmsi, recorded_at DESC);
  `);

  console.log('[DB] Tables and indexes ready');
}

async function upsertVessels(pool, vessels) {
  if (!vessels.length) return 0;
  const values = [];
  const params = [];
  let i = 1;

  for (const v of vessels) {
    values.push(`($${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++})`);
    params.push(v.mmsi, v.imo_number, v.name, v.call_sign, v.ship_type, v.length_m, v.beam_m, v.draught_m, v.destination);
  }

  const query = `
    INSERT INTO vessels (mmsi, imo_number, name, call_sign, ship_type, length_m, beam_m, draught_m, destination)
    VALUES ${values.join(', ')}
    ON CONFLICT (mmsi) DO UPDATE SET
      imo_number = COALESCE(EXCLUDED.imo_number, vessels.imo_number),
      name = COALESCE(NULLIF(EXCLUDED.name, ''), vessels.name),
      call_sign = COALESCE(NULLIF(EXCLUDED.call_sign, ''), vessels.call_sign),
      ship_type = COALESCE(EXCLUDED.ship_type, vessels.ship_type),
      length_m = GREATEST(EXCLUDED.length_m, vessels.length_m),
      beam_m = GREATEST(EXCLUDED.beam_m, vessels.beam_m),
      draught_m = COALESCE(EXCLUDED.draught_m, vessels.draught_m),
      destination = COALESCE(NULLIF(EXCLUDED.destination, ''), vessels.destination),
      last_seen_at = NOW()
  `;

  await pool.query(query, params);
  return vessels.length;
}

async function insertPositions(pool, positions) {
  if (!positions.length) return 0;
  const values = [];
  const params = [];
  let i = 1;

  for (const p of positions) {
    values.push(`($${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++})`);
    params.push(p.mmsi, p.latitude, p.longitude, p.sog, p.cog, p.heading, p.nav_status, p.recorded_at);
  }

  const query = `
    INSERT INTO positions (mmsi, latitude, longitude, sog, cog, heading, nav_status, recorded_at)
    VALUES ${values.join(', ')}
  `;

  await pool.query(query, params);
  return positions.length;
}

// Purge positions older than N days to manage storage
async function purgeOldPositions(pool, retentionDays = 365) {
  const result = await pool.query(
    `DELETE FROM positions WHERE recorded_at < NOW() - INTERVAL '1 day' * $1`,
    [retentionDays]
  );
  return result.rowCount;
}

module.exports = { initDatabase, upsertVessels, insertPositions, purgeOldPositions };
