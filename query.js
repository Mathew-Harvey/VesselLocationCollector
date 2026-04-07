// Utility queries against the collected AIS data
// Run: node query.js <command> [args]

require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('render.com') ? { rejectUnauthorized: false } : false,
});

const commands = {
  // Show database stats
  async stats() {
    const vessels = await pool.query('SELECT COUNT(*) as count FROM vessels');
    const positions = await pool.query('SELECT COUNT(*) as count FROM positions');
    const oldest = await pool.query('SELECT MIN(recorded_at) as oldest FROM positions');
    const newest = await pool.query('SELECT MAX(recorded_at) as newest FROM positions');
    const byType = await pool.query(`
      SELECT ship_type, COUNT(*) as count 
      FROM vessels 
      GROUP BY ship_type 
      ORDER BY count DESC 
      LIMIT 20
    `);

    console.log('\n=== AIS Collector Database Stats ===');
    console.log(`Vessels tracked: ${vessels.rows[0].count}`);
    console.log(`Position records: ${positions.rows[0].count}`);
    console.log(`Data range: ${oldest.rows[0].oldest || 'none'} to ${newest.rows[0].newest || 'none'}`);
    console.log('\nTop vessel types:');
    for (const row of byType.rows) {
      console.log(`  Type ${row.ship_type}: ${row.count} vessels`);
    }
  },

  // Find vessels that have been idle (SOG < 1 knot) for extended periods
  async idle(days = 7) {
    const result = await pool.query(`
      WITH idle_streaks AS (
        SELECT 
          p.mmsi,
          v.name,
          v.length_m,
          v.ship_type,
          p.latitude,
          p.longitude,
          p.recorded_at,
          p.sog,
          COUNT(*) OVER (
            PARTITION BY p.mmsi 
            ORDER BY p.recorded_at 
            RANGE BETWEEN INTERVAL '${parseInt(days)} days' PRECEDING AND CURRENT ROW
          ) as readings_in_window
        FROM positions p
        JOIN vessels v ON v.mmsi = p.mmsi
        WHERE p.sog < 1.0
          AND p.recorded_at > NOW() - INTERVAL '${parseInt(days)} days'
      )
      SELECT 
        mmsi,
        name,
        length_m,
        ship_type,
        ROUND(AVG(latitude)::numeric, 4) as avg_lat,
        ROUND(AVG(longitude)::numeric, 4) as avg_lon,
        COUNT(*) as idle_hours,
        MIN(recorded_at) as idle_since,
        MAX(recorded_at) as last_seen
      FROM idle_streaks
      GROUP BY mmsi, name, length_m, ship_type
      HAVING COUNT(*) > 24
      ORDER BY COUNT(*) DESC
      LIMIT 50
    `);

    console.log(`\n=== Vessels Idle > 24h (last ${days} days) ===`);
    console.log(`Found: ${result.rows.length}\n`);
    for (const row of result.rows) {
      const idleDays = (row.idle_hours / 24).toFixed(1);
      console.log(`${row.name || 'UNKNOWN'} (MMSI:${row.mmsi}) | ${row.length_m}m | idle ~${idleDays}d | ${row.avg_lat},${row.avg_lon}`);
    }
  },

  // Get movement history for a specific vessel
  async vessel(mmsi) {
    if (!mmsi) { console.log('Usage: node query.js vessel <mmsi>'); return; }
    
    const info = await pool.query('SELECT * FROM vessels WHERE mmsi = $1', [mmsi]);
    if (!info.rows.length) { console.log('Vessel not found'); return; }

    const v = info.rows[0];
    console.log(`\n=== ${v.name} (MMSI:${v.mmsi}, IMO:${v.imo_number}) ===`);
    console.log(`Type: ${v.ship_type} | ${v.length_m}m x ${v.beam_m}m | Draught: ${v.draught_m}m`);
    console.log(`Destination: ${v.destination}`);
    console.log(`First seen: ${v.first_seen_at} | Last seen: ${v.last_seen_at}`);

    const positions = await pool.query(`
      SELECT latitude, longitude, sog, cog, nav_status, recorded_at
      FROM positions
      WHERE mmsi = $1
      ORDER BY recorded_at DESC
      LIMIT 100
    `, [mmsi]);

    console.log(`\nLast ${positions.rows.length} positions:`);
    for (const p of positions.rows) {
      const status = p.sog < 1 ? 'IDLE' : `${p.sog}kn`;
      console.log(`  ${p.recorded_at.toISOString().slice(0, 16)} | ${p.latitude.toFixed(4)}, ${p.longitude.toFixed(4)} | ${status}`);
    }
  },

  // Show storage growth rate
  async growth() {
    const result = await pool.query(`
      SELECT 
        DATE(recorded_at) as day,
        COUNT(*) as positions,
        COUNT(DISTINCT mmsi) as unique_vessels
      FROM positions
      GROUP BY DATE(recorded_at)
      ORDER BY day DESC
      LIMIT 30
    `);

    console.log('\n=== Daily Data Growth ===');
    for (const row of result.rows) {
      const sizeEstKB = (row.positions * 80 / 1024).toFixed(0);
      console.log(`${row.day.toISOString().slice(0, 10)} | ${row.positions} positions | ${row.unique_vessels} vessels | ~${sizeEstKB}KB`);
    }

    // Estimate annual storage
    if (result.rows.length > 0) {
      const avgDaily = result.rows.reduce((s, r) => s + parseInt(r.positions), 0) / result.rows.length;
      const annualGB = (avgDaily * 365 * 80 / 1024 / 1024 / 1024).toFixed(2);
      console.log(`\nEstimated annual storage: ~${annualGB}GB (at ${avgDaily.toFixed(0)} positions/day)`);
    }
  },
};

async function main() {
  const cmd = process.argv[2] || 'stats';
  const arg = process.argv[3];

  if (!commands[cmd]) {
    console.log('Available commands: stats, idle [days], vessel <mmsi>, growth');
    process.exit(1);
  }

  try {
    await commands[cmd](arg);
  } finally {
    await pool.end();
  }
}

main().catch(console.error);
