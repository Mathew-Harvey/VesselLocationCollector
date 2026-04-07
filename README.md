# AIS Biofouling Collector

Collects hourly position snapshots for every vessel over 30m via [aisstream.io](https://aisstream.io) (free). Builds the historical movement database needed for biofouling risk prediction.

## What it does

1. Connects to aisstream.io global AIS websocket feed (~300 messages/second)
2. Listens for `ShipStaticData` messages to learn vessel dimensions
3. Filters for vessels >= 30m length (configurable)
4. Stores one position per vessel per hour (deduped in memory)
5. Batches database writes every 5 seconds to handle throughput
6. Auto-reconnects with exponential backoff on disconnection
7. Purges data older than retention period (default 365 days)

## Storage estimates

| Metric | Estimate |
|---|---|
| Vessels >= 30m globally | ~90,000-120,000 |
| Positions per vessel per day | 24 (one per hour) |
| Bytes per position row | ~80 bytes |
| Daily storage | ~200-230MB |
| Monthly storage | ~6-7GB |
| Annual storage | ~75-85GB |

The Render Starter DB (1GB, $7/month) will fill in about 4-5 days at full global collection. Options:

- **Starter DB ($7/month)**: Run for a few days to validate, then upgrade
- **Standard DB ($25/month, 10GB)**: ~45 days of global data
- **Pro DB ($85/month, 100GB)**: ~1 year of global data  
- **Self-hosted PG**: cheapest for large volumes (e.g. $6/month on a VPS)

Alternatively, reduce volume by filtering to specific regions or vessel types.

## Deploy to Render

1. Push this repo to GitHub
2. In Render dashboard: New > Blueprint > connect your repo
3. It will create a Worker + PostgreSQL from `render.yaml`
4. Set the `AISSTREAM_API_KEY` environment variable in the Worker settings
5. Deploy

## Run locally

```bash
# 1. Copy env template
cp .env.example .env

# 2. Edit .env with your API key and a PostgreSQL connection string
#    (use a local PG or a free Render/Supabase/Neon instance for testing)

# 3. Install and run
npm install
node collector.js
```

## Query the data

```bash
# Database stats
node query.js stats

# Find vessels idle > 24h in the last 7 days
node query.js idle 7

# Get movement history for a specific vessel
node query.js vessel 123456789

# Check storage growth rate
node query.js growth
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `AISSTREAM_API_KEY` | required | Your aisstream.io API key |
| `DATABASE_URL` | required | PostgreSQL connection string |
| `MIN_VESSEL_LENGTH_M` | 30 | Minimum vessel length to track |
| `POSITION_INTERVAL_MINUTES` | 60 | How often to store a position per vessel |
| `RETENTION_DAYS` | 365 | Auto-delete positions older than this |
| `BATCH_FLUSH_INTERVAL_MS` | 5000 | How often to write batched data to DB |
| `STATS_INTERVAL_MS` | 60000 | How often to log stats to console |

## Architecture notes

- **No MMSI pre-filter**: aisstream.io limits MMSI filters to 50, so we subscribe to everything and filter client-side by vessel dimensions learned from ShipStaticData messages
- **Cold start**: On first run, the collector doesn't know which vessels are 30m+. It learns this from ShipStaticData messages (broadcast every ~6 minutes per vessel). After ~30 minutes it knows most active vessels. After 24h it has near-complete coverage.
- **Memory usage**: The in-memory vessel registry holds ~120k entries at ~200 bytes each = ~24MB. The hourly dedup map adds another ~10MB. Total steady-state memory: ~50-80MB.
- **Reconnection**: Exponential backoff from 1s to 60s max. On reconnect, the vessel registry persists in memory and is also reloaded from DB on restart.

## Next step: biofouling risk API

This collector is Phase 0. The data it produces feeds into a biofouling risk prediction API that combines:
- Vessel movement patterns (idle time, speed profile) from this collector
- Sea surface temperature from NOAA ERDDAP (free)
- Coating performance profiles (user-provided)
- Validated against real inspection data (FR/PDR ratings)

## API key security

Your aisstream.io API key is free but personal. Don't commit `.env` to git. The `.gitignore` is already set up to prevent this. If deploying to Render, set the key as an environment variable in the dashboard.
