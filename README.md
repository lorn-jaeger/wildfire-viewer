# Wildfire Viewer

Minimal React + Vite frontend with a Dockerized backend stack:

- `db`: PostgreSQL + PostGIS
- `api`: Node/Express service for wildfire queries

## Requirements

- Docker Desktop (or Docker Engine + Compose)
- Node.js 20+ (for frontend local dev)

## Quick Start

1. Start backend services:

```bash
npm run docker:up
```

2. Check logs:

```bash
npm run docker:logs
```

3. Run frontend:

```bash
npm install
npm run dev
```

Frontend: `http://localhost:5173`  
API: `http://localhost:4000`

Create `.env` in project root (or copy from `.env.example`) and set:

```bash
VITE_MAPBOX_TOKEN=your_mapbox_public_token_here
VITE_API_BASE_URL=http://localhost:4000
```

## API Endpoints

- `GET /health`
- `GET /fires`
- `GET /active-fires`
- `GET /active-fires/:fireId`

`/fires` supports these query params:

- `bbox=minLon,minLat,maxLon,maxLat`
- `start=2025-01-01T00:00:00Z`
- `end=2025-12-31T23:59:59Z`
- `minFrp=10`
- `limit=1000` (max 5000)

Example:

```bash
curl "http://localhost:4000/fires?bbox=-125,24,-66,49&start=2024-01-01T00:00:00Z&limit=500"
```

`/active-fires` query params:
- `asOf=2021-08-19` (UTC day)
- `lookbackDays=7`
- `minAreaKm2=1`
- `limit=250` (max 1000)

`/active-fires/:fireId` query params:
- `asOf=2021-08-19`
- `lookbackDays=7`
- `mode=overpass` or `mode=daily`

## Import FIRMS CSV Data

1. Unzip your downloaded FIRMS archive and locate the CSV file.

2. Load CSV into staging table:

```bash
docker compose exec -T db psql -U wildfire -d wildfire -c "\copy fire_detections_stage FROM STDIN WITH (FORMAT csv, HEADER true)" < path/to/fire_data.csv
```

3. Transform staging rows into PostGIS table:

```bash
docker compose exec -T db psql -U wildfire -d wildfire -f /db/import_firms.sql
```

## Build Overpass Polygons With FireAtlas

This project includes a preprocessing script that reuses `fireatlas` clustering + hull logic
to create polygon shapes for each satellite overpass (grouped by timestamp and satellite).

1. Create a Python environment with `uv` and install preprocess dependencies:

```bash
uv venv .venv-preprocess
uv pip install --python .venv-preprocess/bin/python -r preprocess/requirements.txt
```

2. Run preprocessing for a date range (unique fires + size filter):

```bash
.venv-preprocess/bin/python preprocess/fireatlas_overpass_shapes.py \
  --start 2026-02-01 \
  --end 2026-02-21 \
  --db-url "postgres://wildfire:wildfire@localhost:5432/wildfire" \
  --min-fire-area-km2 5 \
  --min-overpasses 2 \
  --write-db \
  --replace-db \
  --db-table fire_perimeters \
  --summary-output data/derived/overpass_shapes_2026-02_summary.csv \
  --output data/derived/overpass_shapes_2026-02.fgb
```

Output attributes include:
- `fire_id` (persistent unique fire track ID)
- `overpass_time` (UTC)
- `satellite`
- `cluster_id`
- `pixel_count`
- `frp_sum`
- `area_km2`
- polygon `geometry`

Useful tuning flags:
- `--min-fire-area-km2`: keep only fire tracks whose max polygon area is at least this value.
- `--min-overpasses`: keep only fire tracks seen in at least this many overpasses.
- `--max-link-hours`: max time gap for linking polygons into the same fire.
- `--max-link-km`: max centroid distance for linking polygons.
- `--min-overlap-frac`: minimum area overlap fraction for linking polygons.
- `--write-db`: insert output polygons into PostGIS table (for the active-fire map UI).
- `--replace-db`: truncate table before writing.

If your DB volume already existed before `db/init/003_fire_perimeters.sql` was added, run:

```bash
docker compose exec -T db psql -U wildfire -d wildfire -f /db/init/003_fire_perimeters.sql
```

## Folder Layout

- `src/`: frontend (React + Vite)
- `backend/`: API service
- `db/init/`: DB init SQL (runs when DB volume is first created)
- `db/import_firms.sql`: staging/import transformation SQL

If you change files in `db/init/`, recreate the DB volume:

```bash
docker compose down -v
docker compose up --build -d
```

## Stop Services

```bash
npm run docker:down
```
