import "dotenv/config";
import cors from "cors";
import express from "express";
import { Pool } from "pg";

const requiredEnv = ["DATABASE_URL"];
for (const key of requiredEnv) {
  if (!process.env[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

const app = express();
const port = Number(process.env.PORT || 4000);
const host = process.env.HOST || "0.0.0.0";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const DEFAULT_LOOKBACK_DAYS = 7;
const MAX_LOOKBACK_DAYS = 30;
const MAX_ACTIVE_FIRES = 1000;

app.use(cors());
app.use(express.json());

function getUtcDayString(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

function isValidUtcDay(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function parseUtcDay(rawValue, fallback = getUtcDayString()) {
  const value = rawValue ? String(rawValue) : fallback;
  return isValidUtcDay(value) ? value : null;
}

function parseIntegerInRange(rawValue, { fallback, min, max }) {
  if (rawValue === undefined || rawValue === null || rawValue === "") {
    return fallback;
  }
  const value = Number.parseInt(String(rawValue), 10);
  if (!Number.isFinite(value) || value < min || value > max) {
    return null;
  }
  return value;
}

function parseNonNegativeNumber(rawValue, { fallback, max }) {
  if (rawValue === undefined || rawValue === null || rawValue === "") {
    return fallback;
  }
  const value = Number(rawValue);
  if (!Number.isFinite(value) || value < 0 || value > max) {
    return null;
  }
  return value;
}

function buildWindowBounds(asOfDay, lookbackDays) {
  const start = new Date(`${asOfDay}T00:00:00.000Z`);
  start.setUTCDate(start.getUTCDate() - (lookbackDays - 1));
  const end = new Date(`${asOfDay}T23:59:59.999Z`);
  return {
    startIso: start.toISOString(),
    endIso: end.toISOString(),
  };
}

function fromPgTimestamp(value) {
  if (!value) {
    return null;
  }
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function isMissingPerimetersTable(error) {
  return error && error.code === "42P01";
}

app.get("/health", async (_req, res, next) => {
  try {
    const result = await pool.query("select now() as now");
    res.json({ status: "ok", databaseTime: result.rows[0].now });
  } catch (error) {
    next(error);
  }
});

app.get("/fires", async (req, res, next) => {
  try {
    const where = [];
    const params = [];
    let idx = 1;

    if (req.query.bbox) {
      const [minLon, minLat, maxLon, maxLat] = String(req.query.bbox)
        .split(",")
        .map(Number);

      const bboxValues = [minLon, minLat, maxLon, maxLat];
      const hasInvalidNumber = bboxValues.some((value) => Number.isNaN(value));
      if (hasInvalidNumber) {
        return res
          .status(400)
          .json({ error: "bbox must be minLon,minLat,maxLon,maxLat" });
      }

      where.push(
        `ST_Intersects(geom, ST_MakeEnvelope($${idx}, $${idx + 1}, $${idx + 2}, $${idx + 3}, 4326))`,
      );
      params.push(...bboxValues);
      idx += 4;
    }

    if (req.query.start) {
      where.push(`detected_at >= $${idx}`);
      params.push(req.query.start);
      idx += 1;
    }

    if (req.query.end) {
      where.push(`detected_at <= $${idx}`);
      params.push(req.query.end);
      idx += 1;
    }

    if (req.query.minFrp) {
      const minFrp = Number(req.query.minFrp);
      if (Number.isNaN(minFrp)) {
        return res.status(400).json({ error: "minFrp must be a number" });
      }
      where.push(`frp >= $${idx}`);
      params.push(minFrp);
      idx += 1;
    }

    const limit = Math.min(Number(req.query.limit) || 1000, 5000);

    const sql = `
      SELECT
        id,
        detected_at,
        frp,
        confidence,
        satellite,
        daynight,
        ST_AsGeoJSON(geom)::json AS geometry
      FROM fire_detections
      ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
      ORDER BY detected_at DESC
      LIMIT $${idx}
    `;
    params.push(limit);

    const result = await pool.query(sql, params);
    const features = result.rows.map((row) => ({
      type: "Feature",
      geometry: row.geometry,
      properties: {
        id: row.id,
        detectedAt: fromPgTimestamp(row.detected_at),
        frp: row.frp,
        confidence: row.confidence,
        satellite: row.satellite,
        daynight: row.daynight,
      },
    }));

    res.json({
      type: "FeatureCollection",
      count: features.length,
      features,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/active-fires", async (req, res, next) => {
  const asOfDay = parseUtcDay(req.query.asOf);
  if (!asOfDay) {
    return res.status(400).json({ error: "asOf must be YYYY-MM-DD" });
  }

  const lookbackDays = parseIntegerInRange(req.query.lookbackDays, {
    fallback: DEFAULT_LOOKBACK_DAYS,
    min: 1,
    max: MAX_LOOKBACK_DAYS,
  });
  if (lookbackDays === null) {
    return res
      .status(400)
      .json({ error: `lookbackDays must be an integer between 1 and ${MAX_LOOKBACK_DAYS}` });
  }

  const minAreaKm2 = parseNonNegativeNumber(req.query.minAreaKm2, {
    fallback: 1,
    max: 500_000,
  });
  if (minAreaKm2 === null) {
    return res.status(400).json({ error: "minAreaKm2 must be a non-negative number" });
  }

  const limit = parseIntegerInRange(req.query.limit, {
    fallback: 250,
    min: 1,
    max: MAX_ACTIVE_FIRES,
  });
  if (limit === null) {
    return res
      .status(400)
      .json({ error: `limit must be an integer between 1 and ${MAX_ACTIVE_FIRES}` });
  }

  const { startIso, endIso } = buildWindowBounds(asOfDay, lookbackDays);

  const sql = `
    WITH windowed AS (
      SELECT
        fire_id,
        overpass_time,
        frp_sum,
        pixel_count,
        area_km2,
        satellite,
        geom
      FROM fire_perimeters
      WHERE overpass_time >= $1
        AND overpass_time <= $2
    ),
    fire_summary AS (
      SELECT
        fire_id,
        MIN(overpass_time) AS first_seen,
        MAX(overpass_time) AS last_seen,
        MAX(area_km2) AS max_area_km2,
        SUM(frp_sum) AS total_frp_sum,
        COUNT(*)::int AS overpass_count
      FROM windowed
      GROUP BY fire_id
      HAVING MAX(area_km2) >= $3
    ),
    latest AS (
      SELECT DISTINCT ON (w.fire_id)
        w.fire_id,
        w.overpass_time,
        w.area_km2,
        w.frp_sum,
        w.pixel_count,
        w.satellite,
        w.geom
      FROM windowed w
      JOIN fire_summary s ON s.fire_id = w.fire_id
      ORDER BY w.fire_id, w.overpass_time DESC
    )
    SELECT
      s.fire_id,
      s.first_seen,
      s.last_seen,
      s.max_area_km2,
      s.total_frp_sum,
      s.overpass_count,
      l.overpass_time AS latest_overpass_time,
      l.area_km2 AS latest_area_km2,
      l.frp_sum AS latest_frp_sum,
      l.pixel_count AS latest_pixel_count,
      l.satellite AS latest_satellite,
      ST_AsGeoJSON(ST_PointOnSurface(l.geom))::json AS marker_geometry
    FROM fire_summary s
    JOIN latest l ON l.fire_id = s.fire_id
    ORDER BY s.max_area_km2 DESC, s.last_seen DESC
    LIMIT $4
  `;

  try {
    const result = await pool.query(sql, [startIso, endIso, minAreaKm2, limit]);

    const features = result.rows.map((row) => ({
      type: "Feature",
      geometry: row.marker_geometry,
      properties: {
        fireId: Number(row.fire_id),
        firstSeen: fromPgTimestamp(row.first_seen),
        lastSeen: fromPgTimestamp(row.last_seen),
        overpassCount: Number(row.overpass_count),
        maxAreaKm2: Number(row.max_area_km2),
        totalFrpSum: Number(row.total_frp_sum ?? 0),
        latestOverpassTime: fromPgTimestamp(row.latest_overpass_time),
        latestAreaKm2: Number(row.latest_area_km2),
        latestFrpSum: Number(row.latest_frp_sum ?? 0),
        latestPixelCount: Number(row.latest_pixel_count ?? 0),
        latestSatellite: row.latest_satellite,
      },
    }));

    res.json({
      type: "FeatureCollection",
      asOfDay,
      lookbackDays,
      minAreaKm2,
      count: features.length,
      features,
    });
  } catch (error) {
    if (isMissingPerimetersTable(error)) {
      return res.status(503).json({
        error: "fire_perimeters table is missing. Run preprocessing and load perimeters first.",
      });
    }
    next(error);
  }
});

app.get("/active-fires/:fireId", async (req, res, next) => {
  const fireId = Number.parseInt(req.params.fireId, 10);
  if (!Number.isFinite(fireId) || fireId <= 0) {
    return res.status(400).json({ error: "fireId must be a positive integer" });
  }

  const asOfDay = parseUtcDay(req.query.asOf);
  if (!asOfDay) {
    return res.status(400).json({ error: "asOf must be YYYY-MM-DD" });
  }

  const lookbackDays = parseIntegerInRange(req.query.lookbackDays, {
    fallback: DEFAULT_LOOKBACK_DAYS,
    min: 1,
    max: MAX_LOOKBACK_DAYS,
  });
  if (lookbackDays === null) {
    return res
      .status(400)
      .json({ error: `lookbackDays must be an integer between 1 and ${MAX_LOOKBACK_DAYS}` });
  }

  const mode = String(req.query.mode || "overpass").toLowerCase();
  if (!["overpass", "daily"].includes(mode)) {
    return res.status(400).json({ error: "mode must be overpass or daily" });
  }

  const { startIso, endIso } = buildWindowBounds(asOfDay, lookbackDays);

  const summarySql = `
    SELECT
      fire_id,
      MIN(overpass_time) AS first_seen,
      MAX(overpass_time) AS last_seen,
      MAX(area_km2) AS max_area_km2,
      SUM(frp_sum) AS total_frp_sum,
      COUNT(*)::int AS overpass_count
    FROM fire_perimeters
    WHERE fire_id = $1
      AND overpass_time >= $2
      AND overpass_time <= $3
    GROUP BY fire_id
  `;

  const overpassSql = `
    SELECT
      overpass_time AS slice_time,
      area_km2,
      frp_sum,
      pixel_count,
      satellite,
      ST_AsGeoJSON(geom)::json AS geometry
    FROM fire_perimeters
    WHERE fire_id = $1
      AND overpass_time >= $2
      AND overpass_time <= $3
    ORDER BY overpass_time ASC
  `;

  const dailySql = `
    SELECT
      date_trunc('day', overpass_time) AS slice_time,
      MAX(area_km2) AS area_km2,
      SUM(frp_sum) AS frp_sum,
      SUM(pixel_count)::int AS pixel_count,
      string_agg(DISTINCT satellite, ',' ORDER BY satellite) AS satellite,
      ST_AsGeoJSON(ST_UnaryUnion(ST_Collect(geom)))::json AS geometry
    FROM fire_perimeters
    WHERE fire_id = $1
      AND overpass_time >= $2
      AND overpass_time <= $3
    GROUP BY 1
    ORDER BY 1 ASC
  `;

  try {
    const [summaryResult, slicesResult] = await Promise.all([
      pool.query(summarySql, [fireId, startIso, endIso]),
      pool.query(mode === "daily" ? dailySql : overpassSql, [fireId, startIso, endIso]),
    ]);

    if (summaryResult.rows.length === 0) {
      return res.status(404).json({
        error: `No fire with id=${fireId} found in window ${startIso}..${endIso}`,
      });
    }

    const summaryRow = summaryResult.rows[0];
    const perimeters = slicesResult.rows.map((row) => ({
      type: "Feature",
      geometry: row.geometry,
      properties: {
        fireId,
        time: fromPgTimestamp(row.slice_time),
        areaKm2: Number(row.area_km2),
        frpSum: Number(row.frp_sum ?? 0),
        pixelCount: Number(row.pixel_count ?? 0),
        satellite: row.satellite ?? null,
      },
    }));

    res.json({
      fireId,
      asOfDay,
      lookbackDays,
      mode,
      summary: {
        firstSeen: fromPgTimestamp(summaryRow.first_seen),
        lastSeen: fromPgTimestamp(summaryRow.last_seen),
        maxAreaKm2: Number(summaryRow.max_area_km2),
        totalFrpSum: Number(summaryRow.total_frp_sum ?? 0),
        overpassCount: Number(summaryRow.overpass_count),
      },
      series: perimeters.map((feature) => ({
        time: feature.properties.time,
        areaKm2: feature.properties.areaKm2,
        frpSum: feature.properties.frpSum,
        pixelCount: feature.properties.pixelCount,
      })),
      perimeters: {
        type: "FeatureCollection",
        count: perimeters.length,
        features: perimeters,
      },
    });
  } catch (error) {
    if (isMissingPerimetersTable(error)) {
      return res.status(503).json({
        error: "fire_perimeters table is missing. Run preprocessing and load perimeters first.",
      });
    }
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(port, host, () => {
  console.log(`API listening on http://${host}:${port}`);
});
