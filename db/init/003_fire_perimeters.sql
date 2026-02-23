CREATE TABLE IF NOT EXISTS fire_perimeters_raw (
  id BIGSERIAL PRIMARY KEY,
  fire_id BIGINT NOT NULL,
  overpass_time TIMESTAMPTZ NOT NULL,
  satellite TEXT,
  cluster_id INTEGER,
  pixel_count INTEGER,
  frp_sum DOUBLE PRECISION,
  area_km2 DOUBLE PRECISION,
  geom geometry(Geometry, 4326) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS fire_perimeters_raw_unique_event_idx
  ON fire_perimeters_raw (fire_id, overpass_time, satellite, cluster_id);

CREATE INDEX IF NOT EXISTS fire_perimeters_raw_fire_time_idx
  ON fire_perimeters_raw (fire_id, overpass_time DESC);

CREATE INDEX IF NOT EXISTS fire_perimeters_raw_time_idx
  ON fire_perimeters_raw (overpass_time DESC);

CREATE INDEX IF NOT EXISTS fire_perimeters_raw_geom_idx
  ON fire_perimeters_raw
  USING GIST (geom);

CREATE TABLE IF NOT EXISTS fire_perimeters_display (
  fire_id BIGINT NOT NULL,
  step_time TIMESTAMPTZ NOT NULL,
  step_index INTEGER NOT NULL,
  step_count INTEGER NOT NULL,
  age_norm DOUBLE PRECISION NOT NULL CHECK (age_norm >= 0 AND age_norm <= 1),
  pixel_count INTEGER NOT NULL,
  frp_sum DOUBLE PRECISION NOT NULL,
  area_step_km2 DOUBLE PRECISION NOT NULL,
  area_cumulative_km2 DOUBLE PRECISION NOT NULL,
  growth_km2 DOUBLE PRECISION NOT NULL,
  satellites TEXT,
  color_hex TEXT NOT NULL,
  geom_step geometry(MultiPolygon, 4326) NOT NULL,
  geom_cumulative geometry(MultiPolygon, 4326) NOT NULL,
  geom_growth geometry(MultiPolygon, 4326) NOT NULL,
  PRIMARY KEY (fire_id, step_time)
);

CREATE INDEX IF NOT EXISTS fire_perimeters_display_fire_step_idx
  ON fire_perimeters_display (fire_id, step_index);

CREATE INDEX IF NOT EXISTS fire_perimeters_display_time_idx
  ON fire_perimeters_display (step_time DESC);

CREATE INDEX IF NOT EXISTS fire_perimeters_display_geom_idx
  ON fire_perimeters_display
  USING GIST (geom_cumulative);
