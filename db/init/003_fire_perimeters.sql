CREATE TABLE IF NOT EXISTS fire_perimeters (
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

CREATE UNIQUE INDEX IF NOT EXISTS fire_perimeters_unique_event_idx
  ON fire_perimeters (fire_id, overpass_time, satellite, cluster_id);

CREATE INDEX IF NOT EXISTS fire_perimeters_fire_time_idx
  ON fire_perimeters (fire_id, overpass_time DESC);

CREATE INDEX IF NOT EXISTS fire_perimeters_time_idx
  ON fire_perimeters (overpass_time DESC);

CREATE INDEX IF NOT EXISTS fire_perimeters_geom_idx
  ON fire_perimeters
  USING GIST (geom);
