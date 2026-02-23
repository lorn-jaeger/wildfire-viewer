CREATE TABLE IF NOT EXISTS fire_detections (
  id BIGSERIAL PRIMARY KEY,
  latitude DOUBLE PRECISION NOT NULL,
  longitude DOUBLE PRECISION NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL,
  frp DOUBLE PRECISION,
  confidence INTEGER,
  satellite TEXT,
  daynight TEXT CHECK (daynight IN ('D', 'N')),
  source_type TEXT NOT NULL CHECK (source_type IN ('archive', 'nrt')),
  source_version TEXT,
  source_file TEXT,
  geom geometry(Point, 4326) GENERATED ALWAYS AS (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
  ) STORED
);

CREATE INDEX IF NOT EXISTS fire_detections_geom_idx
  ON fire_detections
  USING GIST (geom);

CREATE INDEX IF NOT EXISTS fire_detections_detected_at_idx
  ON fire_detections (detected_at DESC);

CREATE INDEX IF NOT EXISTS fire_detections_source_time_idx
  ON fire_detections (source_type, detected_at DESC);

CREATE TABLE IF NOT EXISTS fire_detections_stage (
  latitude TEXT,
  longitude TEXT,
  bright_ti4 TEXT,
  scan TEXT,
  track TEXT,
  acq_date TEXT,
  acq_time TEXT,
  satellite TEXT,
  instrument TEXT,
  confidence TEXT,
  version TEXT,
  daynight TEXT,
  bright_ti5 TEXT,
  frp TEXT,
  type TEXT
);
