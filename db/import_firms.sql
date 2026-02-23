-- Run with:
-- docker compose exec -T db psql -U wildfire -d wildfire -c "\copy fire_detections_stage FROM STDIN WITH (FORMAT csv, HEADER true)" < path/to/fire_data.csv

INSERT INTO fire_detections (
  latitude,
  longitude,
  detected_at,
  frp,
  confidence,
  satellite,
  daynight
)
SELECT
  latitude::DOUBLE PRECISION,
  longitude::DOUBLE PRECISION,
  to_timestamp(acq_date || ' ' || lpad(acq_time, 4, '0'), 'YYYY-MM-DD HH24MI') AT TIME ZONE 'UTC',
  NULLIF(frp, '')::DOUBLE PRECISION,
  CASE
    WHEN confidence ~ '^[0-9]+$' THEN confidence::INTEGER
    ELSE NULL
  END,
  NULLIF(satellite, ''),
  CASE
    WHEN upper(daynight) IN ('D', 'N') THEN upper(daynight)
    ELSE NULL
  END
FROM fire_detections_stage
WHERE latitude ~ '^-?[0-9]+(\.[0-9]+)?$'
  AND longitude ~ '^-?[0-9]+(\.[0-9]+)?$'
  AND acq_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
  AND acq_time ~ '^[0-9]{1,4}$';
