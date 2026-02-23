#!/usr/bin/env python3
"""Build per-overpass polygons from fire_detections using fireatlas clustering+hulls."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import psycopg
from psycopg import sql

from fireatlas import FireIO
from fireatlas import settings
from fireatlas.FireClustering import do_clustering
from fireatlas.FireVector import cal_hull


SATELLITE_ALIAS = {
    "NPP": "SNPP",
    "SNPP": "SNPP",
    "N20": "NOAA20",
    "NOAA20": "NOAA20",
    "N21": "NOAA21",
    "NOAA21": "NOAA21",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate per-overpass polygons using fireatlas clustering and hull logic.",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", "postgres://wildfire:wildfire@localhost:5432/wildfire"),
        help="Postgres connection URL (default from DATABASE_URL env).",
    )
    parser.add_argument("--start", required=True, help="Start day (UTC), format YYYY-MM-DD.")
    parser.add_argument("--end", required=True, help="End day (UTC), format YYYY-MM-DD.")
    parser.add_argument(
        "--cluster-km",
        type=float,
        default=0.7,
        help="Pixel clustering distance in kilometers (default: 0.7).",
    )
    parser.add_argument(
        "--min-pixels",
        type=int,
        default=2,
        help="Minimum pixel count per polygon (default: 2).",
    )
    parser.add_argument(
        "--output",
        default="data/derived/overpass_shapes.fgb",
        help="Output file path (.fgb, .geojson, or .parquet).",
    )
    parser.add_argument(
        "--satellites",
        default="",
        help="Optional comma-separated satellite filter, e.g. NOAA20,SNPP.",
    )
    parser.add_argument(
        "--max-link-hours",
        type=float,
        default=36,
        help="Maximum time gap between linked overpass polygons (default: 36).",
    )
    parser.add_argument(
        "--max-link-km",
        type=float,
        default=10,
        help="Maximum centroid distance for linking polygons (default: 10).",
    )
    parser.add_argument(
        "--min-overlap-frac",
        type=float,
        default=0.05,
        help="Minimum overlap fraction with previous polygon for linking (default: 0.05).",
    )
    parser.add_argument(
        "--min-fire-area-km2",
        type=float,
        default=1.0,
        help="Minimum max polygon area for a fire track to keep (default: 1.0).",
    )
    parser.add_argument(
        "--min-overpasses",
        type=int,
        default=2,
        help="Minimum overpass slices required per fire track (default: 2).",
    )
    parser.add_argument(
        "--summary-output",
        default="",
        help="Optional output path for unique fire summary table (.csv).",
    )
    parser.add_argument(
        "--write-db",
        action="store_true",
        help="Write resulting perimeters into PostGIS table (default table: fire_perimeters).",
    )
    parser.add_argument(
        "--replace-db",
        action="store_true",
        help="Truncate target perimeter table before insert (requires --write-db).",
    )
    parser.add_argument(
        "--db-table",
        default="fire_perimeters",
        help="Target PostGIS table for perimeter rows (default: fire_perimeters).",
    )
    return parser.parse_args()


def normalize_satellite(value: str) -> str:
    sat = str(value).strip().upper()
    return SATELLITE_ALIAS.get(sat, sat)


def fetch_detections(
    db_url: str,
    start_dt: datetime,
    end_exclusive_dt: datetime,
) -> pd.DataFrame:
    query = """
        SELECT
          detected_at,
          satellite,
          latitude AS lat,
          longitude AS lon,
          COALESCE(frp, 0) AS frp
        FROM fire_detections
        WHERE detected_at >= %(start)s
          AND detected_at < %(end)s
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY detected_at
    """

    with psycopg.connect(db_url) as conn:
        return pd.read_sql_query(
            query,
            conn,
            params={"start": start_dt, "end": end_exclusive_dt},
        )


def prepare_pixels(df: pd.DataFrame) -> gpd.GeoDataFrame:
    if df.empty:
        return gpd.GeoDataFrame(columns=["Sat", "datetime", "FRP", "DT", "DS", "ampm", "x", "y"])

    pixels = pd.DataFrame(
        {
            "Lat": pd.to_numeric(df["lat"], errors="coerce"),
            "Lon": pd.to_numeric(df["lon"], errors="coerce"),
            "FRP": pd.to_numeric(df["frp"], errors="coerce").fillna(0),
            "Sat": df["satellite"].map(normalize_satellite),
            "datetime": pd.to_datetime(df["detected_at"], utc=True).dt.tz_convert(None),
            "DT": 0.375,
            "DS": 0.375,
            "input_filename": "postgres",
        }
    )
    pixels = pixels.dropna(subset=["Lat", "Lon", "datetime"]).reset_index(drop=True)
    pixels = FireIO.AFP_setampm(pixels)

    gdf = gpd.GeoDataFrame(
        pixels,
        geometry=gpd.points_from_xy(pixels["Lon"], pixels["Lat"]),
        crs="EPSG:4326",
    ).to_crs(epsg=settings.EPSG_CODE)

    gdf["x"] = gdf.geometry.x
    gdf["y"] = gdf.geometry.y
    gdf["overpass_time"] = gdf["datetime"].dt.floor("min")
    return gdf


def build_overpass_polygons(
    gdf: gpd.GeoDataFrame,
    cluster_km: float,
    min_pixels: int,
) -> gpd.GeoDataFrame:
    rows: list[dict] = []

    if gdf.empty:
        return gpd.GeoDataFrame(columns=["overpass_time", "satellite", "cluster_id", "pixel_count", "frp_sum", "geometry"], geometry="geometry", crs="EPSG:4326")

    for (sat, overpass_time), group in gdf.groupby(["Sat", "overpass_time"], sort=True):
        clustered = do_clustering(group, cluster_km)

        for cid, cluster in clustered.groupby("initial_cid"):
            if len(cluster) < min_pixels:
                continue

            hull = cal_hull(cluster[["x", "y"]].to_numpy())
            if hull.is_empty:
                continue

            rows.append(
                {
                    "overpass_time": overpass_time,
                    "satellite": sat,
                    "cluster_id": int(cid),
                    "pixel_count": int(len(cluster)),
                    "frp_sum": float(cluster["FRP"].sum()),
                    "geometry": hull,
                }
            )

    if not rows:
        return gpd.GeoDataFrame(columns=["overpass_time", "satellite", "cluster_id", "pixel_count", "frp_sum", "geometry"], geometry="geometry", crs="EPSG:4326")

    out = gpd.GeoDataFrame(rows, geometry="geometry", crs=f"EPSG:{settings.EPSG_CODE}")
    out = out.to_crs(epsg=4326)
    out["overpass_time"] = pd.to_datetime(out["overpass_time"], utc=True)
    return out.sort_values(["overpass_time", "satellite", "cluster_id"]).reset_index(drop=True)


def assign_unique_fire_ids(
    polygons: gpd.GeoDataFrame,
    max_link_hours: float,
    max_link_km: float,
    min_overlap_frac: float,
    min_fire_area_km2: float,
    min_overpasses: int,
) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
    if polygons.empty:
        empty = polygons.copy()
        empty["fire_id"] = pd.Series(dtype="int64")
        empty["area_km2"] = pd.Series(dtype="float64")
        summary = pd.DataFrame(
            columns=["fire_id", "first_seen", "last_seen", "overpass_count", "max_area_km2", "total_frp_sum"]
        )
        return empty, summary

    work = polygons.copy()
    work["overpass_time"] = pd.to_datetime(work["overpass_time"], utc=True)

    work_proj = work.to_crs(epsg=settings.EPSG_CODE)
    work_proj["area_km2"] = work_proj.geometry.area / 1_000_000.0
    work_proj["centroid"] = work_proj.geometry.centroid

    work_proj = work_proj.sort_values(["overpass_time", "satellite", "cluster_id"]).reset_index(drop=True)

    tracks: dict[int, dict[str, Any]] = {}
    next_fire_id = 1
    assigned_fire_ids: list[int] = []

    max_link_delta = pd.Timedelta(hours=max_link_hours)

    for row in work_proj.itertuples():
        t = row.overpass_time
        geom = row.geometry
        centroid = row.centroid

        best_fire_id = None
        best_score = float("-inf")

        for fire_id, track in tracks.items():
            age = t - track["last_time"]
            if age < pd.Timedelta(0) or age > max_link_delta:
                continue

            prev_geom = track["last_geom"]
            inter_area = geom.intersection(prev_geom).area
            denom = min(geom.area, prev_geom.area)
            overlap_frac = inter_area / denom if denom > 0 else 0.0
            dist_km = centroid.distance(track["last_centroid"]) / 1000.0

            if overlap_frac >= min_overlap_frac or dist_km <= max_link_km:
                # Weighted score: prioritize overlap, then closeness.
                score = overlap_frac * 1000.0 - dist_km
                if score > best_score:
                    best_score = score
                    best_fire_id = fire_id

        if best_fire_id is None:
            best_fire_id = next_fire_id
            next_fire_id += 1
            tracks[best_fire_id] = {
                "last_time": t,
                "last_geom": geom,
                "last_centroid": centroid,
                "overpass_count": 1,
                "max_area_km2": float(row.area_km2),
                "total_frp_sum": float(row.frp_sum),
                "first_seen": t,
            }
        else:
            track = tracks[best_fire_id]
            track["last_time"] = t
            track["last_geom"] = geom
            track["last_centroid"] = centroid
            track["overpass_count"] += 1
            track["max_area_km2"] = max(track["max_area_km2"], float(row.area_km2))
            track["total_frp_sum"] += float(row.frp_sum)

        assigned_fire_ids.append(best_fire_id)

    work_proj["fire_id"] = assigned_fire_ids

    summary = (
        work_proj.groupby("fire_id", as_index=False)
        .agg(
            first_seen=("overpass_time", "min"),
            last_seen=("overpass_time", "max"),
            overpass_count=("fire_id", "size"),
            max_area_km2=("area_km2", "max"),
            total_frp_sum=("frp_sum", "sum"),
        )
        .sort_values("fire_id")
        .reset_index(drop=True)
    )

    keep_fire_ids = set(
        summary.loc[
            (summary["max_area_km2"] >= min_fire_area_km2)
            & (summary["overpass_count"] >= min_overpasses),
            "fire_id",
        ].tolist()
    )

    filtered = work_proj[work_proj["fire_id"].isin(keep_fire_ids)].copy()
    filtered = filtered.drop(columns=["centroid"])
    filtered = filtered.to_crs(epsg=4326)
    filtered["overpass_time"] = pd.to_datetime(filtered["overpass_time"], utc=True)
    filtered = filtered.sort_values(["fire_id", "overpass_time", "satellite", "cluster_id"]).reset_index(drop=True)

    summary = summary[summary["fire_id"].isin(keep_fire_ids)].reset_index(drop=True)
    return filtered, summary


def write_output(gdf: gpd.GeoDataFrame, output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    output = gdf.copy()

    # Fiona-backed writers do not support pandas StringDtype columns.
    for col in output.columns:
        if col == output.geometry.name:
            continue
        if pd.api.types.is_datetime64_any_dtype(output[col].dtype):
            output[col] = (
                pd.to_datetime(output[col], utc=True)
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                .astype("object")
            )
        if pd.api.types.is_string_dtype(output[col].dtype):
            output[col] = output[col].astype("object")

    suffix = path.suffix.lower()
    if suffix == ".fgb":
        output.to_file(path, driver="FlatGeobuf")
    elif suffix in {".json", ".geojson"}:
        output.to_file(path, driver="GeoJSON")
    elif suffix == ".parquet":
        output.to_parquet(path)
    else:
        raise ValueError("Unsupported output extension. Use .fgb, .geojson, or .parquet.")


def _index_name(table_name: str, suffix: str) -> str:
    safe_name = "".join(ch if ch.isalnum() else "_" for ch in table_name)
    return f"{safe_name[:48]}_{suffix}"


def write_to_postgis(
    gdf: gpd.GeoDataFrame,
    db_url: str,
    table_name: str,
    replace: bool,
) -> int:
    table_ident = sql.Identifier(table_name)
    unique_idx_ident = sql.Identifier(_index_name(table_name, "unique_event_idx"))
    fire_time_idx_ident = sql.Identifier(_index_name(table_name, "fire_time_idx"))
    time_idx_ident = sql.Identifier(_index_name(table_name, "time_idx"))
    geom_idx_ident = sql.Identifier(_index_name(table_name, "geom_idx"))

    create_table_stmt = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
          id BIGSERIAL PRIMARY KEY,
          fire_id BIGINT NOT NULL,
          overpass_time TIMESTAMPTZ NOT NULL,
          satellite TEXT,
          cluster_id INTEGER,
          pixel_count INTEGER,
          frp_sum DOUBLE PRECISION,
          area_km2 DOUBLE PRECISION,
          geom geometry(Geometry, 4326) NOT NULL
        )
        """
    ).format(table_ident)

    create_unique_stmt = sql.SQL(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (fire_id, overpass_time, satellite, cluster_id)"
    ).format(unique_idx_ident, table_ident)
    create_fire_time_stmt = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {} ON {} (fire_id, overpass_time DESC)"
    ).format(fire_time_idx_ident, table_ident)
    create_time_stmt = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {} ON {} (overpass_time DESC)"
    ).format(time_idx_ident, table_ident)
    create_geom_stmt = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (geom)"
    ).format(geom_idx_ident, table_ident)

    truncate_stmt = sql.SQL("TRUNCATE TABLE {}").format(table_ident)
    insert_stmt = sql.SQL(
        """
        INSERT INTO {} (
          fire_id,
          overpass_time,
          satellite,
          cluster_id,
          pixel_count,
          frp_sum,
          area_km2,
          geom
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326)
        )
        ON CONFLICT (fire_id, overpass_time, satellite, cluster_id)
        DO UPDATE SET
          pixel_count = EXCLUDED.pixel_count,
          frp_sum = EXCLUDED.frp_sum,
          area_km2 = EXCLUDED.area_km2,
          geom = EXCLUDED.geom
        """
    ).format(table_ident)

    records = []
    for row in gdf.itertuples(index=False):
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue
        records.append(
            (
                int(row.fire_id),
                pd.to_datetime(row.overpass_time, utc=True).to_pydatetime(),
                None if pd.isna(row.satellite) else str(row.satellite),
                int(row.cluster_id),
                int(row.pixel_count),
                float(row.frp_sum),
                float(row.area_km2),
                geom.wkt,
            )
        )

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_stmt)
            cur.execute(create_unique_stmt)
            cur.execute(create_fire_time_stmt)
            cur.execute(create_time_stmt)
            cur.execute(create_geom_stmt)

            if replace:
                cur.execute(truncate_stmt)

            batch_size = 1000
            for idx in range(0, len(records), batch_size):
                cur.executemany(insert_stmt, records[idx : idx + batch_size])

    return len(records)


def main() -> None:
    args = parse_args()

    start_day = datetime.strptime(args.start, "%Y-%m-%d")
    end_day = datetime.strptime(args.end, "%Y-%m-%d")
    end_exclusive = end_day + timedelta(days=1)

    detections = fetch_detections(args.db_url, start_day, end_exclusive)
    detections["satellite"] = detections["satellite"].map(normalize_satellite)

    if args.satellites.strip():
        wanted = {normalize_satellite(v) for v in args.satellites.split(",") if v.strip()}
        detections = detections[detections["satellite"].isin(wanted)]

    pixels = prepare_pixels(detections)
    polygons = build_overpass_polygons(pixels, args.cluster_km, args.min_pixels)
    linked_polygons, fire_summary = assign_unique_fire_ids(
        polygons=polygons,
        max_link_hours=args.max_link_hours,
        max_link_km=args.max_link_km,
        min_overlap_frac=args.min_overlap_frac,
        min_fire_area_km2=args.min_fire_area_km2,
        min_overpasses=args.min_overpasses,
    )
    write_output(linked_polygons, args.output)

    db_rows_written = 0
    if args.write_db:
        db_rows_written = write_to_postgis(
            gdf=linked_polygons,
            db_url=args.db_url,
            table_name=args.db_table,
            replace=args.replace_db,
        )
        print(f"db_table={args.db_table}")
        print(f"db_rows_written={db_rows_written}")

    if args.summary_output.strip():
        summary_path = Path(args.summary_output)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        fire_summary.to_csv(summary_path, index=False)
        print(f"summary_file={summary_path.resolve()}")

    print(f"input_pixels={len(pixels)}")
    print(f"raw_overpass_polygons={len(polygons)}")
    print(f"output_polygons={len(linked_polygons)}")
    print(f"unique_fires={fire_summary['fire_id'].nunique() if not fire_summary.empty else 0}")
    print(f"output_file={Path(args.output).resolve()}")


if __name__ == "__main__":
    main()
