#!/usr/bin/env python3
"""Build raw and display fire perimeters from fire_detections using FireAtlas clustering."""

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
from shapely.geometry import GeometryCollection
from shapely.geometry import MultiPolygon
from shapely.geometry import Polygon
from shapely.ops import unary_union

try:
    from shapely import make_valid
except ImportError:  # pragma: no cover
    from shapely.validation import make_valid

from fireatlas import FireIO
from fireatlas import settings
from fireatlas.FireClustering import do_clustering
from fireatlas.FireVector import cal_hull


PALETTE = [
    "#fde68a",
    "#fdba74",
    "#fb923c",
    "#f97316",
    "#ea580c",
    "#c2410c",
    "#7c2d12",
]

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
        description="Generate raw and polished fire perimeters using FireAtlas clustering and hull logic.",
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL", "postgres://wildfire:wildfire@localhost:5432/wildfire"),
        help="Postgres connection URL (default from DATABASE_URL env).",
    )
    parser.add_argument("--start", required=True, help="Start day (UTC), format YYYY-MM-DD.")
    parser.add_argument("--end", required=True, help="End day (UTC), format YYYY-MM-DD.")
    parser.add_argument(
        "--source-type",
        choices=["archive", "nrt", "all"],
        default="archive",
        help="Detection source filter (default: archive).",
    )
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
        help="Minimum pixel count per raw polygon (default: 2).",
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
        "--close-buffer-m",
        type=float,
        default=40.0,
        help="Morphological close buffer in meters for display geometry cleanup (default: 40).",
    )
    parser.add_argument(
        "--simplify-m",
        type=float,
        default=30.0,
        help="Topology-preserving simplify tolerance in meters (default: 30).",
    )
    parser.add_argument(
        "--min-part-area-km2",
        type=float,
        default=0.02,
        help="Minimum polygon part area to keep in display geometry (default: 0.02).",
    )
    parser.add_argument(
        "--flicker-centroid-m",
        type=float,
        default=150.0,
        help="Centroid shift threshold for flicker suppression in meters (default: 150).",
    )
    parser.add_argument(
        "--flicker-jaccard",
        type=float,
        default=0.985,
        help="Jaccard overlap threshold for flicker suppression (default: 0.985).",
    )
    parser.add_argument(
        "--raw-output",
        default="data/derived/perimeters_raw.fgb",
        help="Raw output path (.fgb, .geojson, or .parquet).",
    )
    parser.add_argument(
        "--display-output",
        default="data/derived/perimeters_display.fgb",
        help="Display output path (.fgb, .geojson, or .parquet).",
    )
    parser.add_argument(
        "--summary-output",
        default="",
        help="Optional output path for unique fire summary table (.csv).",
    )
    parser.add_argument(
        "--write-db",
        action="store_true",
        help="Write resulting perimeters into PostGIS tables.",
    )
    parser.add_argument(
        "--replace-db",
        action="store_true",
        help="Truncate target perimeter tables before insert (requires --write-db).",
    )
    parser.add_argument(
        "--raw-db-table",
        default="fire_perimeters_raw",
        help="Target table for raw perimeters (default: fire_perimeters_raw).",
    )
    parser.add_argument(
        "--display-db-table",
        default="fire_perimeters_display",
        help="Target table for display perimeters (default: fire_perimeters_display).",
    )
    return parser.parse_args()


def normalize_satellite(value: str) -> str:
    sat = str(value).strip().upper()
    return SATELLITE_ALIAS.get(sat, sat)


def _index_name(table_name: str, suffix: str) -> str:
    safe_name = "".join(ch if ch.isalnum() else "_" for ch in table_name)
    return f"{safe_name[:48]}_{suffix}"


def fetch_detections(
    db_url: str,
    start_dt: datetime,
    end_exclusive_dt: datetime,
    source_type: str,
) -> pd.DataFrame:
    where = [
        "detected_at >= %(start)s",
        "detected_at < %(end)s",
        "latitude IS NOT NULL",
        "longitude IS NOT NULL",
    ]
    params: dict[str, Any] = {"start": start_dt, "end": end_exclusive_dt}

    if source_type != "all":
        where.append("source_type = %(source_type)s")
        params["source_type"] = source_type

    query = f"""
        SELECT
          detected_at,
          satellite,
          latitude AS lat,
          longitude AS lon,
          COALESCE(frp, 0) AS frp,
          source_type
        FROM fire_detections
        WHERE {' AND '.join(where)}
        ORDER BY detected_at
    """

    with psycopg.connect(db_url) as conn:
        return pd.read_sql_query(query, conn, params=params)


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
    rows: list[dict[str, Any]] = []

    if gdf.empty:
        return gpd.GeoDataFrame(
            columns=["overpass_time", "satellite", "cluster_id", "pixel_count", "frp_sum", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

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
        return gpd.GeoDataFrame(
            columns=["overpass_time", "satellite", "cluster_id", "pixel_count", "frp_sum", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

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
            }
        else:
            track = tracks[best_fire_id]
            track["last_time"] = t
            track["last_geom"] = geom
            track["last_centroid"] = centroid

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


def _geometry_parts(geom: Any) -> list[Polygon]:
    if geom is None or geom.is_empty:
        return []
    if isinstance(geom, Polygon):
        return [geom]
    if isinstance(geom, MultiPolygon):
        return list(geom.geoms)
    if isinstance(geom, GeometryCollection):
        parts: list[Polygon] = []
        for item in geom.geoms:
            parts.extend(_geometry_parts(item))
        return parts
    return []


def _to_multipolygon(geom: Any) -> MultiPolygon:
    parts = _geometry_parts(geom)
    if not parts:
        return MultiPolygon([])
    return MultiPolygon(parts)


def _clean_display_geometry(
    geom: Any,
    close_buffer_m: float,
    simplify_m: float,
    min_part_area_m2: float,
) -> MultiPolygon:
    if geom is None or geom.is_empty:
        return MultiPolygon([])

    cleaned = make_valid(geom)
    if cleaned.is_empty:
        return MultiPolygon([])

    cleaned = cleaned.buffer(close_buffer_m).buffer(-close_buffer_m)
    cleaned = make_valid(cleaned)
    if cleaned.is_empty:
        return MultiPolygon([])

    cleaned = cleaned.simplify(simplify_m, preserve_topology=True)
    cleaned = make_valid(cleaned)

    parts = [part for part in _geometry_parts(cleaned) if part.area >= min_part_area_m2]
    if not parts:
        return MultiPolygon([])

    return MultiPolygon(parts)


def _is_flicker_step(
    previous_geom: MultiPolygon,
    candidate_geom: MultiPolygon,
    centroid_threshold_m: float,
    jaccard_threshold: float,
) -> bool:
    if previous_geom.is_empty or candidate_geom.is_empty:
        return False

    centroid_shift = previous_geom.centroid.distance(candidate_geom.centroid)
    if centroid_shift >= centroid_threshold_m:
        return False

    union_geom = previous_geom.union(candidate_geom)
    if union_geom.is_empty:
        return False

    jaccard = previous_geom.intersection(candidate_geom).area / union_geom.area
    return jaccard >= jaccard_threshold


def _age_color(age_norm: float) -> str:
    idx = int(round(age_norm * (len(PALETTE) - 1)))
    idx = max(0, min(len(PALETTE) - 1, idx))
    return PALETTE[idx]


def _series_to_crs(geoms: list[Any], from_epsg: int, to_epsg: int) -> gpd.GeoSeries:
    return gpd.GeoSeries(geoms, crs=f"EPSG:{from_epsg}").to_crs(epsg=to_epsg)


def build_display_perimeters(
    raw_perimeters: gpd.GeoDataFrame,
    close_buffer_m: float,
    simplify_m: float,
    min_part_area_km2: float,
    flicker_centroid_m: float,
    flicker_jaccard: float,
) -> gpd.GeoDataFrame:
    if raw_perimeters.empty:
        return gpd.GeoDataFrame(
            columns=[
                "fire_id",
                "step_time",
                "step_index",
                "step_count",
                "age_norm",
                "pixel_count",
                "frp_sum",
                "area_step_km2",
                "area_cumulative_km2",
                "growth_km2",
                "satellites",
                "color_hex",
                "geom_step",
                "geom_cumulative",
                "geom_growth",
                "geometry",
            ],
            geometry="geometry",
            crs="EPSG:4326",
        )

    min_part_area_m2 = min_part_area_km2 * 1_000_000.0

    raw = raw_perimeters.copy()
    raw["overpass_time"] = pd.to_datetime(raw["overpass_time"], utc=True)
    raw_proj = raw.to_crs(epsg=settings.EPSG_CODE)

    dissolved_rows: list[dict[str, Any]] = []
    grouped = raw_proj.groupby(["fire_id", "overpass_time"], sort=True)
    for (fire_id, step_time), group in grouped:
        union_geom = unary_union(group.geometry.tolist())
        clean_geom = _clean_display_geometry(
            union_geom,
            close_buffer_m=close_buffer_m,
            simplify_m=simplify_m,
            min_part_area_m2=min_part_area_m2,
        )
        if clean_geom.is_empty:
            continue

        satellites = sorted({str(v) for v in group["satellite"].dropna().tolist() if str(v).strip()})

        dissolved_rows.append(
            {
                "fire_id": int(fire_id),
                "step_time": pd.to_datetime(step_time, utc=True),
                "pixel_count": int(group["pixel_count"].sum()),
                "frp_sum": float(group["frp_sum"].sum()),
                "satellites_set": set(satellites),
                "geom_step": _to_multipolygon(clean_geom),
            }
        )

    if not dissolved_rows:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326")

    dissolved_rows.sort(key=lambda row: (row["fire_id"], row["step_time"]))

    filtered_rows: list[dict[str, Any]] = []
    by_fire: dict[int, list[dict[str, Any]]] = {}
    for row in dissolved_rows:
        by_fire.setdefault(row["fire_id"], []).append(row)

    for fire_id, rows in by_fire.items():
        reduced: list[dict[str, Any]] = []
        for row in rows:
            if reduced and _is_flicker_step(
                previous_geom=reduced[-1]["geom_step"],
                candidate_geom=row["geom_step"],
                centroid_threshold_m=flicker_centroid_m,
                jaccard_threshold=flicker_jaccard,
            ):
                reduced[-1]["pixel_count"] += row["pixel_count"]
                reduced[-1]["frp_sum"] += row["frp_sum"]
                reduced[-1]["satellites_set"] |= row["satellites_set"]
                continue
            reduced.append(row)

        cumulative_geom = MultiPolygon([])
        step_count = len(reduced)

        for idx, row in enumerate(reduced, start=1):
            step_geom = _to_multipolygon(row["geom_step"])
            age_norm = 1.0 if step_count <= 1 else (idx - 1) / (step_count - 1)

            cumulative_geom = _to_multipolygon(unary_union([cumulative_geom, step_geom]))
            if idx == 1:
                growth_geom = step_geom
            else:
                growth_geom = _to_multipolygon(make_valid(cumulative_geom.difference(filtered_rows[-1]["geom_cumulative"])))

            filtered_rows.append(
                {
                    "fire_id": fire_id,
                    "step_time": row["step_time"],
                    "step_index": idx,
                    "step_count": step_count,
                    "age_norm": float(age_norm),
                    "pixel_count": int(row["pixel_count"]),
                    "frp_sum": float(row["frp_sum"]),
                    "area_step_km2": float(step_geom.area / 1_000_000.0),
                    "area_cumulative_km2": float(cumulative_geom.area / 1_000_000.0),
                    "growth_km2": float(growth_geom.area / 1_000_000.0),
                    "satellites": ",".join(sorted(row["satellites_set"])),
                    "color_hex": _age_color(age_norm),
                    "geom_step": step_geom,
                    "geom_cumulative": cumulative_geom,
                    "geom_growth": growth_geom,
                }
            )

    display = pd.DataFrame(filtered_rows).sort_values(["fire_id", "step_time", "step_index"]).reset_index(drop=True)

    for col in ["geom_step", "geom_cumulative", "geom_growth"]:
        display[col] = _series_to_crs(display[col].tolist(), settings.EPSG_CODE, 4326)

    return gpd.GeoDataFrame(display, geometry="geom_cumulative", crs="EPSG:4326")


def write_output(gdf: gpd.GeoDataFrame, output_path: str) -> None:
    if not output_path.strip():
        return

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    output = gdf.copy()

    geom_col = output.geometry.name
    for col in output.columns:
        if col == geom_col:
            continue
        if isinstance(output[col].dtype, gpd.array.GeometryDtype):
            output[col] = output[col].apply(lambda geom: None if geom is None else geom.wkt)
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


def write_raw_to_postgis(
    gdf: gpd.GeoDataFrame,
    db_url: str,
    table_name: str,
    replace: bool,
) -> int:
    table_ident = sql.Identifier(table_name)

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

    idx_unique = sql.Identifier(_index_name(table_name, "unique_event_idx"))
    idx_fire_time = sql.Identifier(_index_name(table_name, "fire_time_idx"))
    idx_time = sql.Identifier(_index_name(table_name, "time_idx"))
    idx_geom = sql.Identifier(_index_name(table_name, "geom_idx"))

    create_unique_stmt = sql.SQL(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} (fire_id, overpass_time, satellite, cluster_id)"
    ).format(idx_unique, table_ident)
    create_fire_time_stmt = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {} ON {} (fire_id, overpass_time DESC)"
    ).format(idx_fire_time, table_ident)
    create_time_stmt = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {} ON {} (overpass_time DESC)"
    ).format(idx_time, table_ident)
    create_geom_stmt = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (geom)").format(
        idx_geom,
        table_ident,
    )

    truncate_stmt = sql.SQL("TRUNCATE TABLE {}") .format(table_ident)

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

    records: list[tuple[Any, ...]] = []
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

            for idx in range(0, len(records), 1000):
                cur.executemany(insert_stmt, records[idx : idx + 1000])

    return len(records)


def write_display_to_postgis(
    gdf: gpd.GeoDataFrame,
    db_url: str,
    table_name: str,
    replace: bool,
) -> int:
    table_ident = sql.Identifier(table_name)

    create_table_stmt = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
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
        )
        """
    ).format(table_ident)

    idx_fire_step = sql.Identifier(_index_name(table_name, "fire_step_idx"))
    idx_time = sql.Identifier(_index_name(table_name, "time_idx"))
    idx_geom = sql.Identifier(_index_name(table_name, "geom_idx"))

    create_fire_step_stmt = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {} ON {} (fire_id, step_index)"
    ).format(idx_fire_step, table_ident)
    create_time_stmt = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} (step_time DESC)").format(
        idx_time,
        table_ident,
    )
    create_geom_stmt = sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} USING GIST (geom_cumulative)").format(
        idx_geom,
        table_ident,
    )

    truncate_stmt = sql.SQL("TRUNCATE TABLE {}") .format(table_ident)

    insert_stmt = sql.SQL(
        """
        INSERT INTO {} (
          fire_id,
          step_time,
          step_index,
          step_count,
          age_norm,
          pixel_count,
          frp_sum,
          area_step_km2,
          area_cumulative_km2,
          growth_km2,
          satellites,
          color_hex,
          geom_step,
          geom_cumulative,
          geom_growth
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
          ST_Multi(ST_GeomFromText(%s, 4326)),
          ST_Multi(ST_GeomFromText(%s, 4326)),
          ST_Multi(ST_GeomFromText(%s, 4326))
        )
        ON CONFLICT (fire_id, step_time)
        DO UPDATE SET
          step_index = EXCLUDED.step_index,
          step_count = EXCLUDED.step_count,
          age_norm = EXCLUDED.age_norm,
          pixel_count = EXCLUDED.pixel_count,
          frp_sum = EXCLUDED.frp_sum,
          area_step_km2 = EXCLUDED.area_step_km2,
          area_cumulative_km2 = EXCLUDED.area_cumulative_km2,
          growth_km2 = EXCLUDED.growth_km2,
          satellites = EXCLUDED.satellites,
          color_hex = EXCLUDED.color_hex,
          geom_step = EXCLUDED.geom_step,
          geom_cumulative = EXCLUDED.geom_cumulative,
          geom_growth = EXCLUDED.geom_growth
        """
    ).format(table_ident)

    records: list[tuple[Any, ...]] = []
    for row in gdf.itertuples(index=False):
        if row.geom_step is None or row.geom_step.is_empty:
            continue
        records.append(
            (
                int(row.fire_id),
                pd.to_datetime(row.step_time, utc=True).to_pydatetime(),
                int(row.step_index),
                int(row.step_count),
                float(row.age_norm),
                int(row.pixel_count),
                float(row.frp_sum),
                float(row.area_step_km2),
                float(row.area_cumulative_km2),
                float(row.growth_km2),
                None if pd.isna(row.satellites) else str(row.satellites),
                str(row.color_hex),
                row.geom_step.wkt,
                row.geom_cumulative.wkt,
                row.geom_growth.wkt,
            )
        )

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_stmt)
            cur.execute(create_fire_step_stmt)
            cur.execute(create_time_stmt)
            cur.execute(create_geom_stmt)

            if replace:
                cur.execute(truncate_stmt)

            for idx in range(0, len(records), 1000):
                cur.executemany(insert_stmt, records[idx : idx + 1000])

    return len(records)


def main() -> None:
    args = parse_args()

    start_day = datetime.strptime(args.start, "%Y-%m-%d")
    end_day = datetime.strptime(args.end, "%Y-%m-%d")
    end_exclusive = end_day + timedelta(days=1)

    detections = fetch_detections(args.db_url, start_day, end_exclusive, args.source_type)
    detections["satellite"] = detections["satellite"].map(normalize_satellite)

    if args.satellites.strip():
        wanted = {normalize_satellite(v) for v in args.satellites.split(",") if v.strip()}
        detections = detections[detections["satellite"].isin(wanted)]

    pixels = prepare_pixels(detections)
    raw_polygons = build_overpass_polygons(pixels, args.cluster_km, args.min_pixels)
    linked_raw, fire_summary = assign_unique_fire_ids(
        polygons=raw_polygons,
        max_link_hours=args.max_link_hours,
        max_link_km=args.max_link_km,
        min_overlap_frac=args.min_overlap_frac,
        min_fire_area_km2=args.min_fire_area_km2,
        min_overpasses=args.min_overpasses,
    )

    display_perimeters = build_display_perimeters(
        raw_perimeters=linked_raw,
        close_buffer_m=args.close_buffer_m,
        simplify_m=args.simplify_m,
        min_part_area_km2=args.min_part_area_km2,
        flicker_centroid_m=args.flicker_centroid_m,
        flicker_jaccard=args.flicker_jaccard,
    )

    write_output(linked_raw, args.raw_output)
    write_output(display_perimeters, args.display_output)

    raw_rows_written = 0
    display_rows_written = 0
    if args.write_db:
        raw_rows_written = write_raw_to_postgis(
            gdf=linked_raw,
            db_url=args.db_url,
            table_name=args.raw_db_table,
            replace=args.replace_db,
        )
        display_rows_written = write_display_to_postgis(
            gdf=display_perimeters,
            db_url=args.db_url,
            table_name=args.display_db_table,
            replace=args.replace_db,
        )
        print(f"raw_db_table={args.raw_db_table}")
        print(f"raw_db_rows_written={raw_rows_written}")
        print(f"display_db_table={args.display_db_table}")
        print(f"display_db_rows_written={display_rows_written}")

    if args.summary_output.strip():
        summary_path = Path(args.summary_output)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        fire_summary.to_csv(summary_path, index=False)
        print(f"summary_file={summary_path.resolve()}")

    print(f"source_type={args.source_type}")
    print(f"input_pixels={len(pixels)}")
    print(f"raw_overpass_polygons={len(raw_polygons)}")
    print(f"raw_output_polygons={len(linked_raw)}")
    print(f"display_steps={len(display_perimeters)}")
    print(f"unique_fires={fire_summary['fire_id'].nunique() if not fire_summary.empty else 0}")
    print(f"raw_output_file={Path(args.raw_output).resolve()}")
    print(f"display_output_file={Path(args.display_output).resolve()}")


if __name__ == "__main__":
    main()
