"""
OSM PBF parser for EV charging stations.

Extracts nodes with amenity=charging_station or charging=yes from the
Tunisia OSM dataset, inserts into staging, and transforms to curated.
"""
import argparse
import json
import os
import subprocess
import sys

import psycopg2
from nanoid import generate

NANOID_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"
NANOID_LENGTH = 12
STATION_ID_PREFIX = "STA"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import OSM EV charging stations into platform_db"
    )
    parser.add_argument("--pbf", required=True, help="Path to OSM PBF file")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", default="5432")
    parser.add_argument("--db-name", required=True)
    parser.add_argument("--db-user", required=True)
    parser.add_argument("--db-password", required=True)
    return parser.parse_args()


def extract_ev_stations(pbf_path: str) -> list[dict]:
    """Use osmium to filter and export EV charging station nodes."""
    filtered_path = "/tmp/ev_filtered.osm"

    filter_result = subprocess.run(
        [
            "osmium", "tags-filter",
            pbf_path,
            "amenity=charging_station",
            "charging=yes",
            "-o", filtered_path,
            "--overwrite",
        ],
        capture_output=True, text=True,
    )

    if filter_result.returncode != 0:
        print(f"Error filtering OSM data: {filter_result.stderr}", file=sys.stderr)
        return []

    geojson_result = subprocess.run(
        [
            "osmium", "export",
            "--output-format", "geojson",
            "--overwrite",
            "-o", "/tmp/ev_stations.geojson",
            filtered_path,
        ],
        capture_output=True, text=True,
    )

    if geojson_result.returncode != 0:
        print(f"Error exporting to GeoJSON: {geojson_result.stderr}", file=sys.stderr)
        return []

    with open("/tmp/ev_stations.geojson") as f:
        geojson = json.load(f)

    stations = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates", [None, None])

        osm_id = str(props.get("id", ""))
        if not osm_id:
            continue

        stations.append({
            "osm_id": osm_id,
            "name": props.get("name") or "",
            "lat": coords[1] if len(coords) > 1 else None,
            "lon": coords[0] if len(coords) > 0 else None,
            "tags": props,
            "operator": props.get("operator") or "",
        })

    return [s for s in stations if s["lat"] is not None and s["lon"] is not None]


def insert_staging(conn, stations: list[dict]):
    """Insert raw OSM records into staging table."""
    with conn.cursor() as cur:
        for s in stations:
            cur.execute(
                """
                INSERT INTO gis.osm_charging_stations_temp
                    (osm_id, name, lat, lon, tags)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (s["osm_id"], s["name"], s["lat"], s["lon"],
                 json.dumps(s["tags"])),
            )
    conn.commit()
    print(f"Inserted {len(stations)} records into staging")


def transform_to_curated(conn):
    """Transform staging records into curated table with STA-nanoid IDs."""
    with conn.cursor() as cur:
        cur.execute("SELECT osm_id FROM gis.osm_charging_stations_temp")
        existing = {row[0] for row in cur.fetchall()}

        cur.execute("SELECT osm_id FROM gis.osm_charging_stations")
        already_curated = {row[0] for row in cur.fetchall()}

    new_stations = existing - already_curated
    if not new_stations:
        print("No new stations to transform")
        return

    curated_count = 0
    with conn.cursor() as cur:
        for osm_id in new_stations:
            station_id = f"{STATION_ID_PREFIX}-{generate(NANOID_ALPHABET, NANOID_LENGTH)}"
            cur.execute(
                """
                INSERT INTO gis.osm_charging_stations
                    (station_id, osm_id, name, lat, lon, operator, verified)
                SELECT
                    %s, osm_id,
                    NULLIF(TRIM(name), ''),
                    lat, lon,
                    NULLIF(TRIM(operator), ''),
                    false
                FROM gis.osm_charging_stations_temp
                WHERE osm_id = %s
                ON CONFLICT (osm_id) DO NOTHING
                """,
                (station_id, osm_id),
            )
            if cur.rowcount > 0:
                curated_count += 1

    conn.commit()
    print(f"Transformed {curated_count} records to curated table")


def main():
    args = parse_args()

    conn = psycopg2.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    print("Extracting EV charging stations from OSM Tunisia dataset...")
    stations = extract_ev_stations(args.pbf)
    print(f"Found {len(stations)} charging stations")

    if not stations:
        print("No stations found. Exiting.")
        conn.close()
        return

    print("Inserting into staging table...")
    insert_staging(conn, stations)

    print("Transforming to curated table...")
    transform_to_curated(conn)

    conn.close()
    print("OSM import completed successfully.")


if __name__ == "__main__":
    main()
