#!/usr/bin/env python3
"""
OSM Charging Station Importer for BorneMap.

Pipeline:
  1. Download Tunisia OSM extract (or use provided file)
  2. Parse PBF, filter amenity=charging_station
  3. Insert into gis.osm_charging_stations_temp (staging)
  4. Transform staging -> gis.osm_charging_stations (curated)
  5. Idempotent: skips duplicate osm_id in curated
"""

import json
import os
import sys
import uuid
import hashlib
import base64
from datetime import datetime, timezone

import osmium
import psycopg2
import requests


DB_CONFIG = {
    "host": os.environ.get("PGHOST", "postgres"),
    "port": int(os.environ.get("PGPORT", 5432)),
    "dbname": os.environ.get("PGDATABASE", "bornemap"),
    "user": os.environ.get("PGUSER", "bornemap"),
    "password": os.environ.get("PGPASSWORD", "bornemap"),
}

OSM_URL = os.environ.get(
    "OSM_URL",
    "https://download.geofabrik.de/africa/tunisia-latest.osm.pbf",
)
OSM_FILE = os.environ.get("OSM_FILE", "/data/tunisia-latest.osm.pbf")


def generate_sta_id() -> str:
    """Generate STA-nanoid(12) identifier."""
    raw = uuid.uuid4().bytes
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    nanoid = ""
    for i in range(12):
        nanoid += alphabet[raw[i] % len(alphabet)]
    return f"STA-{nanoid}"


def download_osm(url: str, dest: str) -> str:
    """Download OSM PBF file if not already present."""
    if os.path.exists(dest):
        print(f"File already exists: {dest}")
        return dest
    print(f"Downloading {url} -> {dest} ...")
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    resp = requests.get(url, stream=True, timeout=3600)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {dest}")
    return dest


class ChargingStationHandler(osmium.SimpleHandler):
    """Extract nodes and ways with amenity=charging_station."""

    def __init__(self):
        super().__init__()
        self.stations = []

    def node(self, n):
        tags = dict(n.tags)
        if tags.get("amenity") == "charging_station":
            self.stations.append({
                "osm_id": str(n.id),
                "name": tags.get("name"),
                "lat": n.location.lat,
                "lon": n.location.lon,
                "tags": tags,
            })

    def way(self, w):
        tags = dict(w.tags)
        if tags.get("amenity") == "charging_station":
            centroid = self._way_centroid(w)
            if centroid:
                self.stations.append({
                    "osm_id": str(w.id),
                    "name": tags.get("name"),
                    "lat": centroid[0],
                    "lon": centroid[1],
                    "tags": tags,
                })

    @staticmethod
    def _way_centroid(w):
        """Compute centroid from way nodes (simplified)."""
        if len(w.nodes) == 0:
            return None
        lat_sum = 0.0
        lon_sum = 0.0
        count = 0
        for n in w.nodes:
            if n.location.valid():
                lat_sum += n.location.lat
                lon_sum += n.location.lon
                count += 1
        if count == 0:
            return None
        return (lat_sum / count, lon_sum / count)


def get_db_conn():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def insert_staging(conn, stations):
    """Batch insert stations into staging table."""
    now = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        for s in stations:
            cur.execute(
                """
                INSERT INTO gis.osm_charging_stations_temp
                    (osm_id, name, lat, lon, tags, imported_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (s["osm_id"], s["name"], s["lat"], s["lon"],
                 json.dumps(s["tags"], default=str), now),
            )
    conn.commit()
    print(f"Inserted {len(stations)} rows into staging")


def transform_staging_to_curated(conn):
    """Transform staging rows into curated table with dedup and validation."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (st.osm_id)
                st.osm_id,
                st.name,
                st.lat,
                st.lon,
                st.tags->>'operator' AS operator,
                st.imported_at
            FROM gis.osm_charging_stations_temp st
            WHERE st.lat IS NOT NULL
              AND st.lon IS NOT NULL
              AND ABS(st.lat) <= 90
              AND ABS(st.lon) <= 180
              AND NOT EXISTS (
                  SELECT 1 FROM gis.osm_charging_stations cs
                  WHERE cs.osm_id = st.osm_id
              )
            ORDER BY st.osm_id, st.imported_at DESC
            """
        )
        rows = cur.fetchall()

        insert_count = 0
        for row in rows:
            osm_id, name, lat, lon, operator, imported_at = row
            station_id = generate_sta_id()
            cur.execute(
                """
                INSERT INTO gis.osm_charging_stations
                    (station_id, osm_id, name, lat, lon, operator, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (osm_id) DO NOTHING
                """,
                (station_id, osm_id, name, lat, lon,
                 operator if operator else None, imported_at),
            )
            if cur.rowcount > 0:
                insert_count += 1

    conn.commit()
    print(f"Transformed {insert_count} new stations into curated table")


def main():
    print("STEP 1: Acquire OSM data")
    osm_file = download_osm(OSM_URL, OSM_FILE)

    print("STEP 2: Parse OSM data for charging stations")
    handler = ChargingStationHandler()
    handler.apply_file(osm_file)
    stations = handler.stations
    print(f"Found {len(stations)} charging stations")

    if not stations:
        print("No charging stations found. Exiting.")
        sys.exit(0)

    print("STEP 3: Connect to database")
    conn = get_db_conn()

    try:
        print("STEP 4: Insert into staging table")
        insert_staging(conn, stations)

        print("STEP 5: Transform staging -> curated")
        transform_staging_to_curated(conn)

        print("ETL pipeline completed successfully")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
