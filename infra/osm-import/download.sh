#!/usr/bin/env bash
set -euo pipefail

DOWNLOAD_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_FILE="${DOWNLOAD_DIR}/tunisia-latest.osm.pbf"
OSM_URL="https://download.geofabrik.de/africa/tunisia-latest.osm.pbf"

echo "Downloading Tunisia OSM extract from Geofabrik..."
echo "  URL: ${OSM_URL}"
echo "  Output: ${OUTPUT_FILE}"

curl -L --progress-bar -o "${OUTPUT_FILE}" "${OSM_URL}"

echo ""
echo "Download complete: $(ls -lh "${OUTPUT_FILE}" | awk '{print $5}')"
