#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INPUT_FILE="${SCRIPT_DIR}/tunisia-latest.osm.pbf"
OUTPUT_FILE="${SCRIPT_DIR}/charging_stations.osm.pbf"

if [ ! -f "${INPUT_FILE}" ]; then
    echo "ERROR: Input file not found: ${INPUT_FILE}"
    echo "Run download.sh first to fetch the Tunisia OSM extract."
    exit 1
fi

echo "Filtering OSM data for amenity=charging_station..."
echo "  Input: ${INPUT_FILE}"
echo "  Output: ${OUTPUT_FILE}"

osmium tags-filter "${INPUT_FILE}" amenity=charging_station -o "${OUTPUT_FILE}"

echo ""
echo "Filter complete: $(ls -lh "${OUTPUT_FILE}" | awk '{print $5}')"
