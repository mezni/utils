#!/usr/bin/env bash
set -euo pipefail

echo "Checking OSM import prerequisites..."

PREREQS_MET=true

check_tool() {
    local tool="$1"
    local pkg="$2"
    if ! which "$tool" &>/dev/null; then
        echo "  MISSING: $tool (install with: apt-get install -y $pkg)"
        PREREQS_MET=false
    else
        echo "  FOUND: $tool"
    fi
}

check_tool "osmium" "osmium-tool"
check_tool "ogr2ogr" "gdal-bin"
check_tool "curl" "curl"

if [ "$PREREQS_MET" = false ]; then
    echo ""
    echo "Some prerequisites are missing. Install them with:"
    echo "  sudo apt-get update && sudo apt-get install -y osmium-tool gdal-bin curl"
    exit 1
fi

echo ""
echo "All prerequisites satisfied."
