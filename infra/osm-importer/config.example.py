"""
OSM Importer Configuration

Copy this file to config.py and update with your actual values.
"""

# Database URL
DATABASE_URL = "postgresql://bornemap:bornemap_dev@platform_db:5432/platform_db"

# Region to import
REGION = "tunisia"

# Bounding box (optional, will use Tunisia default if not specified)
# Format: min_lon,min_lat,max_lon,max_lat
# TUNISIA_BBOX = "7.5,30.0,11.6,37.5"

# OSM API settings
OVERPASS_URL = "http://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT = 25

# Import settings
MAX_RETRY_ATTEMPTS = 3
MAX_STATIONS_TO_IMPORT = 1000

# Logging
LOG_LEVEL = "INFO"
