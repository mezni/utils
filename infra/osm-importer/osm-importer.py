#!/usr/bin/env python3
"""
OSM Importer for BorneMap
Fetches charging station data from OpenStreetMap and stores it in the database.

Usage:
    python osm-importer.py
    python osm-importer.py --region tunisia
    python osm-importer.py --bbox 10.0,30.0,11.6,37.5
"""

import sys
import os
import json
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import argparse

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OSMImporter:
    """OSM data importer for charging stations"""

    # Tunisia bounding box
    TUNISIA_BBOX = {
        'min_lat': 30.0,
        'min_lon': 7.5,
        'max_lat': 37.5,
        'max_lon': 11.6,
    }

    def __init__(
        self,
        database_url: str,
        region: str = 'tunisia',
        bbox: Optional[Dict[str, float]] = None,
    ):
        """
        Initialize OSM importer

        Args:
            database_url: PostgreSQL database URL
            region: Region name for import
            bbox: Optional bounding box for import
        """
        self.database_url = database_url
        self.region = region
        self.bbox = bbox or self.TUNISIA_BBOX

        # Create database connection
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)

    async def fetch_osm_data(self) -> List[Dict[str, Any]]:
        """
        Fetch charging station data from OSM API

        Returns:
            List of station data from OSM
        """
        logger.info(f"Fetching OSM data for region: {self.region}")

        # Use Overpass API
        bbox_str = f"{self.bbox['min_lon']},{self.bbox['min_lat']},{self.bbox['max_lon']},{self.bbox['max_lat']}"
        query = f"""
        [out:json][timeout:25];
        (
            way["charging_station"~"."]["amenity"="charging_station"]
              ({bbox_str});
            node["charging_station"~"."]["amenity"="charging_station"]
              ({bbox_str});
        );
        out body;
        >;
        out skel qt;
        """

        url = "http://overpass-api.de/api/interpreter"
        params = {
            'data': query
        }

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        logger.error(f"OSM API returned status {response.status}")
                        return []

                    data = await response.json()
                    return self.parse_osm_data(data)
        except Exception as e:
            logger.error(f"Error fetching OSM data: {e}")
            return []

    def parse_osm_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse OSM response data

        Args:
            data: OSM API response data

        Returns:
            List of parsed station data
        """
        stations = []

        for element in data.get('elements', []):
            if element.get('type') not in ['way', 'node']:
                continue

            try:
                station = self.extract_station_from_element(element)
                if station:
                    stations.append(station)
            except Exception as e:
                logger.warning(f"Error parsing OSM element: {e}")
                continue

        logger.info(f"Parsed {len(stations)} stations from OSM")
        return stations

    def extract_station_from_element(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract station information from OSM element

        Args:
            element: OSM element (way or node)

        Returns:
            Dictionary with station information
        """
        tags = element.get('tags', {})

        # Extract basic information
        name = tags.get('name', 'Unknown Station')
        if not name:
            return None

        # Get location
        if element['type'] == 'node':
            lat = element['lat']
            lon = element['lon']
        else:
            # For ways, get center point
            if 'center' in element:
                lat = element['center']['lat']
                lon = element['center']['lon']
            else:
                return None

        # Extract additional information
        address = tags.get('addr:street', '')
        if not address:
            address = tags.get('addr:city', '') or tags.get('addr:place', '')
        address += f", {tags.get('addr:city', '')}"

        city = tags.get('addr:city', 'Unknown')
        visibility = self.determine_visibility(tags)
        status = 'active'  # Default status

        # Extract connector information
        connectors = []
        power = 0

        # Check for power information
        if 'maxwait' in tags:
            power = 11
            connectors.append('type2')
        if 'capacity' in tags:
            capacity = int(tags.get('capacity', 0))
            if capacity > 2:
                connectors.append('type2')

        return {
            'id': f'sta_{element.get("id", "")[:10]}',
            'name': name,
            'location': {
                'lat': lat,
                'lon': lon
            },
            'address': address,
            'city': city,
            'visibility': visibility,
            'status': status,
            'connector_types': connectors,
            'connector_power': [power],
            'raw_tags': tags,
        }

    def determine_visibility(self, tags: Dict[str, Any]) -> str:
        """
        Determine station visibility based on tags

        Args:
            tags: OSM tags

        Returns:
            Visibility type ('commercial' or 'private_home')
        """
        if tags.get('amenity') == 'charging_station' and tags.get('operator'):
            return 'commercial'

        # Check for private charging
        if tags.get('access') == 'private':
            return 'private_home'

        return 'commercial'

    async def import_to_database(self, stations: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Import OSM stations to database

        Args:
            stations: List of station data

        Returns:
            Statistics about import
        """
        logger.info(f"Importing {len(stations)} stations to database")

        stats = {
            'stations_imported': 0,
            'stations_updated': 0,
            'stations_failed': 0
        }

        session = self.Session()
        try:
            for station in stations:
                try:
                    # Check if station already exists
                    result = session.execute(
                        text('SELECT id FROM inventory.station WHERE id = :id'),
                        {'id': station['id']}
                    )
                    existing = result.fetchone()

                    if existing:
                        # Update existing station
                        station_query = text("""
                            UPDATE inventory.station
                            SET name = :name,
                                location = ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography(POINT, 4326),
                                address = :address,
                                city = :city,
                                visibility = :visibility,
                                status = :status
                            WHERE id = :id
                        """)
                        session.execute(station_query, {
                            **station,
                            'lat': station['location']['lat'],
                            'lon': station['location']['lon'],
                            'status': station.get('status', 'active'),
                        })
                        stats['stations_updated'] += 1
                    else:
                        # Insert new station
                        insert_query = text("""
                            INSERT INTO inventory.station
                            (id, name, location, address, city, visibility, status)
                            VALUES (:id, :name, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography(POINT, 4326), :address, :city, :visibility, :status)
                        """)
                        session.execute(insert_query, {
                            **station,
                            'lat': station['location']['lat'],
                            'lon': station['location']['lon'],
                            'status': station.get('status', 'active'),
                        })
                        stats['stations_imported'] += 1

                except Exception as e:
                    logger.error(f"Error importing station {station.get('id', 'unknown')}: {e}")
                    stats['stations_failed'] += 1
                    continue

            session.commit()
            logger.info(f"Import complete: {stats['stations_imported']} imported, {stats['stations_updated']} updated")

        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            stats['stations_failed'] += len(stations)
        finally:
            session.close()

        return stats

    async def run_import(self) -> Dict[str, Any]:
        """
        Run complete import process

        Returns:
            Import statistics and results
        """
        logger.info(f"Starting OSM import for region: {self.region}")

        start_time = datetime.now()

        # Fetch data
        stations = await self.fetch_osm_data()
        if not stations:
            logger.warning("No stations found from OSM")
            return {
                'status': 'failed',
                'stations_imported': 0,
                'stations_updated': 0,
                'stations_failed': 0,
                'error': 'No stations found',
            }

        # Import to database
        stats = await self.import_to_database(stations)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Log import result
        logger.info(f"Import completed in {duration:.2f} seconds")
        logger.info(f"Results: {stats['stations_imported']} imported, {stats['stations_updated']} updated, {stats['stations_failed']} failed")

        return {
            'status': 'completed',
            'stations_imported': stats['stations_imported'],
            'stations_updated': stats['stations_updated'],
            'stations_failed': stats['stations_failed'],
            'duration_seconds': duration,
        }


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='OSM Importer for BorneMap')
    parser.add_argument('--region', default='tunisia', help='Region name')
    parser.add_argument('--bbox', help='Bounding box in format: min_lon,min_lat,max_lon,max_lat')
    parser.add_argument('--database-url', help='Database URL')

    args = parser.parse_args()

    # Get database URL from environment or argument
    database_url = args.database_url or os.getenv('DATABASE_URL', '')

    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)

    # Parse bounding box if provided
    bbox = None
    if args.bbox:
        try:
            min_lon, min_lat, max_lon, max_lat = map(float, args.bbox.split(','))
            bbox = {
                'min_lat': min_lat,
                'min_lon': min_lon,
                'max_lat': max_lat,
                'max_lon': max_lon
            }
        except ValueError:
            logger.error("Invalid bounding box format. Use: min_lon,min_lat,max_lon,max_lat")
            sys.exit(1)

    # Create importer and run
    importer = OSMImporter(database_url, args.region, bbox)
    results = await importer.run_import()

    # Print results
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    asyncio.run(main())
