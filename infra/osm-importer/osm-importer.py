#!/usr/bin/env python3
import sys
import os
import json
import logging
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class OSMImporter:
    TUNISIA_BBOX = {'min_lat': 30.0, 'min_lon': 7.5, 'max_lat': 37.5, 'max_lon': 11.6}

    def __init__(self, database_url: str, region: str = 'tunisia', bbox: Optional[Dict[str, float]] = None):
        self.database_url = database_url
        self.region = region
        self.bbox = bbox or self.TUNISIA_BBOX
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)

    def fetch_osm_data(self) -> List[Dict[str, Any]]:
        logger.info(f"Fetching OSM data for region: {self.region}")

        bbox_str = f"{self.bbox['min_lon']},{self.bbox['min_lat']},{self.bbox['max_lon']},{self.bbox['max_lat']}"
        query = f"""
        [out:json][timeout:25];
        (
          way["amenity"="charging_station"]({bbox_str});
          node["amenity"="charging_station"]({bbox_str});
        );
        out center;
        """

        try:
            resp = requests.post(
                "https://overpass-api.de/api/interpreter",
                data={'data': query},
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"OSM API returned status {resp.status_code}")
                return []

            data = resp.json()
            return self.parse_osm_data(data)
        except requests.RequestException as e:
            logger.error(f"Error fetching OSM data: {e}")
            return []

    def parse_osm_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        logger.info(f"Parsed {len(stations)} stations from OSM")
        return stations

    def extract_station_from_element(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tags = element.get('tags', {})
        name = tags.get('name', '')
        if not name:
            name = tags.get('operator', 'Unknown Station')

        if element['type'] == 'node':
            lat = element['lat']
            lon = element['lon']
        elif 'center' in element:
            lat = element['center']['lat']
            lon = element['center']['lon']
        else:
            return None

        street = tags.get('addr:street', '')
        city = tags.get('addr:city', 'Unknown')
        address = f"{street}, {city}".strip(", ")

        connectors = []
        power_kw = 0

        socket_types = {
            'socket:type2': 'type2',
            'socket:type2_combo': 'ccs2',
            'socket:chademo': 'chademo',
            'socket:type3': 'type2',
            'socket:type1': 'type2',
            'socket:ccs': 'ccs2',
        }
        for tag_key, connector_type in socket_types.items():
            if tag_key in tags and tags[tag_key] != '0':
                connectors.append(connector_type)

        for kw_tag in ['socket:type2:output', 'socket:chademo:output', 'socket:type2_combo:output']:
            if kw_tag in tags:
                try:
                    val = float(tags[kw_tag])
                    if val > power_kw:
                        power_kw = val
                except (ValueError, TypeError):
                    pass

        if not connectors:
            if tags.get('amenity') == 'charging_station':
                connectors.append('type2')

        if power_kw == 0:
            power_kw = float(tags.get('rating', tags.get('maxpower', '11')))

        return {
            'id': f"sta_{element['id']}",
            'name': name,
            'location': {'lat': lat, 'lon': lon},
            'address': address,
            'city': city,
            'visibility': self.determine_visibility(tags),
            'status': 'active',
            'connector_types': connectors,
            'connector_power': [power_kw],
            'raw_tags': tags,
        }

    def determine_visibility(self, tags: Dict[str, Any]) -> str:
        if tags.get('access') == 'private':
            return 'private_home'
        if tags.get('operator') or tags.get('fee') == 'yes':
            return 'commercial'
        return 'commercial'

    def import_to_database(self, stations: List[Dict[str, Any]]) -> Dict[str, int]:
        logger.info(f"Importing {len(stations)} stations to database")
        stats = {'stations_imported': 0, 'stations_updated': 0, 'stations_failed': 0}

        session = self.Session()
        try:
            for station in stations:
                try:
                    existing = session.execute(
                        text('SELECT id FROM inventory.station WHERE id = :id'),
                        {'id': station['id']},
                    ).fetchone()

                    if existing:
                        session.execute(
                            text("""
                                UPDATE inventory.station
                                SET name = :name,
                                    location = ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                                    address = :address,
                                    city = :city,
                                    visibility = :visibility::station_visibility,
                                    status = :status::station_status
                                WHERE id = :id
                            """),
                            {'id': station['id'], 'name': station['name'],
                             'lon': station['location']['lon'], 'lat': station['location']['lat'],
                             'address': station['address'], 'city': station['city'],
                             'visibility': station['visibility'], 'status': station['status']},
                        )
                        stats['stations_updated'] += 1
                    else:
                        session.execute(
                            text("""
                                INSERT INTO inventory.station
                                (id, name, location, address, city, visibility, status)
                                VALUES (:id, :name,
                                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                                    :address, :city, :visibility::station_visibility, :status::station_status)
                            """),
                            {'id': station['id'], 'name': station['name'],
                             'lon': station['location']['lon'], 'lat': station['location']['lat'],
                             'address': station['address'], 'city': station['city'],
                             'visibility': station['visibility'], 'status': station['status']},
                        )
                        stats['stations_imported'] += 1

                except Exception as e:
                    logger.error(f"Error importing station {station.get('id', 'unknown')}: {e}")
                    stats['stations_failed'] += 1

            session.commit()
            logger.info(f"Import complete: {stats['stations_imported']} imported, {stats['stations_updated']} updated")

        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            stats['stations_failed'] += len(stations)
        finally:
            session.close()

        return stats

    def run_import(self) -> Dict[str, Any]:
        logger.info(f"Starting OSM import for region: {self.region}")
        start_time = datetime.now()

        stations = self.fetch_osm_data()
        if not stations:
            logger.warning("No stations found from OSM")
            return {'status': 'failed', 'stations_imported': 0, 'stations_updated': 0, 'stations_failed': 0, 'error': 'No stations found'}

        stats = self.import_to_database(stations)
        duration = (datetime.now() - start_time).total_seconds()

        logger.info(f"Import completed in {duration:.2f} seconds")
        return {'status': 'completed', **stats, 'duration_seconds': duration}


def main():
    parser = argparse.ArgumentParser(description='OSM Importer for BorneMap')
    parser.add_argument('--region', default='tunisia')
    parser.add_argument('--bbox', help='Format: min_lon,min_lat,max_lon,max_lat')
    parser.add_argument('--database-url')
    args = parser.parse_args()

    database_url = args.database_url or os.getenv('DATABASE_URL', '')
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    bbox = None
    if args.bbox:
        try:
            min_lon, min_lat, max_lon, max_lat = map(float, args.bbox.split(','))
            bbox = {'min_lat': min_lat, 'min_lon': min_lon, 'max_lat': max_lat, 'max_lon': max_lon}
        except ValueError:
            logger.error("Invalid bbox format. Use: min_lon,min_lat,max_lon,max_lat")
            sys.exit(1)

    importer = OSMImporter(database_url, args.region, bbox)
    results = importer.run_import()
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
