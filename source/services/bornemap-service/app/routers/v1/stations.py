"""Stations router for v1 API."""

import math
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from uuid import UUID
from app.database import get_db
from app.models import Station, Charger, ChargerStatus
from app.schemas import StationCreate, StationResponse, StationsListResponse, StationDetailResponse, ChargerSummary

router = APIRouter(prefix="/stations", tags=["stations"])


def calculate_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate Euclidean distance between two points in kilometers."""
    # Simple Euclidean distance (not geodetic)
    lat_diff = lat2 - lat1
    lng_diff = lng2 - lng1
    distance_m = math.sqrt(lat_diff ** 2 + lng_diff ** 2) * 111000  # 1 degree ≈ 111 km
    return distance_m


def add_charger_counts(station, db: Session):
    """Add charger count fields to station response."""
    chargers = db.query(Charger).filter(Charger.station_id == station.id).all()
    charger_count = len(chargers)
    available_count = sum(1 for c in chargers if c.status == ChargerStatus.AVAILABLE)
    return charger_count, available_count


@router.get("", summary="List All Stations", response_model=StationsListResponse, tags=["v1"])
async def list_stations(partner_id: UUID = Query(None), db: Session = Depends(get_db)):
    """
    List All Stations (v1 - Active)
    
    Retrieve all EV charging stations, optionally filtered by partner.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Query Parameters**: `partner_id` (optional UUID)  
    **Returns**: List of stations with charger counts  
    
    **Charger Counts**:
    - `charger_count`: Total number of chargers at station
    - `available_count`: Number of available chargers
    
    **Example**:
    ```
    GET /api/v1/stations?partner_id=550e8400-...
    → 200 OK
    {
      "data": [
        {
          "id": "770e8400-...",
          "partner_id": "550e8400-...",
          "name": "Tunis Central",
          "address": "123 Avenue Bourguiba, Tunis",
          "latitude": 36.8065,
          "longitude": 10.1815,
          "charger_count": 4,
          "available_count": 3,
          "created_at": "2026-01-15T10:30:00Z",
          "updated_at": "2026-06-08T14:30:00Z"
        }
      ],
      "count": 1
    }
    ```
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1stations`
    """
    query = db.query(Station)
    if partner_id:
        query = query.filter(Station.partner_id == partner_id)
    
    stations = query.all()
    data = []
    for station in stations:
        charger_count, available_count = add_charger_counts(station, db)
        station_dict = StationResponse.model_validate(station).model_dump()
        station_dict["charger_count"] = charger_count
        station_dict["available_count"] = available_count
        data.append(StationResponse(**station_dict))
    
    return {
        "data": data,
        "count": len(data)
    }


@router.get("/nearby", summary="Find Nearby Stations", response_model=StationsListResponse, tags=["v1"])
async def get_nearby_stations(
    lat: float = Query(..., ge=-90, le=90),
    lng: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(50, ge=0),
    db: Session = Depends(get_db)
):
    """
    Find Nearby Stations (v1 - Active)
    
    Search for EV charging stations near a given location, ordered by distance.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Query Parameters**:
    - `lat` (required): Latitude (-90 to 90)
    - `lng` (required): Longitude (-180 to 180)
    - `radius_km` (optional): Search radius in kilometers (default: 50)
    
    **Returns**: Stations within radius, ordered by proximity  
    **Distance Calculation**: Euclidean (simplified, not geodetic)  
    
    **Example**:
    ```
    GET /api/v1/stations/nearby?lat=36.8065&lng=10.1815&radius_km=50
    → 200 OK
    {
      "data": [
        {
          "id": "770e8400-...",
          "name": "Tunis Central (5.2 km)",
          "distance_m": 5200,
          ...
        }
      ],
      "count": 1
    }
    ```
    
    **Error Responses**:
    - 422 Unprocessable Entity: Invalid latitude/longitude
    
    **Use Cases**:
    - Mobile app: "Find charging stations near me"
    - Route planning: "Show stations along my route"
    - Regional search: "All stations in Tunis area"
    
    **Note**: v1 uses simplified Euclidean distance. Geodetic distance 
    will be available in v2.
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1stationsnearby`
    """
    all_stations = db.query(Station).all()
    nearby = []
    
    for station in all_stations:
        distance_m = calculate_distance(lat, lng, station.latitude, station.longitude)
        if distance_m <= radius_km * 1000:  # Convert radius to meters
            charger_count, available_count = add_charger_counts(station, db)
            station_dict = StationResponse.model_validate(station).model_dump()
            station_dict["charger_count"] = charger_count
            station_dict["available_count"] = available_count
            nearby.append((StationResponse(**station_dict), distance_m))
    
    # Sort by distance
    nearby.sort(key=lambda x: x[1])
    data = [s[0] for s in nearby]
    
    return {
        "data": data,
        "count": len(data)
    }


@router.post("", summary="Create Station", status_code=201, response_model=StationResponse, tags=["v1"])
async def create_station(station: StationCreate, db: Session = Depends(get_db)):
    """
    Create Station (v1 - Active)
    
    Create a new EV charging station.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Required Fields**:
    - `partner_id` (UUID): Operator of the station
    - `name` (string): Station name
    - `address` (string): Physical address
    - `latitude` (float): -90 to 90
    - `longitude` (float): -180 to 180
    
    **Returns**: 201 Created with station object  
    
    **Example**:
    ```
    POST /api/v1/stations
    {
      "partner_id": "550e8400-...",
      "name": "New Station",
      "address": "456 Rue de la Paix, Sfax",
      "latitude": 34.7406,
      "longitude": 10.7603
    }
    → 201 Created
    {
      "id": "880e8400-...",
      "partner_id": "550e8400-...",
      "name": "New Station",
      ...
      "charger_count": 0,
      "available_count": 0,
      ...
    }
    ```
    
    **Error Responses**:
    - 422 Unprocessable Entity: Invalid coordinates or missing fields
    
    **Documentation**: See `/docs/api/bornemap-service.md#post-apiv1stations`
    """
    db_station = Station(
        partner_id=station.partner_id,
        name=station.name,
        address=station.address,
        latitude=station.latitude,
        longitude=station.longitude,
    )
    db.add(db_station)
    db.commit()
    db.refresh(db_station)
    
    charger_count, available_count = add_charger_counts(db_station, db)
    station_dict = StationResponse.model_validate(db_station).model_dump()
    station_dict["charger_count"] = charger_count
    station_dict["available_count"] = available_count
    return StationResponse(**station_dict)


@router.get("/{station_id}", summary="Get Station", response_model=StationDetailResponse, tags=["v1"])
async def get_station(station_id: UUID, db: Session = Depends(get_db)):
    """
    Get Station (v1 - Active)
    
    Retrieve full details for a specific station, including all chargers.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `station_id` (UUID)  
    **Returns**: 200 OK with station + chargers array, or 404 Not Found  
    
    **Response Includes**:
    - Station details (name, address, coordinates)
    - Chargers array with all chargers at station
    - Charger counts (total & available)
    
    **Example**:
    ```
    GET /api/v1/stations/770e8400-...
    → 200 OK
    {
      "id": "770e8400-...",
      "name": "Tunis Central",
      "chargers": [
        {
          "id": "990e8400-...",
          "connector_type": "Type2",
          "power_kw": 22.0,
          "status": "available"
        }
      ],
      "charger_count": 1,
      "available_count": 1,
      ...
    }
    ```
    
    **Error Responses**:
    - 404 Not Found: Station does not exist
    
    **Use Cases**:
    - Display station details in mobile app
    - Show charger availability at specific station
    - Check charger connector types before visiting
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1stationsid`
    """
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")
    
    chargers = db.query(Charger).filter(Charger.station_id == station_id).all()
    charger_count = len(chargers)
    available_count = sum(1 for c in chargers if c.status == ChargerStatus.AVAILABLE)
    
    return {
        "id": station.id,
        "partner_id": station.partner_id,
        "name": station.name,
        "address": station.address,
        "latitude": station.latitude,
        "longitude": station.longitude,
        "chargers": [ChargerSummary.model_validate(c) for c in chargers],
        "charger_count": charger_count,
        "available_count": available_count,
        "created_at": station.created_at,
        "updated_at": station.updated_at,
    }


@router.put("/{station_id}", summary="Update Station", response_model=StationResponse, tags=["v1"])
async def update_station(station_id: UUID, station_update: StationCreate, db: Session = Depends(get_db)):
    """
    Update Station (v1 - Active)
    
    Update an existing station's information.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `station_id` (UUID)  
    **Body**: Station update with new field values  
    **Returns**: 200 OK with updated station, or 404 Not Found  
    
    **Example**:
    ```
    PUT /api/v1/stations/770e8400-...
    {
      "partner_id": "550e8400-...",
      "name": "Updated Station Name",
      "address": "Updated Address",
      "latitude": 36.8065,
      "longitude": 10.1815
    }
    → 200 OK
    { ... updated station ... }
    ```
    
    **Error Responses**:
    - 404 Not Found: Station does not exist
    - 422 Unprocessable Entity: Invalid coordinates
    
    **Documentation**: See `/docs/api/bornemap-service.md#put-apiv1stationsid`
    """
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")
    
    station.name = station_update.name
    station.address = station_update.address
    station.latitude = station_update.latitude
    station.longitude = station_update.longitude
    db.commit()
    db.refresh(station)
    
    charger_count, available_count = add_charger_counts(station, db)
    station_dict = StationResponse.model_validate(station).model_dump()
    station_dict["charger_count"] = charger_count
    station_dict["available_count"] = available_count
    return StationResponse(**station_dict)


@router.delete("/{station_id}", summary="Delete Station", status_code=204, tags=["v1"])
async def delete_station(station_id: UUID, db: Session = Depends(get_db)):
    """
    Delete Station (v1 - Active)
    
    Delete an existing station from the platform.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `station_id` (UUID)  
    **Returns**: 204 No Content on success, or 404 Not Found  
    
    **Example**:
    ```
    DELETE /api/v1/stations/770e8400-...
    → 204 No Content
    ```
    
    **Error Responses**:
    - 404 Not Found: Station does not exist
    
    **Side Effects**:
    - Deletes station record
    - Associated chargers will also be deleted (cascade delete)
    
    **Documentation**: See `/docs/api/bornemap-service.md#delete-apiv1stationsid`
    """
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")
    
    db.delete(station)
    db.commit()
    return None
