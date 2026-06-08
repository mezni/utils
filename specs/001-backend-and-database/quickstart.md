# Quickstart: API Versioning Implementation

**Phase**: 1 (Design & Contracts)  
**Feature**: API Versioning (`001-backend-and-database`)  
**Created**: 2026-06-08  

---

## Overview

This document guides developers through implementing API versioning in the bornemap-service. By the end of Sprint 1.1, all 16 endpoints will be accessible under the `/api/v1/` prefix.

---

## Architecture Decision: Router-Based Versioning

**Why this approach?**
- Clean separation: v1 code isolated; v2 can be tested independently
- Safe migration: v1 remains unchanged when v2 is added in MVP-2
- Documented: FastAPI auto-generates OpenAPI spec with version tags
- Scalable: adding v3, v4 is trivial (just add new routers)

**Alternative rejected**: Single router with version parameter (`@app.get("/api/{version}/stations")`). Problem: bloats logic, hard to lock v1 schema.

---

## File Structure

```
source/services/bornemap-service/
└── app/
    ├── main.py                    # [MODIFY] Register v1 routers
    ├── routers/
    │   ├── __init__.py
    │   └── v1/
    │       ├── __init__.py
    │       ├── health.py          # [NEW] GET /api/v1/health
    │       ├── partners.py        # [NEW] /api/v1/partners/*
    │       ├── stations.py        # [NEW] /api/v1/stations/*
    │       └── chargers.py        # [NEW] /api/v1/chargers/*
    ├── schemas/
    │   ├── partners.py            # [EXISTING, OK] Pydantic models (reuse)
    │   ├── stations.py            # [EXISTING, OK]
    │   └── chargers.py            # [EXISTING, OK]
    └── models/
        └── inventory.py           # [EXISTING, OK] SQLAlchemy models (no change)
```

---

## Step 1: Create v1 Router Modules

### 1.1 Create `app/routers/v1/__init__.py`

```python
"""v1 API routers. All endpoints versioned at /api/v1/"""

from . import health, partners, stations, chargers

__all__ = ["health", "partners", "stations", "chargers"]
```

### 1.2 Create `app/routers/v1/health.py`

```python
from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def get_health():
    """
    Health check endpoint.
    
    Returns service status and database connectivity.
    """
    # TODO: Add DB connectivity check
    return {
        "status": "ok",
        "service": "bornemap-service",
        "db": "ok"
    }
```

### 1.3 Create `app/routers/v1/partners.py`

```python
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List

from app.models.inventory import Partner
from app.schemas.partners import PartnerIn, PartnerOut
from app.database import get_db

router = APIRouter()

@router.get("/partners", response_model=dict)
def list_partners(db: Session = Depends(get_db)):
    """List all partners."""
    partners = db.query(Partner).all()
    return {
        "data": partners,
        "count": len(partners)
    }

@router.post("/partners", response_model=PartnerOut, status_code=201)
def create_partner(partner: PartnerIn, db: Session = Depends(get_db)):
    """Create a new partner."""
    db_partner = Partner(**partner.dict())
    db.add(db_partner)
    db.commit()
    db.refresh(db_partner)
    return db_partner

@router.get("/partners/{partner_id}", response_model=PartnerOut)
def get_partner(partner_id: str, db: Session = Depends(get_db)):
    """Get a specific partner."""
    partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404, detail="Partner not found")
    return partner

@router.put("/partners/{partner_id}", response_model=PartnerOut)
def update_partner(partner_id: str, partner: PartnerIn, db: Session = Depends(get_db)):
    """Update a partner."""
    db_partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not db_partner:
        raise HTTPException(status_code=404, detail="Partner not found")
    
    for key, value in partner.dict().items():
        setattr(db_partner, key, value)
    
    db.commit()
    db.refresh(db_partner)
    return db_partner

@router.delete("/partners/{partner_id}", status_code=204)
def delete_partner(partner_id: str, db: Session = Depends(get_db)):
    """Delete a partner."""
    db_partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not db_partner:
        raise HTTPException(status_code=404, detail="Partner not found")
    
    db.delete(db_partner)
    db.commit()
```

### 1.4 Create `app/routers/v1/stations.py`

```python
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from math import sqrt
from typing import Optional

from app.models.inventory import Station, Charger
from app.schemas.stations import StationIn, StationOut, StationDetailOut
from app.database import get_db

router = APIRouter()

@router.get("/stations", response_model=dict)
def list_stations(
    db: Session = Depends(get_db),
    partner_id: Optional[str] = Query(None)
):
    """List all stations, optionally filtered by partner."""
    query = db.query(Station)
    if partner_id:
        query = query.filter(Station.partner_id == partner_id)
    
    stations = query.all()
    # Populate charger_count and available_count
    for station in stations:
        station.charger_count = len(station.chargers)
        station.available_count = sum(1 for c in station.chargers if c.status == "available")
    
    return {
        "data": stations,
        "count": len(stations)
    }

@router.get("/stations/nearby", response_model=dict)
def nearby_stations(
    lat: float = Query(...),
    lng: float = Query(...),
    radius_km: float = Query(50.0),
    db: Session = Depends(get_db)
):
    """Get stations nearby (within radius_km)."""
    stations = db.query(Station).all()
    
    # Euclidean distance calculation (in MVP-1)
    nearby = []
    for station in stations:
        distance_m = sqrt(
            (station.latitude - lat) ** 2 + (station.longitude - lng) ** 2
        ) * 111000  # Rough approximation: 1 degree ≈ 111km
        
        if distance_m <= radius_km * 1000:
            station_dict = {
                "id": station.id,
                "partner_id": station.partner_id,
                "name": station.name,
                "address": station.address,
                "latitude": station.latitude,
                "longitude": station.longitude,
                "charger_count": len(station.chargers),
                "available_count": sum(1 for c in station.chargers if c.status == "available"),
                "distance_m": int(distance_m),
                "created_at": station.created_at,
                "updated_at": station.updated_at
            }
            nearby.append((station_dict, distance_m))
    
    # Sort by distance
    nearby.sort(key=lambda x: x[1])
    
    return {
        "data": [item[0] for item in nearby],
        "count": len(nearby)
    }

@router.post("/stations", response_model=StationOut, status_code=201)
def create_station(station: StationIn, db: Session = Depends(get_db)):
    """Create a new station."""
    db_station = Station(**station.dict())
    db.add(db_station)
    db.commit()
    db.refresh(db_station)
    return db_station

@router.get("/stations/{station_id}", response_model=StationDetailOut)
def get_station(station_id: str, db: Session = Depends(get_db)):
    """Get station detail with all chargers."""
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(status_code=404, detail="Station not found")
    return station

@router.put("/stations/{station_id}", response_model=StationOut)
def update_station(station_id: str, station: StationIn, db: Session = Depends(get_db)):
    """Update a station."""
    db_station = db.query(Station).filter(Station.id == station_id).first()
    if not db_station:
        raise HTTPException(status_code=404, detail="Station not found")
    
    for key, value in station.dict().items():
        setattr(db_station, key, value)
    
    db.commit()
    db.refresh(db_station)
    return db_station

@router.delete("/stations/{station_id}", status_code=204)
def delete_station(station_id: str, db: Session = Depends(get_db)):
    """Delete a station."""
    db_station = db.query(Station).filter(Station.id == station_id).first()
    if not db_station:
        raise HTTPException(status_code=404, detail="Station not found")
    
    db.delete(db_station)
    db.commit()
```

### 1.5 Create `app/routers/v1/chargers.py`

```python
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional

from app.models.inventory import Charger
from app.schemas.chargers import ChargerIn, ChargerOut
from app.database import get_db

router = APIRouter()

@router.get("/chargers", response_model=dict)
def list_chargers(
    db: Session = Depends(get_db),
    station_id: Optional[str] = Query(None)
):
    """List all chargers, optionally filtered by station."""
    query = db.query(Charger)
    if station_id:
        query = query.filter(Charger.station_id == station_id)
    
    chargers = query.all()
    return {
        "data": chargers,
        "count": len(chargers)
    }

@router.post("/chargers", response_model=ChargerOut, status_code=201)
def create_charger(charger: ChargerIn, db: Session = Depends(get_db)):
    """Create a new charger."""
    db_charger = Charger(**charger.dict())
    db.add(db_charger)
    db.commit()
    db.refresh(db_charger)
    return db_charger

@router.get("/chargers/{charger_id}", response_model=ChargerOut)
def get_charger(charger_id: str, db: Session = Depends(get_db)):
    """Get a specific charger."""
    charger = db.query(Charger).filter(Charger.id == charger_id).first()
    if not charger:
        raise HTTPException(status_code=404, detail="Charger not found")
    return charger

@router.put("/chargers/{charger_id}", response_model=ChargerOut)
def update_charger(charger_id: str, charger: ChargerIn, db: Session = Depends(get_db)):
    """Update a charger."""
    db_charger = db.query(Charger).filter(Charger.id == charger_id).first()
    if not db_charger:
        raise HTTPException(status_code=404, detail="Charger not found")
    
    for key, value in charger.dict().items():
        setattr(db_charger, key, value)
    
    db.commit()
    db.refresh(db_charger)
    return db_charger

@router.delete("/chargers/{charger_id}", status_code=204)
def delete_charger(charger_id: str, db: Session = Depends(get_db)):
    """Delete a charger."""
    db_charger = db.query(Charger).filter(Charger.id == charger_id).first()
    if not db_charger:
        raise HTTPException(status_code=404, detail="Charger not found")
    
    db.delete(db_charger)
    db.commit()
```

---

## Step 2: Update `app/main.py`

Replace the existing route definitions with router includes:

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import v1
from app.database import engine
from app.models.inventory import Base

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="BorneMap API",
    description="EV station discovery and management for Tunisia",
    version="1.0.0",  # API documentation version (not API versioning)
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# CORS middleware (allow all origins for MVP-1)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register v1 API routers
# All routes will be under /api/v1/ prefix
app.include_router(v1.health.router, prefix="/api/v1", tags=["health"])
app.include_router(v1.partners.router, prefix="/api/v1", tags=["partners"])
app.include_router(v1.stations.router, prefix="/api/v1", tags=["stations"])
app.include_router(v1.chargers.router, prefix="/api/v1", tags=["chargers"])

# Future: when v2 is added in MVP-2, add:
# from app.routers import v2
# app.include_router(v2.health.router, prefix="/api/v2", tags=["health"])
# app.include_router(v2.partners.router, prefix="/api/v2", tags=["partners"])
# ... etc

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
```

---

## Step 3: Testing

### Test Versioned Endpoints

```bash
# Health check
curl http://localhost:8000/api/v1/health

# List partners
curl http://localhost:8000/api/v1/partners

# List stations
curl http://localhost:8000/api/v1/stations

# Nearby stations
curl "http://localhost:8000/api/v1/stations/nearby?lat=36.8065&lng=10.1699&radius_km=50"

# List chargers
curl http://localhost:8000/api/v1/chargers
```

### Test Unversioned Endpoints (Should 404)

```bash
curl http://localhost:8000/api/stations
# Expected: 404 Not Found
```

### Test Invalid Version (Should 404)

```bash
curl http://localhost:8000/api/v999/stations
# Expected: 404 Not Found
```

### View OpenAPI Docs

```
http://localhost:8000/api/docs
```

All v1 endpoints appear grouped under "v1" tag.

---

## Step 4: Documentation Update

Update `docs/api/bornemap-service.md` to document v1 URLs:

- All endpoint examples use `/api/v1/` prefix
- Add section: "API Versioning"
  - Explain v1 is the current version
  - Link to deprecation policy (12-month support window)
  - Link to v2 migration guide (when v2 released in MVP-2)

---

## Step 5: Smoke Tests

Update `tests/test_api.py` to test versioned endpoints:

```python
def test_health_endpoint():
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"

def test_unversioned_endpoint_returns_404():
    response = client.get("/api/health")
    assert response.status_code == 404

def test_invalid_version_returns_404():
    response = client.get("/api/v999/health")
    assert response.status_code == 404

def test_list_partners():
    response = client.get("/api/v1/partners")
    assert response.status_code == 200
    assert "data" in response.json()
    assert "count" in response.json()

# ... repeat for stations, chargers endpoints
```

---

## Summary

By end of Sprint 1.1:
- ✅ All 16 endpoints live under `/api/v1/`
- ✅ Unversioned endpoints return 404
- ✅ Invalid versions return 404
- ✅ OpenAPI docs show all v1 endpoints
- ✅ All smoke tests pass
- ✅ Ready for v2 in MVP-2 (just add `/api/v2/` routers)

---

## Next Steps (MVP-2)

When v2 is designed in MVP-2:
1. Create `app/routers/v2/` with new endpoint logic
2. Register v2 routers in `main.py` (no changes to v1)
3. Mark v1 deprecated in OpenAPI; link to migration guide
4. v1 continues to work for 12 months

This design ensures v1 clients are never disrupted.
