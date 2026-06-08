"""Chargers router for v1 API."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from uuid import UUID
from app.database import get_db
from app.models import Charger
from app.schemas import ChargerCreate, ChargerResponse, ChargersListResponse

router = APIRouter(prefix="/chargers", tags=["chargers"])


@router.get("", summary="List All Chargers", response_model=ChargersListResponse, tags=["v1"])
async def list_chargers(station_id: UUID = Query(None), db: Session = Depends(get_db)):
    """
    List All Chargers (v1 - Active)
    
    Retrieve all EV chargers, optionally filtered by station.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Query Parameters**: `station_id` (optional UUID)  
    **Returns**: List of chargers with status and power rating  
    
    **Charger Status Values**:
    - `available`: Available for use
    - `in_use`: Currently in use
    - `maintenance`: Under maintenance
    
    **Example**:
    ```
    GET /api/v1/chargers?station_id=770e8400-...
    → 200 OK
    {
      "data": [
        {
          "id": "990e8400-...",
          "station_id": "770e8400-...",
          "connector_type": "Type2",
          "power_kw": 22.0,
          "status": "available",
          "created_at": "2026-01-15T10:30:00Z",
          "updated_at": "2026-06-08T14:30:00Z"
        }
      ],
      "count": 1
    }
    ```
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1chargers`
    """
    query = db.query(Charger)
    if station_id:
        query = query.filter(Charger.station_id == station_id)
    
    chargers = query.all()
    return {
        "data": [ChargerResponse.model_validate(c) for c in chargers],
        "count": len(chargers)
    }


@router.post("", summary="Create Charger", status_code=201, response_model=ChargerResponse, tags=["v1"])
async def create_charger(charger: ChargerCreate, db: Session = Depends(get_db)):
    """
    Create Charger (v1 - Active)
    
    Create a new EV charger at a station.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Required Fields**:
    - `station_id` (UUID): Station where charger is located
    - `connector_type` (string): Type2, CCS, CHAdeMO, Tesla, etc.
    - `power_kw` (float): Charger power in kilowatts
    
    **Returns**: 201 Created with charger object  
    **Default Status**: "available"  
    
    **Example**:
    ```
    POST /api/v1/chargers
    {
      "station_id": "770e8400-...",
      "connector_type": "CCS",
      "power_kw": 50.0
    }
    → 201 Created
    {
      "id": "aa0e8400-...",
      "station_id": "770e8400-...",
      "connector_type": "CCS",
      "power_kw": 50.0,
      "status": "available",
      "created_at": "2026-06-08T14:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
    ```
    
    **Connector Types** (examples):
    - `Type2`: IEC 62196 Type 2 (European standard)
    - `CCS`: Combined Charging System
    - `CHAdeMO`: Japanese fast charging
    - `Tesla`: Tesla proprietary connector
    
    **Error Responses**:
    - 422 Unprocessable Entity: Missing fields or invalid power_kw
    
    **Documentation**: See `/docs/api/bornemap-service.md#post-apiv1chargers`
    """
    db_charger = Charger(
        station_id=charger.station_id,
        connector_type=charger.connector_type,
        power_kw=charger.power_kw,
    )
    db.add(db_charger)
    db.commit()
    db.refresh(db_charger)
    return ChargerResponse.model_validate(db_charger)


@router.get("/{charger_id}", summary="Get Charger", response_model=ChargerResponse, tags=["v1"])
async def get_charger(charger_id: UUID, db: Session = Depends(get_db)):
    """
    Get Charger (v1 - Active)
    
    Retrieve details for a specific charger.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `charger_id` (UUID)  
    **Returns**: 200 OK with charger object, or 404 Not Found  
    
    **Example**:
    ```
    GET /api/v1/chargers/990e8400-...
    → 200 OK
    {
      "id": "990e8400-...",
      "station_id": "770e8400-...",
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "available",
      "created_at": "2026-01-15T10:30:00Z",
      "updated_at": "2026-06-08T14:30:00Z"
    }
    ```
    
    **Error Responses**:
    - 404 Not Found: Charger does not exist
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1chargersid`
    """
    charger = db.query(Charger).filter(Charger.id == charger_id).first()
    if not charger:
        raise HTTPException(status_code=404, detail="Charger not found")
    return ChargerResponse.model_validate(charger)


@router.put("/{charger_id}", summary="Update Charger", response_model=ChargerResponse, tags=["v1"])
async def update_charger(charger_id: UUID, charger_update: ChargerCreate, db: Session = Depends(get_db)):
    """
    Update Charger (v1 - Active)
    
    Update an existing charger's information.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `charger_id` (UUID)  
    **Primary Use**: Updating charger status  
    **Returns**: 200 OK with updated charger, or 404 Not Found  
    
    **Example** (Status Update):
    ```
    PUT /api/v1/chargers/990e8400-...
    {
      "connector_type": "Type2",
      "power_kw": 22.0,
      "status": "maintenance"
    }
    → 200 OK
    { ... updated charger with status: "maintenance" ... }
    ```
    
    **Note on Status Field** (MVP-1):
    In MVP-1, status is read-only (updated via separate endpoint in MVP-2).
    Current version allows updating connector_type and power_kw.
    
    **Error Responses**:
    - 404 Not Found: Charger does not exist
    - 422 Unprocessable Entity: Invalid request body
    
    **Documentation**: See `/docs/api/bornemap-service.md#put-apiv1chargersid`
    """
    charger = db.query(Charger).filter(Charger.id == charger_id).first()
    if not charger:
        raise HTTPException(status_code=404, detail="Charger not found")
    
    charger.connector_type = charger_update.connector_type
    charger.power_kw = charger_update.power_kw
    db.commit()
    db.refresh(charger)
    return ChargerResponse.model_validate(charger)


@router.delete("/{charger_id}", summary="Delete Charger", status_code=204, tags=["v1"])
async def delete_charger(charger_id: UUID, db: Session = Depends(get_db)):
    """
    Delete Charger (v1 - Active)
    
    Delete an existing charger from the platform.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `charger_id` (UUID)  
    **Returns**: 204 No Content on success, or 404 Not Found  
    
    **Example**:
    ```
    DELETE /api/v1/chargers/990e8400-...
    → 204 No Content
    ```
    
    **Error Responses**:
    - 404 Not Found: Charger does not exist
    
    **Documentation**: See `/docs/api/bornemap-service.md#delete-apiv1chargersid`
    """
    charger = db.query(Charger).filter(Charger.id == charger_id).first()
    if not charger:
        raise HTTPException(status_code=404, detail="Charger not found")
    
    db.delete(charger)
    db.commit()
    return None
