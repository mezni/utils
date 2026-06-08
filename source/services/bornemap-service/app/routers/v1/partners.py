"""Partners router for v1 API."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from uuid import UUID
from app.database import get_db
from app.models import Partner
from app.schemas import PartnerCreate, PartnerResponse, PartnersListResponse

router = APIRouter(prefix="/partners", tags=["partners"])


@router.get("", summary="List All Partners", response_model=PartnersListResponse, tags=["v1"])
async def list_partners(db: Session = Depends(get_db)):
    """
    List All Partners (v1 - Active)
    
    Retrieve a list of all EV charging station operators (partners) on the platform.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Support Window**: 12 months after v2 release  
    
    **Response**: Returns `{data: [...], count: n}` with all partners
    
    **Pagination**: MVP-1 returns all results (no limit/offset). Pagination 
    will be added in MVP-2.
    
    **Example**:
    ```
    GET /api/v1/partners
    → 200 OK
    {
      "data": [
        {
          "id": "550e8400-...",
          "name": "TuniCharge",
          "created_at": "2026-01-15T10:30:00Z"
        }
      ],
      "count": 1
    }
    ```
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1partners`
    """
    partners = db.query(Partner).all()
    return {
        "data": [PartnerResponse.model_validate(p) for p in partners],
        "count": len(partners)
    }


@router.post("", summary="Create Partner", status_code=201, response_model=PartnerResponse, tags=["v1"])
async def create_partner(partner: PartnerCreate, db: Session = Depends(get_db)):
    """
    Create Partner (v1 - Active)
    
    Create a new EV charging station operator on the platform.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Required Fields**: `name` (string, 1-255 chars)  
    **Returns**: 201 Created with partner object  
    
    **Example**:
    ```
    POST /api/v1/partners
    {
      "name": "New Charging Company"
    }
    → 201 Created
    {
      "id": "660e8400-...",
      "name": "New Charging Company",
      "created_at": "2026-06-08T14:30:00Z"
    }
    ```
    
    **Error Responses**:
    - 422 Unprocessable Entity: Missing or invalid `name`
    
    **Documentation**: See `/docs/api/bornemap-service.md#post-apiv1partners`
    """
    db_partner = Partner(name=partner.name)
    db.add(db_partner)
    db.commit()
    db.refresh(db_partner)
    return PartnerResponse.model_validate(db_partner)


@router.get("/{partner_id}", summary="Get Partner", response_model=PartnerResponse, tags=["v1"])
async def get_partner(partner_id: UUID, db: Session = Depends(get_db)):
    """
    Get Partner (v1 - Active)
    
    Retrieve details for a specific partner by ID.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `partner_id` (UUID)  
    **Returns**: 200 OK with partner object, or 404 Not Found  
    
    **Example**:
    ```
    GET /api/v1/partners/550e8400-...
    → 200 OK
    {
      "id": "550e8400-...",
      "name": "TuniCharge",
      "created_at": "2026-01-15T10:30:00Z"
    }
    ```
    
    **Error Responses**:
    - 404 Not Found: Partner does not exist
    
    **Documentation**: See `/docs/api/bornemap-service.md#get-apiv1partnersid`
    """
    partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404, detail="Partner not found")
    return PartnerResponse.model_validate(partner)


@router.put("/{partner_id}", summary="Update Partner", response_model=PartnerResponse, tags=["v1"])
async def update_partner(partner_id: UUID, partner_update: PartnerCreate, db: Session = Depends(get_db)):
    """
    Update Partner (v1 - Active)
    
    Update an existing partner's information.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `partner_id` (UUID)  
    **Body**: Partner update object with new fields  
    **Returns**: 200 OK with updated partner, or 404 Not Found  
    
    **Example**:
    ```
    PUT /api/v1/partners/550e8400-...
    {
      "name": "Updated Partner Name"
    }
    → 200 OK
    {
      "id": "550e8400-...",
      "name": "Updated Partner Name",
      "created_at": "2026-01-15T10:30:00Z"
    }
    ```
    
    **Error Responses**:
    - 404 Not Found: Partner does not exist
    - 422 Unprocessable Entity: Invalid request body
    
    **Documentation**: See `/docs/api/bornemap-service.md#put-apiv1partnersid`
    """
    partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404, detail="Partner not found")
    
    partner.name = partner_update.name
    db.commit()
    db.refresh(partner)
    return PartnerResponse.model_validate(partner)


@router.delete("/{partner_id}", summary="Delete Partner", status_code=204, tags=["v1"])
async def delete_partner(partner_id: UUID, db: Session = Depends(get_db)):
    """
    Delete Partner (v1 - Active)
    
    Delete an existing partner from the platform.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Path Parameter**: `partner_id` (UUID)  
    **Returns**: 204 No Content on success, or 404 Not Found  
    
    **Example**:
    ```
    DELETE /api/v1/partners/550e8400-...
    → 204 No Content
    ```
    
    **Error Responses**:
    - 404 Not Found: Partner does not exist
    
    **Side Effects**:
    - Deletes partner record
    - Associated stations should be handled per business logic
    
    **Documentation**: See `/docs/api/bornemap-service.md#delete-apiv1partnersid`
    """
    partner = db.query(Partner).filter(Partner.id == partner_id).first()
    if not partner:
        raise HTTPException(status_code=404, detail="Partner not found")
    
    db.delete(partner)
    db.commit()
    return None
