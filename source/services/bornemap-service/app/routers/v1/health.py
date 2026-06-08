"""Health endpoint router for v1 API."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.database import get_db

router = APIRouter(tags=["health"])


@router.get("/health", summary="Service Health Check", tags=["v1"])
async def get_health(db: Session = Depends(get_db)):
    """
    Health Check Endpoint (v1 - Active)
    
    Returns the service health status and database connectivity. This endpoint is 
    used by load balancers and monitoring systems to verify service availability.
    
    **v1 Status**: Active (Sprint 1.1)  
    **Support Window**: 12 months after v2 release  
    
    **Response Fields**:
    - `status` (str): Always "ok" if service is running
    - `service` (str): Service name ("bornemap-service")
    - `db` (str): Database status ("ok" or "error")
    
    **Use Cases**:
    - Load balancer health checks
    - Service monitoring and alerting
    - Dependency verification before API calls
    
    **Example**:
    ```
    GET /api/v1/health
    → 200 OK
    {
      "status": "ok",
      "service": "bornemap-service",
      "db": "ok"
    }
    ```
    
    **Error Handling**:
    - Always returns 200, even if DB is down (marked in "db" field)
    - Never returns 5xx errors
    
    **Documentation**: See `/docs/api/bornemap-service.md#health-endpoint`
    """
    db_status = "ok"
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        db_status = "error"
    
    return {
        "status": "ok",
        "service": "bornemap-service",
        "db": db_status
    }
