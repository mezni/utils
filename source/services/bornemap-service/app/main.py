"""FastAPI application entry point with versioned routing."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.v1 import health, partners, stations, chargers

# Create FastAPI application with versioning documentation
app = FastAPI(
    title="BorneMap API (v1)",
    description="""
    EV station discovery and management platform for Tunisia.
    
    ## API Versioning
    
    All endpoints are versioned via URL path prefix:
    - **v1 endpoints**: `/api/v1/stations`, `/api/v1/partners`, `/api/v1/chargers`, `/api/v1/health`
    - **Support window**: 12 months from v2 release
    - **Unversioned endpoints** (e.g., `/api/stations`) return 404 Not Found
    
    ## Endpoints
    
    This API provides the following resource endpoints:
    - **Health**: Service status and database connectivity
    - **Partners**: EV charging station operators
    - **Stations**: Individual charging locations with addresses and coordinates
    - **Chargers**: Physical charging points with connector types and power ratings
    """,
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register v1 routers under /api/v1 prefix
app.include_router(
    health.router,
    prefix="/api/v1",
    tags=["v1"],
)

app.include_router(
    partners.router,
    prefix="/api/v1",
    tags=["v1"],
)

app.include_router(
    stations.router,
    prefix="/api/v1",
    tags=["v1"],
)

app.include_router(
    chargers.router,
    prefix="/api/v1",
    tags=["v1"],
)


@app.get("/", tags=["root"])
async def root():
    """API root endpoint."""
    return {
        "message": "BorneMap API",
        "version": "1.0.0",
        "docs": "/api/docs",
        "api_prefix": "/api/v1",
    }
