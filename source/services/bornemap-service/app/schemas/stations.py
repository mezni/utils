"""Pydantic schemas for Stations endpoints."""

from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field


class ChargerSummary(BaseModel):
    """Charger summary in station response."""
    id: UUID = Field(..., description="Charger unique identifier")
    connector_type: str = Field(..., description="Connector type (Type2, CCS, CHAdeMO, etc.)")
    power_kw: float = Field(..., description="Charger power in kilowatts")
    status: str = Field(..., description="Charger status (available, in_use, maintenance)")

    class Config:
        from_attributes = True


class StationCreate(BaseModel):
    """Station creation request schema."""
    partner_id: UUID = Field(..., description="Partner ID that owns this station")
    name: str = Field(..., min_length=1, max_length=255, description="Station name")
    address: str = Field(..., min_length=1, max_length=500, description="Station address")
    latitude: float = Field(..., ge=-90, le=90, description="Latitude (-90 to 90)")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude (-180 to 180)")

    class Config:
        json_schema_extra = {
            "example": {
                "partner_id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Station Name",
                "address": "123 Main St, Tunis",
                "latitude": 36.8065,
                "longitude": 10.1815
            }
        }


class StationResponse(BaseModel):
    """Station response schema (list items)."""
    id: UUID = Field(..., description="Station unique identifier")
    partner_id: UUID = Field(..., description="Partner ID that owns this station")
    name: str = Field(..., description="Station name")
    address: str = Field(..., description="Station address")
    latitude: float = Field(..., description="Latitude")
    longitude: float = Field(..., description="Longitude")
    charger_count: int = Field(..., description="Total number of chargers at this station")
    available_count: int = Field(..., description="Number of available chargers")
    created_at: datetime = Field(..., description="Timestamp when station was created")
    updated_at: datetime = Field(..., description="Timestamp when station was last updated")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "partner_id": "660e8400-e29b-41d4-a716-446655440000",
                "name": "Station Name",
                "address": "123 Main St, Tunis",
                "latitude": 36.8065,
                "longitude": 10.1815,
                "charger_count": 2,
                "available_count": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z"
            }
        }


class StationDetailResponse(BaseModel):
    """Station detail response schema (includes chargers)."""
    id: UUID = Field(..., description="Station unique identifier")
    partner_id: UUID = Field(..., description="Partner ID that owns this station")
    name: str = Field(..., description="Station name")
    address: str = Field(..., description="Station address")
    latitude: float = Field(..., description="Latitude")
    longitude: float = Field(..., description="Longitude")
    chargers: list[ChargerSummary] = Field(..., description="List of chargers at this station")
    charger_count: int = Field(..., description="Total number of chargers")
    available_count: int = Field(..., description="Number of available chargers")
    created_at: datetime = Field(..., description="Timestamp when station was created")
    updated_at: datetime = Field(..., description="Timestamp when station was last updated")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "partner_id": "660e8400-e29b-41d4-a716-446655440000",
                "name": "Station Name",
                "address": "123 Main St, Tunis",
                "latitude": 36.8065,
                "longitude": 10.1815,
                "chargers": [
                    {
                        "id": "770e8400-e29b-41d4-a716-446655440000",
                        "connector_type": "Type2",
                        "power_kw": 22.0,
                        "status": "available"
                    }
                ],
                "charger_count": 1,
                "available_count": 1,
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z"
            }
        }


class StationsListResponse(BaseModel):
    """Stations list response schema."""
    data: list[StationResponse] = Field(..., description="List of stations")
    count: int = Field(..., description="Total number of stations")

    class Config:
        json_schema_extra = {
            "example": {
                "data": [
                    {
                        "id": "550e8400-e29b-41d4-a716-446655440000",
                        "partner_id": "660e8400-e29b-41d4-a716-446655440000",
                        "name": "Station Name",
                        "address": "123 Main St, Tunis",
                        "latitude": 36.8065,
                        "longitude": 10.1815,
                        "charger_count": 1,
                        "available_count": 1,
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z"
                    }
                ],
                "count": 1
            }
        }
