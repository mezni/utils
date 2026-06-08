"""Pydantic schemas for Chargers endpoints."""

from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field


class ChargerCreate(BaseModel):
    """Charger creation request schema."""
    station_id: UUID = Field(..., description="Station ID where charger is located")
    connector_type: str = Field(..., min_length=1, max_length=50, description="Connector type")
    power_kw: float = Field(..., gt=0, description="Charger power in kilowatts")

    class Config:
        json_schema_extra = {
            "example": {
                "station_id": "550e8400-e29b-41d4-a716-446655440000",
                "connector_type": "Type2",
                "power_kw": 22.0
            }
        }


class ChargerResponse(BaseModel):
    """Charger response schema."""
    id: UUID = Field(..., description="Charger unique identifier")
    station_id: UUID = Field(..., description="Station ID where charger is located")
    connector_type: str = Field(..., description="Connector type")
    power_kw: float = Field(..., description="Charger power in kilowatts")
    status: str = Field(..., description="Charger status (available, in_use, maintenance)")
    created_at: datetime = Field(..., description="Timestamp when charger was created")
    updated_at: datetime = Field(..., description="Timestamp when charger was last updated")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "station_id": "660e8400-e29b-41d4-a716-446655440000",
                "connector_type": "Type2",
                "power_kw": 22.0,
                "status": "available",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z"
            }
        }


class ChargersListResponse(BaseModel):
    """Chargers list response schema."""
    data: list[ChargerResponse] = Field(..., description="List of chargers")
    count: int = Field(..., description="Total number of chargers")

    class Config:
        json_schema_extra = {
            "example": {
                "data": [
                    {
                        "id": "550e8400-e29b-41d4-a716-446655440000",
                        "station_id": "660e8400-e29b-41d4-a716-446655440000",
                        "connector_type": "Type2",
                        "power_kw": 22.0,
                        "status": "available",
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z"
                    }
                ],
                "count": 1
            }
        }
