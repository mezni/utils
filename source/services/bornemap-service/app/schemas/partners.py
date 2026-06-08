"""Pydantic schemas for Partners endpoints."""

from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field


class PartnerCreate(BaseModel):
    """Partner creation request schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Partner name")

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Partner Company"
            }
        }


class PartnerResponse(BaseModel):
    """Partner response schema."""
    id: UUID = Field(..., description="Partner unique identifier")
    name: str = Field(..., description="Partner name")
    created_at: datetime = Field(..., description="Timestamp when partner was created")

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Partner Company",
                "created_at": "2026-01-01T00:00:00Z"
            }
        }


class PartnersListResponse(BaseModel):
    """Partners list response schema."""
    data: list[PartnerResponse] = Field(..., description="List of partners")
    count: int = Field(..., description="Total number of partners")

    class Config:
        json_schema_extra = {
            "example": {
                "data": [
                    {
                        "id": "550e8400-e29b-41d4-a716-446655440000",
                        "name": "Partner Company",
                        "created_at": "2026-01-01T00:00:00Z"
                    }
                ],
                "count": 1
            }
        }
