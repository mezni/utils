"""SQLAlchemy models for inventory schema."""

from datetime import datetime
from uuid import uuid4
from sqlalchemy import Column, String, Float, DateTime, ForeignKey, Enum, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
import enum

Base = declarative_base()


class Partner(Base):
    """Partner (station owner) entity."""
    __tablename__ = "partner"
    __table_args__ = {"schema": "inventory"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Station(Base):
    """EV charging station entity."""
    __tablename__ = "station"
    __table_args__ = {"schema": "inventory"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    partner_id = Column(UUID(as_uuid=True), ForeignKey("inventory.partner.id"), nullable=False)
    name = Column(String(255), nullable=False)
    address = Column(String(500), nullable=False)
    latitude = Column(Float, nullable=False)  # Range: -90 to 90
    longitude = Column(Float, nullable=False)  # Range: -180 to 180
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class ChargerStatus(str, enum.Enum):
    """Charger status enum."""
    AVAILABLE = "available"
    IN_USE = "in_use"
    MAINTENANCE = "maintenance"


class Charger(Base):
    """EV charger (physical charging point) entity."""
    __tablename__ = "charger"
    __table_args__ = {"schema": "inventory"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    station_id = Column(UUID(as_uuid=True), ForeignKey("inventory.station.id"), nullable=False)
    connector_type = Column(String(50), nullable=False)  # Type2, CCS, CHAdeMO, etc.
    power_kw = Column(Float, nullable=False)  # Power in kilowatts
    status = Column(Enum(ChargerStatus, native_enum=False), nullable=False, default="available")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
