"""Initialize inventory schema with partner, station, charger tables.

Revision ID: 001_init_inventory_schema
Revises: 
Create Date: 2026-06-08

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_init_inventory_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create inventory schema
    op.execute("CREATE SCHEMA IF NOT EXISTS inventory")
    
    # Create partner table
    op.create_table(
        'partner',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='inventory'
    )
    
    # Create station table
    op.create_table(
        'station',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('partner_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('address', sa.String(500), nullable=False),
        sa.Column('latitude', sa.Float(), nullable=False),
        sa.Column('longitude', sa.Float(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['partner_id'], ['inventory.partner.id'], ),
        sa.PrimaryKeyConstraint('id'),
        schema='inventory'
    )
    
    # Create charger table
    op.create_table(
        'charger',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('station_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('connector_type', sa.String(50), nullable=False),
        sa.Column('power_kw', sa.Float(), nullable=False),
        sa.Column('status', sa.Enum('available', 'in_use', 'maintenance', name='chargerstatus'), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['station_id'], ['inventory.station.id'], ),
        sa.PrimaryKeyConstraint('id'),
        schema='inventory'
    )


def downgrade() -> None:
    op.drop_table('charger', schema='inventory')
    op.drop_table('station', schema='inventory')
    op.drop_table('partner', schema='inventory')
    op.execute("DROP SCHEMA IF EXISTS inventory")
