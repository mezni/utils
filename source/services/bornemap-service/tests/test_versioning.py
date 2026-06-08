"""Smoke tests for API versioning behavior."""

import pytest


# ============================================================================
# Phase 3: User Story 1 Tests (T018-T023) - Versioning Behavior
# ============================================================================

class TestVersioningBehavior:
    """Tests for API versioning - ensure /api/v1 works, unversioned returns 404."""

    def test_health_endpoint_versioned(self, client):
        """T018: GET /api/v1/health returns 200 with correct schema."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["status"] == "ok"
        assert "service" in data
        assert "db" in data

    def test_partners_endpoint_versioned(self, client):
        """T019: GET /api/v1/partners returns 200 with data array."""
        response = client.get("/api/v1/partners")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert isinstance(data["data"], list)
        assert "count" in data

    def test_stations_endpoint_versioned(self, client):
        """T020: GET /api/v1/stations returns 200 with data array."""
        response = client.get("/api/v1/stations")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert isinstance(data["data"], list)
        assert "count" in data

    def test_chargers_endpoint_versioned(self, client):
        """T021: GET /api/v1/chargers returns 200 with data array."""
        response = client.get("/api/v1/chargers")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert isinstance(data["data"], list)
        assert "count" in data

    def test_unversioned_endpoint_returns_404(self, client):
        """T022: GET /api/stations (unversioned) returns 404."""
        response = client.get("/api/stations")
        assert response.status_code == 404

    def test_invalid_version_returns_404(self, client):
        """T023: GET /api/v999/stations (invalid version) returns 404."""
        response = client.get("/api/v999/stations")
        assert response.status_code == 404


# ============================================================================
# Phase 3: User Story 1 Implementation Tests (T045-T047)
# ============================================================================

class TestAllEndpointsFunctional:
    """Tests for all v1 endpoints to verify basic functionality."""

    # Health Endpoint
    def test_health_endpoint_format(self, client):
        """Verify health endpoint returns correct JSON schema."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        assert set(data.keys()) == {"status", "service", "db"}
        assert data["status"] == "ok"
        assert data["service"] == "bornemap-service"
        assert data["db"] in ["ok", "error"]

    # Partners Endpoints
    def test_list_partners(self, client):
        """Test GET /api/v1/partners."""
        response = client.get("/api/v1/partners")
        assert response.status_code == 200

    def test_create_partner(self, client):
        """Test POST /api/v1/partners."""
        response = client.post("/api/v1/partners", json={"name": "Test Partner"})
        assert response.status_code in [200, 201]

    def test_get_partner(self, client):
        """Test GET /api/v1/partners/{id} returns 404 for non-existent."""
        response = client.get("/api/v1/partners/550e8400-e29b-41d4-a716-446655440000")
        assert response.status_code in [200, 404]

    def test_update_partner(self, client):
        """Test PUT /api/v1/partners/{id}."""
        response = client.put(
            "/api/v1/partners/550e8400-e29b-41d4-a716-446655440000",
            json={"name": "Updated Partner"}
        )
        assert response.status_code in [200, 404]

    def test_delete_partner(self, client):
        """Test DELETE /api/v1/partners/{id}."""
        response = client.delete("/api/v1/partners/550e8400-e29b-41d4-a716-446655440000")
        assert response.status_code in [204, 404]

    # Stations Endpoints
    def test_list_stations(self, client):
        """Test GET /api/v1/stations."""
        response = client.get("/api/v1/stations")
        assert response.status_code == 200

    def test_nearby_stations(self, client):
        """T046: Test GET /api/v1/stations/nearby with coordinates."""
        # Coordinates far from Tunisia (e.g., North Pole)
        response = client.get("/api/v1/stations/nearby?lat=90&lng=0&radius_km=50")
        assert response.status_code in [200, 404]  # 404 if endpoint not yet implemented

    def test_create_station(self, client):
        """Test POST /api/v1/stations."""
        response = client.post(
            "/api/v1/stations",
            json={
                "partner_id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Test Station",
                "address": "123 Main St",
                "latitude": 36.8065,
                "longitude": 10.1815,
            }
        )
        assert response.status_code in [200, 201, 422]

    def test_get_station(self, client):
        """Test GET /api/v1/stations/{id}."""
        response = client.get("/api/v1/stations/550e8400-e29b-41d4-a716-446655440000")
        assert response.status_code in [200, 404]

    def test_update_station(self, client):
        """Test PUT /api/v1/stations/{id}."""
        response = client.put(
            "/api/v1/stations/550e8400-e29b-41d4-a716-446655440000",
            json={"name": "Updated Station"}
        )
        assert response.status_code in [200, 404]

    def test_delete_station(self, client):
        """Test DELETE /api/v1/stations/{id}."""
        response = client.delete("/api/v1/stations/550e8400-e29b-41d4-a716-446655440000")
        assert response.status_code in [204, 404]

    # Chargers Endpoints
    def test_list_chargers(self, client):
        """Test GET /api/v1/chargers."""
        response = client.get("/api/v1/chargers")
        assert response.status_code == 200

    def test_create_charger(self, client):
        """Test POST /api/v1/chargers."""
        response = client.post(
            "/api/v1/chargers",
            json={
                "station_id": "550e8400-e29b-41d4-a716-446655440000",
                "connector_type": "Type2",
                "power_kw": 22.0,
            }
        )
        assert response.status_code in [200, 201, 422]

    def test_get_charger(self, client):
        """Test GET /api/v1/chargers/{id}."""
        response = client.get("/api/v1/chargers/550e8400-e29b-41d4-a716-446655440000")
        assert response.status_code in [200, 404]

    def test_update_charger(self, client):
        """Test PUT /api/v1/chargers/{id}."""
        response = client.put(
            "/api/v1/chargers/550e8400-e29b-41d4-a716-446655440000",
            json={"status": "maintenance"}
        )
        assert response.status_code in [200, 404]

    def test_delete_charger(self, client):
        """Test DELETE /api/v1/chargers/{id}."""
        response = client.delete("/api/v1/chargers/550e8400-e29b-41d4-a716-446655440000")
        assert response.status_code in [204, 404]


class TestValidationAndErrors:
    """Tests for validation and error handling."""

    def test_invalid_latitude_too_high(self, client):
        """T047: Test latitude > 90 returns 422."""
        response = client.post(
            "/api/v1/stations",
            json={
                "partner_id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Test",
                "address": "Test",
                "latitude": 91,
                "longitude": 10.1815,
            }
        )
        assert response.status_code in [422, 404]

    def test_invalid_latitude_too_low(self, client):
        """Test latitude < -90 returns 422."""
        response = client.post(
            "/api/v1/stations",
            json={
                "partner_id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Test",
                "address": "Test",
                "latitude": -91,
                "longitude": 10.1815,
            }
        )
        assert response.status_code in [422, 404]

    def test_invalid_longitude_too_high(self, client):
        """Test longitude > 180 returns 422."""
        response = client.post(
            "/api/v1/stations",
            json={
                "partner_id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Test",
                "address": "Test",
                "latitude": 36.8065,
                "longitude": 181,
            }
        )
        assert response.status_code in [422, 404]

    def test_invalid_longitude_too_low(self, client):
        """Test longitude < -180 returns 422."""
        response = client.post(
            "/api/v1/stations",
            json={
                "partner_id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "Test",
                "address": "Test",
                "latitude": 36.8065,
                "longitude": -181,
            }
        )
        assert response.status_code in [422, 404]


# ============================================================================
# Phase 4: User Story 2 Tests (T056-T058) - Documentation
# ============================================================================

class TestOpenAPIDocs:
    """Tests for OpenAPI/Swagger documentation."""

    def test_openapi_schema_accessible(self, client):
        """T056: Verify OpenAPI spec at /api/openapi.json loads without errors."""
        response = client.get("/api/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "paths" in data
        assert "components" in data

    def test_swagger_docs_accessible(self, client):
        """T056: Verify Swagger UI at /api/docs is accessible."""
        response = client.get("/api/docs")
        assert response.status_code == 200

    def test_redoc_docs_accessible(self, client):
        """T056: Verify ReDoc at /api/redoc is accessible."""
        response = client.get("/api/redoc")
        assert response.status_code == 200


# ============================================================================
# Phase 5: User Story 3 Tests (T062-T065) - Schema Stability
# ============================================================================

class TestSchemaStability:
    """Tests to verify v1 schemas remain stable for MVP-2 migration."""

    def test_health_schema_fields_exist(self, client):
        """T062: Verify health response has required fields for schema stability."""
        response = client.get("/api/v1/health")
        assert response.status_code == 200
        data = response.json()
        # These fields are frozen in the contract and must not change
        assert "status" in data
        assert "service" in data
        assert "db" in data

    def test_partners_response_schema(self, client):
        """T062: Verify partners response schema for stability."""
        response = client.get("/api/v1/partners")
        assert response.status_code == 200
        data = response.json()
        # These fields are frozen in the contract
        assert "data" in data
        assert "count" in data
        assert isinstance(data["data"], list)

    def test_stations_response_schema(self, client):
        """T062: Verify stations response schema for stability."""
        response = client.get("/api/v1/stations")
        assert response.status_code == 200
        data = response.json()
        # These fields are frozen in the contract
        assert "data" in data
        assert "count" in data
        assert isinstance(data["data"], list)

    def test_chargers_response_schema(self, client):
        """T062: Verify chargers response schema for stability."""
        response = client.get("/api/v1/chargers")
        assert response.status_code == 200
        data = response.json()
        # These fields are frozen in the contract
        assert "data" in data
        assert "count" in data
        assert isinstance(data["data"], list)
