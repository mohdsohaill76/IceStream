import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_get_incidents_list():
    """Test GET /api/v1/incidents returns 200, a list of at least 2 incidents, and required fields."""
    response = client.get("/api/v1/incidents")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2

    required_keys = {"incident_id", "stage", "severity", "message", "status", "timestamp"}
    for incident in data:
        for key in required_keys:
            assert key in incident


def test_verify_mock_incident_001_exists():
    """Verify the mock incident INC-001 exists with correct stage, severity, and status."""
    response = client.get("/api/v1/incidents")
    assert response.status_code == 200

    data = response.json()
    incident_map = {inc["incident_id"]: inc for inc in data}

    assert "INC-001" in incident_map
    inc_001 = incident_map["INC-001"]
    assert inc_001["stage"] == "data_quality"
    assert inc_001["severity"] == "critical"
    assert inc_001["status"] == "open"


def test_verify_mock_incident_002_exists():
    """Verify the mock incident INC-002 exists with correct stage, severity, and status."""
    response = client.get("/api/v1/incidents")
    assert response.status_code == 200

    data = response.json()
    incident_map = {inc["incident_id"]: inc for inc in data}

    assert "INC-002" in incident_map
    inc_002 = incident_map["INC-002"]
    assert inc_002["stage"] == "flink"
    assert inc_002["severity"] == "medium"
    assert inc_002["status"] == "acknowledged"


def test_get_incident_by_id_inc_001():
    """Test GET /api/v1/incidents/INC-001 returns 200 and correct field values."""
    response = client.get("/api/v1/incidents/INC-001")
    assert response.status_code == 200

    data = response.json()
    assert data["incident_id"] == "INC-001"
    assert data["stage"] == "data_quality"
    assert data["severity"] == "critical"
    assert data["status"] == "open"


def test_get_incident_by_id_inc_002():
    """Test GET /api/v1/incidents/INC-002 returns 200 and incident_id INC-002."""
    response = client.get("/api/v1/incidents/INC-002")
    assert response.status_code == 200

    data = response.json()
    assert data["incident_id"] == "INC-002"


def test_get_incident_by_id_not_found():
    """Test GET /api/v1/incidents/DOES-NOT-EXIST returns 404 and error detail."""
    response = client.get("/api/v1/incidents/DOES-NOT-EXIST")
    assert response.status_code == 404

    data = response.json()
    assert "detail" in data
    assert "DOES-NOT-EXIST" in data["detail"]


def test_individual_incident_response_structure():
    """Verify the individual incident response contains all required fields of the contract."""
    response = client.get("/api/v1/incidents/INC-001")
    assert response.status_code == 200

    data = response.json()
    assert "incident_id" in data
    assert "stage" in data
    assert "severity" in data
    assert "message" in data
    assert "status" in data
    assert "timestamp" in data


