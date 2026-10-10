import json
import tempfile
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.incident import Incident
from app.services import incident_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_incident_overrides():
    """Ensure service overrides are cleared after each test."""
    incident_service.set_status_file_override(None)
    incident_service.set_reader_override(None)
    yield
    incident_service.set_status_file_override(None)
    incident_service.set_reader_override(None)


# ==============================================================================
# LIVE REPOSITORY / CONTRACT TESTS
# ==============================================================================


def test_get_incidents_list_live():
    """Test GET /api/v1/incidents returns 200, a valid list, and strict Incident contracts."""
    response = client.get("/api/v1/incidents")
    assert response.status_code == 200

    data = response.json()
    assert isinstance(data, list)

    for item in data:
        validated = Incident(**item)
        assert validated.incident_id == item["incident_id"]
        assert validated.stage in ["kafka", "flink", "data_quality", "iceberg", "dlq"]
        assert validated.severity in ["low", "medium", "high", "critical"]
        assert validated.status in ["open", "acknowledged", "resolved"]
        assert len(validated.message) > 0


def test_incident_lookup_by_id_not_found():
    """Test GET /api/v1/incidents/DOES-NOT-EXIST returns 404 with descriptive error detail."""
    response = client.get("/api/v1/incidents/DOES-NOT-EXIST")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data
    assert "DOES-NOT-EXIST" in data["detail"]


# ==============================================================================
# CIRCUIT BREAKER INCIDENT TESTS
# ==============================================================================


def test_circuit_breaker_open_state_creates_incident():
    """Verify circuit breaker OPEN state generates a critical incident with accurate error rate."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(
            {
                "state": "OPEN",
                "error_rate": 0.045,
                "last_updated": 1791540000.0,
                "pid": 9999,
                "is_alive": True,
            },
            f,
        )
        f_path = Path(f.name)

    class EmptyReader:
        def get_dlq_records(self, limit=100):
            return []

    incident_service.set_status_file_override(f_path)
    incident_service.set_reader_override(EmptyReader())

    try:
        response = client.get("/api/v1/incidents")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        inc = data[0]
        assert inc["incident_id"] == "INC-CB-OPEN-1791540000"
        assert inc["stage"] == "data_quality"
        assert inc["severity"] == "critical"
        assert inc["status"] == "open"
        assert "4.5%" in inc["message"]

        # Verify lookup by ID works for this generated incident
        lookup_res = client.get(f"/api/v1/incidents/{inc['incident_id']}")
        assert lookup_res.status_code == 200
        assert lookup_res.json()["incident_id"] == inc["incident_id"]
    finally:
        f_path.unlink(missing_ok=True)


def test_circuit_breaker_closed_state_no_incident():
    """Verify circuit breaker CLOSED (healthy) state does not create a false incident."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(
            {
                "state": "CLOSED",
                "error_rate": 0.0,
                "last_updated": 1791540000.0,
                "pid": 9999,
                "is_alive": True,
            },
            f,
        )
        f_path = Path(f.name)

    class EmptyReader:
        def get_dlq_records(self, limit=100):
            return []

    incident_service.set_status_file_override(f_path)
    incident_service.set_reader_override(EmptyReader())

    try:
        response = client.get("/api/v1/incidents")
        assert response.status_code == 200
        assert response.json() == []
    finally:
        f_path.unlink(missing_ok=True)


def test_circuit_breaker_half_open_state_creates_incident():
    """Verify circuit breaker HALF_OPEN state generates an acknowledged medium-severity incident."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(
            {
                "state": "HALF_OPEN",
                "error_rate": 0.0,
                "last_updated": 1791540100.0,
                "pid": 9999,
                "is_alive": True,
            },
            f,
        )
        f_path = Path(f.name)

    class EmptyReader:
        def get_dlq_records(self, limit=100):
            return []

    incident_service.set_status_file_override(f_path)
    incident_service.set_reader_override(EmptyReader())

    try:
        response = client.get("/api/v1/incidents")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        inc = data[0]
        assert inc["incident_id"] == "INC-CB-HALF_OPEN-1791540100"
        assert inc["stage"] == "data_quality"
        assert inc["severity"] == "medium"
        assert inc["status"] == "acknowledged"
    finally:
        f_path.unlink(missing_ok=True)


def test_circuit_breaker_stopped_process_creates_incident():
    """Verify unresponsive/stopped Data Quality process produces a critical incident."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        json.dump(
            {
                "state": "STOPPED",
                "error_rate": 0.0,
                "last_updated": 1791540200.0,
                "pid": 9999,
                "is_alive": False,
            },
            f,
        )
        f_path = Path(f.name)

    class EmptyReader:
        def get_dlq_records(self, limit=100):
            return []

    incident_service.set_status_file_override(f_path)
    incident_service.set_reader_override(EmptyReader())

    try:
        response = client.get("/api/v1/incidents")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        inc = data[0]
        assert inc["incident_id"] == "INC-DQ-STOPPED-1791540200"
        assert inc["severity"] == "critical"
        assert "stopped" in inc["message"].lower()
    finally:
        f_path.unlink(missing_ok=True)


# ==============================================================================
# DLQ INCIDENT TESTS
# ==============================================================================


def test_dlq_records_create_stable_incidents():
    """Verify Dead Letter Queue records produce structured incidents with stable IDs."""
    non_existent_path = Path(tempfile.gettempdir()) / "non_existent_status.json"
    incident_service.set_status_file_override(non_existent_path)

    sample_dlq = [
        {
            "transaction_id": "tx-aabb-ccdd",
            "error_reason": "Invalid transaction amount: -100.0",
            "source_stage": "flink",
            "timestamp": "2026-10-09T10:00:00Z",
        },
        {
            "transaction_id": "tx-1122-3344",
            "error_reason": "Missing required customer_id",
            "source_stage": "data_quality",
            "timestamp": "2026-10-09T10:05:00Z",
        },
    ]

    class DLQReader:
        def get_dlq_records(self, limit=100):
            return sample_dlq

    incident_service.set_reader_override(DLQReader())

    response = client.get("/api/v1/incidents")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2

    # Check stable deterministic IDs
    assert data[0]["incident_id"] == "INC-DLQ-TX11223344"
    assert data[0]["stage"] == "data_quality"
    assert data[0]["message"] == "Missing required customer_id"

    assert data[1]["incident_id"] == "INC-DLQ-TXAABBCCDD"
    assert data[1]["stage"] == "flink"
    assert data[1]["severity"] == "high"


def test_dlq_empty_produces_no_incidents():
    """Verify empty DLQ returns an empty list without fabricating records."""
    non_existent_path = Path(tempfile.gettempdir()) / "non_existent_status.json"
    incident_service.set_status_file_override(non_existent_path)

    class EmptyReader:
        def get_dlq_records(self, limit=100):
            return []

    incident_service.set_reader_override(EmptyReader())

    response = client.get("/api/v1/incidents")
    assert response.status_code == 200
    assert response.json() == []


def test_stable_incident_identity_and_deduplication():
    """Verify repeated requests return identical incident IDs and duplicate DLQ records are deduplicated."""
    non_existent_path = Path(tempfile.gettempdir()) / "non_existent_status.json"
    incident_service.set_status_file_override(non_existent_path)

    duplicate_dlq = [
        {
            "transaction_id": "tx-dup-1234",
            "error_reason": "Schema validation failed",
            "source_stage": "flink",
            "timestamp": "2026-10-09T10:00:00Z",
        },
        {
            "transaction_id": "tx-dup-1234",
            "error_reason": "Schema validation failed (retry)",
            "source_stage": "flink",
            "timestamp": "2026-10-09T10:01:00Z",
        },
    ]

    class DupReader:
        def get_dlq_records(self, limit=100):
            return duplicate_dlq

    incident_service.set_reader_override(DupReader())

    # First request
    res1 = client.get("/api/v1/incidents")
    assert res1.status_code == 200
    data1 = res1.json()
    # Deduplication ensures only 1 unique incident ID
    assert len(data1) == 1
    inc_id = data1[0]["incident_id"]

    # Second request must return identical ID
    res2 = client.get("/api/v1/incidents")
    assert res2.status_code == 200
    data2 = res2.json()
    assert len(data2) == 1
    assert data2[0]["incident_id"] == inc_id


# ==============================================================================
# SOURCE-READ FAILURE TESTS (EXPLICIT DIAGNOSTIC INCIDENTS)
# ==============================================================================


def test_unreadable_circuit_status_creates_diagnostic_incident():
    """Verify that corrupt or unreadable status file generates a high-severity diagnostic incident."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        f.write("{invalid json content, not parseable}")
        f_path = Path(f.name)

    class EmptyReader:
        def get_dlq_records(self, limit=100):
            return []

    incident_service.set_status_file_override(f_path)
    incident_service.set_reader_override(EmptyReader())

    try:
        response = client.get("/api/v1/incidents")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

        inc = data[0]
        assert inc["incident_id"] == "INC-DQ-STATUS-UNREADABLE"
        assert inc["severity"] == "high"
        assert "unreadable or corrupt" in inc["message"].lower()
    finally:
        f_path.unlink(missing_ok=True)


def test_dlq_storage_read_failure_creates_diagnostic_incident():
    """Verify that lakehouse DLQ query failure produces a high-severity diagnostic incident."""
    non_existent_path = Path(tempfile.gettempdir()) / "non_existent_status.json"
    incident_service.set_status_file_override(non_existent_path)

    class FailingReader:
        def get_dlq_records(self, limit=100):
            raise RuntimeError("Lakehouse SQLite catalog disk I/O error")

    incident_service.set_reader_override(FailingReader())

    response = client.get("/api/v1/incidents")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1

    inc = data[0]
    assert inc["incident_id"] == "INC-DLQ-STORAGE-ERROR"
    assert inc["stage"] == "iceberg"
    assert inc["severity"] == "high"
    assert "Lakehouse SQLite catalog disk I/O error" in inc["message"]
