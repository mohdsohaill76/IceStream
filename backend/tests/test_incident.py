from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from app.models.incident import Incident


def get_valid_incident_payload():
    """Helper to return a valid incident payload dictionary."""
    return {
        "incident_id": "INC-001",
        "stage": "data_quality",
        "severity": "critical",
        "message": "Transaction error rate exceeded threshold",
        "status": "open",
        "timestamp": datetime.now(timezone.utc),
    }


def test_valid_incident():
    """Test that a valid Incident payload passes validation and populates fields correctly."""
    payload = get_valid_incident_payload()
    incident = Incident(**payload)
    assert incident.incident_id == "INC-001"
    assert incident.stage == "data_quality"
    assert incident.severity == "critical"
    assert incident.message == "Transaction error rate exceeded threshold"
    assert incident.status == "open"
    assert incident.timestamp == payload["timestamp"]


def test_missing_incident_id():
    """Test that missing incident_id raises ValidationError."""
    payload = get_valid_incident_payload()
    del payload["incident_id"]
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_missing_stage():
    """Test that missing stage raises ValidationError."""
    payload = get_valid_incident_payload()
    del payload["stage"]
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_missing_severity():
    """Test that missing severity raises ValidationError."""
    payload = get_valid_incident_payload()
    del payload["severity"]
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_missing_message():
    """Test that missing message raises ValidationError."""
    payload = get_valid_incident_payload()
    del payload["message"]
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_missing_status():
    """Test that missing status raises ValidationError."""
    payload = get_valid_incident_payload()
    del payload["status"]
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_missing_timestamp():
    """Test that missing timestamp raises ValidationError."""
    payload = get_valid_incident_payload()
    del payload["timestamp"]
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_invalid_timestamp():
    """Test that invalid timestamp string raises ValidationError."""
    payload = get_valid_incident_payload()
    payload["timestamp"] = "not-a-valid-timestamp"
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_empty_incident_id():
    """Test that empty incident_id raises ValidationError."""
    payload = get_valid_incident_payload()
    payload["incident_id"] = ""
    with pytest.raises(ValidationError):
        Incident(**payload)


def test_empty_message():
    """Test that empty message raises ValidationError."""
    payload = get_valid_incident_payload()
    payload["message"] = ""
    with pytest.raises(ValidationError):
        Incident(**payload)
