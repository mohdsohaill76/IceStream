from datetime import datetime, timezone

from app.models.incident import Incident

# In-memory mock incident store
_MOCK_INCIDENTS: list[Incident] = [
    Incident(
        incident_id="INC-001",
        stage="data_quality",
        severity="critical",
        message="Transaction error rate exceeded threshold",
        status="open",
        timestamp=datetime.now(timezone.utc),
    ),
    Incident(
        incident_id="INC-002",
        stage="flink",
        severity="medium",
        message="Flink processing latency increased",
        status="acknowledged",
        timestamp=datetime.now(timezone.utc),
    ),
]


def get_incidents() -> list[Incident]:
    """Retrieve the current list of operational incidents."""
    return _MOCK_INCIDENTS


def get_incident_by_id(incident_id: str) -> Incident | None:
    """Retrieve a specific operational incident by its ID."""
    for incident in _MOCK_INCIDENTS:
        if incident.incident_id == incident_id:
            return incident
    return None
