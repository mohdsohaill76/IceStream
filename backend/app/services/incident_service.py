from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import sys
from typing import List, Optional, Set

from app.models.incident import Incident

logger = logging.getLogger("backend.incident_service")

# Retained for backwards-compatibility; not used for live API queries
_MOCK_INCIDENTS: list[Incident] = []

# Overrides for test isolation
_STATUS_FILE_OVERRIDE: Optional[Path] = None
_READER_OVERRIDE: Optional[object] = None


def set_status_file_override(path: Optional[Path]) -> None:
    """Set or clear a circuit status file path override for testing."""
    global _STATUS_FILE_OVERRIDE
    _STATUS_FILE_OVERRIDE = path


def set_reader_override(reader: Optional[object]) -> None:
    """Set or clear an IcebergReader instance override for testing."""
    global _READER_OVERRIDE
    _READER_OVERRIDE = reader


def _ensure_repo_root_in_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def get_status_file_path() -> Path:
    """Return the active path to the shared circuit breaker status file."""
    if _STATUS_FILE_OVERRIDE is not None:
        return _STATUS_FILE_OVERRIDE

    repo_root = Path(__file__).resolve().parents[3]
    return (
        repo_root
        / "data-quality"
        / "data_quality"
        / "app"
        / "circuit_breaker"
        / "shared_circuit_status.json"
    )


def get_reader() -> Optional[object]:
    """Obtain the active IcebergReader instance, or None if unavailable."""
    if _READER_OVERRIDE is not None:
        return _READER_OVERRIDE

    _ensure_repo_root_in_path()
    try:
        from iceberg.src.iceberg_reader import IcebergReader

        return IcebergReader()
    except Exception as e:
        logger.warning(f"Could not initialize IcebergReader for incidents: {e}")
        return None


def _get_circuit_breaker_incidents() -> List[Incident]:
    """Derive incidents from the Data Quality circuit-breaker shared status file."""
    path = get_status_file_path()
    incidents: List[Incident] = []

    if not path.exists():
        logger.debug(f"Circuit status file not found at {path}; no circuit incidents.")
        return incidents

    try:
        raw_text = path.read_text(encoding="utf-8")
        data = json.loads(raw_text)
    except Exception as e:
        logger.error(f"Failed to read or parse circuit status file at {path}: {e}")
        return [
            Incident(
                incident_id="INC-DQ-STATUS-UNREADABLE",
                stage="data_quality",
                severity="high",
                message=f"Circuit status file unreadable or corrupt: {e}",
                status="open",
                timestamp=datetime.now(timezone.utc),
            )
        ]

    state = data.get("state", "CLOSED")
    error_rate = float(data.get("error_rate", 0.0))
    last_updated = float(data.get("last_updated", 0.0))
    is_alive = bool(data.get("is_alive", True))

    ts = (
        datetime.fromtimestamp(last_updated, tz=timezone.utc)
        if last_updated > 0
        else datetime.now(timezone.utc)
    )
    stable_suffix = str(int(last_updated)) if last_updated > 0 else "CURRENT"

    if not is_alive or state == "STOPPED":
        incidents.append(
            Incident(
                incident_id=f"INC-DQ-STOPPED-{stable_suffix}",
                stage="data_quality",
                severity="critical",
                message="Data Quality validator service is stopped or unresponsive",
                status="open",
                timestamp=ts,
            )
        )
    elif state == "OPEN":
        rate_display = error_rate * 100 if error_rate <= 1.0 else error_rate
        incidents.append(
            Incident(
                incident_id=f"INC-CB-OPEN-{stable_suffix}",
                stage="data_quality",
                severity="critical",
                message=f"Circuit breaker tripped (OPEN): transaction error rate {rate_display:.1f}% exceeded 2.0% threshold",
                status="open",
                timestamp=ts,
            )
        )
    elif state == "HALF_OPEN":
        incidents.append(
            Incident(
                incident_id=f"INC-CB-HALF_OPEN-{stable_suffix}",
                stage="data_quality",
                severity="medium",
                message="Circuit breaker in recovery (HALF_OPEN): evaluating trial transaction batch",
                status="acknowledged",
                timestamp=ts,
            )
        )
    # When state is CLOSED and is_alive is True, no incident is produced (healthy state).

    return incidents


def _get_dlq_incidents(limit: int = 100) -> List[Incident]:
    """Derive incidents from Iceberg Dead Letter Queue records."""
    reader = get_reader()
    if reader is None:
        return []

    try:
        records = reader.get_dlq_records(limit=limit)
    except Exception as e:
        logger.error(f"Failed to query DLQ records from lakehouse: {e}")
        return [
            Incident(
                incident_id="INC-DLQ-STORAGE-ERROR",
                stage="iceberg",
                severity="high",
                message=f"Failed to query Dead Letter Queue records from Lakehouse: {e}",
                status="open",
                timestamp=datetime.now(timezone.utc),
            )
        ]

    incidents: List[Incident] = []
    seen_ids: Set[str] = set()

    for record in records:
        tx_id = record.get("transaction_id")
        reason = record.get("error_reason") or "Transaction quarantined in Dead Letter Queue"
        stage = record.get("source_stage") or "dlq"
        raw_ts = record.get("timestamp")

        # Stable, deterministic incident ID
        if tx_id:
            clean_id = str(tx_id).replace("-", "")[:12].upper()
            inc_id = f"INC-DLQ-{clean_id}"
        else:
            digest = hashlib.sha256(f"{raw_ts}:{reason}".encode("utf-8")).hexdigest()[:10].upper()
            inc_id = f"INC-DLQ-{digest}"

        if inc_id in seen_ids:
            continue
        seen_ids.add(inc_id)

        # Parse timestamp safely
        ts = None
        if raw_ts:
            try:
                ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
            except Exception:
                pass
        if ts is None:
            ts = datetime.now(timezone.utc)

        reason_lower = reason.lower()
        if (
            "critical" in reason_lower
            or "negative" in reason_lower
            or "corrupt" in reason_lower
            or "invalid" in reason_lower
            or "-" in reason
        ):
            severity = "high"
        elif "missing" in reason_lower or "error" in reason_lower:
            severity = "medium"
        else:
            severity = "low"

        incidents.append(
            Incident(
                incident_id=inc_id,
                stage=stage,
                severity=severity,
                message=reason,
                status="open",
                timestamp=ts,
            )
        )

    return incidents


def get_incidents() -> List[Incident]:
    """Retrieve operational pipeline incidents derived from real circuit-breaker status and DLQ records."""
    cb_incidents = _get_circuit_breaker_incidents()
    dlq_incidents = _get_dlq_incidents()

    all_incidents = cb_incidents + dlq_incidents

    # Sort newest-to-oldest
    all_incidents.sort(key=lambda inc: inc.timestamp, reverse=True)
    return all_incidents


def get_incident_by_id(incident_id: str) -> Optional[Incident]:
    """Retrieve an operational incident by its unique identifier."""
    incidents = get_incidents()
    for inc in incidents:
        if inc.incident_id == incident_id:
            return inc
    return None
