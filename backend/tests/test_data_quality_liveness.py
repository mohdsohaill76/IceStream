import json
import os
import time
from pathlib import Path
import pytest

from app.services.pipeline_service import _check_data_quality, get_pipeline_status

STATUS_FILE = (
    Path(__file__).resolve().parents[2]
    / "data-quality"
    / "data_quality"
    / "app"
    / "circuit_breaker"
    / "shared_circuit_status.json"
)



@pytest.fixture(autouse=True)
def clean_status_file():
    """Ensure test isolation by cleaning up shared status file."""
    if STATUS_FILE.exists():
        STATUS_FILE.unlink(missing_ok=True)
    yield
    if STATUS_FILE.exists():
        STATUS_FILE.unlink(missing_ok=True)


def test_dq_normal_operation_reports_healthy():
    """Normal running operation with CLOSED circuit reports healthy."""
    STATUS_FILE.write_text(
        json.dumps({
            "state": "CLOSED",
            "is_alive": True,
            "pid": os.getpid(),
            "last_updated": time.time(),
            "error_rate": 0.0,
        }),
        encoding="utf-8",
    )
    stage = _check_data_quality()
    assert stage.name == "data_quality"
    assert stage.status == "healthy"


def test_dq_open_circuit_reports_unhealthy():
    """OPEN circuit breaker state reports unhealthy."""
    STATUS_FILE.write_text(
        json.dumps({
            "state": "OPEN",
            "is_alive": True,
            "pid": os.getpid(),
            "last_updated": time.time(),
            "error_rate": 0.05,
        }),
        encoding="utf-8",
    )
    stage = _check_data_quality()
    assert stage.name == "data_quality"
    assert stage.status == "unhealthy"

    overall = get_pipeline_status()
    assert overall.status == "unhealthy"


def test_dq_half_open_circuit_reports_degraded():
    """HALF_OPEN recovery state reports degraded."""
    STATUS_FILE.write_text(
        json.dumps({
            "state": "HALF_OPEN",
            "is_alive": True,
            "pid": os.getpid(),
            "last_updated": time.time(),
            "error_rate": 0.01,
        }),
        encoding="utf-8",
    )
    stage = _check_data_quality()
    assert stage.name == "data_quality"
    assert stage.status == "degraded"


def test_dq_stopped_process_reports_unhealthy():
    """Explicitly stopped process reports unhealthy."""
    STATUS_FILE.write_text(
        json.dumps({
            "state": "STOPPED",
            "is_alive": False,
            "pid": None,
            "last_updated": time.time(),
            "error_rate": 0.0,
        }),
        encoding="utf-8",
    )
    stage = _check_data_quality()
    assert stage.name == "data_quality"
    assert stage.status == "unhealthy"


def test_dq_dead_pid_stale_heartbeat_reports_unhealthy():
    """Stale heartbeat with dead PID cannot falsely report healthy."""
    STATUS_FILE.write_text(
        json.dumps({
            "state": "CLOSED",
            "is_alive": True,
            "pid": 9999999,  # Non-existent PID
            "last_updated": time.time() - 120,  # 2 minutes ago
            "error_rate": 0.0,
        }),
        encoding="utf-8",
    )
    stage = _check_data_quality()
    assert stage.name == "data_quality"
    assert stage.status == "unhealthy"


def test_dq_stale_heartbeat_without_crash_reports_degraded():
    """Heartbeat older than threshold on running process reports degraded."""
    STATUS_FILE.write_text(
        json.dumps({
            "state": "CLOSED",
            "is_alive": True,
            "pid": os.getpid(),
            "last_updated": time.time() - 120,
            "error_rate": 0.0,
        }),
        encoding="utf-8",
    )
    stage = _check_data_quality()
    assert stage.name == "data_quality"
    assert stage.status == "degraded"


def test_dq_process_restart_recovers_to_healthy():
    """Process restart replaces dead PID state with fresh heartbeat."""
    # First: stale dead PID
    STATUS_FILE.write_text(
        json.dumps({
            "state": "CLOSED",
            "is_alive": True,
            "pid": 9999999,
            "last_updated": time.time() - 120,
        }),
        encoding="utf-8",
    )
    assert _check_data_quality().status == "unhealthy"

    # Second: process restarts and writes current PID with fresh timestamp
    STATUS_FILE.write_text(
        json.dumps({
            "state": "CLOSED",
            "is_alive": True,
            "pid": os.getpid(),
            "last_updated": time.time(),
        }),
        encoding="utf-8",
    )
    assert _check_data_quality().status == "healthy"
