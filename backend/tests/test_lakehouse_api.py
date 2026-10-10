import tempfile
import uuid
from pathlib import Path
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models.lakehouse import LakehouseMetrics, SnapshotMetadata
from app.services.lakehouse_service import (
    LakehouseServiceError,
    get_lakehouse_metrics,
    get_lakehouse_snapshots,
    set_reader_override,
)
from iceberg.catalog.catalog_manager import init_catalog_and_tables
from iceberg.src.iceberg_reader import IcebergReader
from iceberg.src.iceberg_writer import IcebergWriter

client = TestClient(app)


@pytest.fixture
def temp_iceberg_reader():
    """Provides an isolated IcebergReader with clean temporary SQLite catalog."""
    tmp_dir = tempfile.mkdtemp()
    wh_dir = Path(tmp_dir) / "warehouse"
    cat_db = wh_dir / "catalog.db"
    wh_dir.mkdir(parents=True, exist_ok=True)

    reader = IcebergReader(warehouse_dir=wh_dir, catalog_db=cat_db)
    writer = IcebergWriter(warehouse_dir=wh_dir, catalog_db=cat_db)

    # Set as active override for service layer
    set_reader_override(reader)

    yield reader, writer, wh_dir, cat_db

    set_reader_override(None)
    if hasattr(reader.catalog, "engine"):
        try:
            reader.catalog.engine.dispose()
        except Exception:
            pass
    if hasattr(writer.catalog, "engine"):
        try:
            writer.catalog.engine.dispose()
        except Exception:
            pass
    import shutil

    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass


# ==============================================================================
# REAL ICEBERG STORAGE TESTS (Isolated Temp Catalog)
# ==============================================================================


def test_lakehouse_metrics_empty_catalog(temp_iceberg_reader):
    """[REAL ICEBERG STORAGE] Test GET /metrics with an empty isolated Lakehouse catalog.

    Verifies 0 counts and null snapshot IDs without fabricating values.
    """
    response = client.get("/api/v1/lakehouse/metrics")
    assert response.status_code == 200

    data = response.json()
    assert data["clean_transactions_count"] == 0
    assert data["clean_snapshot_id"] is None
    assert data["dlq_transactions_count"] == 0
    assert data["dlq_snapshot_id"] is None

    # Validate against Pydantic model contract
    validated = LakehouseMetrics(**data)
    assert validated.clean_transactions_count == 0
    assert validated.clean_snapshot_id is None


def test_lakehouse_metrics_populated_catalog(temp_iceberg_reader):
    """[REAL ICEBERG STORAGE] Test GET /metrics after committing clean and DLQ records."""
    reader, writer, _, _ = temp_iceberg_reader

    # Write clean record
    tx_id = str(uuid.uuid4())
    writer.write_clean_records(
        [
            {
                "transaction_id": tx_id,
                "customer_id": "cust-1",
                "amount": 99.99,
                "currency": "USD",
                "timestamp": "2026-10-09T12:00:00Z",
                "merchant": "IceStream Store",
                "status": "completed",
            }
        ]
    )

    # Write DLQ record
    writer.write_dlq_records(
        [
            {
                "transaction_id": "bad-tx",
                "raw_payload": "{'bad': true}",
                "error_reason": "Missing customer_id",
                "source_stage": "flink",
                "timestamp": "2026-10-09T12:00:01Z",
            }
        ]
    )

    response = client.get("/api/v1/lakehouse/metrics")
    assert response.status_code == 200

    data = response.json()
    assert data["clean_transactions_count"] == 1
    assert isinstance(data["clean_snapshot_id"], int)
    assert data["dlq_transactions_count"] == 1
    assert isinstance(data["dlq_snapshot_id"], int)


def test_lakehouse_snapshots_empty_catalog(temp_iceberg_reader):
    """[REAL ICEBERG STORAGE] Test GET /snapshots on empty tables returns empty list."""
    response = client.get("/api/v1/lakehouse/snapshots")
    assert response.status_code == 200
    assert response.json() == []


def test_lakehouse_snapshots_populated_and_ordered(temp_iceberg_reader):
    """[REAL ICEBERG STORAGE] Test GET /snapshots returns valid snapshot metadata sorted newest-first."""
    reader, writer, _, _ = temp_iceberg_reader

    # Commit first batch
    writer.write_clean_records(
        [
            {
                "transaction_id": "tx-1",
                "customer_id": "cust-1",
                "amount": 50.0,
                "currency": "USD",
                "timestamp": "2026-10-09T10:00:00Z",
                "merchant": "Store",
                "status": "completed",
            }
        ]
    )

    # Commit second batch to clean table
    writer.write_clean_records(
        [
            {
                "transaction_id": "tx-2",
                "customer_id": "cust-2",
                "amount": 75.0,
                "currency": "USD",
                "timestamp": "2026-10-09T10:01:00Z",
                "merchant": "Store",
                "status": "completed",
            }
        ]
    )

    # Commit to DLQ table
    writer.write_dlq_records(
        [
            {
                "transaction_id": "dlq-1",
                "raw_payload": "{}",
                "error_reason": "Test error",
                "source_stage": "dq",
                "timestamp": "2026-10-09T10:02:00Z",
            }
        ]
    )

    response = client.get("/api/v1/lakehouse/snapshots")
    assert response.status_code == 200

    snapshots = response.json()
    assert len(snapshots) == 3

    # Verify chronological descending order
    for i in range(len(snapshots) - 1):
        assert snapshots[i]["timestamp_ms"] >= snapshots[i + 1]["timestamp_ms"]

    # Verify schema fields for each snapshot
    for s in snapshots:
        validated = SnapshotMetadata(**s)
        assert isinstance(validated.snapshot_id, int)
        assert validated.table_name in [
            "lakehouse.clean_transactions",
            "lakehouse.dlq_transactions",
        ]
        assert validated.operation == "append"
        assert isinstance(validated.summary, dict)
        assert validated.timestamp_ms > 0
        assert validated.committed_at is not None


def test_lakehouse_snapshots_table_filter(temp_iceberg_reader):
    """[REAL ICEBERG STORAGE] Test GET /snapshots table filtering query parameter."""
    reader, writer, _, _ = temp_iceberg_reader

    writer.write_clean_records(
        [
            {
                "transaction_id": "tx-1",
                "customer_id": "c1",
                "amount": 10.0,
                "currency": "USD",
                "timestamp": "2026-10-09T00:00:00Z",
                "merchant": "m",
                "status": "completed",
            }
        ]
    )
    writer.write_dlq_records(
        [
            {
                "transaction_id": "d-1",
                "raw_payload": "{}",
                "error_reason": "err",
                "source_stage": "s",
                "timestamp": "2026-10-09T00:00:00Z",
            }
        ]
    )

    # Filter clean table
    res_clean = client.get("/api/v1/lakehouse/snapshots?table=clean_transactions")
    assert res_clean.status_code == 200
    clean_list = res_clean.json()
    assert len(clean_list) == 1
    assert clean_list[0]["table_name"] == "lakehouse.clean_transactions"

    # Filter DLQ table
    res_dlq = client.get("/api/v1/lakehouse/snapshots?table=dlq_transactions")
    assert res_dlq.status_code == 200
    dlq_list = res_dlq.json()
    assert len(dlq_list) == 1
    assert dlq_list[0]["table_name"] == "lakehouse.dlq_transactions"

    # Filter invalid table returns 400 Bad Request
    res_invalid = client.get("/api/v1/lakehouse/snapshots?table=nonexistent_table")
    assert res_invalid.status_code == 400
    assert "Unknown table" in res_invalid.json()["detail"]


# ==============================================================================
# MOCKED BOUNDARY TESTS FOR FAILURE MODES (CLEARLY LABELED)
# ==============================================================================


def test_lakehouse_metrics_catalog_failure_returns_503():
    """[MOCKED BOUNDARY] Verify GET /metrics returns 503 Service Unavailable when reader raises an error."""

    class FailingReader:
        def get_metrics(self, raise_on_error=False):
            raise RuntimeError("Underlying SQLite database locked or inaccessible")

    set_reader_override(FailingReader())
    try:
        response = client.get("/api/v1/lakehouse/metrics")
        assert response.status_code == 503
        data = response.json()
        assert "detail" in data
        assert "Failed to retrieve Lakehouse metrics" in data["detail"]
    finally:
        set_reader_override(None)


def test_lakehouse_snapshots_catalog_failure_returns_503():
    """[MOCKED BOUNDARY] Verify GET /snapshots returns 503 Service Unavailable when reader raises an error."""

    class FailingReader:
        def get_snapshots(self, table_name=None, raise_on_error=False):
            raise RuntimeError("Snapshot manifest read error")

    set_reader_override(FailingReader())
    try:
        response = client.get("/api/v1/lakehouse/snapshots")
        assert response.status_code == 503
        data = response.json()
        assert "detail" in data
        assert "Failed to retrieve Lakehouse snapshots" in data["detail"]
    finally:
        set_reader_override(None)


def test_lakehouse_service_initialization_failure_returns_503(monkeypatch):
    """[MOCKED BOUNDARY] Verify 503 when IcebergReader initialization itself fails."""
    from app.services import lakehouse_service

    def fail_init(*args, **kwargs):
        raise RuntimeError("Catalog directory permission denied")

    # Clear override so get_reader tries to initialize
    set_reader_override(None)
    monkeypatch.setattr(
        lakehouse_service,
        "get_reader",
        lambda *a, **k: (_ for _ in ()).throw(
            LakehouseServiceError("Could not connect to Iceberg lakehouse catalog: permission denied")
        ),
    )

    res_metrics = client.get("/api/v1/lakehouse/metrics")
    assert res_metrics.status_code == 503
    assert "Could not connect to Iceberg lakehouse catalog" in res_metrics.json()["detail"]

    res_snapshots = client.get("/api/v1/lakehouse/snapshots")
    assert res_snapshots.status_code == 503
    assert "Could not connect to Iceberg lakehouse catalog" in res_snapshots.json()["detail"]


# ==============================================================================
# DEFAULT REPO WAREHOUSE PERSISTENCE CHECK
# ==============================================================================


def test_lakehouse_endpoints_live_repo_warehouse():
    """[REAL ICEBERG STORAGE] Test endpoints against default repo warehouse (if present)."""
    set_reader_override(None)

    resp_metrics = client.get("/api/v1/lakehouse/metrics")
    assert resp_metrics.status_code == 200
    metrics_data = resp_metrics.json()
    assert "clean_transactions_count" in metrics_data
    assert "dlq_transactions_count" in metrics_data
    assert metrics_data["clean_transactions_count"] >= 0
    assert metrics_data["dlq_transactions_count"] >= 0

    resp_snaps = client.get("/api/v1/lakehouse/snapshots")
    assert resp_snaps.status_code == 200
    snaps_data = resp_snaps.json()
    assert isinstance(snaps_data, list)
    if snaps_data:
        first = snaps_data[0]
        assert "snapshot_id" in first
        assert "table_name" in first
        assert "committed_at" in first
        assert "timestamp_ms" in first
