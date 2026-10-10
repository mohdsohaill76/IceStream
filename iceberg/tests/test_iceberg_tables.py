import tempfile
import uuid
import pytest
from pathlib import Path

from iceberg.catalog.catalog_manager import init_catalog_and_tables
from iceberg.src.iceberg_writer import IcebergWriter
from iceberg.src.iceberg_reader import IcebergReader


@pytest.fixture
def temp_iceberg_env():
    tmp_dir = tempfile.mkdtemp()
    wh_dir = Path(tmp_dir) / "warehouse"
    cat_db = wh_dir / "catalog.db"
    wh_dir.mkdir(parents=True, exist_ok=True)
    catalogs_to_dispose = []

    def register_catalog(cat):
        catalogs_to_dispose.append(cat)
        return cat

    yield wh_dir, cat_db, register_catalog

    for cat in catalogs_to_dispose:
        if hasattr(cat, "engine"):
            try:
                cat.engine.dispose()
            except Exception:
                pass
    import shutil
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass


def test_iceberg_table_initialization(temp_iceberg_env):
    wh_dir, cat_db, register = temp_iceberg_env
    catalog, clean_table, dlq_table = init_catalog_and_tables(
        warehouse_dir=wh_dir, catalog_db=cat_db
    )
    register(catalog)
    assert clean_table.name() == ("lakehouse", "clean_transactions")
    assert dlq_table.name() == ("lakehouse", "dlq_transactions")


def test_iceberg_write_clean_records(temp_iceberg_env):
    wh_dir, cat_db, register = temp_iceberg_env
    writer = IcebergWriter(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(writer.catalog)
    reader = IcebergReader(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(reader.catalog)

    tx_id_1 = str(uuid.uuid4())
    tx_id_2 = str(uuid.uuid4())
    records = [
        {
            "transaction_id": tx_id_1,
            "customer_id": "CUST-1001",
            "amount": 149.99,
            "currency": "USD",
            "timestamp": "2026-10-09T10:00:00Z",
            "merchant": "Amazon",
            "status": "SUCCESS",
        },
        {
            "transaction_id": tx_id_2,
            "customer_id": "CUST-1002",
            "amount": 49.50,
            "currency": "EUR",
            "timestamp": "2026-10-09T10:05:00Z",
            "merchant": "Apple",
            "status": "SUCCESS",
        },
    ]

    written = writer.write_clean_records(records)
    assert written == 2

    # Read back and verify
    read_back = reader.get_clean_records()
    assert len(read_back) == 2
    ids = {r["transaction_id"] for r in read_back}
    assert tx_id_1 in ids
    assert tx_id_2 in ids

    # Idempotency / Deduplication test: re-sending the same records should write 0 new rows
    retry_written = writer.write_clean_records(records)
    assert retry_written == 0

    metrics = reader.get_metrics()
    assert metrics["clean_transactions_count"] == 2


def test_iceberg_restart_idempotency(temp_iceberg_env):
    """Verify that a newly instantiated IcebergWriter (simulating process restart)
    reads persisted Iceberg metadata and skips previously committed records."""
    wh_dir, cat_db, register = temp_iceberg_env
    writer1 = IcebergWriter(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(writer1.catalog)
    reader = IcebergReader(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(reader.catalog)

    tx_id = str(uuid.uuid4())
    record = {
        "transaction_id": tx_id,
        "customer_id": "CUST-RESTART",
        "amount": 99.99,
        "currency": "INR",
        "timestamp": "2026-10-09T12:00:00Z",
        "merchant": "Flipkart",
        "status": "SUCCESS",
    }

    # First writer persists
    assert writer1.write_clean_records([record]) == 1

    # Simulate restart by instantiating a fresh writer
    writer2 = IcebergWriter(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(writer2.catalog)

    # Attempting to write the same record must be rejected as duplicate
    assert writer2.write_clean_records([record]) == 0

    reader.refresh()
    assert len(reader.get_clean_records()) == 1



def test_iceberg_write_dlq_records(temp_iceberg_env):
    wh_dir, cat_db, register = temp_iceberg_env
    writer = IcebergWriter(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(writer.catalog)
    reader = IcebergReader(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(reader.catalog)

    # Test Flink-style and DQ-style DLQ entries
    dlq_entries = [
        {
            "raw_payload": '{"transaction_id": "bad-1", "amount": -10}',
            "error_reason": "Invalid transaction amount: -10 (must be positive)",
        },
        {
            "record": {"transaction_id": "bad-2", "currency": "INVALID"},
            "errors": ["Invalid currency code: INVALID"],
        },
    ]

    written = writer.write_dlq_records(dlq_entries)
    assert written == 2

    read_back = reader.get_dlq_records()
    assert len(read_back) == 2

    reasons = [r["error_reason"] for r in read_back]
    assert any("must be positive" in r for r in reasons)
    assert any("Invalid currency code" in r for r in reasons)

    metrics = reader.get_metrics()
    assert metrics["dlq_transactions_count"] == 2


def test_iceberg_append_failure_and_retry(temp_iceberg_env):
    """Verify that an append failure does NOT mark IDs as written,
    allowing successful retry on the subsequent attempt without data loss."""
    wh_dir, cat_db, register = temp_iceberg_env
    writer = IcebergWriter(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(writer.catalog)
    reader = IcebergReader(warehouse_dir=wh_dir, catalog_db=cat_db)
    register(reader.catalog)

    tx_id = str(uuid.uuid4())
    record = {
        "transaction_id": tx_id,
        "customer_id": "CUST-RETRY-01",
        "amount": 150.00,
        "currency": "INR",
        "timestamp": "2026-10-09T14:00:00Z",
        "merchant": "Myntra",
        "status": "SUCCESS",
    }

    original_append = writer.clean_table.append

    # Step 1: Simulate transient failure on first append
    def failing_append(*args, **kwargs):
        raise IOError("Simulated transient lakehouse storage failure")

    writer.clean_table.append = failing_append

    with pytest.raises(IOError, match="Simulated transient lakehouse storage failure"):
        writer.write_clean_records([record])

    # Assert tx_id was NOT committed to in-memory set
    assert tx_id not in writer._written_tx_ids

    # Step 2: Restore normal append and retry the exact same batch
    writer.clean_table.append = original_append
    written = writer.write_clean_records([record])
    assert written == 1
    assert tx_id in writer._written_tx_ids

    # Step 3: Verify record is actually persisted in table
    reader.refresh()
    clean_records = reader.get_clean_records()
    assert any(r["transaction_id"] == tx_id for r in clean_records)

    # Step 4: Verify subsequent duplicate attempts are skipped
    assert writer.write_clean_records([record]) == 0
