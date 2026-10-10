import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from kafka import KafkaConsumer, KafkaProducer

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Backend in sys.path for TestClient
BACKEND_DIR = REPO_ROOT / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.main import app as fastapi_app
from iceberg.src.iceberg_consumer_service import IcebergConsumerService
from iceberg.src.iceberg_reader import IcebergReader

client = TestClient(fastapi_app)

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "ecommerce-transactions"
PROCESSED_TOPIC = "processed-transactions"
QUALITY_CHECKED_TOPIC = "quality-checked-transactions"
DLQ_TOPIC = "transactions-dlq"


def run_data_quality_batch(records: list[dict]) -> dict:
    """Execute Data Quality validation and routing in isolated process."""
    cmd = [
        sys.executable,
        "-c",
        """
import os, json, sys
os.environ['KAFKA_SERVER'] = 'localhost:9092'
os.environ['VALID_OUTPUT_TOPIC'] = 'quality-checked-transactions'
os.environ['DLQ_TOPIC'] = 'transactions-dlq'
from app.main import process_records
records = json.loads(sys.argv[1])
result = process_records(records)
print(json.dumps({
    'total_records': result['total_records'],
    'bad_records': result['bad_records'],
    'valid_count': len(result['valid_records']),
    'dlq_count': len(result['dlq_records']),
    'error_rate': result['error_rate'],
}))
""",
        json.dumps(records),
    ]
    dq_cwd = REPO_ROOT / "data-quality" / "data_quality"
    proc = subprocess.run(cmd, cwd=str(dq_cwd), capture_output=True, text=True, check=True)
    return json.loads(proc.stdout.strip())


@pytest.fixture(scope="module")
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    yield producer
    producer.close()


def test_complete_pipeline_e2e(kafka_producer):
    """Full End-to-End Test:

    1. Generate valid transaction -> Kafka: ecommerce-transactions.
    2. Confirm Flink processes to processed-transactions.
    3. Confirm Data Quality validates and sends to quality-checked-transactions.
    4. Confirm Iceberg persists to lakehouse.clean_transactions.
    5. Generate invalid transaction -> routes to transactions-dlq.
    6. Confirm Iceberg persists DLQ record to lakehouse.dlq_transactions.
    7. Generate subsequent valid transaction -> processes cleanly through the full pipeline.
    8. Confirm FastAPI backend exposes live healthy status and incident feeds.
    """
    unique_run_id = uuid.uuid4().hex[:8]
    reader = IcebergReader()
    iceberg_service = IcebergConsumerService(
        consumer_group=f"e2e-iceberg-{unique_run_id}"
    )

    # -------------------------------------------------------------
    # Step 1: Generate Valid Transaction 1
    # -------------------------------------------------------------
    tx_id_1 = str(uuid.uuid4())
    ts1 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    valid_tx_1 = {
        "transaction_id": tx_id_1,
        "customer_id": f"CUST-101-{unique_run_id}",
        "amount": 250.75,
        "currency": "INR",
        "timestamp": ts1,
        "merchant": "Amazon",
        "status": "SUCCESS",
    }

    # Send to ecommerce-transactions
    kafka_producer.send(INPUT_TOPIC, value=valid_tx_1)
    kafka_producer.flush()

    # -------------------------------------------------------------
    # Step 2: Confirm Flink processes to processed-transactions
    # -------------------------------------------------------------
    consumer_flink = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"e2e-flink-{unique_run_id}-1",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    received_flink_1 = None
    deadline = time.time() + 20
    while time.time() < deadline:
        batch = consumer_flink.poll(timeout_ms=1000)
        for tp, messages in batch.items():
            for m in messages:
                if isinstance(m.value, dict) and m.value.get("transaction_id") == tx_id_1:
                    received_flink_1 = m.value
                    break
            if received_flink_1:
                break
        if received_flink_1:
            break
    consumer_flink.close()

    assert received_flink_1 is not None, f"Flink failed to process {tx_id_1} to {PROCESSED_TOPIC}"
    assert received_flink_1["transaction_id"] == tx_id_1

    # -------------------------------------------------------------
    # Step 3: Data Quality processes to quality-checked-transactions
    # -------------------------------------------------------------
    dq_result = run_data_quality_batch([received_flink_1])
    assert dq_result["bad_records"] == 0
    assert dq_result["valid_count"] == 1

    # Confirm message in quality-checked-transactions
    consumer_dq = KafkaConsumer(
        QUALITY_CHECKED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"e2e-dq-{unique_run_id}-1",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    received_dq_1 = None
    deadline = time.time() + 10
    while time.time() < deadline:
        batch = consumer_dq.poll(timeout_ms=1000)
        for tp, messages in batch.items():
            for m in messages:
                if isinstance(m.value, dict) and m.value.get("transaction_id") == tx_id_1:
                    received_dq_1 = m.value
                    break
            if received_dq_1:
                break
        if received_dq_1:
            break
    consumer_dq.close()

    assert received_dq_1 is not None, f"Record {tx_id_1} not found in {QUALITY_CHECKED_TOPIC}"

    # -------------------------------------------------------------
    # Step 4: Iceberg commits clean record
    # -------------------------------------------------------------
    written_clean = iceberg_service.process_clean_batch(max_records=50, timeout_ms=4000)
    reader.refresh()
    clean_records = reader.get_clean_records()
    clean_ids = {r["transaction_id"] for r in clean_records}
    assert tx_id_1 in clean_ids, f"{tx_id_1} not persisted in Iceberg clean table"

    # -------------------------------------------------------------
    # Step 5 & 6: Generate Invalid Transaction -> DLQ
    # -------------------------------------------------------------
    tx_id_bad = str(uuid.uuid4())
    ts_bad = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    invalid_tx = {
        "transaction_id": tx_id_bad,
        "customer_id": f"CUST-BAD-{unique_run_id}",
        "amount": -80.0,  # Negative amount rejected by Flink
        "currency": "INR",
        "timestamp": ts_bad,
        "merchant": "Target",
        "status": "SUCCESS",
    }

    kafka_producer.send(INPUT_TOPIC, value=invalid_tx)
    kafka_producer.flush()

    consumer_dlq = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"e2e-dlq-{unique_run_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    received_dlq = None
    deadline = time.time() + 20
    while time.time() < deadline:
        batch = consumer_dlq.poll(timeout_ms=1000)
        for tp, messages in batch.items():
            for m in messages:
                val = m.value
                if isinstance(val, dict):
                    raw = str(val.get("raw_payload", ""))
                    if tx_id_bad in raw:
                        received_dlq = val
                        break
            if received_dlq:
                break
        if received_dlq:
            break
    consumer_dlq.close()

    assert received_dlq is not None, f"Invalid record {tx_id_bad} not found in {DLQ_TOPIC}"
    assert "Invalid transaction amount" in received_dlq.get("error_reason", "")

    # Persist DLQ entry to Iceberg DLQ table
    written_dlq = iceberg_service.process_dlq_batch(max_records=50, timeout_ms=4000)
    reader.refresh()
    dlq_records = reader.get_dlq_records()
    assert len(dlq_records) >= 1

    # -------------------------------------------------------------
    # Step 7: Subsequent Valid Transaction 2 continues processing
    # -------------------------------------------------------------
    tx_id_2 = str(uuid.uuid4())
    ts2 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    valid_tx_2 = {
        "transaction_id": tx_id_2,
        "customer_id": f"CUST-102-{unique_run_id}",
        "amount": 499.00,
        "currency": "INR",
        "timestamp": ts2,
        "merchant": "Apple",
        "status": "SUCCESS",
    }

    kafka_producer.send(INPUT_TOPIC, value=valid_tx_2)
    kafka_producer.flush()

    consumer_flink_2 = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"e2e-flink-{unique_run_id}-2",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    received_flink_2 = None
    deadline = time.time() + 20
    while time.time() < deadline:
        batch = consumer_flink_2.poll(timeout_ms=1000)
        for tp, messages in batch.items():
            for m in messages:
                if isinstance(m.value, dict) and m.value.get("transaction_id") == tx_id_2:
                    received_flink_2 = m.value
                    break
            if received_flink_2:
                break
        if received_flink_2:
            break
    consumer_flink_2.close()

    assert received_flink_2 is not None, f"Flink failed to process subsequent valid record {tx_id_2}"

    # Route through DQ and Iceberg
    dq_res_2 = run_data_quality_batch([received_flink_2])
    assert dq_res_2["bad_records"] == 0
    written_clean_2 = iceberg_service.process_clean_batch(max_records=50, timeout_ms=4000)
    reader.refresh()
    clean_records_updated = reader.get_clean_records()
    updated_clean_ids = {r["transaction_id"] for r in clean_records_updated}
    assert tx_id_2 in updated_clean_ids

    # -------------------------------------------------------------
    # Step 8: Confirm Backend API Status & Health
    # -------------------------------------------------------------
    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "healthy"

    status_resp = client.get("/api/v1/pipeline/status")
    assert status_resp.status_code == 200
    status_data = status_resp.json()
    assert status_data["pipeline"] == "transaction_stream"
    assert status_data["status"] == "healthy"
    stage_names = [s["name"] for s in status_data["stages"]]
    assert stage_names == ["kafka", "flink", "data_quality", "iceberg"]
    for s in status_data["stages"]:
        assert s["status"] == "healthy"

    incidents_resp = client.get("/api/v1/incidents")
    assert incidents_resp.status_code == 200
    assert len(incidents_resp.json()) >= 2
