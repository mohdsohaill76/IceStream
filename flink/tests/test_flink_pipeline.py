import json
import pytest
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transforms import (
    parse_and_validate_transaction,
    parse_timestamp_to_epoch_ms,
)


def test_valid_json_parsing():
    valid_payload = json.dumps({
        "transaction_id": "tx_1001",
        "user_id": "usr_50",
        "amount": 99.95,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    })

    result = parse_and_validate_transaction(valid_payload)
    assert result["status"] == "VALID"
    assert result["payload"]["transaction_id"] == "tx_1001"
    assert result["payload"]["amount"] == 99.95
    assert "event_timestamp_ms" in result["payload"]


def test_malformed_json_dlq():
    malformed_json = "{"
    result = parse_and_validate_transaction(malformed_json)

    assert result["status"] == "INVALID"
    assert "Malformed JSON" in result["error_reason"]
    assert result["raw_payload"] == malformed_json


def test_invalid_amount_dlq():
    # Test negative amount
    negative_amt = json.dumps({
        "transaction_id": "tx_1002",
        "user_id": "usr_50",
        "amount": -50.0,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    })
    res1 = parse_and_validate_transaction(negative_amt)
    assert res1["status"] == "INVALID"
    assert "must be positive" in res1["error_reason"]

    # Test non-numeric amount
    string_amt = json.dumps({
        "transaction_id": "tx_1003",
        "user_id": "usr_50",
        "amount": "abc",
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    })
    res2 = parse_and_validate_transaction(string_amt)
    assert res2["status"] == "INVALID"
    assert "must be numeric" in res2["error_reason"]


def test_missing_required_fields_dlq():
    missing_field_payload = json.dumps({
        "transaction_id": "tx_1004",
        "amount": 10.0,
    })
    result = parse_and_validate_transaction(missing_field_payload)
    assert result["status"] == "INVALID"
    assert "Missing required fields" in result["error_reason"]


def test_event_time_watermarks():
    iso_timestamp = "2026-08-31T12:00:00Z"
    epoch_ms = parse_timestamp_to_epoch_ms(iso_timestamp)

    assert epoch_ms is not None
    assert isinstance(epoch_ms, int)
    assert epoch_ms == 1788177600000


def test_duplicate_transaction_filtering():
    """Simulates stateful deduplication logic."""
    seen_transactions = set()
    records = [
        {"transaction_id": "tx_dup_1", "amount": 10.0},
        {"transaction_id": "tx_dup_1", "amount": 10.0},  # Duplicate
        {"transaction_id": "tx_dup_2", "amount": 20.0},
    ]

    valid_outputs = []
    dlq_outputs = []

    for rec in records:
        tx_id = rec["transaction_id"]
        if tx_id in seen_transactions:
            dlq_outputs.append(rec)
        else:
            seen_transactions.add(tx_id)
            valid_outputs.append(rec)

    assert len(valid_outputs) == 2
    assert len(dlq_outputs) == 1
    assert dlq_outputs[0]["transaction_id"] == "tx_dup_1"


def test_kafka_output_serialization():
    valid_payload = {
        "transaction_id": "tx_2000",
        "user_id": "usr_10",
        "amount": 25.0,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
        "processed_at": "2026-08-31T10:00:01Z",
    }

    serialized = json.dumps(valid_payload)
    deserialized = json.loads(serialized)

    assert deserialized["transaction_id"] == "tx_2000"
    assert deserialized["amount"] == 25.0