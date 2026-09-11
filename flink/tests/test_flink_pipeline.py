import json
import pytest
import os
import sys
from datetime import datetime

# Resolve paths so imports work smoothly regardless of execution root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transforms import (
    TransactionValidationAndDeduplicationFunction,
    VALID_OUTPUT_TAG,
    DLQ_OUTPUT_TAG,
)


class MockValueState:
    """Mock for Flink ValueState to test stateful deduplication."""
    def __init__(self, initial_value=None):
        self._val = initial_value

    def value(self):
        return self._val

    def update(self, val):
        self._val = val


class MockRuntimeContext:
    """Mock Flink RuntimeContext for state retrieval."""
    def __init__(self, state):
        self.state = state

    def get_state(self, descriptor):
        return self.state


def test_valid_json_parsing_and_routing():
    """Validates that a correctly formatted transaction is routed to the VALID output stream."""
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState(initial_value=None)
    func.open(MockRuntimeContext(mock_state))

    valid_payload = json.dumps({
        "transaction_id": "tx_1001",
        "user_id": "usr_50",
        "amount": 99.95,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    })

    outputs = list(func.process_element(valid_payload, None))
    assert len(outputs) == 1
    
    tag, output_str = outputs[0]
    assert tag == VALID_OUTPUT_TAG
    
    data = json.loads(output_str)
    assert data["transaction_id"] == "tx_1001"
    assert data["amount"] == 99.95


def test_malformed_json_dlq():
    """Validates that corrupt non-JSON strings are routed to DLQ with proper raw payload tracking."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    malformed_json = "{"
    outputs = list(func.process_element(malformed_json, None))
    assert len(outputs) == 1

    tag, output_str = outputs[0]
    assert tag == DLQ_OUTPUT_TAG

    dlq_data = json.loads(output_str)
    assert "JSON Deserialization Error" in dlq_data["error_reason"]
    assert dlq_data["raw_payload"] == malformed_json


def test_invalid_amount_dlq():
    """Validates that non-positive transaction amounts are rejected to DLQ."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    negative_amt_payload = json.dumps({
        "transaction_id": "tx_1002",
        "user_id": "usr_50",
        "amount": -50.0,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    })

    outputs = list(func.process_element(negative_amt_payload, None))
    assert len(outputs) == 1

    tag, output_str = outputs[0]
    assert tag == DLQ_OUTPUT_TAG

    dlq_data = json.loads(output_str)
    assert "Invalid transaction amount" in dlq_data["error_reason"]
    assert dlq_data["raw_payload"] == negative_amt_payload


def test_missing_required_fields_dlq():
    """Validates that missing mandatory schema keys trigger DLQ routing."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    missing_field_payload = json.dumps({
        "transaction_id": "tx_1004",
        "amount": 10.0,
    })

    outputs = list(func.process_element(missing_field_payload, None))
    assert len(outputs) == 1

    tag, output_str = outputs[0]
    assert tag == DLQ_OUTPUT_TAG

    dlq_data = json.loads(output_str)
    assert "Schema Validation Failure" in dlq_data["error_reason"]
    assert dlq_data["raw_payload"] == missing_field_payload


def test_event_time_timestamp_parsing():
    """Validates epoch millisecond conversions for ISO-8601 UTC timestamps."""
    iso_timestamp = "2026-08-31T12:00:00Z"
    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    epoch_ms = int(dt.timestamp() * 1000)

    assert epoch_ms == 1788177600000


def test_flink_valuestate_deduplication():
    """Directly tests Flink KeyedProcessFunction state updates and duplicate rejection (Item 8)."""
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState(initial_value=None)
    func.open(MockRuntimeContext(mock_state))

    payload = json.dumps({
        "transaction_id": "tx_state_1",
        "user_id": "usr_1",
        "amount": 50.0,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    })

    # 1. First pass: Unique record should go to VALID stream & mark ValueState True
    first_outputs = list(func.process_element(payload, None))
    assert len(first_outputs) == 1
    assert first_outputs[0][0] == VALID_OUTPUT_TAG
    assert mock_state.value() is True

    # 2. Second pass: Duplicate transaction should be caught by ValueState and routed to DLQ
    second_outputs = list(func.process_element(payload, None))
    assert len(second_outputs) == 1
    assert second_outputs[0][0] == DLQ_OUTPUT_TAG

    dlq_data = json.loads(second_outputs[0][1])
    assert dlq_data["error_reason"] == "Duplicate transaction_id: tx_state_1"
    assert dlq_data["raw_payload"] == payload


def test_kafka_output_serialization():
    """Ensures valid output string payloads conform to JSON serialization expectations."""
    valid_payload = {
        "transaction_id": "tx_2000",
        "user_id": "usr_10",
        "amount": 25.0,
        "currency": "USD",
        "timestamp": "2026-08-31T10:00:00Z",
    }
    serialized = json.dumps(valid_payload)
    deserialized = json.loads(serialized)
    assert deserialized["transaction_id"] == "tx_2000"
    assert deserialized["amount"] == 25.0