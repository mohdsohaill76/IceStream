import json
import pytest
import os
import sys
from datetime import datetime

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


def test_valid_customer_id_schema_routing():
    """Verifies that upstream messages matching Mahek's customer_id schema are accepted and routed to VALID stream."""
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState(initial_value=None)
    func.open(MockRuntimeContext(mock_state))

    valid_upstream_payload = json.dumps({
        "transaction_id": "tx_mahek_1001",
        "customer_id": "cust_8821",  # Upstream contract customer_id
        "amount": 299.99,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
    })

    outputs = list(func.process_element(valid_upstream_payload, None))
    assert len(outputs) == 1
    
    tag, output_str = outputs[0]
    assert tag == VALID_OUTPUT_TAG
    
    data = json.loads(output_str)
    assert data["transaction_id"] == "tx_mahek_1001"
    assert data["customer_id"] == "cust_8821"


def test_legacy_user_id_fails_schema_validation():
    """Verifies that payloads lacking customer_id fail schema validation and route to DLQ cleanly."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    legacy_user_id_payload = json.dumps({
        "transaction_id": "tx_legacy_001",
        "user_id": "usr_99",  # Invalid key under official contract
        "amount": 50.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z"
    })

    outputs = list(func.process_element(legacy_user_id_payload, None))
    assert len(outputs) == 1

    tag, output_str = outputs[0]
    assert tag == DLQ_OUTPUT_TAG

    dlq_data = json.loads(output_str)
    assert "Schema Validation Failure" in dlq_data["error_reason"]
    assert "customer_id" in dlq_data["error_reason"]
    assert dlq_data["raw_payload"] == legacy_user_id_payload


def test_malformed_json_does_not_crash():
    """Verifies corrupt non-JSON strings route to DLQ without crashing the process."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    corrupt_payload = "{bad_json_string:"
    outputs = list(func.process_element(corrupt_payload, None))
    assert len(outputs) == 1

    tag, output_str = outputs[0]
    assert tag == DLQ_OUTPUT_TAG
    dlq_data = json.loads(output_str)
    assert "JSON Deserialization Error" in dlq_data["error_reason"]
    assert dlq_data["raw_payload"] == corrupt_payload


def test_invalid_amount_dlq():
    """Validates negative or zero transaction amounts route to DLQ."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    negative_payload = json.dumps({
        "transaction_id": "tx_neg_01",
        "customer_id": "cust_12",
        "amount": -10.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z"
    })

    outputs = list(func.process_element(negative_payload, None))
    assert len(outputs) == 1
    assert outputs[0][0] == DLQ_OUTPUT_TAG
    assert "Invalid transaction amount" in json.loads(outputs[0][1])["error_reason"]


def test_flink_valuestate_deduplication():
    """Directly tests Flink KeyedProcessFunction state updates and duplicate rejection."""
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState(initial_value=None)
    func.open(MockRuntimeContext(mock_state))

    payload = json.dumps({
        "transaction_id": "tx_state_dup_01",
        "customer_id": "cust_100",
        "amount": 75.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
    })

    # Pass 1: Unique -> Valid stream & State update
    first_outputs = list(func.process_element(payload, None))
    assert len(first_outputs) == 1
    assert first_outputs[0][0] == VALID_OUTPUT_TAG
    assert mock_state.value() is True

    # Pass 2: Duplicate -> DLQ stream
    second_outputs = list(func.process_element(payload, None))
    assert len(second_outputs) == 1
    assert second_outputs[0][0] == DLQ_OUTPUT_TAG
    
    dlq_data = json.loads(second_outputs[0][1])
    assert dlq_data["error_reason"] == "Duplicate transaction_id: tx_state_dup_01"
    assert dlq_data["raw_payload"] == payload