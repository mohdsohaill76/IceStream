import json
import pytest
import os
import sys

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


@pytest.fixture
def flink_processor():
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState(initial_value=None)
    func.open(MockRuntimeContext(mock_state))
    return func


def test_valid_customer_id_schema_routing(flink_processor):
    """Verifies that canonical schema records are accepted and emit strictly 7 fields."""
    valid_payload = json.dumps({
        "transaction_id": "tx_mahek_1001",
        "customer_id": "cust_8821",
        "amount": 299.99,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })

    outputs = list(flink_processor.process_element(valid_payload, None))
    assert len(outputs) == 1

    tag, output_str = outputs[0]
    assert tag.tag_id == VALID_OUTPUT_TAG.tag_id

    data = json.loads(output_str)
    assert data["transaction_id"] == "tx_mahek_1001"
    assert data["customer_id"] == "cust_8821"
    assert len(data.keys()) == 7
    assert "user_id" not in data


def test_reject_user_id_leakage(flink_processor):
    """Verifies that payloads containing user_id (even with customer_id) route to DLQ."""
    leaked_payload = json.dumps({
        "transaction_id": "tx_leak_001",
        "customer_id": "cust_8821",
        "user_id": "usr_99",
        "amount": 50.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })

    outputs = list(flink_processor.process_element(leaked_payload, None))
    assert len(outputs) == 1
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id
    dlq_data = json.loads(outputs[0][1])
    assert "user_id" in dlq_data["error_reason"]


def test_legacy_user_id_fails_schema_validation(flink_processor):
    """Verifies that payloads with only user_id route to DLQ."""
    legacy_user_id_payload = json.dumps({
        "transaction_id": "tx_legacy_001",
        "user_id": "usr_99",
        "amount": 50.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })

    outputs = list(flink_processor.process_element(legacy_user_id_payload, None))
    assert len(outputs) == 1
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id


def test_reject_boolean_amount(flink_processor):
    """Verifies that boolean values for amount (e.g., True/False) are rejected."""
    payload = json.dumps({
        "transaction_id": "tx_bool_001",
        "customer_id": "cust_01",
        "amount": True,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })
    outputs = list(flink_processor.process_element(payload, None))
    assert len(outputs) == 1
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id
    assert "Boolean" in json.loads(outputs[0][1])["error_reason"]


def test_reject_invalid_currency_and_status(flink_processor):
    """Verifies that non-standard currencies and statuses route to DLQ."""
    bad_currency = json.dumps({
        "transaction_id": "tx_curr_001",
        "customer_id": "cust_01",
        "amount": 100.0,
        "currency": "INVALID_CURR",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })
    outputs = list(flink_processor.process_element(bad_currency, None))
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id
    assert "currency" in json.loads(outputs[0][1])["error_reason"].lower()


def test_reject_invalid_timestamp(flink_processor):
    """Verifies that unparseable timestamps route to DLQ."""
    bad_ts = json.dumps({
        "transaction_id": "tx_ts_001",
        "customer_id": "cust_01",
        "amount": 100.0,
        "currency": "USD",
        "timestamp": "yesterday-afternoon",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })
    outputs = list(flink_processor.process_element(bad_ts, None))
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id
    assert "timestamp" in json.loads(outputs[0][1])["error_reason"].lower()


def test_malformed_json_does_not_crash(flink_processor):
    """Verifies corrupt non-JSON strings route to DLQ."""
    corrupt_payload = "{bad_json_string:"
    outputs = list(flink_processor.process_element(corrupt_payload, None))
    assert len(outputs) == 1
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id


def test_invalid_amount_dlq(flink_processor):
    """Validates negative or zero transaction amounts route to DLQ."""
    negative_payload = json.dumps({
        "transaction_id": "tx_neg_01",
        "customer_id": "cust_12",
        "amount": -10.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })
    outputs = list(flink_processor.process_element(negative_payload, None))
    assert outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id
    assert "must be positive" in json.loads(outputs[0][1])["error_reason"]


def test_flink_valuestate_deduplication():
    """Tests duplicate rejection via state."""
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState(initial_value=None)
    func.open(MockRuntimeContext(mock_state))

    payload = json.dumps({
        "transaction_id": "tx_state_dup_01",
        "customer_id": "cust_100",
        "amount": 75.0,
        "currency": "USD",
        "timestamp": "2026-09-12T10:00:00Z",
        "merchant": "Amazon",
        "status": "COMPLETED",
    })

    # Pass 1: Unique -> Valid
    first_outputs = list(func.process_element(payload, None))
    assert first_outputs[0][0].tag_id == VALID_OUTPUT_TAG.tag_id

    # Pass 2: Duplicate -> DLQ
    second_outputs = list(func.process_element(payload, None))
    assert second_outputs[0][0].tag_id == DLQ_OUTPUT_TAG.tag_id
    assert "Duplicate" in json.loads(second_outputs[0][1])["error_reason"]