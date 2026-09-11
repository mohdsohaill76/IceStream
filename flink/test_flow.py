import json
import pytest
from src.transforms import (
    TransactionValidationAndDeduplicationFunction,
    VALID_OUTPUT_TAG,
    DLQ_OUTPUT_TAG,
)


class MockValueState:
    """Mock state to simulate Flink ValueState behavior across multiple records."""
    def __init__(self):
        self._seen = False

    def value(self):
        return self._seen

    def update(self, val):
        self._seen = val


class MockRuntimeContext:
    """Mock runtime context to allow open() state initialization."""
    def __init__(self, state):
        self.state = state

    def get_state(self, descriptor):
        return self.state


def test_end_to_end_pipeline_flow_with_duplicates():
    """Validates the complete pipeline flow including successful parsing, stateful deduplication, and DLQ routing (Items 8 & 9)."""
    func = TransactionValidationAndDeduplicationFunction()
    mock_state = MockValueState()
    
    # Initialize function with Flink Runtime Context
    func.open(MockRuntimeContext(mock_state))

    payload_str = json.dumps({
        "transaction_id": "tx_flow_999",
        "user_id": "usr_flow_1",
        "amount": 150.00,
        "currency": "USD",
        "timestamp": "2026-08-31T12:00:00Z"
    })

    # 1. Send first event (Generator yields to VALID stream)
    first_gen = list(func.process_element(payload_str, None))
    assert len(first_gen) == 1
    tag_1, output_val_1 = first_gen[0]

    assert tag_1 == VALID_OUTPUT_TAG
    valid_record = json.loads(output_val_1)
    assert valid_record["transaction_id"] == "tx_flow_999"

    # 2. Send duplicate event (State triggers yield to DLQ stream)
    second_gen = list(func.process_element(payload_str, None))
    assert len(second_gen) == 1
    tag_2, output_val_2 = second_gen[0]

    assert tag_2 == DLQ_OUTPUT_TAG
    
    dlq_record = json.loads(output_val_2)
    assert dlq_record["error_reason"] == "Duplicate transaction_id: tx_flow_999"
    assert dlq_record["raw_payload"] == payload_str


def test_schema_validation_failure_routes_to_dlq():
    """Validates that missing required fields route to DLQ with proper raw payload string."""
    func = TransactionValidationAndDeduplicationFunction()
    func.open(MockRuntimeContext(MockValueState()))

    invalid_payload = json.dumps({
        "transaction_id": "tx_invalid_01",
        "amount": -50.00,  # Invalid negative amount
        "currency": "USD"
    })

    gen_output = list(func.process_element(invalid_payload, None))
    assert len(gen_output) == 1
    tag, output_val = gen_output[0]

    assert tag == DLQ_OUTPUT_TAG
    dlq_record = json.loads(output_val)
    assert "Schema Validation Failure" in dlq_record["error_reason"]
    assert dlq_record["raw_payload"] == invalid_payload


if __name__ == "__main__":
    test_end_to_end_pipeline_flow_with_duplicates()
    test_schema_validation_failure_routes_to_dlq()
    print("All Flink flow unit tests passed successfully!")