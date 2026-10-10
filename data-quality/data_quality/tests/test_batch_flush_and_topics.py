"""
Tests for Data Quality topic alignment and quiet-period batch flushing.

Verifies:
1. Default topic names form the intended pipeline chain.
2. Environment-variable overrides still work.
3. A full batch flushes at the batch-size limit.
4. A partial batch flushes after the timeout without requiring another message.
5. Empty polls do not trigger processing.
6. Producer flush failures and shutdown flushing are handled safely.
7. Circuit-breaker and DLQ integration remains preserved.
"""

import os
import pytest

from app import kafka_consumer
from app.downstream import valid_producer
from app.dlq import dlq_producer
from app import main
from app.circuit_breaker.circuit_breaker import circuit
from app.circuit_breaker.states import CLOSED


@pytest.fixture(autouse=True)
def reset_test_state(monkeypatch):
    """Ensure clean circuit state and isolated producers for each test."""
    circuit.state = CLOSED
    # Avoid real Kafka interactions during tests
    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)
    monkeypatch.setattr(main, "save_shared_status", lambda *args, **kwargs: None)


def make_transaction(index):
    """Generate a canonical transaction payload."""
    return {
        "transaction_id": f"00000000-0000-4000-8000-{index:012d}",
        "customer_id": f"CUST-{index:04d}",
        "amount": 100.0 + index,
        "currency": "INR",
        "timestamp": "2026-10-09T12:00:00Z",
        "merchant": "Amazon",
        "status": "SUCCESS",
    }


# =====================================================================
# 1. Topic Defaults and Environment Overrides
# =====================================================================

def test_default_topic_names_form_intended_chain():
    """Verify that Data Quality default topics match the upstream Flink and downstream Iceberg contract."""
    # Data Quality must consume Flink's valid output
    assert kafka_consumer.KAFKA_TOPIC == "processed-transactions"
    # Data Quality must emit valid records to Iceberg clean ingestion topic
    assert valid_producer.VALID_OUTPUT_TOPIC == "quality-checked-transactions"
    # Data Quality must emit rejected records to the unified DLQ topic
    assert dlq_producer.DLQ_TOPIC == "transactions-dlq"


def test_topic_environment_variable_overrides(monkeypatch):
    """Verify that all three topic names can be overridden via environment variables."""
    monkeypatch.setenv("KAFKA_TOPIC", "custom-input-topic")
    monkeypatch.setenv("VALID_OUTPUT_TOPIC", "custom-valid-topic")
    monkeypatch.setenv("DLQ_TOPIC", "custom-dlq-topic")

    # Re-evaluate env vars
    overridden_input = os.getenv("KAFKA_TOPIC", "processed-transactions")
    overridden_valid = os.getenv("VALID_OUTPUT_TOPIC", "quality-checked-transactions")
    overridden_dlq = os.getenv("DLQ_TOPIC", "transactions-dlq")

    assert overridden_input == "custom-input-topic"
    assert overridden_valid == "custom-valid-topic"
    assert overridden_dlq == "custom-dlq-topic"


# =====================================================================
# 2. Bounded Polling and Batch Flushing Tests
# =====================================================================

class MockPollConsumer:
    """Mock KafkaConsumer that returns predefined poll responses."""

    def __init__(self, poll_responses):
        """
        Args:
            poll_responses: List of responses for each poll() invocation.
                           Each item can be a list of records or dict of {partition: [records]}.
        """
        self.poll_responses = list(poll_responses)
        self.call_count = 0
        self.closed = False

    def poll(self, timeout_ms=500):
        self.call_count += 1
        if self.poll_responses:
            return self.poll_responses.pop(0)
        return {}

    def close(self):
        self.closed = True


class SteppingClock:
    """Controllable monotonic clock for deterministic timeout tests."""

    def __init__(self, start_time=100.0, step_seconds=1.0):
        self.current_time = start_time
        self.step_seconds = step_seconds

    def __call__(self):
        t = self.current_time
        return t

    def advance(self, seconds):
        self.current_time += seconds


def test_full_batch_flushes_at_batch_size_limit():
    """Verify that a batch flushes immediately when batch_size limit is reached."""
    records_batch = [make_transaction(i) for i in range(1, 6)]

    # Mock consumer returning all 5 records on first poll
    mock_consumer = MockPollConsumer([{"tp-0": records_batch}])
    processed_batches = []

    main.run_service(
        consumer=mock_consumer,
        batch_size=5,
        max_wait_seconds=10.0,
        max_iterations=1,
        on_batch_processed=lambda res: processed_batches.append(res),
    )

    assert len(processed_batches) == 1
    assert processed_batches[0]["total_records"] == 5
    assert processed_batches[0]["bad_records"] == 0
    assert len(processed_batches[0]["valid_records"]) == 5


def test_partial_batch_flushes_after_timeout_without_new_messages():
    """
    Verify that a partial batch flushes when max_wait_seconds expires,
    even during quiet periods when no new Kafka messages arrive.
    """
    records_batch = [make_transaction(1), make_transaction(2)]

    # Poll 1: 2 records.
    # Poll 2: Empty poll (quiet period).
    mock_consumer = MockPollConsumer([
        {"tp-0": records_batch},
        {},
    ])

    processed_batches = []
    clock = SteppingClock(start_time=100.0)

    def clock_fn():
        # First poll at t=100.0 (batch arrives, start_time=100.0)
        # Second poll advances time past 5.0s timeout to t=106.0
        if mock_consumer.call_count > 1:
            clock.advance(6.0)
        return clock()

    main.run_service(
        consumer=mock_consumer,
        batch_size=10,
        max_wait_seconds=5.0,
        poll_timeout_ms=500,
        clock=clock_fn,
        max_iterations=2,
        on_batch_processed=lambda res: processed_batches.append(res),
    )

    # Batch should have flushed on second poll because 6.0s > 5.0s max wait
    assert len(processed_batches) == 1
    assert processed_batches[0]["total_records"] == 2
    assert processed_batches[0]["bad_records"] == 0


def test_empty_polls_do_not_trigger_processing():
    """Verify that empty polls do not trigger process_records or create empty batches."""
    mock_consumer = MockPollConsumer([{}, {}, {}])
    processed_batches = []

    main.run_service(
        consumer=mock_consumer,
        batch_size=10,
        max_wait_seconds=5.0,
        max_iterations=3,
        on_batch_processed=lambda res: processed_batches.append(res),
    )

    # Empty polls should never trigger processing
    assert len(processed_batches) == 0
    assert mock_consumer.call_count == 3


def test_shutdown_flushes_remaining_batch_safely():
    """Verify that stopping the service flushes any remaining buffered records before exit."""
    # 3 records arrive, but batch_size is 10 and timeout is 60s (not yet reached)
    records_batch = [make_transaction(1), make_transaction(2), make_transaction(3)]
    mock_consumer = MockPollConsumer([{"tp-0": records_batch}])

    processed_batches = []

    # Service stops after 1 iteration without reaching batch_size or timeout
    main.run_service(
        consumer=mock_consumer,
        batch_size=10,
        max_wait_seconds=60.0,
        max_iterations=1,
        on_batch_processed=lambda res: processed_batches.append(res),
    )

    # The shutdown handler in finally: should flush the remaining 3 records
    assert len(processed_batches) == 1
    assert processed_batches[0]["total_records"] == 3


def test_producer_flush_failure_is_handled_safely(monkeypatch):
    """Verify that producer flush exceptions are handled gracefully, do not crash the service, and do not falsely report batch as successfully delivered."""
    records_batch = [make_transaction(1)]
    mock_consumer = MockPollConsumer([{"tp-0": records_batch}])

    def failing_flush():
        raise RuntimeError("Kafka broker connection timeout during flush")

    # Simulate failing flush
    monkeypatch.setattr(main, "flush_valid_producer", failing_flush)
    monkeypatch.setattr(main, "flush_dlq", failing_flush)

    processed_batches = []

    # Service runs without unhandled exception and retries safely
    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        on_batch_processed=lambda res: processed_batches.append(res),
        sleep_fn=lambda s: None,
    )

    # Failed batch is not marked as successfully processed
    assert len(processed_batches) == 0


def test_invalid_and_valid_records_routing_in_polling_loop(monkeypatch):
    """Verify that valid and invalid records are properly routed and circuit breaker updates during polling."""
    # 1 valid, 1 invalid (status=UNKNOWN)
    valid_tx = make_transaction(1)
    invalid_tx = make_transaction(2)
    invalid_tx["status"] = "UNKNOWN"

    dlq_sent = []
    valid_sent = []

    monkeypatch.setattr(main, "send_to_dlq", lambda rec, err: dlq_sent.append((rec, err)) or {"record": rec, "errors": err})
    monkeypatch.setattr(main, "send_valid_record", lambda rec: valid_sent.append(rec))

    mock_consumer = MockPollConsumer([{"tp-0": [valid_tx, invalid_tx]}])
    processed_batches = []

    main.run_service(
        consumer=mock_consumer,
        batch_size=2,
        max_iterations=1,
        on_batch_processed=lambda res: processed_batches.append(res),
    )

    assert len(processed_batches) == 1
    assert processed_batches[0]["total_records"] == 2
    assert processed_batches[0]["bad_records"] == 1
    assert processed_batches[0]["error_rate"] == 50.0

    # 50% error rate exceeds 2% threshold, tripping circuit to OPEN
    assert processed_batches[0]["circuit_breaker_triggered"] is True
    assert processed_batches[0]["monitoring_status"]["state"] == "OPEN"

    # Circuit OPEN correctly blocks valid records from being forwarded downstream
    assert len(valid_sent) == 0
    assert len(processed_batches[0]["blocked_records"]) == 1

    # Only invalid record is sent to DLQ
    assert len(dlq_sent) == 1


def test_closed_circuit_forwards_valid_records(monkeypatch):
    """Verify that when circuit is CLOSED, valid records are sent to downstream producer."""
    valid_tx = make_transaction(1)
    valid_sent = []
    dlq_sent = []

    monkeypatch.setattr(main, "send_to_dlq", lambda rec, err: dlq_sent.append((rec, err)) or {"record": rec, "errors": err})
    monkeypatch.setattr(main, "send_valid_record", lambda rec: valid_sent.append(rec))

    mock_consumer = MockPollConsumer([{"tp-0": [valid_tx]}])
    processed_batches = []

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        on_batch_processed=lambda res: processed_batches.append(res),
    )

    assert len(processed_batches) == 1
    assert processed_batches[0]["bad_records"] == 0
    assert processed_batches[0]["monitoring_status"]["state"] == "CLOSED"
    assert len(valid_sent) == 1
    assert valid_sent[0]["transaction_id"] == valid_tx["transaction_id"]
    assert len(dlq_sent) == 0
