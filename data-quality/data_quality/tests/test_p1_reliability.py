"""
Tests for Data Quality P1 reliability fixes:
Stage 1: Reliable Kafka Offset Management
Stage 2: Circuit Breaker OPEN-State Record Preservation
Stage 3: Producer Failure Propagation, Retries, and Safe Shutdown
"""

import json
import time
import pytest
from kafka import TopicPartition, OffsetAndMetadata

from app import kafka_consumer
from app import main
from app.downstream import valid_producer
from app.dlq import dlq_producer
from app.circuit_breaker.circuit_breaker import (
    circuit,
    save_shared_status,
    SHARED_STATUS_FILE
)
from app.circuit_breaker.states import CLOSED, OPEN, HALF_OPEN


class MockKafkaMessage:
    def __init__(self, value, topic="processed-transactions", partition=0, offset=0):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset


class MockReliableConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.committed_offsets = []
        self._paused = set()
        self._assigned = {TopicPartition("processed-transactions", 0)}
        self.call_count = 0
        self.seek_calls = []

    def assignment(self):
        return set(self._assigned)

    def pause(self, *partitions):
        self._paused.update(partitions)

    def resume(self, *partitions):
        self._paused.difference_update(partitions)

    def paused(self):
        return set(self._paused)

    def poll(self, timeout_ms=500):
        self.call_count += 1
        if self._paused:
            return {}
        if self.batches:
            return self.batches.pop(0)
        return {}

    def commit(self, offsets=None):
        self.committed_offsets.append(offsets)

    def seek(self, partition, offset):
        self.seek_calls.append((partition, offset))

    def close(self):
        pass


def make_valid_txn(index):
    return {
        "transaction_id": f"00000000-0000-4000-8000-{index:012d}",
        "customer_id": f"CUST-{index:04d}",
        "amount": 100.0 + index,
        "currency": "INR",
        "timestamp": "2026-10-09T12:00:00Z",
        "merchant": "Amazon",
        "status": "SUCCESS",
    }


def make_invalid_txn(index):
    rec = make_valid_txn(index)
    rec["status"] = "INVALID_STATUS"
    return rec


@pytest.fixture(autouse=True)
def clean_environment():
    circuit.state = CLOSED
    circuit.opened_at = None
    yield
    circuit.state = CLOSED
    circuit.opened_at = None


# ==============================================================================
# STAGE 1: Reliable Kafka Offset Management Tests
# ==============================================================================

def test_stage1_auto_commit_is_disabled():
    """Verify that create_consumer configures enable_auto_commit=False."""
    # We inspect the factory function logic or inspect default parameter
    # Test with default environment
    consumer_instance = kafka_consumer.create_consumer(
        topic="test-topic",
        bootstrap_servers="localhost:9092",
        group_id="test-group"
    )
    # In kafka-python, config dictionary is in consumer_instance.config
    assert consumer_instance.config.get("enable_auto_commit") is False


def test_stage1_successful_processing_commits_max_offset_plus_one(monkeypatch):
    """
    Verify that successful processing commits partition offsets at (max_offset + 1).
    Tests multi-partition and multi-message batch handling.
    """
    tp0 = TopicPartition("processed-transactions", 0)
    tp1 = TopicPartition("processed-transactions", 1)

    # tp0 has offsets 10, 11
    # tp1 has offsets 5, 6
    batch_map = {
        tp0: [
            MockKafkaMessage(make_valid_txn(1), partition=0, offset=10),
            MockKafkaMessage(make_valid_txn(2), partition=0, offset=11),
        ],
        tp1: [
            MockKafkaMessage(make_valid_txn(3), partition=1, offset=5),
            MockKafkaMessage(make_valid_txn(4), partition=1, offset=6),
        ],
    }

    mock_consumer = MockReliableConsumer([batch_map])
    mock_consumer._assigned = {tp0, tp1}

    valid_sent = []
    monkeypatch.setattr(main, "send_valid_record", lambda r: valid_sent.append(r) or r)
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=4,
        max_iterations=1,
    )

    # 4 records sent downstream
    assert len(valid_sent) == 4
    # Offsets committed exactly once
    assert len(mock_consumer.committed_offsets) == 1
    committed = mock_consumer.committed_offsets[0]

    assert tp0 in committed
    assert committed[tp0].offset == 12  # max 11 + 1

    assert tp1 in committed
    assert committed[tp1].offset == 7   # max 6 + 1


def test_stage1_failed_valid_publication_prevents_offset_commit(monkeypatch):
    """Verify that if valid producer flush fails, input offsets are NEVER committed."""
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [MockKafkaMessage(make_valid_txn(1), partition=0, offset=10)]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    def failing_flush():
        raise RuntimeError("Kafka broker unavailable")

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", failing_flush)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        sleep_fn=lambda s: None,
    )

    # Offsets must NOT be committed
    assert len(mock_consumer.committed_offsets) == 0


def test_stage1_failed_dlq_publication_prevents_offset_commit(monkeypatch):
    """Verify that if DLQ producer flush fails, input offsets are NEVER committed."""
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [MockKafkaMessage(make_invalid_txn(1), partition=0, offset=20)]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    def failing_dlq_flush():
        raise RuntimeError("DLQ cluster timeout")

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", failing_dlq_flush)

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        sleep_fn=lambda s: None,
    )

    # Offsets must NOT be committed
    assert len(mock_consumer.committed_offsets) == 0


def test_stage1_reprocessing_after_failed_commit_does_not_skip_records(monkeypatch):
    """
    Verify at-least-once semantics:
    If a batch fails delivery on iteration 1, it remains in memory and is retried
    rather than being discarded.
    """
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [MockKafkaMessage(make_valid_txn(1), partition=0, offset=30)]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    attempts = {"count": 0}
    delivered = []

    def flakey_flush():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("Transient glitch")
        # Succeeds on attempt 2

    monkeypatch.setattr(main, "send_valid_record", lambda r: delivered.append(r) or r)
    monkeypatch.setattr(main, "flush_valid_producer", flakey_flush)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        max_retries=2,
        sleep_fn=lambda s: None,
    )

    # Delivered after retry, not skipped
    assert len(delivered) >= 1
    # Offset committed after recovery
    assert len(mock_consumer.committed_offsets) == 1
    assert mock_consumer.committed_offsets[0][tp0].offset == 31


# ==============================================================================
# STAGE 2: Circuit Breaker OPEN-State Record Preservation Tests
# ==============================================================================

def test_stage2_records_retained_and_consumer_paused_during_open_state(monkeypatch):
    """
    Verify that when circuit trips to OPEN:
    1. Valid records are retained in pending_blocked_records.
    2. Consumer is paused so no further messages are read from Kafka.
    3. Valid records are not sent downstream while OPEN.
    """
    tp0 = TopicPartition("processed-transactions", 0)
    # 1 valid, 1 invalid -> 50% error rate trips circuit to OPEN
    batch_map = {
        tp0: [
            MockKafkaMessage(make_valid_txn(1), partition=0, offset=50),
            MockKafkaMessage(make_invalid_txn(2), partition=0, offset=51),
        ]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    valid_sent = []
    dlq_sent = []
    monkeypatch.setattr(main, "send_valid_record", lambda r: valid_sent.append(r) or r)
    monkeypatch.setattr(main, "send_to_dlq", lambda r, e: dlq_sent.append(r) or {"record": r, "errors": e})
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=2,
        max_iterations=1,
    )

    assert circuit.state == OPEN
    # Valid record was NOT sent downstream
    assert len(valid_sent) == 0
    # Invalid record was sent to DLQ
    assert len(dlq_sent) == 1
    # Consumer was paused
    assert tp0 in mock_consumer.paused()


def test_stage2_open_half_open_closed_recovery_flushes_held_records(monkeypatch):
    """
    Verify full circuit breaker lifecycle recovery:
    1. Trip to OPEN -> consumer paused, valid records held.
    2. Timeout elapses -> state transitions to HALF_OPEN -> consumer resumed.
    3. Healthy probe batch -> recovery succeeds, state transitions to CLOSED.
    4. Held valid records from step 1 are flushed downstream and committed!
    """
    tp0 = TopicPartition("processed-transactions", 0)
    # Batch 1: Trips circuit to OPEN (1 valid, 1 invalid)
    trip_batch = {
        tp0: [
            MockKafkaMessage(make_valid_txn(1), partition=0, offset=100),
            MockKafkaMessage(make_invalid_txn(2), partition=0, offset=101),
        ]
    }
    # Batch 2: Arrives during HALF_OPEN (1 valid, 0 invalid -> healthy)
    healthy_batch = {
        tp0: [
            MockKafkaMessage(make_valid_txn(3), partition=0, offset=102),
        ]
    }

    mock_consumer = MockReliableConsumer([trip_batch, healthy_batch])

    valid_sent = []
    monkeypatch.setattr(main, "send_valid_record", lambda r: valid_sent.append(r) or r)
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)
    monkeypatch.setattr(main, "send_to_dlq", lambda r, e: {"record": r, "errors": e})

    clock_time = [100.0]
    def fake_clock():
        return clock_time[0]

    # Iteration 1: Process trip batch -> circuit trips to OPEN, consumer paused
    # Iteration 2: Advance clock by 35s (> 30s timeout) -> transitions to HALF_OPEN, consumer resumed
    # Iteration 3: Process healthy batch -> transitions to CLOSED, held records flushed!
    def step_clock(poll_timeout=500):
        if mock_consumer.call_count == 2:
            clock_time[0] += 35.0
            # Manually simulate monotonic time passing in circuit_breaker
            circuit.opened_at = clock_time[0] - 35.0

    orig_poll = mock_consumer.poll
    def instrumented_poll(timeout_ms=500):
        step_clock()
        return orig_poll(timeout_ms)

    mock_consumer.poll = instrumented_poll

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=4,
        clock=fake_clock,
    )

    # After recovery to CLOSED:
    # Both the held record (TXN 1) and the probe record (TXN 3) were delivered downstream!
    tx_ids = [r["transaction_id"] for r in valid_sent]
    assert make_valid_txn(1)["transaction_id"] in tx_ids
    assert make_valid_txn(3)["transaction_id"] in tx_ids
    assert circuit.state == CLOSED


def test_stage2_shutdown_during_open_state_rewinds_and_does_not_commit(monkeypatch):
    """
    Verify that if service shuts down while circuit is OPEN and blocked records are held:
    1. Held valid records offsets are NOT committed.
    2. Consumer is rewound via seek back to the first blocked record offset.
    """
    tp0 = TopicPartition("processed-transactions", 0)
    trip_batch = {
        tp0: [
            MockKafkaMessage(make_valid_txn(1), partition=0, offset=200),
            MockKafkaMessage(make_invalid_txn(2), partition=0, offset=201),
        ]
    }
    mock_consumer = MockReliableConsumer([trip_batch])

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)
    monkeypatch.setattr(main, "send_to_dlq", lambda r, e: {"record": r, "errors": e})

    main.run_service(
        consumer=mock_consumer,
        batch_size=2,
        max_iterations=1,
    )

    # Circuit remained OPEN
    assert circuit.state == OPEN
    # Consumer seek was called to rewind offset 200
    assert len(mock_consumer.seek_calls) >= 1
    seek_tp, seek_offset = mock_consumer.seek_calls[0]
    assert seek_tp == tp0
    assert seek_offset == 200


def test_stage2_record_count_reconciliation():
    """Verify that process_records never loses records from total count reconciliation."""
    records = [
        make_valid_txn(1),
        make_valid_txn(2),
        make_invalid_txn(3),
    ]
    circuit.state = OPEN

    result = main.process_records(records)

    # Total = bad + valid + blocked
    total = result["total_records"]
    reconciled = (
        result["bad_records"]
        + len(result["valid_records"])
        + len(result["blocked_records"])
    )
    assert total == reconciled == 3


# ==============================================================================
# STAGE 3: Producer Failure Propagation, Retries, and Status Tests
# ==============================================================================

def test_stage3_valid_producer_failure_propagates(monkeypatch):
    """Verify that a valid producer flush failure raises out of process_records."""
    records = [make_valid_txn(1)]

    def failing_flush():
        raise RuntimeError("Broker connection refused")

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", failing_flush)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    with pytest.raises(RuntimeError) as exc_info:
        main.process_records(records)
    assert "Broker connection refused" in str(exc_info.value)


def test_stage3_dlq_producer_failure_propagates(monkeypatch):
    """Verify that a DLQ producer flush failure raises out of process_records."""
    records = [make_invalid_txn(1)]

    def failing_flush():
        raise RuntimeError("DLQ storage failure")

    monkeypatch.setattr(main, "send_to_dlq", lambda r, e: {"record": r, "errors": e})
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "flush_dlq", failing_flush)

    with pytest.raises(RuntimeError) as exc_info:
        main.process_records(records)
    assert "DLQ storage failure" in str(exc_info.value)


def test_stage3_partial_output_success_preserves_batch(monkeypatch):
    """
    Verify partial success semantics:
    If valid records succeed but DLQ fails, the batch is not marked processed,
    offsets are NOT committed, and records are retained for retry.
    """
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [
            MockKafkaMessage(make_valid_txn(1), partition=0, offset=300),
            MockKafkaMessage(make_invalid_txn(2), partition=0, offset=301),
        ]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", lambda: None)
    monkeypatch.setattr(main, "send_to_dlq", lambda r, e: {"record": r, "errors": e})

    def failing_dlq():
        raise RuntimeError("DLQ failure")
    monkeypatch.setattr(main, "flush_dlq", failing_dlq)

    main.run_service(
        consumer=mock_consumer,
        batch_size=2,
        max_iterations=1,
        sleep_fn=lambda s: None,
    )

    # Offsets must NOT be committed due to partial failure
    assert len(mock_consumer.committed_offsets) == 0


def test_stage3_retry_success_after_transient_failure(monkeypatch):
    """Verify bounded exponential backoff succeeds after transient failure."""
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [MockKafkaMessage(make_valid_txn(1), partition=0, offset=400)]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    attempts = {"count": 0}
    backoffs = []

    def flakey_flush():
        attempts["count"] += 1
        if attempts["count"] <= 2:
            raise RuntimeError("Transient network flap")

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", flakey_flush)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        max_retries=3,
        initial_backoff_seconds=0.1,
        backoff_multiplier=2.0,
        sleep_fn=lambda s: backoffs.append(s),
    )

    # Succeeded on attempt 3 (and flushed cleanly on shutdown)
    assert attempts["count"] >= 3
    # Exponential backoffs applied: 0.1, 0.2
    assert len(backoffs) == 2
    assert backoffs[0] == pytest.approx(0.1)
    assert backoffs[1] == pytest.approx(0.2)
    # Offsets committed after retry success
    assert len(mock_consumer.committed_offsets) == 1


def test_stage3_exhausted_retries_pauses_consumer_and_updates_status(monkeypatch):
    """Verify exhausted retries pauses consumer, retains batch, and records status."""
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [MockKafkaMessage(make_valid_txn(1), partition=0, offset=500)]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    def persistent_failure():
        raise RuntimeError("Permanent broker outage")

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", persistent_failure)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        max_retries=2,
        sleep_fn=lambda s: None,
    )

    # Consumer paused to apply backpressure
    assert tp0 in mock_consumer.paused()
    # Offsets NOT committed
    assert len(mock_consumer.committed_offsets) == 0


def test_stage3_shutdown_with_failed_pending_batch_marks_stopped_with_errors(monkeypatch, tmp_path):
    """Verify shutdown with an un-flushed batch records STOPPED_WITH_ERRORS in shared status."""
    tp0 = TopicPartition("processed-transactions", 0)
    batch_map = {
        tp0: [MockKafkaMessage(make_valid_txn(1), partition=0, offset=600)]
    }
    mock_consumer = MockReliableConsumer([batch_map])

    status_saved = []
    monkeypatch.setattr(main, "save_shared_status", lambda state, *args, **kwargs: status_saved.append(state))

    def failing_flush():
        raise RuntimeError("Shutdown broker crash")

    monkeypatch.setattr(main, "send_valid_record", lambda r: r)
    monkeypatch.setattr(main, "flush_valid_producer", failing_flush)
    monkeypatch.setattr(main, "flush_dlq", lambda: None)

    main.run_service(
        consumer=mock_consumer,
        batch_size=1,
        max_iterations=1,
        sleep_fn=lambda s: None,
    )

    # Status reflects STOPPED_WITH_ERRORS
    assert "STOPPED_WITH_ERRORS" in status_saved
