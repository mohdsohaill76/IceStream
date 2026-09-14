import pytest

from app import main
from app.circuit_breaker.circuit_breaker import circuit
from app.circuit_breaker.states import CLOSED


@pytest.fixture(autouse=True)
def reset_circuit():
    # Start every test with a fresh closed circuit
    circuit.state = CLOSED


def make_valid_record(index):
    # Create a valid transaction using the agreed UUID contract
    return {
        "transaction_id": f"00000000-0000-4000-8000-{index:012d}",
        "customer_id": f"CUST{index:03d}",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }


def make_invalid_record(index):
    # Create a valid transaction structure with an invalid status
    record = make_valid_record(index)
    record["status"] = "UNKNOWN"
    return record


def test_process_records(monkeypatch):

    # Sample valid and invalid transactions
    records = [
        make_valid_record(1),
        make_invalid_record(2)
    ]

    # Fake DLQ so the test does not need a Kafka broker
    def fake_send_to_dlq(record, errors):
        return {
            "record": record,
            "errors": errors
        }

    monkeypatch.setattr(main, "send_to_dlq", fake_send_to_dlq)

    result = main.process_records(records)

    assert result["total_records"] == 2
    assert result["bad_records"] == 1
    assert result["error_rate"] == 50.0
    assert result["circuit_breaker_triggered"] is True
    assert len(result["dlq_records"]) == 1


def test_100_records_1_bad_stays_closed(monkeypatch):

    # 1 bad record out of 100 = 1%
    records = [
        make_valid_record(i)
        for i in range(99)
    ]

    records.append(make_invalid_record(100))

    monkeypatch.setattr(
        main,
        "send_to_dlq",
        lambda record, errors: {
            "record": record,
            "errors": errors
        }
    )

    result = main.process_records(records)

    assert result["total_records"] == 100
    assert result["bad_records"] == 1
    assert result["error_rate"] == 1.0
    assert result["circuit_breaker_triggered"] is False
    assert result["monitoring_status"]["state"] == "CLOSED"


def test_100_records_2_bad_stays_closed(monkeypatch):

    # 2 bad records out of 100 = 2%
    records = [
        make_valid_record(i)
        for i in range(98)
    ]

    records.append(make_invalid_record(99))
    records.append(make_invalid_record(100))

    monkeypatch.setattr(
        main,
        "send_to_dlq",
        lambda record, errors: {
            "record": record,
            "errors": errors
        }
    )

    result = main.process_records(records)

    assert result["total_records"] == 100
    assert result["bad_records"] == 2
    assert result["error_rate"] == 2.0
    assert result["circuit_breaker_triggered"] is False
    assert result["monitoring_status"]["state"] == "CLOSED"


def test_100_records_3_bad_opens_circuit(monkeypatch):

    # 3 bad records out of 100 = 3%
    records = [
        make_valid_record(i)
        for i in range(97)
    ]

    records.append(make_invalid_record(98))
    records.append(make_invalid_record(99))
    records.append(make_invalid_record(100))

    monkeypatch.setattr(
        main,
        "send_to_dlq",
        lambda record, errors: {
            "record": record,
            "errors": errors
        }
    )

    result = main.process_records(records)

    assert result["total_records"] == 100
    assert result["bad_records"] == 3
    assert result["error_rate"] == 3.0
    assert result["circuit_breaker_triggered"] is True
    assert result["monitoring_status"]["state"] == "OPEN"

def test_valid_records_are_forwarded(monkeypatch):
    # Valid transactions should be preserved for downstream processing
    records = [
        make_valid_record(1),
        make_valid_record(2),
        make_valid_record(3)
    ]

    monkeypatch.setattr(
        main,
        "send_to_dlq",
        lambda record, errors: {
            "record": record,
            "errors": errors
        }
    )

    result = main.process_records(records)

    # All valid records must be forwarded downstream
    assert result["valid_records"] == records

    # No valid record should be sent to DLQ
    assert result["dlq_records"] == []

    # No validation errors
    assert result["bad_records"] == 0
    assert result["error_rate"] == 0.0

def test_open_circuit_blocks_valid_records(monkeypatch):
    # Create a batch with more than 2% invalid records
    records = [
        make_valid_record(1),
        make_valid_record(2),
        make_valid_record(3),
        make_invalid_record(4),
    ]

    monkeypatch.setattr(
        main,
        "send_to_dlq",
        lambda record, errors: {
            "record": record,
            "errors": errors
        }
    )

    result = main.process_records(records)

    # Error rate is above the 2% threshold
    assert result["error_rate"] == 25.0
    assert result["monitoring_status"]["state"] == "OPEN"

    # Valid records must not be forwarded while circuit is OPEN
    assert result["valid_records"] == []

    # Valid records are temporarily blocked, not treated as invalid
    assert len(result["blocked_records"]) == 3

    # Only the genuinely invalid record goes to DLQ
    assert len(result["dlq_records"]) == 1