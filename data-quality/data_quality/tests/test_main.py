# Test the main data quality flow
import pytest
from app import main
from app.circuit_breaker.circuit_breaker import circuit
from app.circuit_breaker.states import CLOSED

@pytest.fixture(autouse=True)
def reset_circuit():
    # Start every test with a fresh closed circuit
    circuit.state = CLOSED
    
def test_process_records(monkeypatch):
    
    # Sample valid and invalid transactions
    records = [
        {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "SUCCESS"
        },
        {
            "transaction_id": "TXN002",
            "customer_id": "CUST002",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-102",
            "status": "UNKNOWN"
        }
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
        {
            "transaction_id": f"TXN{i}",
            "customer_id": f"CUST{i}",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "SUCCESS"
        }
        for i in range(99)
    ]

    records.append({
        "transaction_id": "BAD001",
        "customer_id": "CUST100",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "UNKNOWN"
    })

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
    circuit.state = CLOSED
    # 2 bad records out of 100 = 2%
    records = [
        {
            "transaction_id": f"TXN{i}",
            "customer_id": f"CUST{i}",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "SUCCESS"
        }
        for i in range(98)
    ]

    for i in range(2):
        records.append({
            "transaction_id": f"BAD00{i}",
            "customer_id": f"CUST_BAD{i}",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "UNKNOWN"
        })

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
    circuit.state = CLOSED
    # 3 bad records out of 100 = 3%
    records = [
        {
            "transaction_id": f"TXN{i}",
            "customer_id": f"CUST{i}",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "SUCCESS"
        }
        for i in range(97)
    ]

    for i in range(3):
        records.append({
            "transaction_id": f"BAD00{i}",
            "customer_id": f"CUST_BAD{i}",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "UNKNOWN"
        })

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