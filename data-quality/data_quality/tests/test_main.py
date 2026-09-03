# Test the main data quality flow
from app import main

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