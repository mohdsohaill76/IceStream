# Test the main data quality flow
from app.main import process_records


def test_process_records():
    # Sample valid and invalid transactions
    records = [
        {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": 2,
            "amount": 500,
            "tax": 50
        },
        {
            "transaction_id": "TXN002",
            "customer_id": "CUST001",
            "product_id": "PROD001",
            "quantity": 0,
            "amount": 500,
            "tax": 50
        }
    ]

    result = process_records(records)

    assert result["total_records"] == 2
    assert result["bad_records"] == 1
    assert result["error_rate"] == 50.0
    assert result["circuit_breaker_triggered"] is True
    assert len(result["dlq_records"]) == 1