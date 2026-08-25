# Test DLQ record creation
from app.dlq.dlq_producer import send_to_dlq
from app.dlq.incident import create_incident


def test_send_to_dlq():
    # Sample invalid transaction
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": None
    }

    # Validation error for the transaction
    errors = ["tax is missing"]

    result = send_to_dlq(record, errors)

    assert result["record"] == record
    assert result["errors"] == errors


def test_create_incident():
    # Sample invalid transaction
    record = {
        "transaction_id": "TXN002",
        "amount": 500,
        "tax": None
    }

    # Validation error for the transaction
    errors = ["tax is missing"]

    result = create_incident(record, errors)

    assert result["transaction_id"] == "TXN002"
    assert result["errors"] == errors