# Test NULL value validation
from app.quality.null_validator import validate_nulls

def test_no_null_values():
    # Transaction with no NULL values
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }

    valid, errors = validate_nulls(record)

    assert valid is True
    assert errors == []


def test_null_currency():
    # Currency should not be NULL
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST002",
        "amount": 500,
        "currency": None,
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-102",
        "status": "SUCCESS"
    }

    valid, errors = validate_nulls(record)

    assert valid is False
    assert "currency is null" in errors