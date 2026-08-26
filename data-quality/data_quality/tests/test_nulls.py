# Test NULL value validation
from app.quality.null_validator import validate_nulls


def test_no_null_values():
    # Sample transaction without NULL values
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": 50
    }

    valid, errors = validate_nulls(record)

    assert valid is True
    assert errors == []


def test_null_tax():
    # Sample transaction with NULL tax
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": None
    }

    valid, errors = validate_nulls(record)

    assert valid is False
    assert "tax is null" in errors