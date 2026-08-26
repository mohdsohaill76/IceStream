# Test transaction schema validation
from app.quality.schema_validator import validate_schema


def test_valid_schema():
    # Sample transaction with all required fields
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": 50
    }

    valid, errors = validate_schema(record)

    assert valid is True
    assert errors == []


def test_missing_field():
    # Transaction with a missing required field
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500
    }

    valid, errors = validate_schema(record)

    assert valid is False
    assert "tax is missing" in errors