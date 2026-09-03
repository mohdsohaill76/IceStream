# Test transaction schema validation
from app.quality.schema_validator import validate_schema

def test_valid_schema():
    # Sample transaction with all required fields
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }

    valid, errors = validate_schema(record)

    assert valid is True
    assert errors == []

def test_missing_field():
    # Transaction with a missing required field
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST002",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-102"
    }

    valid, errors = validate_schema(record)

    assert valid is False
    assert "status is missing" in errors