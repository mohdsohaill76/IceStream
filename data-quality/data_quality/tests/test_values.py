# Test transaction value validation
from app.quality.value_validator import validate_values

def test_valid_values():
    # Sample transaction with valid values
    record = {
        "quantity": 2,
        "amount": 500,
        "tax": 50
    }
    valid, errors = validate_values(record)

    assert valid is True
    assert errors == []

def test_invalid_quantity():
    # Quantity cannot be zero or negative
    record = {
        "quantity": 0,
        "amount": 500,
        "tax": 50
    }
    valid, errors = validate_values(record)

    assert valid is False
    assert "quantity must be greater than 0" in errors

def test_invalid_amount():
    # Amount must be greater than zero
    record = {
        "quantity": 2,
        "amount": 0,
        "tax": 50
    }
    valid, errors = validate_values(record)

    assert valid is False
    assert "amount must be greater than 0" in errors

def test_negative_tax():
    # Tax cannot be negative
    record = {
        "quantity": 2,
        "amount": 500,
        "tax": -10
    }
    valid, errors = validate_values(record)

    assert valid is False
    assert "tax cannot be negative" in errors