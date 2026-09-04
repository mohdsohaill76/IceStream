# Test transaction value validation
from app.quality.value_validator import validate_values


def test_valid_amount():
    record = {
        "amount": 500
    }

    valid, errors = validate_values(record)

    assert valid is True
    assert errors == []


def test_invalid_amount():
    record = {
        "amount": 0
    }

    valid, errors = validate_values(record)

    assert valid is False
    assert "amount must be greater than 0" in errors


def test_invalid_amount_type():
    record = {
        "amount": "five hundred"
    }

    valid, errors = validate_values(record)

    assert valid is False
    assert "amount must be a number" in errors


def test_boolean_amount():
    record = {
        "amount": True
    }

    valid, errors = validate_values(record)

    assert valid is False
    assert "amount must be a number" in errors