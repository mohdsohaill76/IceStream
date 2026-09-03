from app.quality.validator import validate_record


def test_valid_record():
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": 50
    }

    valid, errors = validate_record(record)

    assert valid is True
    assert errors == []


def test_missing_tax():
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": None
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "tax is missing" in errors

def test_invalid_numeric_types():
    # Invalid numeric values should be treated as bad records
    record = {
        "transaction_id": "TXN003",
        "customer_id": "CUST003",
        "product_id": "PROD003",
        "quantity": "two",
        "amount": "five hundred",
        "tax": "ten"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "quantity must be a number" in errors
    assert "amount must be a number" in errors
    assert "tax must be a number" in errors

def test_empty_ids():
    # Empty IDs should be treated as invalid
    record = {
        "transaction_id": "",
        "customer_id": "",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": 50
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "transaction_id cannot be empty" in errors
    assert "customer_id cannot be empty" in errors