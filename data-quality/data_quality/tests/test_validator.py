from app.quality.validator import validate_record

def test_valid_record():
    # Sample valid transaction
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is True
    assert errors == []


def test_missing_amount():
    # Transaction with missing amount
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "amount is missing" in errors


def test_invalid_numeric_types():
    # Invalid numeric values should be treated as bad records
    record = {
        "transaction_id": "TXN003",
        "customer_id": "CUST003",
        "amount": "five hundred",
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "amount must be a number" in errors


def test_empty_ids():
    # Empty IDs should be treated as invalid
    record = {
        "transaction_id": "",
        "customer_id": "",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-101",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "transaction_id cannot be empty" in errors
    assert "customer_id cannot be empty" in errors

def test_invalid_currency():
    record = {
        "transaction_id": "TXN004",
        "customer_id": "CUST004",
        "amount": 500,
        "currency": "INVALID",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-104",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "currency must be INR" in errors