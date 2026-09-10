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

def test_id_must_be_string():
    # IDs must be strings
    record = {
        "transaction_id": 123,
        "customer_id": "CUST005",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-105",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "transaction_id must be a string" in errors

def test_whitespace_only_ids():
    # IDs containing only whitespace are invalid
    record = {
        "transaction_id": "   ",
        "customer_id": "\t",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-105",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "transaction_id cannot be empty" in errors
    assert "customer_id cannot be empty" in errors

def test_whitespace_only_merchant():
    # Merchant containing only whitespace is invalid
    record = {
        "transaction_id": "TXN008",
        "customer_id": "CUST008",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "   ",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "merchant cannot be empty" in errors

def test_boolean_false_amount_is_rejected():
    # False must not be accepted as a monetary amount
    record = {
        "transaction_id": "TXN009",
        "customer_id": "CUST009",
        "amount": False,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-109",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "amount must be a number" in errors

def test_tab_only_merchant_is_rejected():
    # Whitespace-only merchant must be rejected
    record = {
        "transaction_id": "TXN010",
        "customer_id": "CUST010",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "\t",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "merchant cannot be empty" in errors

def test_nan_amount_is_rejected():
    record = {
        "transaction_id": "TXN011",
        "customer_id": "CUST011",
        "amount": float("nan"),
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-111",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "amount must be finite" in errors

def test_positive_infinity_amount_is_rejected():
    record = {
        "transaction_id": "TXN012",
        "customer_id": "CUST012",
        "amount": float("inf"),
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-112",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "amount must be finite" in errors

def test_negative_infinity_amount_is_rejected():
    record = {
        "transaction_id": "TXN013",
        "customer_id": "CUST013",
        "amount": float("-inf"),
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-113",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "amount must be finite" in errors