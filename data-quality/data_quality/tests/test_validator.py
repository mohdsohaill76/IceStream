from app.quality.validator import validate_record


def test_valid_record():
    # Sample valid transaction
    record = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
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
    record = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440001",
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
    record = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440002",
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
        "transaction_id": "550e8400-e29b-41d4-a716-446655440004",
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
    record = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440008",
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
    record = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440009",
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
    record = {
        "transaction_id": "550e8400-e29b-41d4-a716-446655440010",
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
        "transaction_id": "550e8400-e29b-41d4-a716-446655440011",
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
        "transaction_id": "550e8400-e29b-41d4-a716-446655440012",
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
        "transaction_id": "550e8400-e29b-41d4-a716-446655440013",
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


def test_invalid_transaction_uuid():
    # Transaction ID must follow the shared UUID contract
    record = {
        "transaction_id": "TXN001",
        "customer_id": "CUST014",
        "amount": 500,
        "currency": "INR",
        "timestamp": "2026-09-03T10:00:00Z",
        "merchant": "MERCHANT-114",
        "status": "SUCCESS"
    }

    valid, errors = validate_record(record)

    assert valid is False
    assert "transaction_id must be a valid UUID" in errors