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