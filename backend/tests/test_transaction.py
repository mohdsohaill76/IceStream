from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from app.models.transaction import Transaction


def get_valid_payload():
    """Helper to return a valid transaction payload dictionary."""
    return {
        "transaction_id": "tx-1001",
        "customer_id": "cust-2002",
        "amount": 149.50,
        "currency": "USD",
        "timestamp": datetime.now(timezone.utc),
        "merchant": "IceStream Store",
        "status": "completed",
    }


def test_valid_transaction():
    """Test that a valid transaction payload passes validation and initializes properly."""
    payload = get_valid_payload()
    tx = Transaction(**payload)
    assert tx.transaction_id == "tx-1001"
    assert tx.customer_id == "cust-2002"
    assert tx.amount == 149.50
    assert tx.currency == "USD"
    assert tx.merchant == "IceStream Store"
    assert tx.status == "completed"


def test_missing_transaction_id():
    """Test that missing transaction_id raises ValidationError."""
    payload = get_valid_payload()
    del payload["transaction_id"]
    with pytest.raises(ValidationError):
        Transaction(**payload)


def test_missing_customer_id():
    """Test that missing customer_id raises ValidationError."""
    payload = get_valid_payload()
    del payload["customer_id"]
    with pytest.raises(ValidationError):
        Transaction(**payload)


def test_zero_amount():
    """Test that zero amount raises ValidationError."""
    payload = get_valid_payload()
    payload["amount"] = 0.0
    with pytest.raises(ValidationError):
        Transaction(**payload)


def test_negative_amount():
    """Test that negative amount raises ValidationError."""
    payload = get_valid_payload()
    payload["amount"] = -15.00
    with pytest.raises(ValidationError):
        Transaction(**payload)


def test_missing_timestamp():
    """Test that missing timestamp raises ValidationError."""
    payload = get_valid_payload()
    del payload["timestamp"]
    with pytest.raises(ValidationError):
        Transaction(**payload)


def test_invalid_timestamp():
    """Test that invalid timestamp string raises ValidationError."""
    payload = get_valid_payload()
    payload["timestamp"] = "invalid-date-string"
    with pytest.raises(ValidationError):
        Transaction(**payload)
