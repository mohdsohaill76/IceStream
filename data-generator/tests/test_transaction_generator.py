from kafka.serializer import Serializer
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transaction_generator import generate_transaction


def test_transaction_has_required_fields():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    required_fields = {
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "timestamp",
        "merchant",
        "status",
    }

    assert required_fields.issubset(transaction.keys())


def test_transaction_id_is_present():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    assert transaction["transaction_id"]
    assert isinstance(transaction["transaction_id"], str)


def test_amount_is_valid():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    assert isinstance(transaction["amount"], float)
    assert 100 <= transaction["amount"] <= 5000


def test_status_is_valid():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    assert transaction["status"] in {
        "SUCCESS",
        "PENDING",
        "FAILED",
    }


def test_customer_id_is_valid():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    assert transaction["customer_id"].startswith("CUST-")


def test_currency_is_valid():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    assert transaction["currency"] == "INR"


def test_null_injection():
    with patch("transaction_generator.NULL_INJECTION_RATE", 1), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0):
        transaction = generate_transaction()

    assert any(value is None for value in transaction.values())


def test_schema_change_injection():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 1):
        transaction = generate_transaction()

    assert transaction["unexpected_field"] == "SCHEMA_CHANGE"