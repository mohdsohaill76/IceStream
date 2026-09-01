import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from jsonschema import validate

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transaction_generator import (
    generate_transaction,
    generate_payload,
    get_message_key,
    get_next_transaction,
    on_send_error,
    on_send_success,
)


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "transaction_schema.json"

with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
    TRANSACTION_SCHEMA = json.load(schema_file)


def generate_valid_transaction():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0), \
         patch("transaction_generator.BAD_AMOUNT_RATE", 0), \
         patch("transaction_generator.MISSING_FIELD_RATE", 0), \
         patch("transaction_generator.INVALID_VALUE_RATE", 0):

        return generate_transaction()


def test_transaction_has_required_fields():
    transaction = generate_valid_transaction()

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
    transaction = generate_valid_transaction()

    assert transaction["transaction_id"]
    assert isinstance(transaction["transaction_id"], str)


def test_amount_is_valid():
    transaction = generate_valid_transaction()

    assert isinstance(transaction["amount"], float)
    assert 100 <= transaction["amount"] <= 5000


def test_status_is_valid():
    transaction = generate_valid_transaction()

    assert transaction["status"] in {
        "SUCCESS",
        "PENDING",
        "FAILED",
    }


def test_customer_id_is_valid():
    transaction = generate_valid_transaction()

    assert transaction["customer_id"].startswith("CUST-")


def test_currency_is_valid():
    transaction = generate_valid_transaction()

    assert transaction["currency"] == "INR"


def test_null_injection():
    with patch("transaction_generator.NULL_INJECTION_RATE", 1), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0), \
         patch("transaction_generator.BAD_AMOUNT_RATE", 0), \
         patch("transaction_generator.MISSING_FIELD_RATE", 0), \
         patch("transaction_generator.INVALID_VALUE_RATE", 0):

        transaction = generate_transaction()

    assert any(value is None for value in transaction.values())


def test_schema_change_injection():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 1), \
         patch("transaction_generator.BAD_AMOUNT_RATE", 0), \
         patch("transaction_generator.MISSING_FIELD_RATE", 0), \
         patch("transaction_generator.INVALID_VALUE_RATE", 0):

        transaction = generate_transaction()

    assert transaction["unexpected_field"] == "SCHEMA_CHANGE"


def test_duplicate_generation():
    previous_transaction = {
        "transaction_id": "duplicate-test-id",
        "customer_id": "CUST-1234",
        "amount": 500.0,
        "currency": "INR",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "merchant": "MERCHANT-100",
        "status": "SUCCESS",
    }

    with patch("transaction_generator.DUPLICATE_RATE", 1):
        transaction, is_duplicate = get_next_transaction(
            previous_transaction
        )

    assert is_duplicate is True
    assert transaction == previous_transaction


def test_bad_amount_generation():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0), \
         patch("transaction_generator.BAD_AMOUNT_RATE", 1), \
         patch("transaction_generator.MISSING_FIELD_RATE", 0), \
         patch("transaction_generator.INVALID_VALUE_RATE", 0):

        transaction = generate_transaction()

    assert transaction["amount"] <= 0


def test_missing_required_field():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0), \
         patch("transaction_generator.BAD_AMOUNT_RATE", 0), \
         patch("transaction_generator.MISSING_FIELD_RATE", 1), \
         patch("transaction_generator.INVALID_VALUE_RATE", 0):

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

    assert not required_fields.issubset(transaction.keys())


def test_invalid_value_generation():
    with patch("transaction_generator.NULL_INJECTION_RATE", 0), \
         patch("transaction_generator.SCHEMA_CHANGE_RATE", 0), \
         patch("transaction_generator.BAD_AMOUNT_RATE", 0), \
         patch("transaction_generator.MISSING_FIELD_RATE", 0), \
         patch("transaction_generator.INVALID_VALUE_RATE", 1):

        transaction = generate_transaction()

    invalid_found = (
        transaction.get("currency") == "INVALID"
        or transaction.get("status") == "UNKNOWN"
        or transaction.get("customer_id") == ""
        or transaction.get("merchant") == ""
        or transaction.get("timestamp") == "not-a-date"
    )

    assert invalid_found


def test_malformed_json_generation():
    with patch("transaction_generator.MALFORMED_JSON_RATE", 1):
        payload = generate_payload()

    assert isinstance(payload, str)
    assert payload == '{"transaction_id": "broken", "amount": '


def test_valid_transaction_matches_json_schema():
    transaction = generate_valid_transaction()

    validate(
        instance=transaction,
        schema=TRANSACTION_SCHEMA,
    )


def test_message_key_uses_transaction_id():
    transaction = generate_valid_transaction()

    key = get_message_key(transaction)

    assert key == transaction["transaction_id"]


def test_message_key_is_none_when_transaction_id_missing():
    transaction = {
        "customer_id": "CUST-1234",
        "amount": 500.0,
    }

    assert get_message_key(transaction) is None


def test_delivery_success_callback(capsys):
    metadata = MagicMock()
    metadata.topic = "ecommerce-transactions"
    metadata.partition = 1
    metadata.offset = 25

    on_send_success(metadata)

    captured = capsys.readouterr()

    assert "ecommerce-transactions" in captured.out
    assert "partition=1" in captured.out
    assert "offset=25" in captured.out


def test_delivery_error_callback(capsys):
    on_send_error(Exception("Kafka unavailable"))

    captured = capsys.readouterr()

    assert "Kafka delivery failed" in captured.out
    assert "Kafka unavailable" in captured.out