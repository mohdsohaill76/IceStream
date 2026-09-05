import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from jsonschema import validate


# --------------------------------------------------
# Paths
# --------------------------------------------------

DATA_GENERATOR_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = DATA_GENERATOR_DIR / "src"

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(DATA_GENERATOR_DIR))


# --------------------------------------------------
# Imports
# --------------------------------------------------

from transaction_generator import (
    generate_transaction,
    generate_payload,
    get_next_transaction,
    get_message_key,
    on_send_success,
    on_send_error,
)

from consumer_verification import process_message


# --------------------------------------------------
# Load shared JSON schema
# --------------------------------------------------

SCHEMA_PATH = DATA_GENERATOR_DIR / "transaction_schema.json"

with open(
    SCHEMA_PATH,
    "r",
    encoding="utf-8",
) as schema_file:
    TRANSACTION_SCHEMA = json.load(schema_file)


# --------------------------------------------------
# Helper
# --------------------------------------------------

def generate_valid_transaction():
    with patch(
        "transaction_generator.NULL_INJECTION_RATE",
        0,
    ), patch(
        "transaction_generator.SCHEMA_CHANGE_RATE",
        0,
    ), patch(
        "transaction_generator.BAD_AMOUNT_RATE",
        0,
    ), patch(
        "transaction_generator.MISSING_FIELD_RATE",
        0,
    ), patch(
        "transaction_generator.INVALID_VALUE_RATE",
        0,
    ):
        return generate_transaction()


# --------------------------------------------------
# Basic transaction tests
# --------------------------------------------------

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


def test_amount_is_valid():
    transaction = generate_valid_transaction()

    assert isinstance(
        transaction["amount"],
        (int, float),
    )

    assert transaction["amount"] > 0


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


# --------------------------------------------------
# Anomaly tests
# --------------------------------------------------

def test_null_injection():
    with patch(
        "transaction_generator.NULL_INJECTION_RATE",
        1,
    ), patch(
        "transaction_generator.SCHEMA_CHANGE_RATE",
        0,
    ), patch(
        "transaction_generator.BAD_AMOUNT_RATE",
        0,
    ), patch(
        "transaction_generator.MISSING_FIELD_RATE",
        0,
    ), patch(
        "transaction_generator.INVALID_VALUE_RATE",
        0,
    ):
        transaction = generate_transaction()

    assert any(
        value is None
        for value in transaction.values()
    )


def test_schema_change_injection():
    with patch(
        "transaction_generator.NULL_INJECTION_RATE",
        0,
    ), patch(
        "transaction_generator.SCHEMA_CHANGE_RATE",
        1,
    ), patch(
        "transaction_generator.BAD_AMOUNT_RATE",
        0,
    ), patch(
        "transaction_generator.MISSING_FIELD_RATE",
        0,
    ), patch(
        "transaction_generator.INVALID_VALUE_RATE",
        0,
    ):
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

    with patch(
        "transaction_generator.DUPLICATE_RATE",
        1,
    ):
        transaction, is_duplicate = get_next_transaction(
            previous_transaction
        )

    assert is_duplicate is True
    assert transaction == previous_transaction


def test_bad_amount_generation():
    with patch(
        "transaction_generator.NULL_INJECTION_RATE",
        0,
    ), patch(
        "transaction_generator.SCHEMA_CHANGE_RATE",
        0,
    ), patch(
        "transaction_generator.BAD_AMOUNT_RATE",
        1,
    ), patch(
        "transaction_generator.MISSING_FIELD_RATE",
        0,
    ), patch(
        "transaction_generator.INVALID_VALUE_RATE",
        0,
    ):
        transaction = generate_transaction()

    assert transaction["amount"] <= 0


def test_missing_required_field():
    with patch(
        "transaction_generator.NULL_INJECTION_RATE",
        0,
    ), patch(
        "transaction_generator.SCHEMA_CHANGE_RATE",
        0,
    ), patch(
        "transaction_generator.BAD_AMOUNT_RATE",
        0,
    ), patch(
        "transaction_generator.MISSING_FIELD_RATE",
        1,
    ), patch(
        "transaction_generator.INVALID_VALUE_RATE",
        0,
    ):
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

    assert not required_fields.issubset(
        transaction.keys()
    )


def test_invalid_value_generation():
    with patch(
        "transaction_generator.NULL_INJECTION_RATE",
        0,
    ), patch(
        "transaction_generator.SCHEMA_CHANGE_RATE",
        0,
    ), patch(
        "transaction_generator.BAD_AMOUNT_RATE",
        0,
    ), patch(
        "transaction_generator.MISSING_FIELD_RATE",
        0,
    ), patch(
        "transaction_generator.INVALID_VALUE_RATE",
        1,
    ):
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
    with patch(
        "transaction_generator.MALFORMED_JSON_RATE",
        1,
    ):
        payload = generate_payload()

    assert isinstance(payload, str)

    assert payload == (
        '{"transaction_id": "broken", "amount": '
    )


# --------------------------------------------------
# JSON schema validation
# --------------------------------------------------

def test_valid_transaction_matches_json_schema():
    transaction = generate_valid_transaction()

    validate(
        instance=transaction,
        schema=TRANSACTION_SCHEMA,
    )


# --------------------------------------------------
# Kafka message key tests
# --------------------------------------------------

def test_message_key_uses_transaction_id():
    transaction = generate_valid_transaction()

    key = get_message_key(transaction)

    assert key == transaction["transaction_id"]


def test_message_key_is_none_when_transaction_id_missing():
    transaction = generate_valid_transaction()

    transaction.pop(
        "transaction_id",
        None,
    )

    key = get_message_key(transaction)

    assert key is None


# --------------------------------------------------
# Kafka callback tests
# --------------------------------------------------

def test_delivery_success_callback(caplog):
    metadata = MagicMock()

    metadata.topic = "ecommerce-transactions"
    metadata.partition = 1
    metadata.offset = 25

    with caplog.at_level("INFO"):
        on_send_success(metadata)

    assert "ecommerce-transactions" in caplog.text
    assert "partition=1" in caplog.text
    assert "offset=25" in caplog.text


def test_delivery_error_callback(caplog):
    with caplog.at_level("ERROR"):
        on_send_error(
            Exception("Kafka unavailable")
        )

    assert "Kafka delivery failed" in caplog.text
    assert "Kafka unavailable" in caplog.text


# --------------------------------------------------
# Consumer malformed JSON test
# --------------------------------------------------

def test_malformed_json_does_not_crash_consumer():
    dlq_producer = MagicMock()

    malformed_message = (
        b'{"transaction_id": "broken", "amount": '
    )

    result = process_message(
        malformed_message,
        dlq_producer=dlq_producer,
    )

    assert result is None

    dlq_producer.send.assert_called_once_with(
        "ecommerce-transactions-dlq",
        value='{"transaction_id": "broken", "amount": ',
    )