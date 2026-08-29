import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transaction_generator import generate_transaction


def test_transaction_has_required_fields():
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
    transaction = generate_transaction()

    assert transaction["transaction_id"]
    assert isinstance(transaction["transaction_id"], str)


def test_amount_is_valid():
    transaction = generate_transaction()

    assert isinstance(transaction["amount"], float)
    assert 100 <= transaction["amount"] <= 5000


def test_status_is_valid():
    transaction = generate_transaction()

    assert transaction["status"] in {
        "SUCCESS",
        "PENDING",
        "FAILED",
    }


def test_customer_id_is_valid():
    transaction = generate_transaction()

    assert transaction["customer_id"].startswith("CUST-")


def test_currency_is_valid():
    transaction = generate_transaction()

    assert transaction["currency"] == "INR"


def test_null_injection():
    null_found = False

    for _ in range(200):
        transaction = generate_transaction()

        if any(value is None for value in transaction.values()):
            null_found = True
            break

    assert null_found


def test_schema_change_injection():
    schema_change_found = False

    for _ in range(200):
        transaction = generate_transaction()

        if "unexpected_field" in transaction:
            schema_change_found = True
            break

    assert schema_change_found