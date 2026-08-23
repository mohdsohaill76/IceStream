import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transaction_generator import generate_transaction


def test_transaction_has_required_fields():
    transaction = generate_transaction()

    required_fields = {
        "transaction_id",
        "customer_id",
        "product_id",
        "amount",
        "tax_amount",
        "timestamp",
        "payment_status",
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


def test_payment_status_is_valid():
    transaction = generate_transaction()

    assert transaction["payment_status"] in {
        "SUCCESS",
        "PENDING",
        "FAILED",
    }


def test_customer_and_product_ids():
    transaction = generate_transaction()

    assert transaction["customer_id"].startswith("CUST-")
    assert transaction["product_id"].startswith("PROD-")