from app.dlq.dlq_producer import send_to_dlq


def test_send_to_dlq():
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": None
    }

    errors = ["tax is missing"]

    result = send_to_dlq(record, errors)

    assert result["record"] == record
    assert result["errors"] == errors