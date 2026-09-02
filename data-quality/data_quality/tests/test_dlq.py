
from app.dlq import dlq_producer
from app.dlq.incident import create_incident

def test_send_to_dlq(monkeypatch):
    # Sample invalid transaction
    record = {
        "transaction_id": "TXN002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "amount": 500,
        "tax": None
    }

    errors = ["tax is missing"]

    # Fake Kafka producer so test does not need Kafka
    class FakeProducer:
        def send(self, topic, value):
            self.topic = topic
            self.value = value

        def flush(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(
        dlq_producer,
        "create_dlq_producer",
        lambda: FakeProducer()
    )

    result = dlq_producer.send_to_dlq(record, errors)

    assert result["record"] == record
    assert result["errors"] == errors

def test_create_incident():
    # Sample invalid transaction
    record = {
        "transaction_id": "TXN002",
        "amount": 500,
        "tax": None
    }

    errors = ["tax is missing"]

    result = create_incident(record, errors)

    assert result["transaction_id"] == "TXN002"
    assert result["errors"] == errors