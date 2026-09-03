# Test Kafka consumer settings and message reading

from app import kafka_consumer

def test_kafka_consumer_settings():
    # Check the Kafka connection settings
    assert kafka_consumer.KAFKA_SERVER == "localhost:9092"
    assert kafka_consumer.KAFKA_TOPIC == "ecommerce-transactions"
    assert kafka_consumer.KAFKA_GROUP_ID == "icestream-data-quality"

def test_consume_records(monkeypatch):
    # Fake Kafka message
    class FakeMessage:
        value = {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-101",
            "status": "SUCCESS"
        }

    # Fake consumer
    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    # Replace Kafka consumer with the fake consumer
    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    records = list(kafka_consumer.consume_records())

    assert len(records) == 1
    assert records[0]["transaction_id"] == "TXN001"
    assert records[0]["status"] == "SUCCESS"