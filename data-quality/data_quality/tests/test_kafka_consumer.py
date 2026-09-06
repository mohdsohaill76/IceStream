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


def test_malformed_json_is_sent_to_dlq(monkeypatch):
    # Fake malformed Kafka message
    class FakeMessage:
        value = b'{"transaction_id": "TXN001", invalid_json}'

    # Fake consumer
    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    # Fake DLQ function
    def fake_send_to_dlq(record, errors):
        dlq_calls.append({
            "record": record,
            "errors": errors
        })

    # Replace Kafka consumer and DLQ function
    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        fake_send_to_dlq
    )

    records = list(kafka_consumer.consume_records())

    # Malformed message must not be returned
    assert records == []

    # Malformed message must be sent to DLQ
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == (
        '{"transaction_id": "TXN001", invalid_json}'
    )
    assert "Malformed JSON" in dlq_calls[0]["errors"][0]


def test_malformed_json_does_not_stop_next_valid_message(monkeypatch):
    # First message is malformed
    class MalformedMessage:
        value = b'{"transaction_id": "TXN001", invalid_json}'

    # Second message is valid
    class ValidMessage:
        value = b'''{
            "transaction_id": "TXN002",
            "customer_id": "CUST002",
            "amount": 500,
            "currency": "INR",
            "timestamp": "2026-09-03T10:00:00Z",
            "merchant": "MERCHANT-102",
            "status": "SUCCESS"
        }'''

    # Fake consumer sends malformed message first,
    # then a valid message
    class FakeConsumer:
        def __iter__(self):
            return iter([MalformedMessage(), ValidMessage()])

        def close(self):
            pass

    dlq_calls = []

    # Fake DLQ function
    def fake_send_to_dlq(record, errors):
        dlq_calls.append({
            "record": record,
            "errors": errors
        })

    # Replace Kafka consumer and DLQ function
    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        fake_send_to_dlq
    )

    records = list(kafka_consumer.consume_records())

    # Malformed message goes to DLQ
    assert len(dlq_calls) == 1

    # Consumer continues and processes valid message
    assert len(records) == 1
    assert records[0]["transaction_id"] == "TXN002"
    assert records[0]["status"] == "SUCCESS"