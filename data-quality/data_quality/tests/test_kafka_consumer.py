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

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

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
    class FakeMessage:
        value = b'{"transaction_id": "TXN001", invalid_json}'

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    def fake_send_to_dlq(record, errors):
        dlq_calls.append({
            "record": record,
            "errors": errors
        })

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

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == (
        '{"transaction_id": "TXN001", invalid_json}'
    )
    assert "Malformed JSON" in dlq_calls[0]["errors"][0]


def test_malformed_json_does_not_stop_next_valid_message(monkeypatch):
    class MalformedMessage:
        value = b'{"transaction_id": "TXN001", invalid_json}'

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

    class FakeConsumer:
        def __iter__(self):
            return iter([MalformedMessage(), ValidMessage()])

        def close(self):
            pass

    dlq_calls = []

    def fake_send_to_dlq(record, errors):
        dlq_calls.append({
            "record": record,
            "errors": errors
        })

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

    assert len(dlq_calls) == 1
    assert len(records) == 1
    assert records[0]["transaction_id"] == "TXN002"
    assert records[0]["status"] == "SUCCESS"


def test_none_payload_is_sent_to_dlq(monkeypatch):
    class FakeMessage:
        value = None

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    def fake_send_to_dlq(record, errors):
        dlq_calls.append({
            "record": record,
            "errors": errors
        })

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

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] is None


def test_string_payload_is_sent_to_dlq(monkeypatch):
    class FakeMessage:
        value = "invalid payload"

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: dlq_calls.append({
            "record": record,
            "errors": errors
        })
    )

    records = list(kafka_consumer.consume_records())

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == "invalid payload"


def test_integer_payload_is_sent_to_dlq(monkeypatch):
    class FakeMessage:
        value = 12345

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: dlq_calls.append({
            "record": record,
            "errors": errors
        })
    )

    records = list(kafka_consumer.consume_records())

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == "12345"


def test_json_null_is_sent_to_dlq(monkeypatch):
    class FakeMessage:
        value = b"null"

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: dlq_calls.append({
            "record": record,
            "errors": errors
        })
    )

    records = list(kafka_consumer.consume_records())

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == "null"


def test_json_true_is_sent_to_dlq(monkeypatch):
    class FakeMessage:
        value = b"true"

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: dlq_calls.append({
            "record": record,
            "errors": errors
        })
    )

    records = list(kafka_consumer.consume_records())

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == "true"


def test_json_list_is_sent_to_dlq(monkeypatch):
    class FakeMessage:
        value = b"[]"

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    dlq_calls = []

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: dlq_calls.append({
            "record": record,
            "errors": errors
        })
    )

    records = list(kafka_consumer.consume_records())

    assert records == []
    assert len(dlq_calls) == 1
    assert dlq_calls[0]["record"]["raw_message"] == "[]"


def test_invalid_payload_does_not_stop_next_valid_message(monkeypatch):
    class InvalidMessage:
        value = None

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

    class FakeConsumer:
        def __iter__(self):
            return iter([
                InvalidMessage(),
                ValidMessage()
            ])

        def close(self):
            pass

    dlq_calls = []

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: dlq_calls.append({
            "record": record,
            "errors": errors
        })
    )

    records = list(kafka_consumer.consume_records())

    assert len(dlq_calls) == 1
    assert len(records) == 1
    assert records[0]["transaction_id"] == "TXN002"
    assert records[0]["status"] == "SUCCESS"


def test_malformed_message_count(monkeypatch):
    class FakeMessage:
        value = b'{"bad_json":'

    class FakeConsumer:
        def __iter__(self):
            return iter([FakeMessage()])

        def close(self):
            pass

    monkeypatch.setattr(
        kafka_consumer,
        "create_consumer",
        lambda: FakeConsumer()
    )

    monkeypatch.setattr(
        kafka_consumer,
        "send_to_dlq",
        lambda record, errors: None
    )

    records = list(kafka_consumer.consume_records())

    assert records == []