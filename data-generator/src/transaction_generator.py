import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")

GENERATION_RATE = float(os.getenv("GENERATION_RATE", "1"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "0"))

NULL_INJECTION_RATE = float(os.getenv("NULL_INJECTION_RATE", "0.05"))
SCHEMA_CHANGE_RATE = float(os.getenv("SCHEMA_CHANGE_RATE", "0.05"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.05"))

BAD_AMOUNT_RATE = float(os.getenv("BAD_AMOUNT_RATE", "0.03"))
MISSING_FIELD_RATE = float(os.getenv("MISSING_FIELD_RATE", "0.03"))
INVALID_VALUE_RATE = float(os.getenv("INVALID_VALUE_RATE", "0.03"))
MALFORMED_JSON_RATE = float(os.getenv("MALFORMED_JSON_RATE", "0.02"))


if GENERATION_RATE <= 0:
    raise ValueError("GENERATION_RATE must be greater than 0")

if MAX_EVENTS < 0:
    raise ValueError("MAX_EVENTS cannot be negative")


RATE_NAMES = {
    "NULL_INJECTION_RATE": NULL_INJECTION_RATE,
    "SCHEMA_CHANGE_RATE": SCHEMA_CHANGE_RATE,
    "DUPLICATE_RATE": DUPLICATE_RATE,
    "BAD_AMOUNT_RATE": BAD_AMOUNT_RATE,
    "MISSING_FIELD_RATE": MISSING_FIELD_RATE,
    "INVALID_VALUE_RATE": INVALID_VALUE_RATE,
    "MALFORMED_JSON_RATE": MALFORMED_JSON_RATE,
}

for rate_name, rate_value in RATE_NAMES.items():
    if not 0 <= rate_value <= 1:
        raise ValueError(f"{rate_name} must be between 0 and 1")


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        if isinstance(value, bytes):
            return value

        if isinstance(value, str):
            return value.encode("utf-8")

        return json.dumps(value).encode("utf-8")


class StringSerializer(Serializer):
    def serialize(self, topic, value):
        if value is None:
            return None

        return str(value).encode("utf-8")


def create_producer(max_retries=5, retry_delay=2):
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=JsonSerializer(),
                key_serializer=StringSerializer(),
                retries=5,
                retry_backoff_ms=1000,
                acks="all",
            )

            print("Connected to Kafka successfully.")
            return producer

        except Exception as error:
            last_error = error

            print(
                f"Kafka connection failed. "
                f"Retry {attempt}/{max_retries} in {retry_delay} seconds..."
            )

            if attempt < max_retries:
                time.sleep(retry_delay)

    raise RuntimeError(
        f"Could not connect to Kafka after {max_retries} attempts."
    ) from last_error


def on_send_success(record_metadata):
    print(
        f"Delivered to topic={record_metadata.topic}, "
        f"partition={record_metadata.partition}, "
        f"offset={record_metadata.offset}"
    )


def on_send_error(error):
    print(f"Kafka delivery failed: {error}")


def generate_transaction():
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(100, 5000), 2),
        "currency": "INR",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant": f"MERCHANT-{random.randint(100, 999)}",
        "status": random.choice(["SUCCESS", "PENDING", "FAILED"]),
    }

    if random.random() < NULL_INJECTION_RATE:
        field = random.choice(list(transaction.keys()))
        transaction[field] = None

    if random.random() < SCHEMA_CHANGE_RATE:
        transaction["unexpected_field"] = "SCHEMA_CHANGE"

    if random.random() < BAD_AMOUNT_RATE:
        transaction["amount"] = random.choice(
            [0, -1, -100, -999.99]
        )

    if random.random() < MISSING_FIELD_RATE:
        required_fields = [
            "transaction_id",
            "customer_id",
            "amount",
            "currency",
            "timestamp",
            "merchant",
            "status",
        ]

        field_to_remove = random.choice(required_fields)
        transaction.pop(field_to_remove, None)

    if random.random() < INVALID_VALUE_RATE:
        invalid_cases = [
            ("currency", "INVALID"),
            ("status", "UNKNOWN"),
            ("customer_id", ""),
            ("merchant", ""),
            ("timestamp", "not-a-date"),
        ]

        field, value = random.choice(invalid_cases)

        if field in transaction:
            transaction[field] = value

    return transaction


def generate_payload():
    if random.random() < MALFORMED_JSON_RATE:
        return '{"transaction_id": "broken", "amount": '

    return generate_transaction()


def get_next_transaction(previous_transaction=None):
    if (
        previous_transaction is not None
        and random.random() < DUPLICATE_RATE
    ):
        if isinstance(previous_transaction, dict):
            return previous_transaction.copy(), True

        return previous_transaction, True

    return generate_payload(), False


def get_message_key(transaction):
    if isinstance(transaction, dict):
        return transaction.get("transaction_id")

    return None


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="IceStream Kafka transaction generator"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Generate a fixed number of events and then stop.",
    )

    args = parser.parse_args()

    if args.count is not None and args.count <= 0:
        parser.error("--count must be greater than 0")

    return args


def main():
    args = parse_arguments()

    max_events = args.count

    if max_events is None and MAX_EVENTS > 0:
        max_events = MAX_EVENTS

    print("IceStream Kafka Producer Started")
    print(f"Kafka Server: {KAFKA_SERVER}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Generation Rate: {GENERATION_RATE} transaction(s)/second")
    print(f"Duplicate Rate: {DUPLICATE_RATE}")

    if max_events is None:
        print("Max Events: unlimited")
    else:
        print(f"Max Events: {max_events}")

    producer = create_producer()
    previous_transaction = None
    event_count = 0

    try:
        while max_events is None or event_count < max_events:
            transaction, is_duplicate = get_next_transaction(
                previous_transaction
            )

            if is_duplicate:
                print("Duplicate generated")
            else:
                if isinstance(transaction, dict):
                    previous_transaction = transaction.copy()
                else:
                    previous_transaction = transaction

            message_key = get_message_key(transaction)

            try:
                producer.send(
                    KAFKA_TOPIC,
                    key=message_key,
                    value=transaction,
                ).add_callback(
                    on_send_success
                ).add_errback(
                    on_send_error
                )

                event_count += 1

                print(
                    f"Sent event {event_count}: "
                    f"key={message_key}, value={transaction}"
                )

            except KafkaError as error:
                print(f"Kafka send error: {error}")

            if max_events is None or event_count < max_events:
                time.sleep(1 / GENERATION_RATE)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.flush()
        producer.close()

    print(f"Producer finished. Total events sent: {event_count}")


if __name__ == "__main__":
    main()