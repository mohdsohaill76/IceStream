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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
GENERATION_RATE = float(os.getenv("GENERATION_RATE", "1"))
NULL_INJECTION_RATE = float(os.getenv("NULL_INJECTION_RATE", "0.05"))
SCHEMA_CHANGE_RATE = float(os.getenv("SCHEMA_CHANGE_RATE", "0.05"))


if GENERATION_RATE <= 0:
    raise ValueError("GENERATION_RATE must be greater than 0")


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value).encode("utf-8")


def create_producer(max_retries=5, retry_delay=2):
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=JsonSerializer(),
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


def generate_transaction():
    amount = round(random.uniform(100, 5000), 2)

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "amount": amount,
        "currency": "INR",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "merchant": f"MERCHANT-{random.randint(100, 999)}",
        "status": random.choice(["SUCCESS", "PENDING", "FAILED"]),
    }

    # Deliberate NULL-value injection
    if random.random() < NULL_INJECTION_RATE:
        field = random.choice(list(transaction.keys()))
        transaction[field] = None

    # Deliberate schema-change injection
    if random.random() < SCHEMA_CHANGE_RATE:
        transaction["unexpected_field"] = "SCHEMA_CHANGE"

    return transaction


def main():
    print("IceStream Kafka Producer Started")
    print(f"Kafka Server: {KAFKA_SERVER}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Generation Rate: {GENERATION_RATE} transaction(s)/second")

    producer = create_producer()

    try:
        while True:
            transaction = generate_transaction()

            try:
                producer.send(KAFKA_TOPIC, transaction)
                print("Sent:", transaction)

            except KafkaError as error:
                print(f"Kafka send error: {error}")

            time.sleep(1 / GENERATION_RATE)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
