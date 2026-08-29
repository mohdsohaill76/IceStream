import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
GENERATION_RATE = float(os.getenv("GENERATION_RATE", "1"))
NULL_INJECTION_RATE = float(os.getenv("NULL_INJECTION_RATE", "0.05"))
SCHEMA_CHANGE_RATE = float(os.getenv("SCHEMA_CHANGE_RATE", "0.05"))


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


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

            producer.send(KAFKA_TOPIC, transaction)
            producer.flush()

            print("Sent:", transaction)

            time.sleep(1 / GENERATION_RATE)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()