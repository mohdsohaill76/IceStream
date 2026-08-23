import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "transactions"

# Number of transactions generated per second
GENERATION_RATE = 1


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_transaction():
    amount = round(random.uniform(100, 5000), 2)
    tax_amount = round(amount * 0.18, 2)

    # Deliberate bad-data injection
    # Around 5% of records will contain a NULL tax amount.
    inject_null = random.random() < 0.05

    # Around 2% of records will contain an unexpected field
    # to simulate a schema change.
    inject_schema_change = random.random() < 0.02

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": f"PROD-{random.randint(100, 999)}",
        "amount": amount,
        "tax_amount": None if inject_null else tax_amount,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payment_status": random.choice(
            ["SUCCESS", "PENDING", "FAILED"]
        ),
    }

    # Add an unexpected field to simulate a schema change.
    if inject_schema_change:
        transaction["unexpected_field"] = "SCHEMA_CHANGE"

    return transaction


def main():
    print("IceStream Kafka Producer Started")
    print(f"Kafka Server: {KAFKA_SERVER}")
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Generation Rate: {GENERATION_RATE} transaction(s)/second")

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