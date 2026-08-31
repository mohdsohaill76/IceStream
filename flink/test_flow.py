import json
import time
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVER = "localhost:9092"

def run_test():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("--- 1. Sending Test Events ---")
    # Send 1 Valid Record
    producer.send("ecommerce-transactions", {
        "transaction_id": "tx_valid_100",
        "user_id": "usr_55",
        "amount": 199.99,
        "currency": "USD",
        "timestamp": "2026-08-31T14:55:00Z"
    })

    # Send 1 Invalid Record (Negative Amount)
    producer.send("ecommerce-transactions", {
        "transaction_id": "tx_invalid_200",
        "user_id": "usr_56",
        "amount": -50.00,
        "currency": "USD",
        "timestamp": "2026-08-31T14:55:00Z"
    })

    producer.flush()
    print("Test events dispatched to Kafka.\n")

    time.sleep(3)  # Allow Flink time to process

    print("--- 2. Checking 'processed-transactions' (Valid Sink) ---")
    c_valid = KafkaConsumer("processed-transactions", bootstrap_servers=BOOTSTRAP_SERVER, auto_offset_reset="earliest", consumer_timeout_ms=3000)
    for msg in c_valid:
        print("RECEIVED VALID:", msg.value.decode("utf-8"))

    print("\n--- 3. Checking 'transactions-dlq' (DLQ Sink) ---")
    c_dlq = KafkaConsumer("transactions-dlq", bootstrap_servers=BOOTSTRAP_SERVER, auto_offset_reset="earliest", consumer_timeout_ms=3000)
    for msg in c_dlq:
        print("RECEIVED DLQ:", msg.value.decode("utf-8"))

if __name__ == "__main__":
    run_test()