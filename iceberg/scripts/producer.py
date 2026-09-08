import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

for i in range(1, 11):
    transaction = {
        "transaction_id": f"TX{i:04d}",
        "customer_id": f"C{i:03d}",
        "amount": 100.50 + i,
        "tax_amount": 10.05,
        "region": "EU",
        "event_time": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    producer.send(
        "processed-transactions",
        value=transaction
    )

    print("Sent:", transaction)
    time.sleep(1)

producer.flush()
producer.close()
