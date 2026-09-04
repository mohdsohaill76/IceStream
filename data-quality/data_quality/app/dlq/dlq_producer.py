# Send invalid records to the Dead Letter Queue

import json
import os

from kafka import KafkaProducer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "ecommerce-transactions-dlq")

producer = None

def create_dlq_producer():
    # Create Kafka producer only when needed
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

def send_to_dlq(record, errors):
    global producer

    # Create a DLQ record
    dlq_record = {
        "record": record,
        "errors": errors
    }

    # Create the producer only once
    if producer is None:
        producer = create_dlq_producer()

    # Reuse the same producer
    producer.send(DLQ_TOPIC, value=dlq_record)
    producer.flush()

    return dlq_record