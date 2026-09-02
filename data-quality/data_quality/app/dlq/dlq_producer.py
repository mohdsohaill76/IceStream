# Send invalid records to the Dead Letter Queue

import json
import os

from kafka import KafkaProducer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "ecommerce-transactions-dlq")

def create_dlq_producer():
    # Create Kafka producer for DLQ
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

def send_to_dlq(record, errors):
    # Create a DLQ record with original data and errors
    dlq_record = {
        "record": record,
        "errors": errors
    }

    producer = create_dlq_producer()

    try:
        # Send invalid record to Kafka DLQ topic
        producer.send(DLQ_TOPIC, value=dlq_record)
        producer.flush()
    finally:
        # Close the producer
        producer.close()

    return dlq_record