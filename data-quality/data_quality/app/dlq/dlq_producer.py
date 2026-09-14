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

def get_dlq_producer():
    # Reuse one producer for the lifetime of the service
    global producer

    if producer is None:
        producer = create_dlq_producer()

    return producer

def send_to_dlq(record, errors):
    # Create the DLQ record
    dlq_record = {
        "record": record,
        "errors": errors
    }

    # Send asynchronously to Kafka.
    # Flush is intentionally not called for every record.
    get_dlq_producer().send(
        DLQ_TOPIC,
        value=dlq_record
    )

    return dlq_record

def flush_dlq():
    # Flush all pending DLQ messages at batch/shutdown boundaries
    if producer is not None:
        producer.flush()

def close_dlq_producer():
    # Flush and close the producer during shutdown
    global producer

    if producer is not None:
        producer.flush()
        producer.close()
        producer = None