# Send valid records to the downstream Kafka topic

import json
import os

from kafka import KafkaProducer

KAFKA_SERVER = os.getenv(
    "KAFKA_SERVER",
    "localhost:9092"
)

VALID_OUTPUT_TOPIC = os.getenv(
    "VALID_OUTPUT_TOPIC",
    "processed-transactions"
)

producer = None

def create_valid_producer():
    # Create Kafka producer for valid transactions
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

def get_valid_producer():
    # Reuse one producer for the service lifetime
    global producer

    if producer is None:
        producer = create_valid_producer()

    return producer

def send_valid_record(record):
    # Forward a valid transaction downstream
    get_valid_producer().send(
        VALID_OUTPUT_TOPIC,
        value=record
    )

    return record

def flush_valid_producer():
    # Flush pending valid records
    if producer is not None:
        producer.flush()

def close_valid_producer():
    # Flush and close producer during shutdown
    global producer

    if producer is not None:
        producer.flush()
        producer.close()
        producer = None