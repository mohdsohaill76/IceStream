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
    "quality-checked-transactions"
)

producer = None
_pending_futures = []

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
    global _pending_futures
    future = get_valid_producer().send(
        VALID_OUTPUT_TOPIC,
        value=record
    )
    if future is not None:
        _pending_futures.append(future)

    return record

def flush_valid_producer(timeout=10.0):
    # Flush pending valid records and verify broker delivery acknowledgements
    global _pending_futures
    if producer is not None:
        try:
            producer.flush(timeout=timeout)
        except TypeError:
            producer.flush()
        errors = []
        for f in _pending_futures:
            if hasattr(f, "get"):
                try:
                    f.get(timeout=timeout)
                except Exception as e:
                    errors.append(e)
        _pending_futures = []
        if errors:
            raise errors[0]
    else:
        _pending_futures = []

def close_valid_producer():
    # Flush and close producer during shutdown
    global producer, _pending_futures
    if producer is not None:
        try:
            flush_valid_producer()
        finally:
            producer.close()
            producer = None
            _pending_futures = []