# Send invalid records to the Dead Letter Queue

import json
import os

from kafka import KafkaProducer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions-dlq")

producer = None
_pending_futures = []

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
    global _pending_futures
    dlq_record = {
        "record": record,
        "errors": errors
    }

    # Send asynchronously to Kafka.
    # Flush is intentionally not called for every record.
    future = get_dlq_producer().send(
        DLQ_TOPIC,
        value=dlq_record
    )
    if future is not None:
        _pending_futures.append(future)

    return dlq_record

def flush_dlq(timeout=10.0):
    # Flush all pending DLQ messages at batch/shutdown boundaries and verify delivery acknowledgements
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

def close_dlq_producer():
    # Flush and close the producer during shutdown
    global producer, _pending_futures
    if producer is not None:
        try:
            flush_dlq()
        finally:
            producer.close()
            producer = None
            _pending_futures = []