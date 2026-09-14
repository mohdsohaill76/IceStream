# Consume transaction messages from Kafka

import json
import os

from kafka import KafkaConsumer
from app.dlq.dlq_producer import send_to_dlq


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")
KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "icestream-data-quality"
)


def create_consumer():
    # Create Kafka consumer
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )


def consume_records(include_errors=False):
    # Read records from Kafka
    consumer = create_consumer()

    try:
        for message in consumer:
            payload = message.value

            try:
                # Kafka tombstone message
                if payload is None:
                    error = "Invalid Kafka payload: payload is None"

                    send_to_dlq(
                        {"raw_message": None},
                        [error]
                    )

                    # Production path can count this as a failed record.
                    if include_errors:
                        yield {
                            "__consumer_error__": True,
                            "__dlq_sent__": True,
                            "raw_message": None,
                            "error": error
                        }

                    continue

                # Some tests/integrations may already provide a dictionary
                if isinstance(payload, dict):
                    record = payload
                    raw_message = str(payload)

                else:
                    # Kafka transaction payload must be bytes
                    if not isinstance(payload, bytes):
                        error = "Invalid Kafka payload: expected bytes"

                        send_to_dlq(
                            {"raw_message": str(payload)},
                            [error]
                        )

                        if include_errors:
                            yield {
                                "__consumer_error__": True,
                                "__dlq_sent__": True,
                                "raw_message": str(payload),
                                "error": error
                            }

                        continue

                    # Decode the Kafka payload
                    raw_message = payload.decode("utf-8")

                    # Parse JSON
                    record = json.loads(raw_message)

                # JSON must contain a transaction object
                if not isinstance(record, dict):
                    error = (
                        "Invalid JSON payload: "
                        "transaction must be an object"
                    )

                    send_to_dlq(
                        {"raw_message": raw_message},
                        [error]
                    )

                    if include_errors:
                        yield {
                            "__consumer_error__": True,
                            "__dlq_sent__": True,
                            "raw_message": raw_message,
                            "error": error
                        }

                    continue

                # Return successfully decoded transaction objects
                yield record

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                # Preserve malformed payload in the DLQ
                if isinstance(payload, bytes):
                    raw_message = payload.decode(
                        "utf-8",
                        errors="replace"
                    )
                else:
                    raw_message = str(payload)

                error_message = f"Malformed JSON: {error}"

                send_to_dlq(
                    {"raw_message": raw_message},
                    [error_message]
                )

                # Only production processing needs the error marker.
                # Existing direct consumer tests keep the old behavior.
                if include_errors:
                    yield {
                        "__consumer_error__": True,
                        "__dlq_sent__": True,
                        "raw_message": raw_message,
                        "error": error_message
                    }

                continue

            except Exception as error:
                # Prevent one bad Kafka message from stopping the consumer
                raw_message = str(payload)
                error_message = f"Invalid Kafka payload: {error}"

                send_to_dlq(
                    {"raw_message": raw_message},
                    [error_message]
                )

                if include_errors:
                    yield {
                        "__consumer_error__": True,
                        "__dlq_sent__": True,
                        "raw_message": raw_message,
                        "error": error_message
                    }

                continue

    finally:
        # Close Kafka consumer
        consumer.close()