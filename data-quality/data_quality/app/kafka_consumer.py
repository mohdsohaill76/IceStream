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

def consume_records():
    # Read records from Kafka
    consumer = create_consumer()

    try:
        for message in consumer:
            try:
                # Kafka messages may already contain a dictionary
                if isinstance(message.value, dict):
                    record = message.value

                else:
                    # Decode and parse JSON messages
                    record = json.loads(
                        message.value.decode("utf-8")
                    )

                # Return successfully parsed records
                yield record

            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                # Convert malformed Kafka payload into a safe DLQ record
                if isinstance(message.value, bytes):
                    raw_message = message.value.decode(
                        "utf-8",
                        errors="replace"
                    )
                else:
                    raw_message = str(message.value)

                send_to_dlq(
                    {
                        "raw_message": raw_message
                    },
                    [f"Malformed JSON: {error}"]
                )

                # Continue processing the next Kafka message
                continue

    finally:
        # Close Kafka consumer
        consumer.close()