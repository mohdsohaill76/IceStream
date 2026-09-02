# Consume transaction messages from Kafka

import json
import os

from kafka import KafkaConsumer

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "icestream-data-quality")

def create_consumer():
    # Create Kafka consumer
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

def consume_records():
    # Read records from Kafka
    consumer = create_consumer()

    try:
        for message in consumer:
            # Return the received transaction
            yield message.value

    finally:
        # Close Kafka consumer
        consumer.close()