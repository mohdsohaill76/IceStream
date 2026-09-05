import json
import logging
import os

from kafka import KafkaConsumer, KafkaProducer
from kafka.serializer import Serializer


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce-transactions")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "ecommerce-transactions-dlq")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


class StringSerializer(Serializer):
    def serialize(self, topic, value):
        if value is None:
            return None
        return str(value).encode("utf-8")


def create_dlq_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=StringSerializer(),
    )


def process_message(raw_value, dlq_producer=None):
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")

        message = json.loads(raw_value)

        logger.info("Received valid message: %s", message)

        return message

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        logger.error(
            "Malformed JSON received. Sending to DLQ. Error: %s",
            error,
        )

        if dlq_producer is not None:
            try:
                dlq_producer.send(
                    DLQ_TOPIC,
                    value=raw_value,
                )
                logger.warning(
                    "Malformed message sent to DLQ topic: %s",
                    DLQ_TOPIC,
                )
            except Exception as dlq_error:
                logger.error(
                    "Failed to send malformed message to DLQ: %s",
                    dlq_error,
                )

        return None


def main():
    logger.info("IceStream Kafka Consumer Started")
    logger.info("Kafka Server: %s", KAFKA_SERVER)
    logger.info("Kafka Topic: %s", KAFKA_TOPIC)
    logger.info("DLQ Topic: %s", DLQ_TOPIC)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="icestream-verification-consumer",
    )

    dlq_producer = create_dlq_producer()

    try:
        for message in consumer:
            process_message(
                message.value,
                dlq_producer=dlq_producer,
            )

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")

    finally:
        consumer.close()
        dlq_producer.flush()
        dlq_producer.close()


if __name__ == "__main__":
    main()