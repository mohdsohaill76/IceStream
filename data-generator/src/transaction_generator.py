import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.serializer import Serializer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_SERVER = os.getenv(
    "KAFKA_SERVER",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ecommerce-transactions",
)


GENERATION_RATE = float(
    os.getenv("GENERATION_RATE", "1")
)

NULL_INJECTION_RATE = float(
    os.getenv("NULL_INJECTION_RATE", "0.05")
)

SCHEMA_CHANGE_RATE = float(
    os.getenv("SCHEMA_CHANGE_RATE", "0.05")
)

DUPLICATE_RATE = float(
    os.getenv("DUPLICATE_RATE", "0.05")
)

BAD_AMOUNT_RATE = float(
    os.getenv("BAD_AMOUNT_RATE", "0.03")
)

MISSING_FIELD_RATE = float(
    os.getenv("MISSING_FIELD_RATE", "0.03")
)

INVALID_VALUE_RATE = float(
    os.getenv("INVALID_VALUE_RATE", "0.03")
)

MALFORMED_JSON_RATE = float(
    os.getenv("MALFORMED_JSON_RATE", "0.02")
)

MAX_EVENTS = int(
    os.getenv("MAX_EVENTS", "0")
)


if GENERATION_RATE <= 0:
    raise ValueError(
        "GENERATION_RATE must be greater than 0."
    )


def validate_rate(name, value):
    if not 0 <= value <= 1:
        raise ValueError(
            f"{name} must be between 0 and 1."
        )


validate_rate("NULL_INJECTION_RATE", NULL_INJECTION_RATE)
validate_rate("SCHEMA_CHANGE_RATE", SCHEMA_CHANGE_RATE)
validate_rate("DUPLICATE_RATE", DUPLICATE_RATE)
validate_rate("BAD_AMOUNT_RATE", BAD_AMOUNT_RATE)
validate_rate("MISSING_FIELD_RATE", MISSING_FIELD_RATE)
validate_rate("INVALID_VALUE_RATE", INVALID_VALUE_RATE)
validate_rate("MALFORMED_JSON_RATE", MALFORMED_JSON_RATE)


if MAX_EVENTS < 0:
    raise ValueError(
        "MAX_EVENTS cannot be negative."
    )


STATUSES = [
    "SUCCESS",
    "PENDING",
    "FAILED",
]


class JsonSerializer(Serializer):
    def serialize(self, topic, value):
        if value is None:
            return None

        if isinstance(value, bytes):
            return value

        if isinstance(value, str):
            return value.encode("utf-8")

        return json.dumps(value).encode("utf-8")


class StringSerializer(Serializer):
    def serialize(self, topic, value):
        if value is None:
            return None

        if isinstance(value, bytes):
            return value

        return str(value).encode("utf-8")


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=StringSerializer(),
        value_serializer=JsonSerializer(),
        acks="all",
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
    )

    logger.info(
        "Connected to Kafka successfully."
    )

    return producer


def on_send_success(
    record_metadata,
    event_number=None,
    message_key=None,
    transaction=None,
):
    if event_number is not None:
        logger.info(
            "Sent event %s after Kafka acknowledgement: "
            "key=%s, value=%s | "
            "topic=%s, partition=%s, offset=%s",
            event_number,
            message_key,
            transaction,
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    else:
        logger.info(
            "Delivered to topic=%s, partition=%s, offset=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )


def on_send_error(
    error,
    event_number=None,
    message_key=None,
):
    if event_number is not None:
        logger.error(
            "Kafka delivery failed for event %s "
            "(key=%s): %s",
            event_number,
            message_key,
            error,
        )

    else:
        logger.error(
            "Kafka delivery failed: %s",
            error,
        )


def generate_transaction():
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": (
            f"CUST-{random.randint(1000, 9999)}"
        ),
        "amount": round(
            random.uniform(1, 5000),
            2,
        ),
        "currency": "INR",
        "timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "merchant": (
            f"MERCHANT-{random.randint(100, 999)}"
        ),
        "status": random.choice(
            STATUSES
        ),
    }

    if random.random() < NULL_INJECTION_RATE:
        field = random.choice(
            [
                "customer_id",
                "merchant",
                "status",
            ]
        )

        transaction[field] = None


    if random.random() < SCHEMA_CHANGE_RATE:
        transaction[
            "unexpected_field"
        ] = "SCHEMA_CHANGE"


    if random.random() < BAD_AMOUNT_RATE:
        transaction["amount"] = random.choice(
            [
                0,
                -1,
                -100,
            ]
        )


    if random.random() < MISSING_FIELD_RATE:
        field = random.choice(
            [
                "transaction_id",
                "customer_id",
                "amount",
                "currency",
                "timestamp",
                "merchant",
                "status",
            ]
        )

        transaction.pop(
            field,
            None,
        )


    if random.random() < INVALID_VALUE_RATE:
        field, value = random.choice(
            [
                ("currency", "INVALID"),
                ("status", "UNKNOWN"),
                ("customer_id", ""),
                ("merchant", ""),
                ("timestamp", "not-a-date"),
            ]
        )

        transaction[field] = value


    return transaction


def generate_payload():
    if random.random() < MALFORMED_JSON_RATE:
        return (
            '{"transaction_id": "broken", "amount": '
        )

    return generate_transaction()


def get_next_transaction(
    previous_transaction=None,
):
    if (
        previous_transaction is not None
        and random.random() < DUPLICATE_RATE
    ):
        if isinstance(
            previous_transaction,
            dict,
        ):
            return (
                previous_transaction.copy(),
                True,
            )

        return (
            previous_transaction,
            True,
        )

    return (
        generate_payload(),
        False,
    )


def get_message_key(transaction):
    if isinstance(
        transaction,
        dict,
    ):
        return transaction.get(
            "transaction_id"
        )

    return None


def send_transaction(
    producer,
    transaction,
    event_number,
):
    message_key = get_message_key(
        transaction
    )

    future = producer.send(
        KAFKA_TOPIC,
        key=message_key,
        value=transaction,
    )

    future.add_callback(
        lambda record_metadata:
        on_send_success(
            record_metadata,
            event_number=event_number,
            message_key=message_key,
            transaction=transaction,
        )
    )

    future.add_errback(
        lambda error:
        on_send_error(
            error,
            event_number=event_number,
            message_key=message_key,
        )
    )

    return future


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "IceStream Kafka Transaction Generator"
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help=(
            "Number of events to generate. "
            "Overrides MAX_EVENTS."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    max_events = (
        args.count
        if args.count is not None
        else MAX_EVENTS
    )

    if max_events < 0:
        raise ValueError(
            "--count cannot be negative."
        )

    logger.info(
        "IceStream Kafka Producer Started"
    )

    logger.info(
        "Kafka Server: %s",
        KAFKA_SERVER,
    )

    logger.info(
        "Kafka Topic: %s",
        KAFKA_TOPIC,
    )

    logger.info(
        "Generation Rate: %s transaction(s)/second",
        GENERATION_RATE,
    )

    logger.info(
        "Max Events: %s",
        max_events if max_events > 0 else "unlimited",
    )

    producer = None
    event_count = 0
    previous_transaction = None

    try:
        producer = create_producer()

        while True:
            if (
                max_events > 0
                and event_count >= max_events
            ):
                break

            transaction, is_duplicate = (
                get_next_transaction(
                    previous_transaction
                )
            )

            event_number = (
                event_count + 1
            )

            if is_duplicate:
                logger.info(
                    "Generated duplicate event %s",
                    event_number,
                )

            send_transaction(
                producer,
                transaction,
                event_number,
            )

            previous_transaction = transaction

            event_count += 1

            if (
                max_events > 0
                and event_count >= max_events
            ):
                break

            time.sleep(
                1 / GENERATION_RATE
            )

    except KeyboardInterrupt:
        logger.info(
            "Producer stopped by user."
        )

    except KafkaError as error:
        logger.error(
            "Kafka error: %s",
            error,
        )
        raise

    except Exception as error:
        logger.error(
            "Unexpected producer error: %s",
            error,
        )
        raise

    finally:
        if producer is not None:
            logger.info(
                "Waiting for pending Kafka deliveries..."
            )

            producer.flush()
            producer.close()

        logger.info(
            "Producer finished. "
            "Total events submitted: %s",
            event_count,
        )


if __name__ == "__main__":
    main()