# Consume transaction messages from Kafka

import json
import os

from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from app.dlq.dlq_producer import send_to_dlq


KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "processed-transactions")
KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "icestream-data-quality"
)


class TrackedRecord(dict):
    """
    A dict subclass carrying Kafka message metadata (_topic, _partition, _offset)
    as Python instance attributes without exposing them as dict keys.
    This preserves strict transaction schema validation while enabling exact offset commits.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._topic = None
        self._partition = None
        self._offset = None


def create_consumer(topic=None, bootstrap_servers=None, group_id=None):
    # Create Kafka consumer with explicit manual offset management
    target_topic = topic or KAFKA_TOPIC
    target_servers = bootstrap_servers or KAFKA_SERVER
    target_group = group_id or KAFKA_GROUP_ID

    return KafkaConsumer(
        target_topic,
        bootstrap_servers=target_servers,
        group_id=target_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )


def decode_message(payload, include_errors=False):
    """
    Decodes and validates a single raw Kafka message value.
    If the payload is malformed or invalid, routes it to the Dead Letter Queue (DLQ).
    Returns the decoded record dictionary, or if an error occurred and include_errors is True,
    returns an error dictionary with __consumer_error__=True. Otherwise returns None.
    """
    try:
        # Kafka tombstone message
        if payload is None:
            error = "Invalid Kafka payload: payload is None"

            send_to_dlq(
                {"raw_message": None},
                [error]
            )

            if include_errors:
                return {
                    "__consumer_error__": True,
                    "__dlq_sent__": True,
                    "raw_message": None,
                    "error": error
                }

            return None

        # Some tests/integrations may already provide a dictionary
        if isinstance(payload, dict):
            return payload

        # Kafka transaction payload must be bytes
        if not isinstance(payload, bytes):
            error = "Invalid Kafka payload: expected bytes"

            send_to_dlq(
                {"raw_message": str(payload)},
                [error]
            )

            if include_errors:
                return {
                    "__consumer_error__": True,
                    "__dlq_sent__": True,
                    "raw_message": str(payload),
                    "error": error
                }

            return None

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
                return {
                    "__consumer_error__": True,
                    "__dlq_sent__": True,
                    "raw_message": raw_message,
                    "error": error
                }

            return None

        # Return successfully decoded transaction object
        return record

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

        if include_errors:
            return {
                "__consumer_error__": True,
                "__dlq_sent__": True,
                "raw_message": raw_message,
                "error": error_message
            }

        return None

    except Exception as error:
        # Prevent one bad Kafka message from stopping the consumer
        raw_message = str(payload)
        error_message = f"Invalid Kafka payload: {error}"

        send_to_dlq(
            {"raw_message": raw_message},
            [error_message]
        )

        if include_errors:
            return {
                "__consumer_error__": True,
                "__dlq_sent__": True,
                "raw_message": raw_message,
                "error": error_message
            }

        return None


def poll_records(consumer, timeout_ms=500, include_errors=False):
    """
    Polls Kafka for records with a bounded timeout, preventing busy-spinning.
    Decodes returned records and routes invalid payloads to DLQ.
    Attaches message metadata (_topic, _partition, _offset) to returned TrackedRecord dicts.
    Returns a list of decoded records (or error records if include_errors=True).
    Empty polls return an empty list [].
    """
    if consumer is None:
        return []

    messages_with_meta = []
    if hasattr(consumer, "poll"):
        records_map = consumer.poll(timeout_ms=timeout_ms)
        if isinstance(records_map, dict):
            for tp_key, partition_records in records_map.items():
                tp_topic = getattr(tp_key, "topic", None)
                tp_partition = getattr(tp_key, "partition", None)
                if tp_topic is None and isinstance(tp_key, str) and "-" in tp_key:
                    parts = tp_key.rsplit("-", 1)
                    tp_topic = parts[0]
                    try:
                        tp_partition = int(parts[1])
                    except ValueError:
                        pass
                for msg in partition_records:
                    messages_with_meta.append((msg, tp_topic, tp_partition))
        elif isinstance(records_map, list):
            for msg in records_map:
                messages_with_meta.append((msg, None, None))
    elif hasattr(consumer, "__iter__"):
        try:
            for msg in consumer:
                messages_with_meta.append((msg, None, None))
        except Exception:
            pass

    if not messages_with_meta:
        return []

    decoded = []
    for msg, tp_topic, tp_partition in messages_with_meta:
        payload = getattr(msg, "value", msg)
        rec = decode_message(payload, include_errors=include_errors)
        if rec is not None:
            if isinstance(rec, dict):
                tracked = TrackedRecord(rec)
                tracked._topic = getattr(msg, "topic", None) or tp_topic
                tracked._partition = (
                    getattr(msg, "partition", None)
                    if getattr(msg, "partition", None) is not None
                    else tp_partition
                )
                tracked._offset = getattr(msg, "offset", None)
                decoded.append(tracked)
            else:
                decoded.append(rec)

    return decoded


def get_offsets_to_commit(records):
    """
    Computes a mapping of TopicPartition -> OffsetAndMetadata(max_offset + 1, '')
    for all tracked records in the batch that have topic, partition, and offset.
    Returns None if no trackable offsets are found.
    """
    if not records or not isinstance(records, list):
        return None

    max_offsets = {}
    for r in records:
        topic = getattr(r, "_topic", None)
        partition = getattr(r, "_partition", None)
        offset = getattr(r, "_offset", None)
        if topic is not None and partition is not None and offset is not None:
            try:
                tp = TopicPartition(topic, int(partition))
                offset_val = int(offset)
                if tp not in max_offsets or offset_val > max_offsets[tp]:
                    max_offsets[tp] = offset_val
            except Exception:
                pass

    if not max_offsets:
        return None

    return {
        tp: OffsetAndMetadata(max_offset + 1, "")
        for tp, max_offset in max_offsets.items()
    }


def commit_offsets(consumer, offsets=None):
    """
    Explicitly commits offsets to Kafka.
    If offsets dict is provided, commits the specific partition offsets.
    If offsets is None, commits currently consumed positions if supported.
    Raises any exception from consumer.commit() to the caller.
    """
    if consumer is None or not hasattr(consumer, "commit"):
        return

    if offsets:
        consumer.commit(offsets=offsets)
    else:
        consumer.commit()


def pause_consumer(consumer):
    """Safely suspends partition fetching while maintaining heartbeats."""
    if consumer is None:
        return
    if hasattr(consumer, "pause") and hasattr(consumer, "assignment"):
        try:
            assigned = consumer.assignment()
            if assigned:
                consumer.pause(*assigned)
        except Exception:
            pass


def resume_consumer(consumer):
    """Resumes fetching on previously paused partitions."""
    if consumer is None:
        return
    if hasattr(consumer, "resume"):
        try:
            if hasattr(consumer, "paused"):
                paused = consumer.paused()
                if paused:
                    consumer.resume(*paused)
            elif hasattr(consumer, "assignment"):
                assigned = consumer.assignment()
                if assigned:
                    consumer.resume(*assigned)
        except Exception:
            pass


def consume_records(include_errors=False, consumer=None):
    # Read records from Kafka
    created_locally = False
    if consumer is None:
        consumer = create_consumer()
        created_locally = True

    try:
        for message in consumer:
            payload = getattr(message, "value", message)
            rec = decode_message(payload, include_errors=include_errors)
            if rec is not None:
                yield rec

    finally:
        # Close Kafka consumer
        if created_locally and hasattr(consumer, "close"):
            consumer.close()
