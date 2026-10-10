import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer

from iceberg.src.iceberg_writer import IcebergWriter

logger = logging.getLogger("iceberg.consumer")

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
CLEAN_TOPIC = os.getenv("QUALITY_CHECKED_TOPIC", "quality-checked-transactions")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "transactions-dlq")
CONSUMER_GROUP = os.getenv("ICEBERG_CONSUMER_GROUP", "icestream-iceberg-writer")


class IcebergConsumerService:
    """Consumes clean transactions and DLQ events from Kafka and appends them to Apache Iceberg."""

    def __init__(
        self,
        kafka_server: str = KAFKA_SERVER,
        clean_topic: str = CLEAN_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
        consumer_group: str = CONSUMER_GROUP,
        writer: Optional[IcebergWriter] = None,
    ):
        self.kafka_server = kafka_server
        self.clean_topic = clean_topic
        self.dlq_topic = dlq_topic
        self.consumer_group = consumer_group
        self.writer = writer or IcebergWriter()

    def process_clean_batch(
        self,
        max_records: int = 50,
        timeout_ms: int = 4000,
        consumer: Optional[KafkaConsumer] = None,
    ) -> int:
        """Poll and write a batch of clean records from Kafka with at-least-once commits."""
        owns_consumer = False
        if consumer is None:
            owns_consumer = True
            consumer = KafkaConsumer(
                self.clean_topic,
                bootstrap_servers=self.kafka_server,
                group_id=f"{self.consumer_group}-clean",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        records = []
        start = time.time()
        timeout_sec = timeout_ms / 1000.0
        written = 0
        try:
            while time.time() - start < timeout_sec:
                batch = consumer.poll(timeout_ms=1000)
                for tp, msgs in batch.items():
                    for m in msgs:
                        if isinstance(m.value, dict):
                            records.append(m.value)
                        if len(records) >= max_records:
                            break
                    if len(records) >= max_records:
                        break
                if records:
                    break

            if records:
                # 1. Append to Iceberg first
                written = self.writer.write_clean_records(records)
                # 2. Commit offsets only after durable Iceberg append
                try:
                    consumer.commit()
                except Exception as e:
                    logger.warning(f"Error committing clean consumer offset: {e}")
        finally:
            if owns_consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

        return written

    def process_dlq_batch(
        self,
        max_records: int = 50,
        timeout_ms: int = 4000,
        consumer: Optional[KafkaConsumer] = None,
    ) -> int:
        """Poll and write a batch of DLQ records from Kafka with at-least-once commits."""
        owns_consumer = False
        if consumer is None:
            owns_consumer = True
            consumer = KafkaConsumer(
                self.dlq_topic,
                bootstrap_servers=self.kafka_server,
                group_id=f"{self.consumer_group}-dlq",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
        records = []
        start = time.time()
        timeout_sec = timeout_ms / 1000.0
        written = 0
        try:
            while time.time() - start < timeout_sec:
                batch = consumer.poll(timeout_ms=1000)
                for tp, msgs in batch.items():
                    for m in msgs:
                        if isinstance(m.value, dict):
                            records.append(m.value)
                        if len(records) >= max_records:
                            break
                    if len(records) >= max_records:
                        break
                if records:
                    break

            if records:
                # 1. Append to Iceberg DLQ first
                written = self.writer.write_dlq_records(records)
                # 2. Commit offsets only after durable Iceberg append
                try:
                    consumer.commit()
                except Exception as e:
                    logger.warning(f"Error committing DLQ consumer offset: {e}")
        finally:
            if owns_consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

        return written

    def run_continuous(self, poll_interval: float = 1.0) -> None:
        """Continuously poll Kafka and write records to Iceberg tables."""
        logger.info(f"Starting continuous Iceberg consumer service on {self.kafka_server}...")
        clean_consumer = None
        dlq_consumer = None
        try:
            clean_consumer = KafkaConsumer(
                self.clean_topic,
                bootstrap_servers=self.kafka_server,
                group_id=f"{self.consumer_group}-clean",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            dlq_consumer = KafkaConsumer(
                self.dlq_topic,
                bootstrap_servers=self.kafka_server,
                group_id=f"{self.consumer_group}-dlq",
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            while True:
                try:
                    self.process_clean_batch(max_records=100, timeout_ms=1000, consumer=clean_consumer)
                    self.process_dlq_batch(max_records=100, timeout_ms=1000, consumer=dlq_consumer)
                    time.sleep(poll_interval)
                except KeyboardInterrupt:
                    logger.info("Stopping Iceberg consumer service.")
                    break
                except Exception as e:
                    logger.error(f"Error in Iceberg consumer loop: {e}", exc_info=True)
                    time.sleep(poll_interval)
        finally:
            if clean_consumer is not None:
                try:
                    clean_consumer.close()
                except Exception:
                    pass
            if dlq_consumer is not None:
                try:
                    dlq_consumer.close()
                except Exception:
                    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    service = IcebergConsumerService()
    service.run_continuous()
