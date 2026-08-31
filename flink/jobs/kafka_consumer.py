import os
import sys
import json
import logging
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)

# Resolve module paths for src execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.transforms import ParseAndValidateMapFunction, TransactionDeduplicatorFunction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlinkKafkaConsumer")


class TransactionTimestampAssigner(TimestampAssigner):
    """Extracts event timestamp for Flink event-time processing and watermarking."""

    def extract_timestamp(self, value_json_str: str, record_timestamp: int) -> int:
        try:
            record = json.loads(value_json_str)
            if record.get("status") == "VALID":
                return record["payload"].get("event_timestamp_ms", record_timestamp)
        except Exception:
            pass
        return record_timestamp


def get_flink_module_status(job_name: str, status: str = "HEALTHY") -> dict:
    """Dynamic contract payload integrated with Person 1's Backend Pipeline Status API."""
    return {
        "module": "flink",
        "status": status,
        "job_name": job_name,
        "consumer_group": "flink_ecommerce_group",
        "target_topic": "ecommerce-transactions",
        "output_topics": {
            "valid": "processed-transactions",
            "dlq": "transactions-dlq",
        },
        "metrics": {
            "records_processed_metric": "records_processed_count",
            "processing_errors_metric": "records_invalid_count",
            "duplicates_metric": "records_duplicate_count",
        },
    }


def run_flink_job():
    job_name = "IceStream-Flink-Kafka-Consumer"
    logger.info(f"Initializing Flink Engine with contract: {get_flink_module_status(job_name)}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 1. Dependency Setup (Kafka Connector JAR)
    jar_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "lib", "flink-sql-connector-kafka-3.0.1-1.18.jar")
    )
    if not os.path.exists(jar_path):
        # Fallback to alternate local filename if present
        jar_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "lib", "flink-sql-connector-kafka.jar")
        )

    if os.path.exists(jar_path):
        clean_jar_path = jar_path.replace("\\", "/")
        formatted_path = f"file:///{clean_jar_path}"
        env.add_jars(formatted_path)
        logger.info(f"Loaded Kafka Connector JAR: {formatted_path}")
    else:
        logger.warning(f"Kafka JAR not found at: {jar_path}. Pipeline execution may fail on Kafka sources/sinks.")

    # 2. Checkpointing & Fault Tolerance Configuration
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            restart_attempts=3,
            delay_between_attempts=10000,  # 10s delay
        )
    )

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # 3. Configure Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics("ecommerce-transactions")
        .set_group_id("flink_ecommerce_group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka_Ecommerce_Source",
    )

    # 4. Parsing & Validation Stage
    parsed_stream = raw_stream.map(ParseAndValidateMapFunction(), output_type=Types.STRING())

    # 5. Event-Time & Watermark Strategy (5-Second Bounded Out-of-Orderness)
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(TransactionTimestampAssigner())
    )

    watermarked_stream = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # 6. Stateful Deduplication (Keyed by transaction_id)
    processed_stream = watermarked_stream.key_by(
        lambda record_str: json.loads(record_str).get("transaction_id", "UNKNOWN"),
        key_type=Types.STRING(),
    ).process(TransactionDeduplicatorFunction(), output_type=Types.STRING())

    # 7. Format Outputs for Sinks
    def format_valid_output(record_str: str) -> str:
        data = json.loads(record_str)
        return json.dumps(data["payload"])

    def format_dlq_output(record_str: str) -> str:
        data = json.loads(record_str)
        return json.dumps({
            "raw_payload": data.get("raw_payload", ""),
            "error_reason": data.get("error_reason", "Validation error"),
            "transaction_id": data.get("transaction_id", "UNKNOWN"),
            "failed_at": data.get("failed_at", ""),
        })

    valid_stream = processed_stream.filter(
        lambda record_str: json.loads(record_str).get("status") == "VALID"
    ).map(format_valid_output, output_type=Types.STRING())

    dlq_stream = processed_stream.filter(
        lambda record_str: json.loads(record_str).get("status") == "INVALID"
    ).map(format_dlq_output, output_type=Types.STRING())

    # 8. Configure Kafka Sinks
    valid_kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("processed-transactions")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    dlq_kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("transactions-dlq")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Attach Sinks
    valid_stream.sink_to(valid_kafka_sink)
    dlq_stream.sink_to(dlq_kafka_sink)

    logger.info("Starting IceStream Flink Consumer Execution...")
    env.execute(job_name)


if __name__ == "__main__":
    run_flink_job()