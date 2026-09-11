import os
import sys
import json
import logging
from datetime import datetime
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
)
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)

# Resolve module paths for src execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.transforms import (
    TransactionValidationAndDeduplicationFunction,
    VALID_OUTPUT_TAG,
    DLQ_OUTPUT_TAG,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlinkKafkaConsumer")

# --- ITEM 12: Externalized Configuration Constants ---
CONFIG = {
    "job_name": "IceStream-Flink-Kafka-Consumer",
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "input_topic": os.getenv("KAFKA_INPUT_TOPIC", "ecommerce-transactions"),
    "valid_output_topic": os.getenv("KAFKA_VALID_TOPIC", "processed-transactions"),
    "dlq_output_topic": os.getenv("KAFKA_DLQ_TOPIC", "transactions-dlq"),
    "consumer_group": os.getenv("KAFKA_CONSUMER_GROUP", "flink_ecommerce_group"),
    "parallelism": int(os.getenv("FLINK_PARALLELISM", "1")),
    "checkpoint_interval_ms": 10000,
    "max_out_of_orderness_sec": 5,
}


class RawTransactionTimestampAssigner(TimestampAssigner):
    """Extracts event timestamp from raw JSON for Flink event-time processing and watermarking."""

    def extract_timestamp(self, value_json_str: str, record_timestamp: int) -> int:
        try:
            record = json.loads(value_json_str)
            if isinstance(record, dict):
                ts_val = record.get("timestamp")
                if isinstance(ts_val, str):
                    dt = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                    return int(dt.timestamp() * 1000)
        except Exception:
            pass
        return record_timestamp


def safe_extract_tx_id(value_str: str) -> str:
    """Safely extracts transaction_id for key_by, defaulting to 'UNKNOWN' for malformed payloads."""
    try:
        data = json.loads(value_str)
        if isinstance(data, dict):
            tx_id = data.get("transaction_id")
            if tx_id is not None and str(tx_id).strip() != "":
                return str(tx_id)
    except Exception:
        pass
    return "UNKNOWN"


def get_flink_module_status(job_name: str, status: str = "HEALTHY") -> dict:
    """Dynamic contract payload integrated with Person 1's Backend Pipeline Status API."""
    return {
        "module": "flink",
        "status": status,
        "job_name": job_name,
        "consumer_group": CONFIG["consumer_group"],
        "target_topic": CONFIG["input_topic"],
        "output_topics": {
            "valid": CONFIG["valid_output_topic"],
            "dlq": CONFIG["dlq_output_topic"],
        },
        "metrics": {
            "records_processed_metric": "records_processed_count",
            "processing_errors_metric": "records_invalid_count",
            "duplicates_metric": "records_duplicate_count",
        },
    }


def run_flink_job():
    job_name = CONFIG["job_name"]
    logger.info(f"Initializing Flink Engine with contract: {get_flink_module_status(job_name)}")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(CONFIG["parallelism"])

    # 1. Dependency Setup (Kafka Connector JAR)
    jar_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "lib", "flink-sql-connector-kafka-3.0.1-1.18.jar")
    )
    if not os.path.exists(jar_path):
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

    # 2. Checkpointing, Fault Tolerance & Persistent State Storage Configuration (Item 10)
    env.enable_checkpointing(CONFIG["checkpoint_interval_ms"], CheckpointingMode.EXACTLY_ONCE)
    
    # FIX: Wrapped string path in FileSystemCheckpointStorage object required by PyFlink
    env.get_checkpoint_config().set_checkpoint_storage(
        FileSystemCheckpointStorage("file:///tmp/flink-checkpoints")
    )
    
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            restart_attempts=3,
            delay_between_attempts=10000,  # 10s delay
        )
    )

    # 3. Configure Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(CONFIG["bootstrap_servers"])
        .set_topics(CONFIG["input_topic"])
        .set_group_id(CONFIG["consumer_group"])
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka_Ecommerce_Source",
    )

    # 4. Event-Time & Watermark Strategy (Bounded Out-of-Orderness)
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(CONFIG["max_out_of_orderness_sec"]))
        .with_timestamp_assigner(RawTransactionTimestampAssigner())
    )

    watermarked_stream = raw_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # 5. Stateful Deduplication, Validation & Side Outputs
    processed_stream = watermarked_stream.key_by(
        safe_extract_tx_id,
        key_type=Types.STRING(),
    ).process(TransactionValidationAndDeduplicationFunction(), output_type=Types.STRING())

    # Extract Side Outputs (Valid vs DLQ streams)
    valid_stream = processed_stream.get_side_output(VALID_OUTPUT_TAG)
    dlq_stream = processed_stream.get_side_output(DLQ_OUTPUT_TAG)

    # 6. Configure Kafka Sinks using externalized configurations
    valid_kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(CONFIG["bootstrap_servers"])
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(CONFIG["valid_output_topic"])
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    dlq_kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(CONFIG["bootstrap_servers"])
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(CONFIG["dlq_output_topic"])
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    # Attach Sinks to Side Outputs
    valid_stream.sink_to(valid_kafka_sink)
    dlq_stream.sink_to(dlq_kafka_sink)

    logger.info("Starting IceStream Flink Consumer Execution with Side Outputs...")
    env.execute(job_name)


if __name__ == "__main__":
    run_flink_job()