import os
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FlinkKafkaConsumer")

# --- CONTRACT FOR PERSON 1 (BACKEND PIPELINE STATUS API) ---
# Person 1's GET /api/v1/pipeline/status endpoint expects this status payload structure
FLINK_MODULE_STATUS_CONTRACT = {
    "module": "flink",
    "status": "HEALTHY",  # HEALTHY, UNHEALTHY, or PAUSED
    "job_name": "IceStream-Flink-Kafka-Consumer",
    "consumer_group": "flink_ecommerce_group",
    "target_topic": "ecommerce-transactions",
    "metrics": {
        "records_processed": 0,
        "processing_errors": 0
    }
}

def run_flink_job():
    logger.info(f"Initializing Flink Engine with contract: {FLINK_MODULE_STATUS_CONTRACT}")

    # 1. Initialize Stream Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 2. Add Kafka Connector JAR
    jar_path = os.path.abspath(os.path.join("lib", "flink-sql-connector-kafka.jar"))
    if os.path.exists(jar_path):
        # Convert backslashes outside the f-string placeholder
        clean_path = jar_path.replace("\\", "/")
        formatted_jar_uri = f"file:///{clean_path}"
        env.add_jars(formatted_jar_uri)
        logger.info(f"Successfully loaded Kafka JAR: {formatted_jar_uri}")
    else:
        logger.warning(f"Kafka JAR not found at: {jar_path}")

    # 3. Configure Kafka Source to consume Person 2's topic
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("ecommerce-transactions") \
        .set_group_id("flink_ecommerce_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 4. Read Data Stream
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Transaction Source"
    )

    # 5. Print records to stdout (Week 1 Deliverable)
    stream.print()

    logger.info("Starting IceStream Flink Consumer Execution...")
    env.execute("IceStream-Flink-Kafka-Consumer")

if __name__ == "__main__":
    run_flink_job()