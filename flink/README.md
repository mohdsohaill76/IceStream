# Flink Stream Processing Module

## Responsibility
The flink module handles real-time stream processing, consuming raw events from Apache Kafka, performing canonical schema validation and stateful deduplication, and routing clean streams for downstream lakehouse storage and data quality analysis.

## Module Owner
Person 3: Flink Stream Processing

## Key Features & Pipeline Implementation
- Kafka Source (jobs/kafka_consumer.py): Consumes raw transaction streams from ecommerce-transactions using KafkaSource.
- Canonical Upstream Contract Enforcement (src/transforms.py via TransactionValidationAndDeduplicationFunction): Strictly enforces the canonical 7-field schema (transaction_id, customer_id, amount, currency, timestamp, merchant, status). Any presence of legacy user_id is rejected directly to DLQ without silent conversion or downstream data leakage.
- Strict Data Sanitization & Value Validation:
  - Rejects boolean amounts (amount: true/false) to DLQ via explicit type guard.
  - Enforces positive numeric amounts (amount > 0).
  - Validates currency against standard ISO-4217 codes (USD, EUR, GBP, CAD, AUD, JPY, INR).
  - Validates status against canonical lifecycle states (COMPLETED, PENDING, FAILED, CANCELLED).
  - Rejects empty or whitespace-only strings for transaction_id, customer_id, and merchant.
- Event-Time & Watermarking: Assigns event timestamps from payloads using RawTransactionTimestampAssigner with a 5-second Bounded Out-of-Orderness watermark strategy. Unparseable ISO-8601 timestamps fail validation and are routed to DLQ rather than falling back to Kafka arrival times.
- Stateful Deduplication with 24h TTL: Uses Flink Keyed ValueState configured with a 24-hour StateTtlConfig (expiration on create/write) to prevent unbounded state memory growth while detecting duplicate transaction_id records.
- Side Outputs & Raw Payload Preservation: Employs Flink OutputTag side outputs (valid-transactions and dlq-transactions) to isolate valid records from dead-letter queue records while preserving exact un-mutated raw_payload strings and descriptive error_reason for auditability. Valid records forwarded downstream contain strictly the 7 canonical fields.
- Fault Tolerance & Configurable Checkpointing: Configured with 10-second Exactly-Once checkpointing backed by configurable checkpoint storage (FLINK_CHECKPOINT_DIR, defaulting to file:///tmp/flink-checkpoints) and fixed-delay restarts.
- Dual Kafka Outputs:
  - Valid stream -> processed-transactions (for Downstream Data Quality & Iceberg Ingestion)
  - DLQ stream -> transactions-dlq (for Audit Logging & Monitoring)
- Externalized Configuration: Externalizes broker addresses (KAFKA_BOOTSTRAP_SERVERS), topic names, and checkpoint storage via environment variables (.env.example).
- Metrics & Backend Status API Integration: Exposes contract status definitions (get_flink_module_status) and metric key definitions for FastAPI backend status monitoring.
- Automated Testing & Automated Setup: Full unit test coverage via pytest (tests/test_flink_pipeline.py - 9 passing tests) and automated JAR provisioning (lib/download_kafka_jar.py).

---

## Data Lineage & Downstream Integration

1. Upstream Producer (Mahek - Data Generator): Writes raw JSON payloads to ecommerce-transactions.
2. Flink Pipeline Engine (Basil - Flink Consumer): Consumes from ecommerce-transactions, enforces canonical schema, deduplicates records via TTL-backed ValueState, and routes streams.
3. Downstream Sink (Harsh - Data Quality / Iceberg): Consumes clean events from processed-transactions and failure logs from transactions-dlq.

(For complete contract details, see DOWNSTREAM_INTEGRATION.md)

---

## Canonical JSON Schemas

### 1. Valid Transaction Schema (processed-transactions)
Consumed downstream by Data Quality and Iceberg Ingestion:

{
  "transaction_id": "FLINK-QA-CLEAN-001",
  "customer_id": "cust_qa_01",
  "amount": 250.00,
  "currency": "USD",
  "timestamp": "2026-09-14T20:00:00Z",
  "merchant": "Amazon",
  "status": "COMPLETED"
}

### 2. Dead Letter Queue Schema (transactions-dlq)
Consumed downstream by Data Quality & Audit Monitoring:

{
  "raw_payload": "{\"transaction_id\": \"FLINK-QA-BAD-001\", \"amount\": 50.0, \"currency\": \"USD\", \"timestamp\": \"2026-09-14T20:01:00Z\", \"merchant\": \"Amazon\", \"status\": \"COMPLETED\"}",
  "error_reason": "Schema Validation Failure: Missing required fields ['customer_id']"
}

{
  "raw_payload": "{\"transaction_id\": \"FLINK-QA-LEAK-001\", \"customer_id\": \"cust_qa_01\", \"user_id\": \"usr_qa_99\", \"amount\": 100.0, \"currency\": \"USD\", \"timestamp\": \"2026-09-14T20:02:00Z\", \"merchant\": \"Ebay\", \"status\": \"COMPLETED\"}",
  "error_reason": "Schema Validation Failure: Legacy 'user_id' field is forbidden in canonical stream"
}

{
  "raw_payload": "{\"transaction_id\": \"FLINK-QA-BOOL-001\", \"customer_id\": \"cust_qa_02\", \"amount\": true, \"currency\": \"USD\", \"timestamp\": \"2026-09-14T20:03:00Z\", \"merchant\": \"Target\", \"status\": \"COMPLETED\"}",
  "error_reason": "Invalid transaction amount: Boolean values (True/False) are not allowed"
}

{
  "raw_payload": "{\"transaction_id\": \"FLINK-QA-DUP-001\", \"customer_id\": \"cust_qa_03\", \"amount\": 75.0, \"currency\": \"USD\", \"timestamp\": \"2026-09-14T20:04:00Z\", \"merchant\": \"BestBuy\", \"status\": \"COMPLETED\"}",
  "error_reason": "Duplicate transaction_id: FLINK-QA-DUP-001"
}

---

## Health & Metrics Contract Integration

Dynamic status contract exposed for backend API aggregation (GET /api/v1/pipeline/status):

{
  "module": "flink",
  "status": "HEALTHY",
  "job_name": "IceStream-Flink-Kafka-Consumer",
  "consumer_group": "flink_ecommerce_group",
  "target_topic": "ecommerce-transactions",
  "output_topics": {
    "valid": "processed-transactions",
    "dlq": "transactions-dlq"
  },
  "metrics": {
    "records_processed_metric": "records_processed_count",
    "processing_errors_metric": "records_invalid_count",
    "duplicates_metric": "records_duplicate_count"
  }
}

---

## Prerequisites & Supported Runtime

- Operating System: Windows 11 / Linux / WSL2
- Python: 3.11.x (apache-flink==1.18.1)
- Java: OpenJDK 11 or 17 with JAVA_HOME configured
- Kafka Connector JAR: flink-sql-connector-kafka-3.0.1-1.18.jar (~5.3 MB)
- Message Broker: Apache Kafka 3.x running on localhost:9092

---

## Execution Steps

### 1. Environment Setup
Navigate to the flink module directory, activate your virtual environment, and install dependencies:
cd flink
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

### 2. Environment Configuration
Copy-Item .env.example .env

### 3. Provision Connector JAR
Download the compatible Kafka SQL connector JAR directly into lib/:
python lib/download_kafka_jar.py
Confirmed path: flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar (5,571,859 bytes).

### 4. Run Automated Unit Test Suite
python -m pytest -vv tests/test_flink_pipeline.py
Expected test summary: 9 passed, 0 failed.

### 5. Execute Flink Streaming Pipeline
Ensure Kafka and Zookeeper are active, then run:
python jobs/kafka_consumer.py

---

## Module Structure

flink/
|-- .env.example               # Environment template (KAFKA_BOOTSTRAP_SERVERS, FLINK_CHECKPOINT_DIR, topics)
|-- DOWNSTREAM_INTEGRATION.md  # Topic contracts & stream lineage documentation
|-- jobs/
|   `-- kafka_consumer.py      # Main PyFlink execution job & streaming DAG setup
|-- lib/
|   |-- download_kafka_jar.py  # Automated Maven JAR dependency downloader
|   `-- flink-sql-connector-kafka-3.0.1-1.18.jar # Kafka connector binary (provisioned via script; not committed)
|-- src/
|   |-- __init__.py            # Python package declaration marker
|   `-- transforms.py          # TransactionValidationAndDeduplicationFunction, 24h State TTL, DLQ routing
|-- tests/
|   |-- __init__.py            # Test package discovery marker
|   `-- test_flink_pipeline.py # 9 unit tests verifying schema, bool rejection, state TTL, and DLQ
|-- test_flow.py               # E2E integration test suite
|-- requirements.txt           # Pinned dependencies (apache-flink==1.18.1)
`-- README.md                  # Complete module documentation & execution guide