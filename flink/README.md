# Flink Stream Processing Module

## Responsibility
The flink module handles real-time stream processing for the IceStream platform. It consumes raw transaction streams from Apache Kafka, enforces strict canonical schema validation and stateful deduplication, and routes clean streams downstream for Apache Iceberg lakehouse ingestion and Data Quality monitoring.

## Module Owner
Person 3: Flink Stream Processing (Basil)

---

## Key Features & Pipeline Implementation
- Kafka Ingestion (jobs/kafka_consumer.py): Consumes raw transaction streams from ecommerce-transactions using KafkaSource.
- Canonical Contract Enforcement (src/transforms.py): Strictly enforces the canonical 7-field schema (transaction_id, customer_id, amount, currency, timestamp, merchant, status). The presence of legacy user_id is rejected immediately to the DLQ to prevent downstream schema leakage.
- Strict Data Sanitization & Value Validation:
  - Rejects boolean amounts (amount: true/false) via explicit type guarding.
  - Rejects non-finite values (NaN, Infinity, -Infinity).
  - Enforces positive numeric amounts (amount > 0).
  - Validates currency against ISO-4217 codes (USD, EUR, GBP, CAD, AUD, JPY, INR).
  - Validates status against canonical lifecycle states (SUCCESS, PENDING, FAILED).
  - Rejects empty, missing, or whitespace-only strings for key identifiers.
- Event-Time & Watermarking: Assigns event timestamps from payloads using RawTransactionTimestampAssigner with a 5-second Bounded Out-of-Orderness watermark strategy. Unparseable ISO-8601 timestamps fail validation and are routed to DLQ rather than falling back silently.
- Stateful Deduplication with 24h TTL: Employs Flink Keyed ValueState configured with a 24-hour StateTtlConfig (expiration on write/create) to prevent unbounded memory growth while filtering duplicate transaction_id records.
- Side Outputs & Raw Payload Preservation: Uses Flink OutputTag side outputs (valid-transactions and dlq-transactions) to isolate valid records from dead-letter queue records while preserving exact, un-mutated raw_payload strings and descriptive error_reason messages for full auditability.
- Dual Kafka Outputs:
  - Valid stream -> processed-transactions (for Harsh's Data Quality & Iceberg Ingestion)
  - DLQ stream -> ecommerce-transactions-dlq (for Audit Logging & Monitoring)
- Externalized Configuration: Externalizes broker addresses (KAFKA_BOOTSTRAP_SERVERS), topic names, and checkpoint storage via environment variables (.env.example).
- Health & Metrics Backend API: Exposes contract status definitions (get_flink_module_status) and metric keys for FastAPI backend aggregation.

---

## Data Lineage & Downstream Integration

[Mahek: Data Generator]
       |
       v (topic: ecommerce-transactions)
[Basil: Flink Stream Processor] --- (Stateful 24h Deduplication & Schema Sanitizer)
       |
       +---> (topic: processed-transactions) -----> [Harsh: Iceberg Lakehouse & DQ Validations]
       |
       +---> (topic: ecommerce-transactions-dlq) -> [DLQ Dead-Letter Auditing & DQ Metrics]

---

## Canonical JSON Schemas

### 1. Valid Transaction Schema (processed-transactions)
Consumed downstream by Data Quality and Iceberg Ingestion:

{
  "transaction_id": "tx_20260924_001",
  "customer_id": "cust_101",
  "amount": 250.00,
  "currency": "USD",
  "timestamp": "2026-09-24T10:00:00Z",
  "merchant": "Amazon",
  "status": "SUCCESS"
}

### 2. Dead Letter Queue Schema (ecommerce-transactions-dlq)
Consumed downstream for Data Quality error audits:

{
  "raw_payload": "{\"transaction_id\": \"tx_bad_01\", \"amount\": 50.0, \"currency\": \"USD\", \"timestamp\": \"2026-09-24T10:01:00Z\", \"merchant\": \"Amazon\", \"status\": \"SUCCESS\"}",
  "error_reason": "Schema Validation Failure: Missing required fields ['customer_id']"
}

{
  "raw_payload": "{\"transaction_id\": \"tx_bad_02\", \"customer_id\": \"cust_101\", \"user_id\": \"usr_99\", \"amount\": 100.0, \"currency\": \"USD\", \"timestamp\": \"2026-09-24T10:02:00Z\", \"merchant\": \"Ebay\", \"status\": \"SUCCESS\"}",
  "error_reason": "Schema Validation Failure: Legacy 'user_id' field is forbidden in canonical stream"
}

{
  "raw_payload": "{\"transaction_id\": \"tx_bad_03\", \"customer_id\": \"cust_102\", \"amount\": true, \"currency\": \"USD\", \"timestamp\": \"2026-09-24T10:03:00Z\", \"merchant\": \"Target\", \"status\": \"SUCCESS\"}",
  "error_reason": "Invalid transaction amount: Boolean values (True/False) are not allowed"
}

{
  "raw_payload": "{\"transaction_id\": \"tx_dup_01\", \"customer_id\": \"cust_103\", \"amount\": 75.0, \"currency\": \"USD\", \"timestamp\": \"2026-09-24T10:04:00Z\", \"merchant\": \"BestBuy\", \"status\": \"SUCCESS\"}",
  "error_reason": "Duplicate transaction_id: tx_dup_01"
}

---

## Prerequisites & Runtime Environment

- Operating System: Linux / WSL2 / Windows 11
- Python: Python 3.11.x (matches apache-flink==1.19.2 pre-built wheels)
- Java: OpenJDK 11 or 17 with JAVA_HOME configured
- Kafka Connector: flink-sql-connector-kafka-3.0.1-1.18.jar
- Message Broker: Apache Kafka 4.x KRaft mode via root docker-compose.yml (localhost:9092)

---

## Execution Guide

### Option A: Linux / WSL2 (Team Leader Reproducible Setup)

#### 1. System Prerequisites
sudo apt update && sudo apt install -y openjdk-11-jdk python3.11 python3.11-venv
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc

#### 2. Virtual Environment Setup (Native POSIX)
mkdir -p ~/.venvs
python3.11 -m venv ~/.venvs/flink
source ~/.venvs/flink/bin/activate
pip install --upgrade pip

#### 3. Install Dependencies and Run Tests
cd /mnt/c/Users/LENOVO/Desktop/IceStream/flink
pip install -r requirements.txt
pytest tests/test_flink_pipeline.py -v
(All 10 unit tests will pass in < 1 second.)

---

### Option B: Windows Native Setup

cd C:\Users\LENOVO\Desktop\IceStream\flink
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
pytest tests/test_flink_pipeline.py -v

---

## Running the Streaming Pipeline End-to-End

1. Start Kafka Broker (KRaft Mode):
   cd ..
   docker compose up -d

2. Generate Test Transactions (Upstream):
   python data-generator/src/transaction_generator.py --count 50

3. Start Flink Consumer:
   cd flink
   python jobs/kafka_consumer.py

---

## Module Structure

flink/
|-- .env.example              # Configuration template (Brokers, Checkpoints, Topics)
|-- DOWNSTREAM_INTEGRATION.md # Contract specification for downstream Lakehouse/DQ
|-- jobs/
|   `-- kafka_consumer.py     # Main Flink streaming pipeline job
|-- lib/
|   |-- download_kafka_jar.py # Automated connector JAR utility
|   `-- flink-sql-connector-kafka-3.0.1-1.18.jar # Kafka SQL connector binary
|-- src/
|   |-- __init__.py
|   `-- transforms.py         # Schema validation, NaN/Infinity guards, 24h State TTL deduplication
|-- tests/
|   |-- __init__.py
|   `-- test_flink_pipeline.py# 10 unit tests covering schema, enums, numbers, and DLQ routing
|-- test_flow.py              # End-to-end integration test runner
|-- requirements.txt          # Pinned dependencies (apache-flink==1.19.2)
`-- README.md                 # Module documentation and execution instructions