# Flink Stream Processing Module

## Responsibility
The `flink` module handles real-time stream processing, consuming raw events from Apache Kafka, performing validation and stateful deduplication, and routing clean streams for downstream lakehouse storage and data quality analysis.

## Module Owner
**Person 3: Flink Stream Processing**

## Key Features & Pipeline Implementation
- **Kafka Source (`jobs/kafka_consumer.py`)**: Consumes raw transaction streams from `ecommerce-transactions` using `KafkaSource`.
- **Upstream Contract Compliance (`src/transforms.py`)**: Parses incoming string messages into JSON and validates required fields (`transaction_id`, `customer_id`, `amount`, `currency`, `timestamp`), cleanly handling missing keys and malformed payloads.
- **Event-Time & Watermarking**: Assigns event timestamps from payloads using `RawTransactionTimestampAssigner` with a 5-second Bounded Out-of-Orderness watermark strategy.
- **Stateful Deduplication**: Uses Flink Keyed `ValueState` to detect duplicate `transaction_id` records across execution streams.
- **Side Outputs & Raw Payload Preservation**: Employs Flink `OutputTag` side outputs to isolate valid records from dead-letter queue (DLQ) records while preserving exact un-mutated `raw_payload` strings for auditability.
- **Fault Tolerance & Checkpointing**: Configured with 10-second Exactly-Once checkpointing backed by `FileSystemCheckpointStorage` (`file:///tmp/flink-checkpoints`) and fixed-delay restarts.
- **Dual Kafka Outputs**:
  - Valid events → `processed-transactions` (for Downstream Data Quality & Iceberg Ingestion)
  - Invalid / Duplicate events → `transactions-dlq` (for Audit Logging & Monitoring)
- **Externalized Configuration**: Externalizes topics, parallelism, and bootstrap servers via environment variables (`CONFIG`).
- **Metrics & Backend Status API Integration**: Provides contract status definitions (`get_flink_module_status`) and metric key definitions for FastAPI backend status monitoring.
- **Automated Testing & Automated Setup**: Full unit coverage via `pytest` (`tests/`) and automated JAR provisioning (`lib/download_kafka_jar.py`).

---

## Data Lineage & Downstream Integration

1. **Upstream Producer (Mahek - Data Generator)**: Writes raw JSON payloads to `ecommerce-transactions`.
2. **Flink Pipeline Engine**: Consumes from `ecommerce-transactions`, validates schema, deduplicates records via `ValueState`, and routes streams.
3. **Downstream Sink (Harsh - Data Quality / Iceberg)**: Consumes clean events from `processed-transactions` and failure logs from `transactions-dlq`.

*(For complete contract details, see `DOWNSTREAM_INTEGRATION.md`)*

---

## Output JSON Schemas

### 1. Valid Transaction Schema (`processed-transactions`)
*Consumed downstream by Data Quality and Iceberg Ingestion*

```json
{
  "transaction_id": "tx_987654",
  "customer_id": "cust_102",
  "amount": 149.99,
  "currency": "USD",
  "timestamp": "2026-09-12T12:00:00Z"
}
```

### 2. Dead Letter Queue Schema (`transactions-dlq`)
*Consumed downstream by Data Quality & Audit Monitoring*

```json
{
  "raw_payload": "{\"transaction_id\": \"tx_999\", \"user_id\": \"usr_102\", \"amount\": -25.0}",
  "error_reason": "Invalid transaction amount: -25.0 (Must be > 0)"
}
```

```json
{
  "raw_payload": "{\"transaction_id\": \"tx_legacy_001\", \"user_id\": \"usr_99\", \"amount\": 50.0}",
  "error_reason": "Schema Validation Failure: Missing fields ['customer_id']"
}
```

---

## Health & Metrics Contract Integration (Person 1)

Dynamic status contract exposed for backend API aggregation (`GET /api/v1/pipeline/status`):
```json
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
```

---

## Prerequisites & Setup

- **Python Runtime**: Python 3.10 or Python 3.11 (`apache-flink==1.18.0`).
- **Java Runtime**: OpenJDK 11 or 17 with `JAVA_HOME` properly configured.
- **Kafka Connector JAR**: Automated via `lib/download_kafka_jar.py` or manually placed in `lib/flink-sql-connector-kafka-3.0.1-1.18.jar`.
- **Message Broker**: Apache Kafka running on `localhost:9092` (or configured via environment variables).

---

## Execution Steps

### 1. Environment Setup
```powershell
# Navigate to the flink module directory
cd flink

# Activate your virtual environment
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Provision Dependencies & Connector JARs
Automatically fetch required Maven connector binaries:
```powershell
python lib/download_kafka_jar.py
```

### 3. Run Automated Unit Tests
Verify pipeline transforms, schema validation, DLQ routing, and Flink `ValueState` deduplication:
```powershell
pytest tests/
```

### 4. Execute Flink Streaming Job
```powershell
python jobs/kafka_consumer.py
```

---

## Module Structure

```text
flink/
├── DOWNSTREAM_INTEGRATION.md  # Topic contracts & stream lineage documentation
├── jobs/
│   └── kafka_consumer.py      # Main PyFlink execution job & streaming DAG setup
├── lib/
│   ├── download_kafka_jar.py  # Automated Maven JAR dependency downloader
│   └── flink-sql-connector-kafka-3.0.1-1.18.jar # Kafka connector JAR
├── src/
│   ├── __init__.py
│   └── transforms.py          # KeyedProcessFunction, validation, DLQ side outputs
├── tests/
│   ├── __init__.py
│   └── test_flink_pipeline.py # Unit tests for customer_id, DLQ routing, and state
├── test_flow.py               # E2E integration testing producer/consumer script
├── README.md                  # Complete module documentation
└── requirements.txt           # Pinned PyFlink, pytest, and connector dependencies
```