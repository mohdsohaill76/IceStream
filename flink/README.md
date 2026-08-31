# Flink Stream Processing Module

## Responsibility
The `flink` module handles real-time stream processing, consuming raw events from Apache Kafka, performing transformations and aggregations, and preparing the processed streams for lakehouse storage in Apache Iceberg.

## Module Owner
**Person 3: Flink Stream Processing**

## Key Features & Pipeline Implementation
- **Kafka Source (`jobs/kafka_consumer.py`)**: Consumes raw transaction streams from `ecommerce-transactions` via `KafkaSource`.
- **JSON Parsing & Validation (`src/transforms.py`)**: Parses incoming string messages into JSON, validates required fields (`transaction_id`, `user_id`, `amount`, `currency`, `timestamp`), and checks data types without crashing the job.
- **Event-Time & Watermarking**: Assigns event timestamps from payloads and applies a 5-second Bounded Out-of-Orderness watermark strategy.
- **Stateful Deduplication**: Uses Flink Keyed `ValueState` with a 24-hour State TTL (`set_state_visibility`, `set_update_type`) to drop duplicate `transaction_id` records.
- **Fault Tolerance & Checkpointing**: Configured with 10-second Exactly-Once checkpointing and a 3-attempt fixed-delay restart strategy.
- **Dual Kafka Outputs**:
  - Valid events → `processed-transactions` (for Person 4 Iceberg Sink & Person 5 Data Quality)
  - Invalid / Duplicate events → `transactions-dlq` (for Person 5 Data Quality & Monitoring)
- **Metrics & Backend Status API Integration**: Exposes live counters (`records_processed_count`, `records_invalid_count`, `records_duplicate_count`) and provides status contracts for Person 1's Backend Status API (`GET /api/v1/pipeline/status`).
- **Automated & Integration Testing**: Unit coverage via `pytest` (`tests/`) and end-to-end integration test flow (`test_flow.py`).

---

## Output JSON Schemas

### 1. Valid Transaction Schema (`processed-transactions`)
*Consumed by Person 4 (Iceberg Sink) and Person 5 (Data Quality)*

```json
{
  "transaction_id": "tx_987654",
  "user_id": "usr_102",
  "amount": 149.99,
  "currency": "USD",
  "timestamp": "2026-08-31T12:00:00Z",
  "event_timestamp_ms": 1788177600000,
  "processed_at": "2026-08-31T12:00:01.123456+00:00"
}
```

### 2. Dead Letter Queue Schema (`transactions-dlq`)
*Consumed by Person 5 (Data Quality & Monitoring)*

```json
{
  "raw_payload": "{\"transaction_id\": \"tx_999\", \"amount\": -25.0}",
  "error_reason": "Invalid amount: must be positive (> 0)",
  "transaction_id": "tx_999",
  "failed_at": "2026-08-31T12:00:01.123456+00:00"
}
```

---

## Health & Metrics Contract Integration (Person 1)

The job tracks state metrics via Flink's runtime metric group:
- `records_processed_count`: Total valid unique transactions processed.
- `records_invalid_count`: Total malformed, missing field, or invalid records sent to DLQ.
- `records_duplicate_count`: Total duplicate transaction IDs filtered to DLQ.

Dynamic contract response format for status API integration:
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

## Prerequisites & Dependencies

- **Python Runtime**: Python 3.10 or Python 3.11 (`apache-flink==1.18.1`).
- **Java Runtime**: OpenJDK 11 or OpenJDK 17 (Java 11 recommended). Ensure `JAVA_HOME` environment variable is set.
- **Kafka Connector JAR**: `flink-sql-connector-kafka-3.0.1-1.18.jar` placed inside `flink/lib/`.
- **Message Broker**: Apache Kafka running on `localhost:9092`.

---

## Quick Start & Execution

### 1. Set Up Virtual Environment (Python 3.11)
```bash
# Navigate to the flink module directory
cd flink

# Create virtual environment using Python 3.11
py -3.11 -m venv venv

# Activate virtual environment (Windows PowerShell)
.\venv\Scripts\Activate.ps1
```

### 2. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Run Automated Unit Tests
```bash
python -m pytest tests
```

### 4. Run the Flink Kafka Consumer Job
```bash
python jobs/kafka_consumer.py
```

---

## Live End-to-End Integration Testing

### 1. Verification Steps

1. **Verify Kafka Broker Connectivity**:
   ```powershell
   Test-NetConnection -ComputerName localhost -Port 9092
   ```
   *(Ensure `TcpTestSucceeded : True` is returned).*

2. **Start the Flink Processing Engine (Terminal 1)**:
   ```powershell
   python jobs/kafka_consumer.py
   ```
   *Keep this window open to allow Flink to continuously consume events.*

3. **Execute Live Integration Test Suite (Terminal 2)**:
   ```powershell
   python test_flow.py
   ```

### 2. Live Verification Results

End-to-end integration test output confirming live parsing, validation, enrichment, stateful deduplication, and dual-sink Kafka output routing:

```text
--- 1. Sending Test Events ---
Test events dispatched to Kafka.

--- 2. Checking 'processed-transactions' (Valid Sink) ---
RECEIVED VALID: {"transaction_id": "tx_valid_100", "user_id": "usr_55", "amount": 199.99, "currency": "USD", "timestamp": "2026-08-31T14:55:00Z", "event_timestamp_ms": 1788188100000, "processed_at": "2026-08-31T10:44:16.205483+00:00"}

--- 3. Checking 'transactions-dlq' (DLQ Sink) ---
RECEIVED DLQ: {"raw_payload": "{\"transaction_id\": \"tx_invalid_200\", \"user_id\": \"usr_56\", \"amount\": -50.0, \"currency\": \"USD\", \"timestamp\": \"2026-08-31T14:55:00Z\"}", "error_reason": "Invalid amount: must be positive (> 0)", "transaction_id": "tx_invalid_200", "failed_at": "2026-08-31T10:44:16.205483+00:00"}
RECEIVED DLQ: {"raw_payload": "{\"transaction_id\": \"tx_valid_100\", \"user_id\": \"usr_55\", \"amount\": 199.99, \"currency\": \"USD\", \"timestamp\": \"2026-08-31T14:55:00Z\", \"event_timestamp_ms\": 1788188100000, "processed_at": "2026-08-31T10:44:16.205483+00:00\"}", "error_reason": "Duplicate transaction_id detected", "transaction_id": "tx_valid_100", "failed_at": "2026-08-31T10:44:17.316149+00:00"}
```

---

## Module Structure

```text
flink/
├── jobs/
│   └── kafka_consumer.py      # Primary Flink streaming job execution entrypoint
├── lib/
│   └── flink-sql-connector-kafka-3.0.1-1.18.jar # Flink-Kafka connector dependency
├── src/
│   ├── __init__.py
│   └── transforms.py          # JSON parsing, validation, watermarking & stateful deduplication
├── tests/
│   ├── __init__.py
│   └── test_flink_pipeline.py # Unit tests for parsing, DLQ routing, state & metrics
├── test_flow.py               # Live end-to-end integration test producer/consumer script
├── README.md                  # Comprehensive module documentation
└── requirements.txt           # PyFlink, pytest, and connector dependencies
```