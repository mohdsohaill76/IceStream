# Apache Iceberg Lakehouse Module

## Responsibility
The `iceberg` module manages table formats, catalog configurations, schemas, and persistence for the Apache Iceberg lakehouse layer in the IceStream platform. It persists validated, canonical transactions to the clean storage table and dead-letter queue records to the DLQ table.

## Module Architecture & Features
- **Engine & Client**: Powered by PyIceberg (`0.12.0`) and Apache PyArrow (`25.0.1`).
- **Catalog Implementation**: SQLite-backed SQL catalog (`sqlite:///iceberg/warehouse/iceberg_catalog.db`) with warehouse directory located at `iceberg/warehouse`.
- **Tables**:
  - `lakehouse.clean_transactions`: Persists canonical valid transactions.
  - `lakehouse.dlq_transactions`: Persists quarantined DLQ events from both Flink and Data Quality stages.
- **Idempotency & Deduplication**: The `IcebergWriter` enforces idempotent appends by tracking committed `transaction_id` entries, preventing duplicate writes on consumer restarts or retries.
- **Table Refresh & Scans**: `IcebergReader` provides real-time metadata refreshes, full Arrow-based scans, and live snapshot metric retrieval (`clean_transactions_count`, `dlq_transactions_count`, `snapshot_id`).
- **Kafka Streaming Consumer**: `IcebergConsumerService` subscribes to `quality-checked-transactions` and `transactions-dlq`, appending batches with offset tracking.

---

## Canonical Table Schemas

### 1. `lakehouse.clean_transactions`
| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `transaction_id` | String | Yes | Canonical UUID identifier |
| `customer_id` | String | Yes | Customer ID |
| `amount` | Double | Yes | Numeric transaction amount |
| `currency` | String | Yes | ISO currency code (e.g. INR, USD) |
| `timestamp` | String | Yes | ISO-8601 transaction timestamp |
| `merchant` | String | Yes | Merchant identifier |
| `status` | String | Yes | Lifecycle status (e.g. SUCCESS, PENDING) |

### 2. `lakehouse.dlq_transactions`
| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `transaction_id` | String | No | Extracted transaction ID or UNKNOWN |
| `raw_payload` | String | No | Unaltered original raw message |
| `error_reason` | String | No | Detailed validation or processing error reason |
| `source_stage` | String | No | Source component (`flink` or `data_quality`) |
| `timestamp` | String | No | Arrival timestamp in ISO-8601 format |

---

## Running the Iceberg Lakehouse Service

### 1. Start the Consumer Daemon
To continuously stream records from Kafka into Iceberg:
```bash
python -m iceberg.src.iceberg_consumer_service
```

### 2. Querying or Inspecting Lakehouse Tables
```python
from iceberg.src.iceberg_reader import IcebergReader

reader = IcebergReader()
metrics = reader.get_metrics()
print("Clean transactions:", metrics["clean_transactions_count"])
print("DLQ transactions:", metrics["dlq_transactions_count"])

# Read back recent clean records
clean_records = reader.get_clean_records(limit=10)
for r in clean_records:
    print(r)
```

### 3. Running Unit Tests
```bash
pytest iceberg/tests -o pythonpath=. -v
```
