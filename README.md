# IceStream — Real-Time Lakehouse Observability

IceStream is a real-time Lakehouse observability and streaming data platform designed to monitor, validate, and track e-commerce transaction streams across Apache Kafka, Apache Flink, Data Quality validators, and Apache Iceberg. It provides automated schema enforcement, data-quality inspection, circuit-breaker protection, restart-safe Lakehouse persistence, and an interactive monitoring dashboard.

---

## System Architecture & End-to-End Flow

```
[ Data Generator ]
        │ (Kafka: ecommerce-transactions)
        ▼
[ Apache Flink ] ──(Invalid / Malformed / Non-finite)──> [ transactions-dlq ]
        │ (Valid: processed-transactions)                       │
        ▼                                                       │
[ Data Quality & Circuit Breaker ] ──(Rule / Null Rejections)───┤
        │ (Valid: quality-checked-transactions)                 │
        ▼                                                       ▼
[ Iceberg Clean Table ]                                 [ Iceberg DLQ Table ]
(lakehouse.clean_transactions)                          (lakehouse.dlq_transactions)
        ▲                                                       ▲
        └───────────────────┬───────────────────────────────────┘
                            │
               [ FastAPI Backend API :8000 ]
               (Health, Status, Incidents)
                            │
                            ▼
              [ React Flow UI Dashboard :5173 ]
```

---

## Canonical Frontend Declaration

- **Canonical UI Directory:** `frontend/`
- **Legacy Mirror Directory:** `icestream-ui/` (retained intact for auditability; mirrors `frontend/`)
- **Technologies:** React 19, Vite 8, TailwindCSS v4, `@xyflow/react` (React Flow 12), Lucide Icons.
- **Environment Configuration:** Configured via `frontend/.env` with `VITE_API_URL=http://localhost:8000`.

---

## Reproducible Startup Sequence

Follow these steps from a fresh terminal at the repository root:

### 1. Start Infrastructure Containers (Kafka & Flink Cluster)
```bash
docker compose up -d
```
- Starts Kafka KRaft broker at `localhost:9092`
- Starts Flink JobManager at `http://localhost:8081`
- Starts Flink TaskManager on `icestream-network`

### 2. Submit Flink Streaming Job
Submit the Python streaming consumer into the Flink JobManager container:
```bash
docker exec -d icestream-flink-jobmanager flink run -py /opt/flink/flink_app/jobs/kafka_consumer.py
```
Verify the job is running at `http://localhost:8081` or via:
```bash
docker exec icestream-flink-jobmanager flink list
```

### 3. Start Data Quality Service
In a separate terminal or background process:
```bash
cd data-quality/data_quality
python app/main.py
```

### 4. Start Iceberg Consumer Service
In a separate terminal or background process:
```bash
python -m iceberg.src.iceberg_consumer_service
```
This consumes from `quality-checked-transactions` and `transactions-dlq`, appending them with restart-safe idempotency to `lakehouse.clean_transactions` and `lakehouse.dlq_transactions`.

### 5. Start FastAPI Backend
```bash
cd backend
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Exposes:
- `GET http://localhost:8000/health`
- `GET http://localhost:8000/api/v1/pipeline/status`
- `GET http://localhost:8000/api/v1/incidents`

### 6. Start Canonical Frontend UI
```bash
cd frontend
npm install
npm run dev
```
Open `http://localhost:5173` to view the live React Flow pipeline dashboard.

---

## Data Contract Specifications

### Canonical 7-Field Transaction Contract
All valid pipeline transactions must conform to:
- `transaction_id`: String (strict UUID v4 format)
- `customer_id`: String (non-empty)
- `amount`: Float/Double (positive, finite number; boolean rejected)
- `currency`: String (ISO-4217 code; Data Quality enforces `INR`)
- `timestamp`: String (ISO-8601 UTC timestamp format)
- `merchant`: String (non-empty)
- `status`: String (`SUCCESS`, `PENDING`, `FAILED`)

### DLQ Ingestion Contract
Iceberg accepts both Flink-format (`raw_payload`, `error_reason`) and Data Quality-format (`record`, `errors`) payloads and normalizes them into `lakehouse.dlq_transactions`.

---

## Automated Test Verification

Run all modular and integration test suites:

```bash
# 1. Backend tests (36 tests)
pytest backend/tests -v

# 2. Flink transformation and deduplication tests (11 tests)
pytest flink/tests -v

# 3. Data Quality & Circuit Breaker tests (59 tests)
pytest data-quality/data_quality/tests -v

# 4. Data Generator tests (19 tests)
pytest data-generator/tests -v

# 5. Apache Iceberg Lakehouse tests (4 tests)
pytest iceberg/tests -v

# 6. Live Flink-Kafka E2E Integration test
pytest tests/integration/test_flink_e2e_integration.py -v

# 7. Complete Full-Pipeline E2E Test (Generator -> Kafka -> Flink -> DQ -> Iceberg -> Backend)
pytest tests/integration/test_full_pipeline_e2e.py -v
```
