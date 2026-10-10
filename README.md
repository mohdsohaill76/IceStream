# IceStream — Real-Time Lakehouse Observability

**IceStream** is a real-time streaming data and lakehouse observability platform designed to monitor e-commerce transaction pipelines, validate data quality, isolate invalid records, and track pipeline health across Apache Kafka, Apache Flink, and Apache Iceberg.

It combines a FastAPI backend with a React Flow dashboard to visualize pipeline stages, monitor incidents, and inspect lakehouse metrics and snapshots.

<p align="center">
  <a href="https://icestream-0tnc.onrender.com"><strong>🚀 View Live Dashboard</strong></a>
</p>

## Project Links

| Resource | Link |
|---|---|
| GitHub Repository | [IceStream](https://github.com/mohdsohaill76/IceStream) |
| Live Frontend | [icestream-0tnc.onrender.com](https://icestream-0tnc.onrender.com) |
| Backend Health | Replace with the actual backend URL from Render, followed by `/health` |

> **Deployment status:** The frontend and FastAPI backend have been deployed on Render. End-to-end connectivity between the cloud services and the complete Kafka → Flink → Data Quality → Iceberg pipeline still requires verification.

---

## Key Features

- **Real-Time Streaming:** Ingests mock e-commerce transactions through Apache Kafka.
- **Stream Processing:** Uses Apache Flink to consume and process transaction events.
- **Data Quality Validation:** Detects missing, invalid, and malformed transaction data.
- **Dead Letter Queue (DLQ):** Separates rejected records for inspection and troubleshooting.
- **Circuit Breaker Foundation:** Supports reliability controls based on transaction error rates.
- **Lakehouse Storage:** Uses Apache Iceberg for transaction tables and snapshot-based data management.
- **Pipeline Observability:** Exposes backend APIs for pipeline health, incidents, metrics, and snapshots.
- **Interactive Dashboard:** Visualizes pipeline stages using React Flow, with status indicators and navigation.
- **Containerized Backend:** Packages the FastAPI service using Docker for deployment.

## System Architecture

```text
Python Transaction Generator
            |
            v
     Apache Kafka
            |
            v
      Apache Flink
            |
            v
      Data Quality
        /     \
       /       \
   Valid       Invalid
     |            |
     v            v
Quality-Checked   DLQ
 Transactions     Records
     |            |
     v            v
     Apache Iceberg
  Clean Table + DLQ Table
            |
            v
      FastAPI Backend
            |
            v
    React Flow Dashboard
```

The diagram represents the intended end-to-end architecture. Actual cloud connectivity between all pipeline components must be validated separately.

## Technology Stack

| Component | Technologies |
|---|---|
| Data Generation | Python |
| Message Streaming | Apache Kafka |
| Stream Processing | Apache Flink |
| Data Quality | Python, custom validation, circuit-breaker logic |
| Lakehouse | Apache Iceberg |
| Backend | Python, FastAPI |
| Frontend | React, Vite, React Flow |
| Testing | Pytest |
| Containerization | Docker |
| Deployment | Render |

## Repository Structure

```text
IceStream/
├── backend/             # FastAPI backend and API tests
├── data-generator/      # Mock transaction generator
├── data-quality/        # Validation and reliability logic
├── flink/               # Flink streaming jobs
├── iceberg/             # Iceberg lakehouse integration
├── frontend/             # Canonical React Flow dashboard
├── docker/               # Docker-related configuration
├── docs/                 # Project documentation
├── tests/                # Integration tests
├── docker-compose.yml
└── README.md
```

**Canonical frontend:** `frontend/` is the maintained application used for deployment. The separate `icestream-ui/` directory is an older implementation and is not the production frontend.

## Backend API

The FastAPI backend provides the following documented endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Backend health check |
| GET | `/api/v1/pipeline/status` | Pipeline stage status |
| GET | `/api/v1/lakehouse/metrics` | Lakehouse metrics |
| GET | `/api/v1/lakehouse/snapshots` | Iceberg snapshot information |
| GET | `/api/v1/incidents` | Retrieve incidents |
| GET | `/api/v1/incidents/{id}` | Retrieve an individual incident |

Endpoint responses may depend on downstream services and local or cloud infrastructure availability.

## Local Setup

### Prerequisites

- Python 3.11
- Node.js and npm
- Docker and Docker Compose
- Git

### 1. Clone the repository

```bash
git clone https://github.com/mohdsohaill76/IceStream.git
cd IceStream
```

### 2. Start Kafka and Flink

```bash
docker compose up -d
```

Check that the containers are running before proceeding.

### 3. Start the Flink job

Use the job path configured in the repository's Docker Compose setup:

```bash
docker exec -d icestream-flink-jobmanager flink run -py /opt/flink/flink_app/jobs/kafka_consumer.py
```

Verify that the job is running through the Flink dashboard or the Flink CLI.

### 4. Start the Data Quality service

Open a new terminal from the repository root:

```bash
cd data-quality/data_quality
python app/main.py
```

### 5. Start the Iceberg consumer

Open another terminal from the repository root:

```bash
python -m iceberg.src.iceberg_consumer_service
```

This service consumes quality-checked transactions and DLQ records for lakehouse persistence. Ensure the required Python dependencies and catalog configuration are available.

### 6. Start the backend

From the repository root:

```bash
cd backend
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Backend URL: `http://localhost:8000`

Health endpoint: `http://localhost:8000/health`

### 7. Start the frontend

Open another terminal from the repository root:

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173` in your browser.

### Environment Configuration

Create local environment files from the relevant `.env.example` templates and configure the values required by your environment.

For the frontend, configure:

```env
VITE_API_URL=http://localhost:8000
```

For production, set `VITE_API_URL` to the actual backend URL in the Render Static Site environment settings **before building**.

Vite embeds `VITE_*` variables into the frontend bundle at build time. After changing these variables, trigger a new frontend build and deployment.

Do not commit secrets or private credentials.

## Transaction Data Contract

The canonical transaction contract contains seven fields:

| Field | Description |
|---|---|
| `transaction_id` | Unique transaction identifier |
| `customer_id` | Customer identifier |
| `amount` | Positive, finite transaction amount |
| `currency` | Currency code; the current Data Quality rules enforce INR |
| `timestamp` | ISO 8601 UTC timestamp |
| `merchant` | Merchant name |
| `status` | Transaction status: `SUCCESS`, `PENDING`, or `FAILED` |

Validation rules determine whether a record proceeds through the pipeline or is rejected.

## Reliability and Data Quality

The project is designed to support:

- Null and invalid-field detection.
- Transaction schema validation.
- Malformed-record isolation.
- Error-rate calculation.
- Circuit-breaker behavior using a configurable threshold.
- DLQ inspection and persistence.
- Pipeline health monitoring and incident reporting.

The 2% error-rate threshold is part of the planned reliability behavior and should not be interpreted as proof that a production circuit breaker has been verified end to end.

## Testing and Verification

The latest reported local test results are:

| Component | Reported result |
|---|---:|
| Backend API | 56 passed |
| Data Quality | 83 passed |
| Apache Flink | 11 passed |
| Data Generator | 19 passed |
| Apache Iceberg | 5 passed |
| Total reported automated tests | 174 passed |
| Frontend production build | Successful |
| Docker image build | Successful |

These are reported local verification results, not a guarantee of production health. Re-run the tests against the current checkout before relying on them as the latest status.

Typical test commands:

```bash
python -m pytest backend/tests -v
python -m pytest data-quality/data_quality/tests -v
python -m pytest flink/tests -v
python -m pytest data-generator/tests -v
python -m pytest iceberg/tests -v
```

Integration tests may require Kafka, Flink, Iceberg, and their supporting infrastructure to be running.

## Deployment

The frontend is hosted on Render as a Static Site, and the FastAPI backend is hosted as a Render Web Service.

For a complete cloud deployment, the following components must also be deployed and configured to communicate with one another:

- Kafka broker
- Flink JobManager and TaskManager
- Data Quality service
- Iceberg consumer
- Durable Iceberg warehouse and catalog
- Backend access to downstream services

A successful frontend or backend deployment alone does not establish that the complete streaming pipeline is operational.

## Future Improvements

- Complete cloud deployment of the streaming infrastructure.
- Verify end-to-end transaction flow from Kafka to Iceberg.
- Connect live pipeline events to the monitoring dashboard.
- Expand incident monitoring and circuit-breaker observability.
- Improve automated integration testing and deployment checks.

## Project Goal

IceStream aims to demonstrate how real-time data engineering systems can combine streaming, data validation, lakehouse persistence, and observability to build a more reliable transaction-processing pipeline.
