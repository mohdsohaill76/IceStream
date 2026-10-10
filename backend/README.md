# IceStream - Backend & Integration Layer

## Overview
This module is the backend and integration layer for the **IceStream** project. Built with **FastAPI** and **Pydantic**, it provides REST API services to expose pipeline operational health and incident status for monitoring and observability.

---

## Current Functionality & Scope

The backend currently provides:
- **Service Health Check**: Basic endpoint to verify backend service availability.
- **Pipeline Status API**: REST endpoint returning operational health across the core pipeline stages (`kafka`, `flink`, `data_quality`, `iceberg`).
- **Incident Monitoring API**: REST endpoints to list active operational incidents and fetch incident details by identifier.
- **Data Contracts**: Strongly typed Pydantic models for `Transaction`, `PipelineStatus`, `PipelineStage`, and `Incident`.

---

## Current Integration Status & Limitations

> **Important**: The backend is in early-stage development.
> - **Kafka integration**: Not implemented yet
> - **Flink integration**: Not implemented yet
> - **Iceberg integration**: Not implemented yet
> - **Data Quality integration**: Not implemented yet
> - **Frontend integration**: Not implemented yet
> - **Databases & WebSockets**: Not implemented yet
> - **Mock Data**: Pipeline status and incident records are currently served from **in-memory mock stores** in the service layer.

---

## Current API Endpoints

Detailed endpoint specifications and payload examples are documented in [docs/api/backend-api.md](../docs/api/backend-api.md).

| Method | Endpoint | Description | Status |
|---|---|---|---|
| `GET` | `/health` | Service health check | Implemented |
| `GET` | `/api/v1/pipeline/status` | Current streaming pipeline status | Mock / In-Memory |
| `GET` | `/api/v1/incidents` | List all operational pipeline incidents | Mock / In-Memory |
| `GET` | `/api/v1/incidents/{incident_id}` | Retrieve incident details by ID | Mock / In-Memory |

---

## Project Structure

```text
backend/
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── config.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── incident.py
│   │   ├── pipeline.py
│   │   └── transaction.py
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── incidents.py
│   │   └── pipeline.py
│   └── services/
│       ├── __init__.py
│       ├── incident_service.py
│       └── pipeline_service.py
│
├── tests/
│   ├── __init__.py
│   ├── test_health.py
│   ├── test_incident.py
│   ├── test_incidents_api.py
│   ├── test_pipeline.py
│   ├── test_pipeline_api.py
│   └── test_transaction.py
│
├── requirements.txt
└── README.md
```

---

## Getting Started

### 1. Prerequisites
- Python 3.11+
- Virtual environment (recommended)

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Backend Locally

```bash
uvicorn app.main:app --reload
```

The API will be accessible at `http://127.0.0.1:8000`. Interactive OpenAPI documentation is available at `http://127.0.0.1:8000/docs`.

### 4. Run Automated Tests

Execute the test suite with `pytest`:

```bash
python -m pytest
```
