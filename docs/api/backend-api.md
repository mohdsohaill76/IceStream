# IceStream Backend API Reference

This document provides technical documentation for the current IceStream Backend REST API endpoints, response schemas, and architecture.

---

## Architecture Overview

The backend follows a layered and modular architecture:

```text
Route (FastAPI APIRouter)
  ↓
Service (Business & Data Logic)
  ↓
Pydantic Model (Validation & Data Contract)
```

- **Routes (`app/routes/`)**: Kept thin; responsible for request routing, parameter parsing, HTTP status codes, and error translation.
- **Services (`app/services/`)**: Encapsulate data retrieval, business rules, and state management.
- **Pydantic Models (`app/models/`)**: Define and enforce strict data contracts and response schemas.

---

## Base URL & Prefixes

- **Health Check**: `/health`
- **API v1 Prefix**: `/api/v1`

---

## Current Endpoints

### 1. Health Check

Checks whether the backend service is running.

- **Endpoint**: `GET /health`
- **Response Code**: `200 OK`
- **Response Format**: `application/json`

#### Response Body
```json
{
  "status": "healthy"
}
```

---

### 2. Pipeline Status

Retrieves the current operational status and health of the streaming pipeline stages.

> **Note**: The current implementation uses **mock / in-memory status** and is **NOT** yet connected to live Kafka, Flink, Data Quality, or Iceberg components.

- **Endpoint**: `GET /api/v1/pipeline/status`
- **Response Code**: `200 OK`
- **Response Model**: `PipelineStatus`

#### Response Schema

| Field | Type | Description |
|---|---|---|
| `pipeline` | string | Name/identifier of the streaming pipeline (e.g. `transaction_stream`) |
| `status` | string | Overall pipeline health status (e.g. `healthy`) |
| `stages` | list of objects | List of constituent pipeline stages |
| `stages[].name` | string | Name of the pipeline stage |
| `stages[].status` | string | Operational status of the stage |

#### Current Mock Stages
1. `kafka`
2. `flink`
3. `data_quality`
4. `iceberg`

#### Example Response
```json
{
  "pipeline": "transaction_stream",
  "status": "healthy",
  "stages": [
    {
      "name": "kafka",
      "status": "healthy"
    },
    {
      "name": "flink",
      "status": "healthy"
    },
    {
      "name": "data_quality",
      "status": "healthy"
    },
    {
      "name": "iceberg",
      "status": "healthy"
    }
  ]
}
```

---

### 3. List Incidents

Retrieves the list of operational incidents across the pipeline.

> **Note**: The current implementation reads from an **in-memory mock incident store**.

- **Endpoint**: `GET /api/v1/incidents`
- **Response Code**: `200 OK`
- **Response Model**: `list[Incident]`

#### Response Schema (Per Incident)

| Field | Type | Description |
|---|---|---|
| `incident_id` | string | Unique incident identifier (e.g. `INC-001`) |
| `stage` | string | Pipeline stage where the incident occurred (e.g. `data_quality`, `flink`) |
| `severity` | string | Incident severity level (`critical`, `medium`, etc.) |
| `message` | string | Human-readable incident description |
| `status` | string | Current lifecycle status (`open`, `acknowledged`, `resolved`) |
| `timestamp` | string (ISO 8601) | Timestamp when the incident occurred |

#### Example Response
```json
[
  {
    "incident_id": "INC-001",
    "stage": "data_quality",
    "severity": "critical",
    "message": "Transaction error rate exceeded threshold",
    "status": "open",
    "timestamp": "2026-08-22T08:38:51.123456Z"
  },
  {
    "incident_id": "INC-002",
    "stage": "flink",
    "severity": "medium",
    "message": "Flink processing latency increased",
    "status": "acknowledged",
    "timestamp": "2026-08-22T08:38:51.123456Z"
  }
]
```

---

### 4. Get Incident by ID

Retrieves details for a specific operational incident by its identifier.

- **Endpoint**: `GET /api/v1/incidents/{incident_id}`
- **Path Parameters**:
  - `incident_id` (string, required): Identifier of the incident (e.g. `INC-001`, `INC-002`)
- **Response Model**: `Incident`

#### Responses

- **`200 OK`**: Incident found.

```json
{
  "incident_id": "INC-001",
  "stage": "data_quality",
  "severity": "critical",
  "message": "Transaction error rate exceeded threshold",
  "status": "open",
  "timestamp": "2026-08-22T08:38:51.123456Z"
}
```

- **`404 Not Found`**: Incident not found in store.

```json
{
  "detail": "Incident with ID 'DOES-NOT-EXIST' not found"
}
```

---

## Current Integration Status

- **Kafka integration**: Not implemented yet
- **Flink integration**: Not implemented yet
- **Iceberg integration**: Not implemented yet
- **Data Quality integration**: Not implemented yet
- **Frontend integration**: Not implemented yet
- **Pipeline and incident data**: Currently mock / in-memory
