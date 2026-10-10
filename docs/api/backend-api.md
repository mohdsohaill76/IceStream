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

Retrieves operational incidents dynamically aggregated from the live pipeline state, including:
1. **Circuit-breaker state changes**: Derived from `shared_circuit_status.json` (active when state is `OPEN`, `HALF_OPEN`, or validator is `STOPPED`).
2. **Dead Letter Queue events**: Derived from actual records stored in `lakehouse.dlq_transactions` via `IcebergReader`.

> **Persistence Note**: DLQ events are durably stored in the Iceberg Lakehouse (`lakehouse.dlq_transactions`). Active circuit breaker trip incidents reflect current state from `shared_circuit_status.json`. When the circuit recovers back to `CLOSED`, the active trip incident is resolved and no longer emitted.

- **Endpoint**: `GET /api/v1/incidents`
- **Response Code**: `200 OK`
- **Response Model**: `list[Incident]`

#### Response Schema (Per Incident)

| Field | Type | Description |
|---|---|---|
| `incident_id` | string | Unique, deterministic incident identifier (e.g. `INC-CB-OPEN-1791540000`, `INC-DLQ-E25E1DDD768C`) |
| `stage` | string | Pipeline stage where the incident occurred (`data_quality`, `flink`, `iceberg`, `dlq`) |
| `severity` | string | Severity level (`critical`, `high`, `medium`, `low`) |
| `message` | string | Diagnostic error description or quarantine reason |
| `status` | string | Operational lifecycle status (`open`, `acknowledged`, `resolved`) |
| `timestamp` | string (ISO 8601 UTC) | Timestamp when the incident occurred or was quarantined |

#### Example Response
```json
[
  {
    "incident_id": "INC-CB-OPEN-1791540000",
    "stage": "data_quality",
    "severity": "critical",
    "message": "Circuit breaker tripped (OPEN): transaction error rate 4.5% exceeded 2.0% threshold",
    "status": "open",
    "timestamp": "2026-10-09T09:51:50Z"
  },
  {
    "incident_id": "INC-DLQ-E25E1DDD768C",
    "stage": "flink",
    "severity": "high",
    "message": "Invalid transaction amount: -75.0 (must be positive)",
    "status": "open",
    "timestamp": "2026-10-09T09:51:44.420120Z"
  }
]
```

---

### 4. Get Incident by ID

Retrieves details for a specific operational incident by its deterministic identifier.

- **Endpoint**: `GET /api/v1/incidents/{incident_id}`
- **Path Parameters**:
  - `incident_id` (string, required): Identifier of the incident (e.g. `INC-CB-OPEN-1791540000`, `INC-DLQ-E25E1DDD768C`)
- **Response Model**: `Incident`

#### Responses

- **`200 OK`**: Incident found.

```json
{
  "incident_id": "INC-CB-OPEN-1791540000",
  "stage": "data_quality",
  "severity": "critical",
  "message": "Circuit breaker tripped (OPEN): transaction error rate 4.5% exceeded 2.0% threshold",
  "status": "open",
  "timestamp": "2026-10-09T09:51:50Z"
}
```

- **`404 Not Found`**: Incident not found in active state or DLQ history.

```json
{
  "detail": "Incident with ID 'DOES-NOT-EXIST' not found"
}
```

---

### 5. Lakehouse Metrics

Retrieves real-time committed transaction counts and active snapshot IDs for Iceberg clean and DLQ tables from `IcebergReader`.

- **Endpoint**: `GET /api/v1/lakehouse/metrics`
- **Response Code**: `200 OK`
- **Response Model**: `LakehouseMetrics`

#### Response Schema

| Field | Type | Description |
|---|---|---|
| `clean_transactions_count` | integer (≥ 0) | Total row count committed in `lakehouse.clean_transactions` |
| `clean_snapshot_id` | integer or null | Current active snapshot ID for clean table (`null` if empty) |
| `dlq_transactions_count` | integer (≥ 0) | Total row count committed in `lakehouse.dlq_transactions` |
| `dlq_snapshot_id` | integer or null | Current active snapshot ID for DLQ table (`null` if empty) |

#### Example Response (200 OK)
```json
{
  "clean_transactions_count": 25,
  "clean_snapshot_id": 8065307196847926507,
  "dlq_transactions_count": 166,
  "dlq_snapshot_id": 2263522881038657553
}
```

#### Example Response on Empty Catalog (200 OK)
```json
{
  "clean_transactions_count": 0,
  "clean_snapshot_id": null,
  "dlq_transactions_count": 0,
  "dlq_snapshot_id": null
}
```

#### Error Response (503 Service Unavailable)
Returned when the underlying Iceberg catalog database or tables are inaccessible (avoids silently reporting zero counts).
```json
{
  "detail": "Failed to retrieve Lakehouse metrics: Underlying SQLite database locked or inaccessible"
}
```

---

### 6. Lakehouse Snapshots History

Retrieves historical commit snapshot metadata for Apache Iceberg clean and DLQ tables, sorted chronologically descending (newest commit first).

- **Endpoint**: `GET /api/v1/lakehouse/snapshots`
- **Query Parameters**:
  - `table` (string, optional): Filter snapshots by table name (`clean_transactions` or `dlq_transactions`). If omitted, snapshots from both tables are returned.
- **Response Code**: `200 OK`
- **Response Model**: `list[SnapshotMetadata]`

#### Response Schema (Per Snapshot)

| Field | Type | Description |
|---|---|---|
| `snapshot_id` | integer | Unique 64-bit integer identifier for the Iceberg snapshot |
| `table_name` | string | Full table name (`lakehouse.clean_transactions` or `lakehouse.dlq_transactions`) |
| `committed_at` | string (ISO-8601 UTC) | Timestamp when the snapshot commit occurred |
| `timestamp_ms` | integer | Commit timestamp in epoch milliseconds |
| `operation` | string or null | Operation that produced the snapshot (e.g. `append`, `replace`) |
| `summary` | object | Snapshot summary key-value pairs recorded by PyIceberg |
| `parent_snapshot_id` | integer or null | Parent snapshot ID in commit lineage (`null` if root snapshot) |

#### Example Response (200 OK)
```json
[
  {
    "snapshot_id": 8065307196847926507,
    "table_name": "lakehouse.clean_transactions",
    "committed_at": "2026-10-09T09:51:51.690000Z",
    "timestamp_ms": 1791539511690,
    "operation": "append",
    "summary": {
      "operation": "append",
      "added-files-size": "2869",
      "added-data-files": "1",
      "added-records": "1",
      "total-data-files": "22",
      "total-delete-files": "0",
      "total-records": "25",
      "total-files-size": "63274",
      "total-position-deletes": "0",
      "total-equality-deletes": "0"
    },
    "parent_snapshot_id": 366429276759270401
  }
]
```

#### Error Responses

- **`400 Bad Request`**: Unknown or unsupported table name filter passed in query parameter.
```json
{
  "detail": "Unknown table 'invalid'. Supported tables: 'lakehouse.clean_transactions', 'lakehouse.dlq_transactions'"
}
```

- **`503 Service Unavailable`**: Catalog or table storage access failure.
```json
{
  "detail": "Failed to retrieve Lakehouse snapshots: Snapshot manifest read error"
}
```

---

## Current Integration Status

- **Kafka integration**: Real probe active via broker socket connectivity check (`PipelineStatus`)
- **Flink integration**: Real probe active via Flink REST API `/jobs/overview` (`PipelineStatus`)
- **Data Quality integration**: Real probe active via cross-process `shared_circuit_status.json`, PID liveness, and heartbeat expiration check (`PipelineStatus`)
- **Iceberg integration**:
  - Live probe in `PipelineStatus` via `init_catalog_and_tables`
  - Real table row counts and snapshot IDs via `GET /api/v1/lakehouse/metrics`
  - Real commit history and PyIceberg summary metadata via `GET /api/v1/lakehouse/snapshots`
- **Incidents API**: Real-time aggregation active; dynamically derives circuit breaker incidents from `shared_circuit_status.json` and quarantined transactions from Iceberg `lakehouse.dlq_transactions`
- **Frontend canonical**: Canonical directory is `frontend/`; `icestream-ui/` is archived mirror
