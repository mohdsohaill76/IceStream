# IceStream

IceStream is a real-time Lakehouse observability system designed to monitor, validate, and track streaming data pipelines running across Apache Kafka, Apache Flink, and Apache Iceberg. It provides automated data-quality inspection, transaction health tracking, circuit-breaker alerting, and end-to-end pipeline visualization through an interactive monitoring dashboard.

## High-Level Architecture

```
[ Data Generator ] ──> [ Apache Kafka ] ──> [ Apache Flink ] ──> [ Apache Iceberg (Lakehouse) ]
                              │                    │                        │
                              ▼                    ▼                        ▼
                      [ Data Quality & Reliability Inspection ] ──> [ DLQ / Remediation ]
                                                   │
                                                   ▼
                                      [ FastAPI / Backend API ]
                                                   │
                                                   ▼
                                  [ React Flow Dashboard UI ]
```

## Technology Stack

- **Streaming & Ingestion:** Apache Kafka
- **Stream Processing:** Apache Flink
- **Lakehouse Storage:** Apache Iceberg
- **Backend & Data Quality:** Python (FastAPI, pytest, PyIceberg)
- **Frontend Dashboard:** React, React Flow, WebSockets
- **Protocols & Communication:** REST API, WebSockets

## Team Module Ownership

- **Person 1 (Backend + Integration Lead):** Backend structure, API layer, integration between components, pipeline state/incident communication, overall system integration.
- **Person 2 (Kafka + Data Generation):** Python transaction generator, Kafka producer, transaction stream, deliberate bad-data injection.
- **Person 3 (Flink Stream Processing):** Kafka to Flink stream ingestion, real-time processing, future Flink to Iceberg integration.
- **Person 4 (Iceberg / Lakehouse):** Iceberg catalog, main transaction table, dead-letter queue (DLQ) table, snapshots, and future time-travel functionality.
- **Person 5 (Data Quality + Reliability):** Data-quality rules, NULL/schema-change detection, error-rate calculation, future 2% circuit breaker, DLQ, and remediation.
- **Person 6 (Frontend + Monitoring):** React, React Flow pipeline visualization, future WebSocket integration, incident/status dashboard.

## Current Development Status

Week 1 — Foundation Setup
