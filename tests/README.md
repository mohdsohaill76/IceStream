# System Tests

## Responsibility
The `tests` directory houses system-level integration and end-to-end (E2E) test suites for validating cross-component interactions and pipeline resilience across IceStream.

## Planned Implementation
- `integration/`: Integration tests verifying component boundaries (e.g., Kafka to Flink, Flink to Iceberg, DQ evaluation triggers)
- `e2e/`: Full pipeline end-to-end tests validating the complete flow from synthetic data generation through ingestion, quality monitoring, lakehouse storage, and frontend dashboard notifications
