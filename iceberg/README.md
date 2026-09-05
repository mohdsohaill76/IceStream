# Iceberg Lakehouse Module

## Responsibility
The `iceberg` module manages table formats, catalog configurations, schemas, and snapshot lifecycle for the Apache Iceberg storage layer in the IceStream platform.

## Module Owner
**Person 4: Iceberg / Lakehouse**

## Planned Implementation
- Table schema definitions for the main transaction table and Dead-Letter Queue (DLQ) table (`schemas/`)
- Iceberg catalog configuration and client access utilities (`catalog/`)
- Snapshot management, metadata operations, and time-travel query capabilities
- Lakehouse storage and schema validation tests (`tests/`)
