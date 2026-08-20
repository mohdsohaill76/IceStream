# Flink Stream Processing Module

## Responsibility
The `flink` module handles real-time stream processing, consuming raw events from Apache Kafka, performing transformations and aggregations, and preparing the processed streams for lakehouse storage in Apache Iceberg.

## Module Owner
**Person 3: Flink Stream Processing**

## Planned Implementation
- Kafka source stream consumer logic (`src/`)
- Stream processing jobs, windowing, and transformations (`jobs/`)
- Flink-to-Iceberg sink connector integration
- Stream processing unit and pipeline tests (`tests/`)
