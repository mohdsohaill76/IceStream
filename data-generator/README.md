# Data Generator Module

## Responsibility
The `data-generator` module is responsible for producing synthetic transaction datasets and publishing realistic event streams to Apache Kafka. It also provides testing utilities to inject deliberate anomalies and malformed data into the pipeline.

## Module Owner
**Person 2: Kafka + Data Generation**

## Planned Implementation
- Python-based transaction event generator (`src/`)
- Apache Kafka producer integration
- Configurable transaction throughput and schema variations
- Deliberate bad-data injection scenarios (malformed fields, schema anomalies, unexpected NULLs)
- Unit and generator verification tests (`tests/`)
