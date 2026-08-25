# Flink Stream Processing Module

## Responsibility
The `flink` module handles real-time stream processing, consuming raw events from Apache Kafka, performing transformations and aggregations, and preparing the processed streams for lakehouse storage in Apache Iceberg[cite: 1, 2].

## Module Owner
**Person 3: Flink Stream Processing**[cite: 1, 2]

## Planned Implementation
- Kafka source stream consumer logic (`src/`)[cite: 2]
- Stream processing jobs, windowing, and transformations (`jobs/`)[cite: 2]
- Health and metrics contract integration for Person 1's Backend Pipeline Status API (`GET /api/v1/pipeline/status`)[cite: 1]
- Flink-to-Iceberg sink connector integration[cite: 1, 2]
- Stream processing unit and pipeline tests (`tests/`)[cite: 2]

## Prerequisites & Dependencies
- **Python Runtime**: Python 3.10 or Python 3.11 (Python 3.13 is currently unsupported by PyFlink wheels on Windows)
- **Message Broker**: Apache Kafka running on `localhost:9092` with topics published by `data-generator`[cite: 2]
- **Python Package**: `apache-flink`

## Quick Start & Execution

### 1. Set Up Virtual Environment (Python 3.11)
```bash
# Navigate to the flink module directory
cd flink

# Create virtual environment using Python 3.11
py -3.11 -m venv venv

# Activate virtual environment (Windows PowerShell)
.\venv\Scripts\activate
```

### 2. Install Dependencies
```bash
pip install --upgrade pip
pip install apache-flink
```

### 3. Run the Flink Kafka Consumer Job
```bash
python jobs/kafka_consumer.py
```

## Module Structure
```text
flink/
├── jobs/
│   └── kafka_consumer.py   # Primary Flink stream consumer job
├── lib/                     # Flink-Kafka connector JAR dependencies
├── src/                     # Shared stream transformation logic
├── tests/                   # Stream processing unit tests
└── README.md                # Module documentation
```