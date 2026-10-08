# IceStream

## Project

**IceStream – Real-Time Lakehouse Observability**

## Role

** Data Quality & Circuit Breaker Developer **

## Technologies

- Python
- Apache Kafka
- Pytest
- Docker
- Git & Github

## Data Quality

The Data Quality module validates incoming transaction data and identifies data-quality issues.

Validation includes:

Required fields
- NULL values
- Empty values
- Invalid data types
- Invalid transaction ID
- Invalid amount
- Invalid currency
- Invalid timestamp
- Invalid transaction status

## Error Rate

The module calculates the error rate for processed records.

Error Rate = (Bad Records / Total Records) × 100

The Circuit Breaker threshold is 2%.

## Circuit Breaker

The Circuit Breaker protects the pipeline when the data error rate exceeds the configured threshold.

States:

CLOSED
OPEN
HALF_OPEN

Recovery is handled by checking the pipeline status after the recovery timeout.

## Dead Letter Queue

Invalid records are sent to the Dead Letter Queue (DLQ) along with their validation errors.

The DLQ helps keep invalid records separate from normal processing.

## Kafka Handling

The module consumes transaction messages from Kafka and handles invalid Kafka payloads.

Invalid or malformed messages are sent to the DLQ with the related error information.

## Testing

Automated tests cover:

- Data validation
- Schema validation
- Error rate calculation
- Circuit Breaker
- Kafka consumer handling
- DLQ handling
- Recovery handling

Current test result:

59 tests passed

Run tests using:

pytest

## Docker

Build the Docker image:

docker build -t icestream-data-quality .

Run the service:

docker run --rm icestream-data-quality

## Project Structure

The Data Quality module is organized into the following folders and files:

app/
      - circuit_breaker/
      - quality/
      - dlq/
      - downstream/
      - monitoring/
      - kafka_consumer.py
      - config.py
      - main.py
tests/
requirements.txt
Dockerfile

## Project Responsibility

The Data Quality & Circuit Breaker module is responsible for:

- Transaction data validation
- Error-rate calculation
- Circuit Breaker management
- DLQ processing
- Kafka payload handling
- Recovery status management