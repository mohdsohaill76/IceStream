## IceStream - Data Quality & Circuit Breaker
# Overview

This module is responsible for maintaining data quality in the IceStream pipeline.

It validates transaction data, calculates the error rate, sends invalid records to the DLQ, and manages the Circuit Breaker.

The Circuit Breaker threshold is 2%.

Responsibilities
Validate transaction data
Detect NULL values
Detect invalid values
Detect schema errors
Calculate error rate
Manage Circuit Breaker
Send invalid records to DLQ
Manage recovery status
Handle invalid Kafka messages
Data Quality Flow
Incoming Data
      |
      v
Data Validation
      |
      v
Error Rate Calculation
      |
      v
Error Rate > 2%?
    /       \
   No       Yes
   |         |
   v         v
Normal    Circuit Breaker
Processing     |
               v
              DLQ
Validation

The following transaction fields are validated:

transaction_id
customer_id
amount
currency
timestamp
merchant
status

Validation includes:

Required fields
NULL values
Empty values
Data types
UUID format
Amount value
Currency
Timestamp
Transaction status
Error Rate

The error rate is calculated using:

Error Rate = (Bad Records / Total Records) * 100

The Circuit Breaker is activated when the error rate is greater than 2%.

Circuit Breaker

The Circuit Breaker has three states:

CLOSED
   |
   | Error Rate > 2%
   v
OPEN
   |
   | Recovery Timeout
   v
HALF_OPEN

Recovery result:

HALF_OPEN
   |
   +-- Success --> CLOSED
   |
   +-- Failure --> OPEN
Dead Letter Queue

Invalid records are sent to the Dead Letter Queue (DLQ).

Each DLQ record contains:

Record
Validation errors

This keeps invalid records separate from normal processing.

Kafka Handling

The module consumes transaction messages from Kafka.

It handles:

Valid JSON messages
Invalid JSON messages
Invalid payloads
Non-object JSON data

Invalid Kafka messages are sent to the DLQ with the related error information.

Project Structure
data-quality/
|
├── app/
│   ├── circuit_breaker/
│   ├── quality/
│   ├── dlq/
│   ├── downstream/
│   ├── monitoring/
│   ├── kafka_consumer.py
│   ├── config.py
│   └── main.py
|
├── tests/
├── requirements.txt
└── Dockerfile
Testing

Tests cover:

Data validation
Schema validation
Error rate calculation
Circuit Breaker
Kafka consumer
DLQ handling
Recovery handling

Current test result:

59 tests passed

Run tests with:

pytest
Docker

Build the image:

docker build -t icestream-data-quality .

Run the service:

docker run --rm icestream-data-quality
Technologies
Python
Apache Kafka
Pytest
Docker
Git & GitHub
Project Role

Data Quality + Circuit Breaker Developer

Responsible for data validation, error-rate calculation, Circuit Breaker handling, DLQ processing, and recovery status in the IceStream team project.