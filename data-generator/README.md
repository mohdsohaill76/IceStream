# IceStream Data Generator

The IceStream Data Generator produces synthetic e-commerce transaction data and publishes it to Apache Kafka for downstream processing by Apache Flink.

## Data Flow

```text
Python Data Generator
        |
        v
Apache Kafka
Topic: ecommerce-transactions
        |
        v
Apache Flink
```

## Kafka Configuration

Default Kafka configuration:

```text
Kafka Server: localhost:9092
Topic: ecommerce-transactions
Partitions: 3
Replication Factor: 1
```

The configuration can be changed using environment variables:

```text
KAFKA_SERVER
KAFKA_TOPIC
KAFKA_PARTITIONS
KAFKA_REPLICATION_FACTOR
```

## Installation

From the project root, install the required Python packages:

```powershell
pip install -r data-generator\requirements.txt
```

## Create Kafka Topic

Make sure Kafka is running before creating the topic.

Run:

```powershell
python data-generator\create_topic.py
```

By default, the script creates:

```text
Topic: ecommerce-transactions
Partitions: 3
Replication Factor: 1
```

## Run the Data Generator

Run continuously:

```powershell
python data-generator\src\transaction_generator.py
```

Generate a fixed number of records using `--count`:

```powershell
python data-generator\src\transaction_generator.py --count 10
```

The command above generates 10 events and then stops.

## MAX_EVENTS

A fixed number of events can also be configured using the `MAX_EVENTS` environment variable.

Example in PowerShell:

```powershell
$env:MAX_EVENTS="10"
python data-generator\src\transaction_generator.py
```

`--count` takes priority over `MAX_EVENTS` when both are provided.

The default value of `MAX_EVENTS` is `0`, which means unlimited generation.

## Generation Rate

`GENERATION_RATE` controls how many transactions are generated per second.

Default:

```text
GENERATION_RATE=1
```

Example:

```powershell
$env:GENERATION_RATE="5"
```

## Anomaly Generation

The generator supports configurable anomaly injection for testing downstream validation, deduplication, and error-handling logic.

Default rates:

```text
NULL_INJECTION_RATE=0.05
SCHEMA_CHANGE_RATE=0.05
DUPLICATE_RATE=0.05
BAD_AMOUNT_RATE=0.03
MISSING_FIELD_RATE=0.03
INVALID_VALUE_RATE=0.03
MALFORMED_JSON_RATE=0.02
```

### Anomaly Types

- `NULL_INJECTION_RATE` - injects a null value into a transaction field.
- `SCHEMA_CHANGE_RATE` - adds an unexpected field to simulate schema changes.
- `DUPLICATE_RATE` - generates duplicate transactions for deduplication testing.
- `BAD_AMOUNT_RATE` - generates zero or negative transaction amounts.
- `MISSING_FIELD_RATE` - removes a required field from a transaction.
- `INVALID_VALUE_RATE` - generates invalid field values.
- `MALFORMED_JSON_RATE` - generates malformed JSON for DLQ/error-path testing.

All rate values must be between `0` and `1`.

Example:

```powershell
$env:DUPLICATE_RATE="0.10"
$env:BAD_AMOUNT_RATE="0.05"
python data-generator\src\transaction_generator.py --count 20
```

## Transaction Schema

Normal transactions use the following fields:

```text
transaction_id
customer_id
amount
currency
timestamp
merchant
status
```

The shared JSON Schema is stored in:

```text
data-generator/transaction_schema.json
```

The `jsonschema` package is included in the project requirements for schema validation tests.

## Kafka Message Key

For valid dictionary-based transactions, `transaction_id` is used as the Kafka message key.

This provides a stable key for downstream Kafka/Flink processing.

## Kafka Delivery Handling

Kafka sends use delivery callbacks.

A successful delivery acknowledgement records:

```text
topic
partition
offset
```

Delivery errors are handled through the Kafka error callback.

The producer also includes retry configuration for temporary Kafka delivery failures.

## Consumer Verification

Run the verification consumer with:

```powershell
python data-generator\consumer_verification.py
```

The consumer listens to:

```text
ecommerce-transactions
```

Valid JSON messages continue through normal processing.

Malformed JSON is caught safely instead of terminating the consumer and is forwarded to the configured DLQ topic.

Default DLQ topic:

```text
ecommerce-transactions-dlq
```

It can be changed with:

```text
KAFKA_DLQ_TOPIC
```

## Tests

Run all Data Generator tests from the project root:

```powershell
python -m pytest data-generator\tests -q
```

The tests cover transaction generation, schema validation, anomaly generation, duplicate generation, Kafka message keys, delivery callbacks, and malformed JSON consumer handling.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_SERVER` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `ecommerce-transactions` | Kafka transaction topic |
| `KAFKA_PARTITIONS` | `3` | Number of topic partitions |
| `KAFKA_REPLICATION_FACTOR` | `1` | Kafka replication factor |
| `KAFKA_DLQ_TOPIC` | `ecommerce-transactions-dlq` | Malformed-message DLQ topic |
| `GENERATION_RATE` | `1` | Transactions generated per second |
| `MAX_EVENTS` | `0` | Maximum events; `0` means unlimited |
| `NULL_INJECTION_RATE` | `0.05` | Null injection probability |
| `SCHEMA_CHANGE_RATE` | `0.05` | Schema-change probability |
| `DUPLICATE_RATE` | `0.05` | Duplicate probability |
| `BAD_AMOUNT_RATE` | `0.03` | Invalid amount probability |
| `MISSING_FIELD_RATE` | `0.03` | Missing-field probability |
| `INVALID_VALUE_RATE` | `0.03` | Invalid-value probability |
| `MALFORMED_JSON_RATE` | `0.02` | Malformed JSON probability |

## Notes

- Kafka must be running before the producer, consumer, or topic-creation script is used.
- The generator defaults to the `ecommerce-transactions` topic expected by the downstream pipeline.
- Topic provisioning defaults to 3 partitions.
- Malformed JSON is handled without terminating the verification consumer.