# Data Generator Module

## Responsibility

The `data-generator` module generates synthetic transaction events and publishes them to Apache Kafka. It also supports deliberate NULL-value and schema-change injection for testing downstream data-quality handling.

## Module Owner

**Person 2: Kafka + Data Generation**

## Transaction Fields

Each normal transaction contains:

- `transaction_id`
- `customer_id`
- `amount`
- `currency`
- `timestamp`
- `merchant`
- `status`

The shared transaction contract is available in:

`data-generator/transaction_schema.json`

## Install Dependencies

From the project root:

```powershell
pip install -r data-generator\requirements.txt
```

## Start Kafka with Docker

A `docker-compose.yml` file is provided in the project root.

If Docker is installed, run:

```powershell
docker compose up -d
```

Kafka will be available at:

`localhost:9092`

Automatic topic creation is disabled so that required topics are provisioned explicitly.

## Create Kafka Topic

Run:

```powershell
python data-generator\create_topic.py
```

This creates the `transactions` topic if it does not already exist.

## Run Producer

```powershell
python data-generator\src\transaction_generator.py
```

The producer generates transaction records and sends them to the Kafka `transactions` topic.

## Verify Messages with Consumer

Open another terminal and run:

```powershell
python data-generator\consumer_verification.py
```

The consumer displays transactions received from Kafka.

## Run Tests

```powershell
python -m pytest data-generator\tests -q
```

## Environment Variables

The module supports these environment variables:

- `KAFKA_SERVER` — Kafka broker address. Default: `localhost:9092`
- `KAFKA_TOPIC` — Kafka topic name. Default: `transactions`
- `GENERATION_RATE` — Transactions generated per second. Must be greater than `0`.
- `NULL_INJECTION_RATE` — Probability of deliberately injecting a NULL value.
- `SCHEMA_CHANGE_RATE` — Probability of deliberately adding an unexpected field.
- `KAFKA_PARTITIONS` — Number of topic partitions. Default: `1`
- `KAFKA_REPLICATION_FACTOR` — Topic replication factor. Default: `1`

## Reliability and Testing

The producer includes Kafka connection retries and Kafka send retries. Messages are sent without flushing after every individual message, allowing Kafka to batch records more efficiently.

Tests use controlled injection rates so NULL-injection and schema-change tests remain deterministic.

## Data Quality Scenarios

The generator can deliberately produce invalid records for downstream data-quality testing:

- NULL values in transaction fields
- An additional `unexpected_field` to simulate schema changes

These deliberate anomalies may violate the normal transaction contract and are intended for testing purposes.