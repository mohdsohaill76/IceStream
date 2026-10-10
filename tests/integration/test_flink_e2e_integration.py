import json
import time
import uuid
import pytest
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "ecommerce-transactions"
VALID_TOPIC = "processed-transactions"
DLQ_TOPIC = "transactions-dlq"


def test_flink_pipeline_valid_dlq_valid_flow():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    unique_run_id = str(uuid.uuid4())[:8]
    valid_id_1 = str(uuid.uuid4())
    invalid_id_2 = str(uuid.uuid4())
    valid_id_3 = str(uuid.uuid4())

    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    ts1 = (now + timedelta(seconds=10)).isoformat().replace("+00:00", "Z")
    ts2 = (now + timedelta(seconds=11)).isoformat().replace("+00:00", "Z")
    ts3 = (now + timedelta(seconds=12)).isoformat().replace("+00:00", "Z")

    # 1. Produce first valid record
    rec1 = {
        "transaction_id": valid_id_1,
        "customer_id": f"cust-1-{unique_run_id}",
        "amount": 149.99,
        "currency": "USD",
        "timestamp": ts1,
        "merchant": "Amazon",
        "status": "COMPLETED",
    }
    producer.send(INPUT_TOPIC, rec1)
    producer.flush()
    print(f"\n[E2E] Produced valid record 1: {valid_id_1} with ts={ts1}", flush=True)

    # 2. Produce invalid record (negative amount)
    rec2 = {
        "transaction_id": invalid_id_2,
        "customer_id": f"cust-2-{unique_run_id}",
        "amount": -75.00,
        "currency": "USD",
        "timestamp": ts2,
        "merchant": "BestBuy",
        "status": "COMPLETED",
    }
    producer.send(INPUT_TOPIC, rec2)
    producer.flush()
    print(f"[E2E] Produced invalid record 2 (amount -75): {invalid_id_2} with ts={ts2}", flush=True)

    # 3. Produce second valid record (after invalid record)
    rec3 = {
        "transaction_id": valid_id_3,
        "customer_id": f"cust-3-{unique_run_id}",
        "amount": 299.50,
        "currency": "EUR",
        "timestamp": ts3,
        "merchant": "Apple",
        "status": "COMPLETED",
    }
    producer.send(INPUT_TOPIC, rec3)
    producer.flush()
    print(f"[E2E] Produced valid record 3 (after invalid): {valid_id_3} with ts={ts3}", flush=True)

    # 4. Verify valid records in processed-transactions
    consumer_valid = KafkaConsumer(
        VALID_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"e2e-verifier-valid-{unique_run_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=15000,
    )

    found_valid_ids = set()
    start_time = time.time()
    while time.time() - start_time < 30:
        batch = consumer_valid.poll(timeout_ms=2000)
        for tp, msgs in batch.items():
            for m in msgs:
                val = m.value
                tx_id = val.get("transaction_id")
                if tx_id in {valid_id_1, valid_id_3}:
                    found_valid_ids.add(tx_id)
                    print(f"[E2E] Received valid transaction: {tx_id} -> {val}", flush=True)
        if valid_id_1 in found_valid_ids and valid_id_3 in found_valid_ids:
            break

    consumer_valid.close()

    assert valid_id_1 in found_valid_ids, f"Valid record 1 ({valid_id_1}) was not found in {VALID_TOPIC}"
    assert valid_id_3 in found_valid_ids, f"Valid record 3 ({valid_id_3}) was not found in {VALID_TOPIC}"

    # 5. Verify invalid record in transactions-dlq
    consumer_dlq = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"e2e-verifier-dlq-{unique_run_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=15000,
    )

    found_dlq_records = []
    start_time = time.time()
    while time.time() - start_time < 30:
        batch = consumer_dlq.poll(timeout_ms=2000)
        for tp, msgs in batch.items():
            for m in msgs:
                val = m.value
                raw = val.get("raw_payload", "")
                if invalid_id_2 in str(raw) or val.get("transaction_id") == invalid_id_2:
                    found_dlq_records.append(val)
                    print(f"[E2E] Received DLQ record: {val}")
        if found_dlq_records:
            break

    consumer_dlq.close()

    assert len(found_dlq_records) > 0, f"Invalid record ({invalid_id_2}) was not found in {DLQ_TOPIC}"
    dlq_record = found_dlq_records[0]
    assert "error_reason" in dlq_record, "DLQ record missing error_reason"
    assert "Invalid transaction amount" in dlq_record["error_reason"] or "Validation" in dlq_record["error_reason"]

    print("\n[E2E] ALL VERIFICATIONS PASSED:")
    print(f"  - Valid record 1: {valid_id_1} verified in {VALID_TOPIC}")
    print(f"  - Invalid record: {invalid_id_2} verified in {DLQ_TOPIC}")
    print(f"  - Valid record 3: {valid_id_3} verified in {VALID_TOPIC} after invalid record!")


if __name__ == "__main__":
    test_flink_pipeline_valid_dlq_valid_flow()
