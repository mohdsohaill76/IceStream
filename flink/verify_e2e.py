import json
import time
import sys
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS = "icestream-kafka:9092"
INPUT_TOPIC = "ecommerce-transactions"
VALID_TOPIC = "processed-transactions"
DLQ_TOPIC = "transactions-dlq"

print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer_valid = KafkaConsumer(
    VALID_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000,
)

consumer_dlq = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000,
)

# Allow consumer group coordination to settle
time.sleep(2)

print("\n--- STEP 1: Sending Valid Record 1 ---")
rec1 = {
    "transaction_id": "90000001-1111-2222-3333-444455556666",
    "customer_id": "cust_valid_101",
    "amount": 199.99,
    "currency": "USD",
    "timestamp": "2026-10-09T10:00:00Z",
    "merchant": "Amazon",
    "status": "COMPLETED"
}
producer.send(INPUT_TOPIC, rec1)
producer.flush()
print(f"Sent: {rec1['transaction_id']} to {INPUT_TOPIC}")

print(f"Waiting for {rec1['transaction_id']} in {VALID_TOPIC}...")
found_valid_1 = False
start_t = time.time()
while time.time() - start_t < 20:
    records = consumer_valid.poll(timeout_ms=2000)
    for tp, msgs in records.items():
        for msg in msgs:
            print(f"Received in {VALID_TOPIC}: {msg.value}")
            if msg.value.get("transaction_id") == rec1["transaction_id"]:
                found_valid_1 = True
                break
    if found_valid_1:
        break

if not found_valid_1:
    print(f"ERROR: Did not receive {rec1['transaction_id']} in {VALID_TOPIC}")
    sys.exit(1)
print("SUCCESS: Valid Record 1 verified in processed-transactions!")

print("\n--- STEP 2: Sending Invalid Record 2 (Negative Amount) ---")
rec2 = {
    "transaction_id": "90000002-1111-2222-3333-444455556666",
    "customer_id": "cust_invalid_102",
    "amount": -50.00,
    "currency": "USD",
    "timestamp": "2026-10-09T10:01:00Z",
    "merchant": "BestBuy",
    "status": "COMPLETED"
}
producer.send(INPUT_TOPIC, rec2)
producer.flush()
print(f"Sent: {rec2['transaction_id']} to {INPUT_TOPIC}")

print(f"Waiting for {rec2['transaction_id']} in {DLQ_TOPIC}...")
found_dlq = False
start_t = time.time()
while time.time() - start_t < 20:
    records = consumer_dlq.poll(timeout_ms=2000)
    for tp, msgs in records.items():
        for msg in msgs:
            print(f"Received in {DLQ_TOPIC}: {msg.value}")
            raw = msg.value.get("raw_payload", "")
            if rec2["transaction_id"] in str(raw) or msg.value.get("transaction_id") == rec2["transaction_id"]:
                found_dlq = True
                break
    if found_dlq:
        break

if not found_dlq:
    print(f"ERROR: Did not receive {rec2['transaction_id']} in {DLQ_TOPIC}")
    sys.exit(1)
print("SUCCESS: Invalid Record 2 verified in transactions-dlq!")

print("\n--- STEP 3: Sending Valid Record 3 (After Invalid Record) ---")
rec3 = {
    "transaction_id": "90000003-1111-2222-3333-444455556666",
    "customer_id": "cust_valid_103",
    "amount": 450.50,
    "currency": "USD",
    "timestamp": "2026-10-09T10:02:00Z",
    "merchant": "Target",
    "status": "COMPLETED"
}
producer.send(INPUT_TOPIC, rec3)
producer.flush()
print(f"Sent: {rec3['transaction_id']} to {INPUT_TOPIC}")

print(f"Waiting for {rec3['transaction_id']} in {VALID_TOPIC}...")
found_valid_3 = False
start_t = time.time()
while time.time() - start_t < 20:
    records = consumer_valid.poll(timeout_ms=2000)
    for tp, msgs in records.items():
        for msg in msgs:
            print(f"Received in {VALID_TOPIC}: {msg.value}")
            if msg.value.get("transaction_id") == rec3["transaction_id"]:
                found_valid_3 = True
                break
    if found_valid_3:
        break

if not found_valid_3:
    print(f"ERROR: Did not receive {rec3['transaction_id']} in {VALID_TOPIC}")
    sys.exit(1)
print("SUCCESS: Valid Record 3 successfully processed after invalid record!")

print("\n==========================================")
print("ALL END-TO-END VERIFICATION CHECKS PASSED!")
print("==========================================")
