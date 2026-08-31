import json
from datetime import datetime, timezone
from pyflink.datastream import KeyedProcessFunction, MapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
from pyflink.common.typeinfo import Types
from pyflink.common.time import Time

REQUIRED_FIELDS = ["transaction_id", "user_id", "amount", "currency", "timestamp"]


def parse_timestamp_to_epoch_ms(ts_val) -> int:
    """Converts ISO-8601 strings or numeric epoch values to epoch milliseconds."""
    if isinstance(ts_val, (int, float)):
        return int(ts_val if ts_val > 1e11 else ts_val * 1000)
    if isinstance(ts_val, str):
        try:
            dt = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    return None


def parse_and_validate_transaction(raw_json_str: str) -> dict:
    """Parses raw JSON string and validates field presence, data types, and value bounds."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Validate JSON formatting
    try:
        data = json.loads(raw_json_str)
    except Exception as e:
        return {
            "status": "INVALID",
            "error_reason": f"Malformed JSON: {str(e)}",
            "raw_payload": raw_json_str,
            "transaction_id": "UNKNOWN",
            "failed_at": now_iso,
        }

    if not isinstance(data, dict):
        return {
            "status": "INVALID",
            "error_reason": "Payload must be a JSON object",
            "raw_payload": raw_json_str,
            "transaction_id": "UNKNOWN",
            "failed_at": now_iso,
        }

    tx_id = str(data.get("transaction_id", "UNKNOWN"))

    # 2. Validate required fields
    missing_fields = [f for f in REQUIRED_FIELDS if f not in data or data[f] is None]
    if missing_fields:
        return {
            "status": "INVALID",
            "error_reason": f"Missing required fields: {', '.join(missing_fields)}",
            "raw_payload": raw_json_str,
            "transaction_id": tx_id,
            "failed_at": now_iso,
        }

    # 3. Validate numeric positive amount
    try:
        amount = float(data["amount"])
        if amount <= 0:
            return {
                "status": "INVALID",
                "error_reason": "Invalid amount: must be positive (> 0)",
                "raw_payload": raw_json_str,
                "transaction_id": tx_id,
                "failed_at": now_iso,
            }
        data["amount"] = amount
    except (ValueError, TypeError):
        return {
            "status": "INVALID",
            "error_reason": "Invalid amount type: must be numeric",
            "raw_payload": raw_json_str,
            "transaction_id": tx_id,
            "failed_at": now_iso,
        }

    # 4. Parse timestamp to Epoch Milliseconds for Watermarking
    epoch_ms = parse_timestamp_to_epoch_ms(data["timestamp"])
    if epoch_ms is None:
        return {
            "status": "INVALID",
            "error_reason": "Invalid timestamp format: expected ISO-8601 string or epoch millis",
            "raw_payload": raw_json_str,
            "transaction_id": tx_id,
            "failed_at": now_iso,
        }

    data["event_timestamp_ms"] = epoch_ms
    data["processed_at"] = now_iso
    return {
        "status": "VALID",
        "payload": data,
        "transaction_id": tx_id,
    }


class ParseAndValidateMapFunction(MapFunction):
    """Flink MapFunction that parses records and tracks validation metrics."""

    def open(self, runtime_context: RuntimeContext):
        metrics_group = runtime_context.get_metrics_group()
        self.error_counter = metrics_group.counter("records_invalid_count")

    def map(self, value: str):
        result = parse_and_validate_transaction(value)
        if result["status"] == "INVALID":
            self.error_counter.inc()
        return json.dumps(result)


class TransactionDeduplicatorFunction(KeyedProcessFunction):
    """Deduplicates transactions by transaction_id using Flink Keyed State with a 24-hour State TTL."""

    def open(self, runtime_context: RuntimeContext):
        ttl_config = (
            StateTtlConfig.new_builder(Time.hours(24))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )

        state_descriptor = ValueStateDescriptor("seen_tx_state", Types.BOOLEAN())
        state_descriptor.enable_time_to_live(ttl_config)
        self.seen_state = runtime_context.get_state(state_descriptor)

        metrics_group = runtime_context.get_metrics_group()
        self.processed_counter = metrics_group.counter("records_processed_count")
        self.duplicate_counter = metrics_group.counter("records_duplicate_count")

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        record = json.loads(value)

        # Pass through previously flagged invalid records straight to DLQ
        if record.get("status") == "INVALID":
            yield json.dumps(record)
            return

        # Check for duplicates in state
        if self.seen_state.value() is True:
            self.duplicate_counter.inc()
            now_iso = datetime.now(timezone.utc).isoformat()
            dlq_record = {
                "status": "INVALID",
                "error_reason": "Duplicate transaction_id detected",
                "raw_payload": json.dumps(record.get("payload")),
                "transaction_id": record.get("transaction_id"),
                "failed_at": now_iso,
            }
            yield json.dumps(dlq_record)
        else:
            self.seen_state.update(True)
            self.processed_counter.inc()
            yield json.dumps(record)