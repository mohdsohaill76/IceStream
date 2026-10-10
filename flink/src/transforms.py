import json
import logging
import math
import uuid
from datetime import datetime

try:
    from pyflink.common import Time
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import RuntimeContext
    from pyflink.datastream.functions import KeyedProcessFunction
    from pyflink.datastream.output_tag import OutputTag
    from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
except ImportError:
    # Lightweight shims for local testing / non-PyFlink execution environments
    class Time:
        @staticmethod
        def hours(h: int):
            return h * 3600

    class Types:
        @staticmethod
        def STRING():
            return "STRING"

        @staticmethod
        def BOOLEAN():
            return "BOOLEAN"

    class RuntimeContext:
        pass

    class KeyedProcessFunction:
        class Context:
            pass

    class OutputTag:
        def __init__(self, tag_id: str, type_info=None):
            self.tag_id = tag_id
            self.type_info = type_info

    class ValueStateDescriptor:
        def __init__(self, name: str, type_info):
            self.name = name
            self.type_info = type_info
            self.ttl_config = None

        def enable_time_to_live(self, ttl_config):
            self.ttl_config = ttl_config

    class StateTtlConfig:
        class UpdateType:
            OnCreateAndWrite = "OnCreateAndWrite"

        class StateVisibility:
            NeverReturnExpired = "NeverReturnExpired"

        class Builder:
            def __init__(self, time_val):
                self.time_val = time_val
                self.update_type = None
                self.state_visibility = None

            def set_update_type(self, update_type):
                self.update_type = update_type
                return self

            def set_state_visibility(self, state_visibility):
                self.state_visibility = state_visibility
                return self

            def build(self):
                return self

        @staticmethod
        def new_builder(time_val):
            return StateTtlConfig.Builder(time_val)

logger = logging.getLogger("FlinkTransforms")

VALID_OUTPUT_TAG = OutputTag("valid-transactions", Types.STRING())
DLQ_OUTPUT_TAG = OutputTag("dlq-transactions", Types.STRING())

CANONICAL_SCHEMA_FIELDS = {
    "transaction_id": str,
    "customer_id": str,
    "amount": (int, float),
    "currency": str,
    "timestamp": str,
    "merchant": str,
    "status": str,
}

ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "CAD", "AUD", "JPY", "INR"}
# Supports Mahek's generator ("SUCCESS", "PENDING", "FAILED") and legacy ("COMPLETED", "CANCELLED")
ALLOWED_STATUSES = {"SUCCESS", "COMPLETED", "PENDING", "FAILED", "CANCELLED"}


def validate_transaction_schema(data: dict) -> tuple[bool, str, dict]:
    """
    Validates canonical fields, checks enums, rejects boolean amounts,
    verifies ISO-8601 timestamps, checks finiteness, and constructs a clean canonical record.
    """
    # 1. Reject any legacy user_id to prevent leakage
    if "user_id" in data:
        return False, "Schema Validation Failure: Legacy 'user_id' field is forbidden in canonical stream", {}

    # 2. Check for missing or null required canonical fields (catches NULL_INJECTION & MISSING_FIELD)
    missing_or_null = [f for f in CANONICAL_SCHEMA_FIELDS if f not in data or data[f] is None]
    if missing_or_null:
        return False, f"Schema Validation Failure: Missing or null required fields {missing_or_null}", {}

    # 3. Reject unexpected fields (catches SCHEMA_CHANGE_RATE: 'unexpected_field')
    unexpected_fields = [k for k in data.keys() if k not in CANONICAL_SCHEMA_FIELDS]
    if unexpected_fields:
        return False, f"Schema Validation Failure: Unexpected fields detected {unexpected_fields}", {}

    # 4. Data Type Checks & Reject Boolean amounts (bool inherits from int in Python)
    for field, expected_type in CANONICAL_SCHEMA_FIELDS.items():
        val = data[field]
        if field == "amount" and isinstance(val, bool):
            return False, "Invalid transaction amount: Boolean values (True/False) are not allowed", {}
        if not isinstance(val, expected_type):
            return False, f"Schema Validation Failure: Field '{field}' expected {expected_type}, got {type(val).__name__}", {}

    # 5. String sanity: Catch empty/whitespace strings (catches customer_id="" and merchant="")
    for str_field in ["transaction_id", "customer_id", "merchant"]:
        if not str(data[str_field]).strip():
            return False, f"Schema Validation Failure: Field '{str_field}' cannot be empty or whitespace", {}

    # Strict UUID validation for transaction_id
    raw_tx_id = str(data["transaction_id"]).strip()
    try:
        uuid.UUID(raw_tx_id)
    except Exception:
        return False, f"Schema Validation Failure: Field 'transaction_id' must be a valid UUID, got '{raw_tx_id}'", {}

    # 6. Numeric checks: Reject NaN, Infinity, zero, and negative amounts
    amount = data["amount"]
    if not math.isfinite(amount):
        return False, f"Invalid transaction amount: Non-finite values (NaN/Infinity) are not allowed, got {amount}", {}

    if amount <= 0:
        return False, f"Invalid transaction amount: {amount} (must be positive)", {}

    # 7. Currency and Status validations
    currency = str(data["currency"]).strip().upper()
    if currency not in ALLOWED_CURRENCIES:
        return False, f"Invalid currency code: '{data['currency']}' (allowed: {sorted(list(ALLOWED_CURRENCIES))})", {}

    status = str(data["status"]).strip().upper()
    if status not in ALLOWED_STATUSES:
        return False, f"Invalid status: '{data['status']}' (allowed: {sorted(list(ALLOWED_STATUSES))})", {}

    # 8. Strict ISO-8601 Timestamp Validation (catches timestamp='not-a-date')
    raw_ts = str(data["timestamp"]).strip()
    try:
        datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
    except Exception as e:
        return False, f"Invalid timestamp format '{raw_ts}': {str(e)}", {}

    # 9. Build strictly canonical payload
    clean_record = {
        "transaction_id": str(data["transaction_id"]).strip(),
        "customer_id": str(data["customer_id"]).strip(),
        "amount": float(amount),
        "currency": currency,
        "timestamp": raw_ts,
        "merchant": str(data["merchant"]).strip(),
        "status": status,
    }

    return True, "", clean_record


class TransactionValidationAndDeduplicationFunction(KeyedProcessFunction):
    def __init__(self):
        self.seen_state = None

    def open(self, runtime_context: RuntimeContext):
        # 24-hour State TTL to prevent unbounded state growth
        ttl_config = (
            StateTtlConfig.new_builder(Time.hours(24))
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        state_desc = ValueStateDescriptor("seen_tx_ids", Types.BOOLEAN())
        state_desc.enable_time_to_live(ttl_config)
        self.seen_state = runtime_context.get_state(state_desc)

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        # 1. Parse JSON (Catches MALFORMED_JSON_RATE)
        try:
            data = json.loads(value)
        except Exception as e:
            logger.warning(f"Failed to parse payload: {value}. Error: {str(e)}")
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": f"Malformed JSON: {str(e)}"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        if not isinstance(data, dict):
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": "Invalid JSON root: Expected a JSON object"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        tx_id = str(data.get("transaction_id", "")).strip() or "UNKNOWN"

        # 2. Keyed State Deduplication (Catches DUPLICATE_RATE)
        if self.seen_state.value():
            logger.info(f"Duplicate transaction detected for ID: {tx_id}")
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": f"Duplicate transaction_id: {tx_id}"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # 3. Strict Schema & Contract Validation
        is_valid, error_reason, clean_record = validate_transaction_schema(data)
        if not is_valid:
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": error_reason
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # 4. Mark Transaction Key as Seen in State
        self.seen_state.update(True)

        # 5. Route strictly clean canonical Record
        yield VALID_OUTPUT_TAG, json.dumps(clean_record)
