import json
import logging
from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

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


def validate_transaction_schema(data: dict) -> tuple[bool, str]:
    # 1. Reject legacy user_id without customer_id (No silent conversion)
    if "user_id" in data and "customer_id" not in data:
        return False, "Schema Validation Failure: Legacy 'user_id' provided without required 'customer_id'"

    # 2. Check for missing required fields from canonical contract
    missing_fields = [f for f in CANONICAL_SCHEMA_FIELDS if f not in data or data[f] is None]
    if missing_fields:
        return False, f"Schema Validation Failure: Missing required fields {missing_fields}"

    # 3. Check for unexpected fields according to agreed contract
    unexpected_fields = [k for k in data if k not in CANONICAL_SCHEMA_FIELDS and k != "user_id"]
    if unexpected_fields:
        return False, f"Schema Validation Failure: Unexpected fields detected {unexpected_fields}"

    # 4. Check data types
    for field, expected_type in CANONICAL_SCHEMA_FIELDS.items():
        if not isinstance(data[field], expected_type):
            return False, f"Schema Validation Failure: Field '{field}' expected {expected_type}, got {type(data[field]).__name__}"

    # 5. Check business rules (amount must be positive)
    if data["amount"] <= 0:
        return False, f"Invalid transaction amount: {data['amount']} (must be positive)"

    return True, ""


class TransactionValidationAndDeduplicationFunction(KeyedProcessFunction):
    def __init__(self):
        self.seen_state = None

    def open(self, runtime_context: RuntimeContext):
        # 24-hour State TTL to prevent unbounded state memory growth
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
        # 1. Parse JSON Payload
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

        tx_id = data.get("transaction_id") or "UNKNOWN"

        # 2. Keyed State Deduplication Check
        if self.seen_state.value():
            logger.info(f"Duplicate transaction detected for ID: {tx_id}")
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": f"Duplicate transaction_id: {tx_id}"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # 3. Strict Schema & Contract Validation
        is_valid, error_reason = validate_transaction_schema(data)
        if not is_valid:
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": error_reason
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # 4. Mark Transaction Key as Seen in State
        self.seen_state.update(True)

        # 5. Route Valid Record
        yield VALID_OUTPUT_TAG, value