import json
import logging
from pyflink.common.typeinfo import Types
from pyflink.datastream import RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import ValueStateDescriptor

logger = logging.getLogger("FlinkTransforms")

VALID_OUTPUT_TAG = OutputTag("valid-transactions", Types.STRING())
DLQ_OUTPUT_TAG = OutputTag("dlq-transactions", Types.STRING())


class TransactionValidationAndDeduplicationFunction(KeyedProcessFunction):
    def __init__(self):
        self.seen_state = None

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor("seen_tx_ids", Types.BOOLEAN())
        self.seen_state = runtime_context.get_state(state_desc)

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        # 1. Parse JSON Payload
        try:
            data = json.loads(value)
        except Exception as e:
            logger.warning(f"Failed to parse payload: {value}. Error: {str(e)}")
            dlq_payload = json.dumps({
                "raw_payload": value,
                "error_reason": f"JSON Deserialization Error: {str(e)}"
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
                "raw_payload": value,  # FIXED: Preserves exact raw string, preventing double-encoding
                "error_reason": f"Duplicate transaction_id: {tx_id}"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # 3. Data Integrity & Schema Validation
        required_fields = ["transaction_id", "user_id", "amount", "currency", "timestamp"]
        missing_fields = [f for f in required_fields if f not in data or data[f] is None]

        if missing_fields:
            dlq_payload = json.dumps({
                "raw_payload": value,  # FIXED
                "error_reason": f"Schema Validation Failure: Missing fields {missing_fields}"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # Amount numeric validation
        try:
            amount = float(data["amount"])
            if amount <= 0:
                dlq_payload = json.dumps({
                    "raw_payload": value,  # FIXED
                    "error_reason": f"Invalid transaction amount: {amount} (Must be > 0)"
                })
                yield DLQ_OUTPUT_TAG, dlq_payload
                return
        except (ValueError, TypeError):
            dlq_payload = json.dumps({
                "raw_payload": value,  # FIXED
                "error_reason": f"Invalid amount type: {data.get('amount')}"
            })
            yield DLQ_OUTPUT_TAG, dlq_payload
            return

        # 4. Mark Transaction Key as Seen in State
        self.seen_state.update(True)

        # 5. Route Valid Record
        yield VALID_OUTPUT_TAG, value