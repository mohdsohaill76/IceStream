from datetime import datetime
from app.config import REQUIRED_FIELDS

def validate_record(record):
    errors = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"{field} is missing")

    # Check ID type and empty values
    for field in ["transaction_id", "customer_id"]:
        if field in record and record[field] is not None:
            if not isinstance(record[field], str):
                errors.append(f"{field} must be a string")
            elif not record[field].strip():
                errors.append(f"{field} cannot be empty")

    # Check amount
    if "amount" in record and record["amount"] is not None:
        if not isinstance(record["amount"], (int, float)):
            errors.append("amount must be a number")
        elif record["amount"] <= 0:
            errors.append("amount must be greater than 0")

    # Check currency
    if "currency" in record and record["currency"] is not None:
       if not isinstance(record["currency"], str) or record["currency"] != "INR":
        errors.append("currency must be INR")

    # Check merchant
    if "merchant" in record and record["merchant"] is not None:
        if not isinstance(record["merchant"], str) or record["merchant"] == "":
            errors.append("merchant cannot be empty")

    # Check status
    if "status" in record and record["status"] is not None:
        if record["status"] not in ["SUCCESS", "PENDING", "FAILED"]:
            errors.append("status is invalid")

    # Check timestamp
    if "timestamp" in record and record["timestamp"] is not None:
        try:
            datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            errors.append("timestamp is invalid")

    return len(errors) == 0, errors