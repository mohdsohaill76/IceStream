# Validate transaction values
import math

def validate_values(record):
    errors = []

    # Check amount
    if "amount" in record and record["amount"] is not None:
        if isinstance(record["amount"], bool):
            errors.append("amount must be a number")
        elif not isinstance(record["amount"], (int, float)):
            errors.append("amount must be a number")
        elif not math.isfinite(record["amount"]):
            errors.append("amount must be finite")
        elif record["amount"] <= 0:
            errors.append("amount must be greater than 0")

    return len(errors) == 0, errors