from app.config import REQUIRED_FIELDS


def validate_record(record):
    errors = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"{field} is missing")

    # Check that IDs are not empty
    for field in ["transaction_id", "customer_id"]:
        if field in record and record[field] == "":
            errors.append(f"{field} cannot be empty")

    # Check quantity
    if "quantity" in record and record["quantity"] is not None:
        if not isinstance(record["quantity"], (int, float)):
            errors.append("quantity must be a number")
        elif record["quantity"] <= 0:
            errors.append("quantity must be greater than 0")

    # Check amount
    if "amount" in record and record["amount"] is not None:
        if not isinstance(record["amount"], (int, float)):
            errors.append("amount must be a number")
        elif record["amount"] <= 0:
            errors.append("amount must be greater than 0")

    # Check tax
    if "tax" in record and record["tax"] is not None:
        if not isinstance(record["tax"], (int, float)):
            errors.append("tax must be a number")
        elif record["tax"] < 0:
            errors.append("tax cannot be negative")

    return len(errors) == 0, errors
