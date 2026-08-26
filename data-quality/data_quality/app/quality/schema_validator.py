# Validate the structure of a transaction
from app.config import REQUIRED_FIELDS


def validate_schema(record):
    errors = []

    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"{field} is missing")

    return len(errors) == 0, errors