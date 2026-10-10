# Check transaction fields for NULL values
from app.config import REQUIRED_FIELDS


def validate_nulls(record):
    errors = []

    # Check required fields for NULL values
    for field in REQUIRED_FIELDS:
        if field in record and record[field] is None:
            errors.append(f"{field} is null")

    return len(errors) == 0, errors