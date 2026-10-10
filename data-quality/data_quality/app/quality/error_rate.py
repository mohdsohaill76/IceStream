# Calculate the percentage of bad records

def calculate_error_rate(bad_records, total_records):
    # Avoid division by zero
    if total_records == 0:
        return 0.0

    error_rate = (bad_records / total_records) * 100

    return error_rate