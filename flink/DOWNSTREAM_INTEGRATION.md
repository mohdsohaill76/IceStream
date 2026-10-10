# IceStream Lineage & Downstream Integration Contract

## Stream Pipeline Architecture

1. **Upstream Producer (Mahek - Data Generator):**
   - **Topic:** `ecommerce-transactions`
   - **Contract Schema (Canonical 7-Field Specification):**
     ```json
     {
       "transaction_id": "string",
       "customer_id": "string",
       "amount": "positive finite float (non-boolean, non-NaN, non-inf)",
       "currency": "string (ISO-4217: USD, EUR, GBP, CAD, AUD, JPY, INR)",
       "timestamp": "ISO-8601 UTC string (YYYY-MM-DDTHH:MM:SSZ)",
       "merchant": "string (non-empty)",
       "status": "string enum (COMPLETED, PENDING, FAILED, CANCELLED)"
     }
     ```

2. **Stream Processing (Flink Engine):**
   - **Consumer Group:** `flink_ecommerce_group`
   - **Ingestion Topic:** `ecommerce-transactions`
   - **State Backend:** `FileSystemCheckpointStorage` (Configurable via `FLINK_CHECKPOINT_DIR`, defaults to Linux/WSL `/tmp/flink-checkpoints` or Windows temp)
   - **Outputs (Side Outputs):**
     - **Valid Output Topic:** `processed-transactions`
     - **DLQ Output Topic:** `transactions-dlq` (Unified pipeline DLQ)

3. **Downstream Processing (Harsh - Data Quality / Iceberg Ingestion):**
   - **Target Consumption Topic:** `processed-transactions` (Validated, deduplicated, canonical 7-field stream)
   - **DLQ Consumption Topic:** `transactions-dlq` (Audit logs, anomaly rejection, and schema drift inspection)

---

## Canonical Output Schema (`processed-transactions`)

All records emitted by the Flink streaming consumer conform strictly to the 7-field canonical schema. Downstream consumers (Data Quality checks and Iceberg sink) can rely on these invariant types and constraints:

| Field Name | Type | Constraints & Verification | Example |
|---|---|---|---|
| `transaction_id` | `String (UUID)` | Valid UUID format, non-empty, unique within 24h state window | `"550e8400-e29b-41d4-a716-446655440000"` |
| `customer_id` | `String` | Non-empty, non-whitespace. Incoming `user_id` is forbidden and rejected | `"CUST-1001"` |
| `amount` | `Float` | Strict finite positive number (`math.isfinite(x) and x > 0`). Booleans, `NaN`, and `+/-Infinity` rejected | `149.95` |
| `currency` | `String` | Uppercase ISO-4217 standard (`USD`, `EUR`, `GBP`, `CAD`, `AUD`, `JPY`, `INR`) | `"USD"` |
| `timestamp` | `String` | Strict ISO-8601 format (`YYYY-MM-DDTHH:MM:SSZ`), zero arrival-time fallbacks | `"2026-09-18T12:00:00Z"` |
| `merchant` | `String` | Non-empty, non-whitespace merchant/store identifier | `"Amazon"` |
| `status` | `String` | Enumerated string: `SUCCESS`, `COMPLETED`, `PENDING`, `FAILED`, `CANCELLED` | `"SUCCESS"` |

*Note: Any transaction carrying unexpected non-canonical properties is rejected directly to the DLQ to prevent downstream schema drift.*

---

## Dead-Letter Queue (DLQ) Contract (`transactions-dlq`)

Any payload violating contract types, missing required canonical attributes, carrying legacy fields, or arriving as a duplicate transaction within 24 hours is routed to `transactions-dlq` with the following envelope:

```json
{
  "raw_payload": "{\"transaction_id\": \"550e8400-e29b-41d4-a716-446655440000\", ...}",
  "error_reason": "Schema Validation Failure: Legacy 'user_id' field is forbidden in canonical stream"
}