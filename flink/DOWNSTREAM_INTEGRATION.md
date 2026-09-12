# IceStream Lineage & Downstream Integration Contract

## Stream Pipeline Architecture
1. **Upstream Producer (Mahek - Data Generator):**
   - **Topic:** `ecommerce-transactions`
   - **Contract Schema:**
     ```json
     {
       "transaction_id": "string",
       "customer_id": "string",
       "amount": float,
       "currency": "string",
       "timestamp": "ISO-8601 string"
     }
     ```

2. **Stream Processing (Flink Engine):**
   - **Consumer Group:** `flink_ecommerce_group`
   - **Ingestion Topic:** `ecommerce-transactions`
   - **State Backend:** FileSystemCheckpointStorage (`file:///tmp/flink-checkpoints`)
   - **Outputs (Side Outputs):**
     - **Valid Output Topic:** `processed-transactions`
     - **DLQ Output Topic:** `transactions-dlq`

3. **Downstream Processing (Harsh - Data Quality / Iceberg Ingestion):**
   - **Target Consumption Topic:** `processed-transactions` (Validated & Deduplicated stream)
   - **DLQ Topic:** `transactions-dlq` (Audit logs & error analysis)