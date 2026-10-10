import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table

from iceberg.catalog.catalog_manager import (
    CLEAN_TABLE_NAME,
    DLQ_TABLE_NAME,
    get_catalog,
    init_catalog_and_tables,
)

logger = logging.getLogger("iceberg.writer")


class IcebergWriter:
    """Manages appending validated transactions and DLQ records to Apache Iceberg tables."""

    def __init__(
        self,
        warehouse_dir: Optional[str | Path] = None,
        catalog_db: Optional[str | Path] = None,
    ):
        self.catalog, self.clean_table, self.dlq_table = init_catalog_and_tables(
            warehouse_dir=warehouse_dir, catalog_db=catalog_db
        )
        self.clean_pa_schema = schema_to_pyarrow(self.clean_table.schema())
        self.dlq_pa_schema = schema_to_pyarrow(self.dlq_table.schema())
        self._written_tx_ids: Set[str] = set()
        self._load_existing_tx_ids()

    def _load_existing_tx_ids(self) -> None:
        """Cache existing transaction IDs from Iceberg metadata to guarantee restart-safe idempotency."""
        try:
            self.clean_table.refresh()
            arrow_table = self.clean_table.scan().to_arrow()
            if arrow_table.num_rows > 0:
                tx_col = arrow_table.column("transaction_id").to_pylist()
                self._written_tx_ids.update(tx_col)
        except Exception as e:
            logger.debug(f"Snapshot scan during init: {e}")

    def write_clean_records(self, records: List[Dict[str, Any]]) -> int:
        """Write canonical valid transactions to lakehouse.clean_transactions.

        Enforces restart-safe idempotency and schema compatibility.
        """
        if not records:
            return 0

        # Refresh existing IDs to guard against concurrent writers
        self._load_existing_tx_ids()

        filtered_records = []
        new_tx_ids = []
        for r in records:
            tx_id = str(r.get("transaction_id", ""))
            if not tx_id:
                continue
            # Idempotency check: avoid duplicate appends on retry
            if tx_id in self._written_tx_ids or tx_id in new_tx_ids:
                logger.info(f"Skipping duplicate transaction_id: {tx_id}")
                continue

            filtered_records.append({
                "transaction_id": str(r["transaction_id"]),
                "customer_id": str(r["customer_id"]),
                "amount": float(r["amount"]),
                "currency": str(r["currency"]),
                "timestamp": str(r["timestamp"]),
                "merchant": str(r["merchant"]),
                "status": str(r["status"]),
            })
            new_tx_ids.append(tx_id)

        if not filtered_records:
            return 0

        pydict = {
            "transaction_id": [r["transaction_id"] for r in filtered_records],
            "customer_id": [r["customer_id"] for r in filtered_records],
            "amount": [r["amount"] for r in filtered_records],
            "currency": [r["currency"] for r in filtered_records],
            "timestamp": [r["timestamp"] for r in filtered_records],
            "merchant": [r["merchant"] for r in filtered_records],
            "status": [r["status"] for r in filtered_records],
        }

        pa_table = pa.Table.from_pydict(pydict, schema=self.clean_pa_schema)
        self.clean_table.append(pa_table)
        # Only mark as written once durable append succeeds
        self._written_tx_ids.update(new_tx_ids)
        logger.info(f"Successfully appended {len(filtered_records)} records to {CLEAN_TABLE_NAME}")
        return len(filtered_records)


    def write_dlq_records(self, records: List[Dict[str, Any]], source_stage: str = "unknown") -> int:
        """Write Dead Letter Queue entries to lakehouse.dlq_transactions.

        Handles both Flink schema (raw_payload, error_reason) and DQ schema (record, errors).
        """
        if not records:
            return 0

        normalized = []
        now_str = datetime.now(timezone.utc).isoformat()

        for r in records:
            tx_id = None
            raw_payload = None
            error_reason = None
            stage = source_stage

            if "raw_payload" in r and "error_reason" in r:
                # Flink DLQ format
                raw_payload = str(r["raw_payload"])
                error_reason = str(r["error_reason"])
                stage = "flink"
                try:
                    parsed = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
                    if isinstance(parsed, dict):
                        tx_id = parsed.get("transaction_id")
                except Exception:
                    pass
            elif "record" in r and "errors" in r:
                # Data Quality DLQ format
                inner_record = r["record"]
                errors = r["errors"]
                stage = "data_quality"
                raw_payload = json.dumps(inner_record) if isinstance(inner_record, dict) else str(inner_record)
                error_reason = "; ".join(errors) if isinstance(errors, list) else str(errors)
                if isinstance(inner_record, dict):
                    tx_id = inner_record.get("transaction_id")
            else:
                raw_payload = json.dumps(r)
                error_reason = "Unrecognized DLQ structure"
                if isinstance(r, dict):
                    tx_id = r.get("transaction_id")

            normalized.append({
                "transaction_id": str(tx_id) if tx_id else "UNKNOWN",
                "raw_payload": str(raw_payload) if raw_payload else "",
                "error_reason": str(error_reason) if error_reason else "Unknown error",
                "source_stage": stage,
                "timestamp": now_str,
            })

        pydict = {
            "transaction_id": [n["transaction_id"] for n in normalized],
            "raw_payload": [n["raw_payload"] for n in normalized],
            "error_reason": [n["error_reason"] for n in normalized],
            "source_stage": [n["source_stage"] for n in normalized],
            "timestamp": [n["timestamp"] for n in normalized],
        }

        pa_table = pa.Table.from_pydict(pydict, schema=self.dlq_pa_schema)
        self.dlq_table.append(pa_table)
        logger.info(f"Successfully appended {len(normalized)} records to {DLQ_TABLE_NAME}")
        return len(normalized)
