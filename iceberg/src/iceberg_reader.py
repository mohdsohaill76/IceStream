import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from iceberg.catalog.catalog_manager import (
    CLEAN_TABLE_NAME,
    DLQ_TABLE_NAME,
    init_catalog_and_tables,
)

logger = logging.getLogger("iceberg.reader")


class IcebergReader:
    """Provides scan and query utilities for Iceberg clean and DLQ tables."""

    def __init__(
        self,
        warehouse_dir: Optional[str | Path] = None,
        catalog_db: Optional[str | Path] = None,
    ):
        self.catalog, self.clean_table, self.dlq_table = init_catalog_and_tables(
            warehouse_dir=warehouse_dir, catalog_db=catalog_db
        )

    def refresh(self) -> None:
        """Refresh table metadata to view the latest committed snapshots."""
        try:
            self.clean_table.refresh()
        except Exception:
            pass
        try:
            self.dlq_table.refresh()
        except Exception:
            pass

    def get_clean_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch records from lakehouse.clean_transactions."""
        try:
            self.clean_table.refresh()
            arrow_table = self.clean_table.scan().to_arrow()
            if arrow_table.num_rows == 0:
                return []
            records = arrow_table.to_pylist()
            return records[:limit]
        except Exception as e:
            logger.warning(f"Failed to scan clean transactions: {e}")
            return []

    def get_dlq_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch records from lakehouse.dlq_transactions."""
        try:
            self.dlq_table.refresh()
            arrow_table = self.dlq_table.scan().to_arrow()
            if arrow_table.num_rows == 0:
                return []
            records = arrow_table.to_pylist()
            return records[:limit]
        except Exception as e:
            logger.warning(f"Failed to scan dlq transactions: {e}")
            return []

    def get_metrics(self, raise_on_error: bool = False) -> Dict[str, Any]:
        """Return counts and snapshot statistics for lakehouse tables.

        Args:
            raise_on_error: If True, re-raises any caught exceptions during
                table refresh or scan, avoiding silent zero reporting.
        """
        clean_count = 0
        clean_snapshot_id = None
        dlq_count = 0
        dlq_snapshot_id = None

        try:
            self.clean_table.refresh()
            clean_arrow = self.clean_table.scan().to_arrow()
            clean_count = clean_arrow.num_rows
            clean_snap = self.clean_table.current_snapshot()
            clean_snapshot_id = clean_snap.snapshot_id if clean_snap else None
        except Exception as e:
            logger.warning(f"Failed to scan clean transactions: {e}")
            if raise_on_error:
                raise

        try:
            self.dlq_table.refresh()
            dlq_arrow = self.dlq_table.scan().to_arrow()
            dlq_count = dlq_arrow.num_rows
            dlq_snap = self.dlq_table.current_snapshot()
            dlq_snapshot_id = dlq_snap.snapshot_id if dlq_snap else None
        except Exception as e:
            logger.warning(f"Failed to scan dlq transactions: {e}")
            if raise_on_error:
                raise

        return {
            "clean_transactions_count": clean_count,
            "clean_snapshot_id": clean_snapshot_id,
            "dlq_transactions_count": dlq_count,
            "dlq_snapshot_id": dlq_snapshot_id,
        }

    def get_snapshots(
        self,
        table_name: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> List[Dict[str, Any]]:
        """Fetch snapshot commit history for clean and/or DLQ tables.

        Args:
            table_name: Optional filter for 'clean_transactions' or 'dlq_transactions'.
            raise_on_error: If True, re-raises any encountered catalog/table errors.

        Returns:
            List of snapshot metadata dictionaries sorted newest-first.
        """
        targets: List[tuple[str, Any]] = []
        if table_name:
            norm = table_name.lower().strip()
            if "clean" in norm:
                targets.append((CLEAN_TABLE_NAME, self.clean_table))
            elif "dlq" in norm:
                targets.append((DLQ_TABLE_NAME, self.dlq_table))
            else:
                raise ValueError(
                    f"Unknown table '{table_name}'. Supported tables: "
                    f"'{CLEAN_TABLE_NAME}', '{DLQ_TABLE_NAME}'"
                )
        else:
            targets = [
                (CLEAN_TABLE_NAME, self.clean_table),
                (DLQ_TABLE_NAME, self.dlq_table),
            ]

        results: List[Dict[str, Any]] = []

        for tbl_name, tbl in targets:
            try:
                tbl.refresh()
                table_snapshots = tbl.snapshots()
                for snap in table_snapshots:
                    ts_ms = snap.timestamp_ms
                    committed_at = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

                    operation = None
                    if hasattr(snap, "summary") and snap.summary:
                        raw_op = getattr(snap.summary, "operation", None)
                        if hasattr(raw_op, "value"):
                            operation = raw_op.value
                        elif raw_op:
                            operation = str(raw_op)

                    summary_dict: Dict[str, Any] = {}
                    if hasattr(snap, "summary") and snap.summary:
                        if hasattr(snap.summary, "model_dump"):
                            summary_dict = snap.summary.model_dump()
                        elif hasattr(snap.summary, "dict"):
                            summary_dict = snap.summary.dict()

                    parent_id = snap.parent_snapshot_id if snap.parent_snapshot_id is not None else None

                    results.append({
                        "snapshot_id": snap.snapshot_id,
                        "table_name": tbl_name,
                        "committed_at": committed_at,
                        "timestamp_ms": ts_ms,
                        "operation": operation,
                        "summary": summary_dict,
                        "parent_snapshot_id": parent_id,
                    })
            except Exception as e:
                logger.warning(f"Failed to retrieve snapshots for {tbl_name}: {e}")
                if raise_on_error:
                    raise

        results.sort(key=lambda item: item["timestamp_ms"], reverse=True)
        return results
