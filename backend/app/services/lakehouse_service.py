import logging
from pathlib import Path
import sys
from typing import List, Optional

from app.models.lakehouse import LakehouseMetrics, SnapshotMetadata

logger = logging.getLogger("backend.lakehouse_service")


def _ensure_repo_root_in_path() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


class LakehouseServiceError(Exception):
    """Raised when the Lakehouse catalog or tables cannot be accessed."""

    pass


# Optional cached reader instance for application lifecycle
_READER_OVERRIDE: Optional[object] = None


def set_reader_override(reader: Optional[object]) -> None:
    """Set or clear a reader override for testing or dependency injection."""
    global _READER_OVERRIDE
    _READER_OVERRIDE = reader


def get_reader(
    warehouse_dir: Optional[str | Path] = None,
    catalog_db: Optional[str | Path] = None,
):
    """Obtain an IcebergReader instance, using override if present."""
    if _READER_OVERRIDE is not None:
        return _READER_OVERRIDE

    _ensure_repo_root_in_path()
    try:
        from iceberg.src.iceberg_reader import IcebergReader

        return IcebergReader(warehouse_dir=warehouse_dir, catalog_db=catalog_db)
    except Exception as e:
        logger.error(f"Failed to initialize IcebergReader: {e}")
        raise LakehouseServiceError(f"Could not connect to Iceberg lakehouse catalog: {e}") from e


def get_lakehouse_metrics(reader: Optional[object] = None) -> LakehouseMetrics:
    """Fetch current row counts and snapshot IDs for clean and DLQ tables."""
    active_reader = reader if reader is not None else get_reader()

    try:
        raw_metrics = active_reader.get_metrics(raise_on_error=True)
        return LakehouseMetrics(
            clean_transactions_count=raw_metrics["clean_transactions_count"],
            clean_snapshot_id=raw_metrics["clean_snapshot_id"],
            dlq_transactions_count=raw_metrics["dlq_transactions_count"],
            dlq_snapshot_id=raw_metrics["dlq_snapshot_id"],
        )
    except LakehouseServiceError:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve Lakehouse metrics: {e}")
        raise LakehouseServiceError(f"Failed to retrieve Lakehouse metrics: {e}") from e


def get_lakehouse_snapshots(
    table: Optional[str] = None,
    reader: Optional[object] = None,
) -> List[SnapshotMetadata]:
    """Fetch historical commit snapshots for clean and/or DLQ tables."""
    active_reader = reader if reader is not None else get_reader()

    try:
        raw_snapshots = active_reader.get_snapshots(table_name=table, raise_on_error=True)
        return [SnapshotMetadata(**snap) for snap in raw_snapshots]
    except ValueError:
        # Invalid table filter parameter — let caller handle as 400
        raise
    except LakehouseServiceError:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve Lakehouse snapshots: {e}")
        raise LakehouseServiceError(f"Failed to retrieve Lakehouse snapshots: {e}") from e
