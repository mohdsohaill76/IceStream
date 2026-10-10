import os
from pathlib import Path
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    DoubleType,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_WAREHOUSE_DIR = BASE_DIR / "warehouse"
DEFAULT_CATALOG_DB = DEFAULT_WAREHOUSE_DIR / "iceberg_catalog.db"

CLEAN_TABLE_NAME = "lakehouse.clean_transactions"
DLQ_TABLE_NAME = "lakehouse.dlq_transactions"

# Canonical transaction schema matching contracts
CLEAN_TRANSACTIONS_SCHEMA = Schema(
    NestedField(field_id=1, name="transaction_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="customer_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="amount", field_type=DoubleType(), required=True),
    NestedField(field_id=4, name="currency", field_type=StringType(), required=True),
    NestedField(field_id=5, name="timestamp", field_type=StringType(), required=True),
    NestedField(field_id=6, name="merchant", field_type=StringType(), required=True),
    NestedField(field_id=7, name="status", field_type=StringType(), required=True),
)

# DLQ transactions schema
DLQ_TRANSACTIONS_SCHEMA = Schema(
    NestedField(field_id=1, name="transaction_id", field_type=StringType(), required=False),
    NestedField(field_id=2, name="raw_payload", field_type=StringType(), required=False),
    NestedField(field_id=3, name="error_reason", field_type=StringType(), required=False),
    NestedField(field_id=4, name="source_stage", field_type=StringType(), required=False),
    NestedField(field_id=5, name="timestamp", field_type=StringType(), required=False),
)


def get_warehouse_path(warehouse_dir: str | Path | None = None) -> str:
    path = Path(warehouse_dir) if warehouse_dir else DEFAULT_WAREHOUSE_DIR
    path.mkdir(parents=True, exist_ok=True)
    return str(path.resolve()).replace("\\", "/")


def get_catalog_uri(catalog_db: str | Path | None = None) -> str:
    path = Path(catalog_db) if catalog_db else DEFAULT_CATALOG_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{str(path.resolve()).replace(chr(92), '/')}"


def get_catalog(
    warehouse_dir: str | Path | None = None,
    catalog_db: str | Path | None = None,
    catalog_name: str = "default",
):
    """Load or connect to the SQLite-backed PyIceberg catalog."""
    wh_path = get_warehouse_path(warehouse_dir)
    uri = get_catalog_uri(catalog_db)

    catalog = load_catalog(
        catalog_name,
        **{
            "type": "sql",
            "uri": uri,
            "warehouse": wh_path,
        },
    )
    return catalog


def init_catalog_and_tables(
    warehouse_dir: str | Path | None = None,
    catalog_db: str | Path | None = None,
):
    """Initialize catalog, create 'lakehouse' namespace and tables if not already present."""
    catalog = get_catalog(warehouse_dir=warehouse_dir, catalog_db=catalog_db)

    # Ensure lakehouse namespace exists
    try:
        catalog.create_namespace("lakehouse")
    except Exception:
        pass

    # Ensure clean_transactions table exists
    try:
        clean_table = catalog.load_table(CLEAN_TABLE_NAME)
    except Exception:
        clean_table = catalog.create_table(
            CLEAN_TABLE_NAME,
            schema=CLEAN_TRANSACTIONS_SCHEMA,
        )

    # Ensure dlq_transactions table exists
    try:
        dlq_table = catalog.load_table(DLQ_TABLE_NAME)
    except Exception:
        dlq_table = catalog.create_table(
            DLQ_TABLE_NAME,
            schema=DLQ_TRANSACTIONS_SCHEMA,
        )

    return catalog, clean_table, dlq_table
