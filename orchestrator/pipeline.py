"""Main ETL pipeline: extract -> transform -> load for a single QBO client."""

import json
import logging
from pathlib import Path

from config.settings import Settings
from config.tables import ENTITY_TO_SCHEMA_MAP
from auth.oauth_manager import OAuthManager
from extract.qbo_client import QBOClient
from extract.entity_extractor import extract_all_entities
from extract.report_extractor import extract_all_reports, flatten_report_rows
from transform.flatteners import (
    flatten_invoice_lines,
    flatten_bill_lines,
    flatten_purchase_lines,
    flatten_payment_lines,
    flatten_estimate_lines,
)
from transform.schema_mapper import map_to_schema
from transform.data_quality import (
    enforce_types,
    deduplicate,
    add_date_key,
    INVOICE_TYPES,
    INVOICE_LINE_TYPES,
    BILL_TYPES,
    PAYMENT_TYPES,
    PURCHASE_TYPES,
    ACCOUNT_TYPES,
    CUSTOMER_TYPES,
)
from load.sql_loader import SQLLoader
from load.raw_archiver import archive_raw_json

logger = logging.getLogger(__name__)

# Tables that produce both a header fact table and a line-item fact table
LINE_ITEM_TABLES = {
    "Invoice": (flatten_invoice_lines, "fact_invoice_line", INVOICE_LINE_TYPES, ["invoice_id", "line_id"]),
    "Bill": (flatten_bill_lines, "fact_bill_line", INVOICE_LINE_TYPES, ["bill_id", "line_id"]),
    "Purchase": (flatten_purchase_lines, "fact_purchase_line", INVOICE_LINE_TYPES, ["purchase_id", "line_id"]),
    "Payment": (flatten_payment_lines, "fact_payment_line", None, ["payment_id", "linked_invoice_id"]),
    "Estimate": (flatten_estimate_lines, "fact_estimate_line", INVOICE_LINE_TYPES, ["estimate_id", "line_id"]),
}

# Type enforcement rules per target table
TYPE_RULES = {
    "fact_invoice": INVOICE_TYPES,
    "fact_bill": BILL_TYPES,
    "fact_payment": PAYMENT_TYPES,
    "fact_purchase": PURCHASE_TYPES,
    "dim_account": ACCOUNT_TYPES,
    "dim_customer": CUSTOMER_TYPES,
}

# Dedup key columns per target table
DEDUP_KEYS = {
    "fact_invoice": ["invoice_id"],
    "fact_bill": ["bill_id"],
    "fact_payment": ["payment_id"],
    "fact_purchase": ["purchase_id"],
    "fact_estimate": ["estimate_id"],
    "dim_account": ["account_id"],
    "dim_customer": ["customer_id"],
    "dim_vendor": ["vendor_id"],
    "dim_item": ["item_id"],
    "dim_employee": ["employee_id"],
}

# Date columns to convert to date_key integers
DATE_KEY_COLUMNS = {
    "fact_invoice": [("txn_date", "txn_date_key"), ("due_date", "due_date_key")],
    "fact_bill": [("txn_date", "txn_date_key"), ("due_date", "due_date_key")],
    "fact_payment": [("txn_date", "txn_date_key")],
    "fact_purchase": [("txn_date", "txn_date_key")],
    "fact_estimate": [("txn_date", "txn_date_key")],
}


def run_pipeline_for_client(client_config: dict, settings: Settings):
    """Execute full ETL for a single client."""
    client_id = client_config["client_id"]
    logger.info(f"=== Starting pipeline for client: {client_id} ===")

    # Initialize components
    oauth = OAuthManager(settings, client_id)
    qbo = QBOClient(oauth, settings)
    loader = SQLLoader(settings, client_id)

    # --- Phase 1: Extract all entity tables ---
    logger.info("Phase 1: Extracting entity tables...")
    raw_entities = extract_all_entities(qbo)

    # --- Phase 2: Transform and load each entity ---
    logger.info("Phase 2: Transform and load entities...")
    for table_name, raw_records in raw_entities.items():
        if not raw_records:
            continue

        # Archive raw JSON
        archive_raw_json(settings, client_id, table_name, raw_records)

        # Map to star schema (header/dimension table)
        target_table = ENTITY_TO_SCHEMA_MAP.get(table_name)
        if not target_table:
            logger.warning(f"  No schema mapping for {table_name}, skipping")
            continue

        df = map_to_schema(table_name, raw_records)
        if df.empty:
            continue

        # Apply type enforcement
        type_rules = TYPE_RULES.get(target_table)
        if type_rules:
            df = enforce_types(df, type_rules)

        # Deduplicate
        dedup_keys = DEDUP_KEYS.get(target_table)
        if dedup_keys:
            df = deduplicate(df, dedup_keys)

        # Add date keys
        date_keys = DATE_KEY_COLUMNS.get(target_table, [])
        for date_col, key_col in date_keys:
            df = add_date_key(df, date_col, key_col)

        # Load header/dimension table
        loader.upsert(target_table, df)

        # Flatten and load line items if applicable
        if table_name in LINE_ITEM_TABLES:
            flattener, line_table, line_types, line_dedup_keys = LINE_ITEM_TABLES[table_name]
            df_lines = flattener(raw_records)
            if not df_lines.empty:
                if line_types:
                    df_lines = enforce_types(df_lines, line_types)
                df_lines = deduplicate(df_lines, line_dedup_keys)
                loader.upsert(line_table, df_lines)

    # --- Phase 3: Extract and load reports ---
    logger.info("Phase 3: Extracting reports...")
    try:
        raw_reports = extract_all_reports(qbo)
        for report_name, report_data in raw_reports.items():
            if not report_data:
                continue
            archive_raw_json(settings, client_id, f"report_{report_name}", report_data)
            rows = flatten_report_rows(report_data)
            if rows:
                import pandas as pd
                df_report = pd.DataFrame(rows)
                loader.upsert(f"report_{report_name.lower()}", df_report)
    except Exception as e:
        logger.error(f"Report extraction failed: {e}")

    logger.info(f"=== Pipeline complete for client: {client_id} ===")


def run_pipeline_all_clients(settings: Settings):
    """Run pipeline for all registered clients."""
    clients_file = Path("config/clients.json")
    if not clients_file.exists():
        logger.error(f"Clients file not found: {clients_file}")
        return

    with open(clients_file) as f:
        clients = json.load(f)

    logger.info(f"Running pipeline for {len(clients)} client(s)")
    for client in clients:
        try:
            run_pipeline_for_client(client, settings)
        except Exception as e:
            logger.error(
                f"Pipeline failed for {client.get('client_id', '?')}: {e}",
                exc_info=True,
            )


def main():
    """CLI entry point for manual pipeline runs."""
    import argparse

    parser = argparse.ArgumentParser(description="Run QBO ETL pipeline")
    parser.add_argument(
        "--client-id",
        help="Run for a specific client only (default: all clients)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings()

    if args.client_id:
        run_pipeline_for_client(
            {"client_id": args.client_id}, settings
        )
    else:
        run_pipeline_all_clients(settings)


if __name__ == "__main__":
    main()
