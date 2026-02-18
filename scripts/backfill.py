"""One-time historical data backfill for a QBO client.

Pulls reports for previous years to populate historical trends in Power BI.

Usage:
    python scripts/backfill.py --client-id pilot_001 --start-year 2023
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from auth.oauth_manager import OAuthManager
from extract.qbo_client import QBOClient
from extract.entity_extractor import extract_all_entities
from extract.report_extractor import extract_report, flatten_report_rows
from transform.flatteners import (
    flatten_invoice_lines,
    flatten_bill_lines,
    flatten_purchase_lines,
    flatten_payment_lines,
    flatten_estimate_lines,
)
from transform.schema_mapper import map_to_schema
from transform.data_quality import enforce_types, add_date_key
from config.tables import ENTITY_TO_SCHEMA_MAP, REPORT_ENDPOINTS
from load.sql_loader import SQLLoader
from load.raw_archiver import archive_raw_json
from orchestrator.pipeline import (
    LINE_ITEM_TABLES,
    TYPE_RULES,
    DEDUP_KEYS,
    DATE_KEY_COLUMNS,
    run_pipeline_for_client,
)

logger = logging.getLogger(__name__)


def backfill_reports(
    qbo: QBOClient,
    loader: SQLLoader,
    settings,
    client_id: str,
    start_year: int,
    end_year: int,
):
    """Pull reports for each year in the range."""
    report_names = [
        "ProfitAndLoss",
        "ProfitAndLossCash",
        "BalanceSheet",
        "CashFlow",
        "AgedReceivables",
        "AgedPayables",
    ]

    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        logger.info(f"Backfilling reports for {year}...")

        for report_name in report_names:
            try:
                report_data = extract_report(qbo, report_name, start_date, end_date)
                if report_data:
                    archive_raw_json(
                        settings, client_id,
                        f"backfill_report_{report_name}_{year}",
                        report_data,
                    )
                    rows = flatten_report_rows(report_data)
                    if rows:
                        df = pd.DataFrame(rows)
                        df["_year"] = year
                        loader.upsert(f"report_{report_name.lower()}", df)
                        logger.info(f"  {report_name} {year}: {len(rows)} rows")
            except Exception as e:
                logger.error(f"  {report_name} {year} failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Backfill historical QBO data")
    parser.add_argument("--client-id", required=True, help="Client ID to backfill")
    parser.add_argument(
        "--start-year", type=int, default=2023,
        help="First year to backfill (default: 2023)",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="Last year to backfill (default: current year)",
    )
    parser.add_argument(
        "--entities-only", action="store_true",
        help="Only backfill entity tables (skip reports)",
    )
    parser.add_argument(
        "--reports-only", action="store_true",
        help="Only backfill report endpoints (skip entities)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    settings = Settings()
    end_year = args.end_year or date.today().year

    logger.info(f"Backfilling client {args.client_id} for {args.start_year}-{end_year}")

    # Initialize QBO connection
    oauth = OAuthManager(settings, args.client_id)
    qbo = QBOClient(oauth, settings)
    loader = SQLLoader(settings, args.client_id)

    # Entity tables contain all historical data (no date filter needed)
    if not args.reports_only:
        logger.info("Running full entity pipeline...")
        run_pipeline_for_client({"client_id": args.client_id}, settings)

    # Reports need to be pulled per year
    if not args.entities_only:
        backfill_reports(
            qbo, loader, settings, args.client_id,
            args.start_year, end_year,
        )

    logger.info("Backfill complete")


if __name__ == "__main__":
    main()
