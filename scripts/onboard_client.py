"""Onboard a new QBO client: run OAuth flow, register in clients.json, init schema.

Usage:
    python scripts/onboard_client.py --client-id pilot_001 --name "Craig's Landscaping"
"""

import argparse
import json
import logging
import sqlite3
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import Settings
from auth.qbo_auth_flow import run_oauth_flow
from load.sql_loader import SQLLoader

logger = logging.getLogger(__name__)
CLIENTS_FILE = Path("config/clients.json")


def load_clients() -> list[dict]:
    if CLIENTS_FILE.exists():
        with open(CLIENTS_FILE) as f:
            return json.load(f)
    return []


def save_clients(clients: list[dict]):
    CLIENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CLIENTS_FILE, "w") as f:
        json.dump(clients, f, indent=2)


def init_database(settings: Settings):
    """Create schema tables and populate dim_date."""
    scripts_dir = Path(__file__).parent
    loader = SQLLoader(settings, "system")

    schema_file = scripts_dir / "create_schema.sql"
    if schema_file.exists():
        logger.info("Creating schema tables...")
        loader.execute_sql_file(str(schema_file))

    date_file = scripts_dir / "populate_dim_date.sql"
    if date_file.exists():
        logger.info("Populating dim_date...")
        loader.execute_sql_file(str(date_file))

    logger.info("Database initialized")


def main():
    parser = argparse.ArgumentParser(description="Onboard a new QBO client")
    parser.add_argument("--client-id", required=True, help="Unique client ID (e.g., 'pilot_001')")
    parser.add_argument("--name", required=True, help="Client display name")
    parser.add_argument("--industry", default="", help="Client industry")
    parser.add_argument("--skip-oauth", action="store_true", help="Skip OAuth flow (use if tokens already stored)")
    parser.add_argument("--init-db", action="store_true", help="Initialize database schema (run once)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    settings = Settings()

    # Init database if requested
    if args.init_db:
        init_database(settings)

    # Check for duplicate client
    clients = load_clients()
    if any(c["client_id"] == args.client_id for c in clients):
        logger.error(f"Client '{args.client_id}' already exists in clients.json")
        return

    # Run OAuth flow
    realm_id = None
    if not args.skip_oauth:
        if not settings.QBO_CLIENT_ID or not settings.QBO_CLIENT_SECRET:
            logger.error("Set QBO_CLIENT_ID and QBO_CLIENT_SECRET in .env file first")
            return
        realm_id = run_oauth_flow(settings, args.client_id)

    # Register client
    from datetime import date
    client_record = {
        "client_id": args.client_id,
        "client_name": args.name,
        "qbo_realm_id": realm_id or "pending",
        "industry": args.industry,
        "onboarded_date": date.today().isoformat(),
        "is_active": True,
    }
    clients.append(client_record)
    save_clients(clients)

    print(f"\nClient onboarded successfully:")
    print(f"  ID:       {args.client_id}")
    print(f"  Name:     {args.name}")
    print(f"  Realm:    {realm_id or 'pending'}")
    print(f"\nNext steps:")
    print(f"  1. Run the pipeline:  python -m orchestrator.pipeline --client-id {args.client_id}")
    print(f"  2. Open Power BI and connect to the database")


if __name__ == "__main__":
    main()
