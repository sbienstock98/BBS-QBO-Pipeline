import logging

from config.tables import ENTITY_TABLES
from extract.qbo_client import QBOClient

logger = logging.getLogger(__name__)


def extract_all_entities(qbo: QBOClient) -> dict[str, list[dict]]:
    """Extract all entity tables from QBO.

    Returns a dict of {table_name: [records...]}.
    """
    results = {}
    for table in ENTITY_TABLES:
        logger.info(f"Extracting entity: {table}")
        try:
            if table == "CompanyInfo":
                # CompanyInfo uses a different endpoint (GET by ID)
                info = qbo.get_company_info()
                results[table] = [info] if info else []
            else:
                records = qbo.query_all(table)
                results[table] = records
        except Exception as e:
            logger.error(f"Failed to extract {table}: {e}")
            results[table] = []
    return results


def extract_entity(qbo: QBOClient, table: str) -> list[dict]:
    """Extract a single entity table from QBO."""
    logger.info(f"Extracting entity: {table}")
    if table == "CompanyInfo":
        info = qbo.get_company_info()
        return [info] if info else []
    return qbo.query_all(table)
