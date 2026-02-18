import logging
from datetime import date, timedelta

from config.tables import REPORT_ENDPOINTS
from extract.qbo_client import QBOClient

logger = logging.getLogger(__name__)


def extract_all_reports(
    qbo: QBOClient,
    start_date: str = None,
    end_date: str = None,
) -> dict[str, dict]:
    """Extract all report endpoints from QBO.

    Args:
        start_date: Start date in YYYY-MM-DD format. Defaults to Jan 1 of current year.
        end_date: End date in YYYY-MM-DD format. Defaults to today.

    Returns a dict of {report_name: raw_report_json}.
    """
    today = date.today()
    if not start_date:
        start_date = f"{today.year}-01-01"
    if not end_date:
        end_date = today.isoformat()

    results = {}
    for report_name, config in REPORT_ENDPOINTS.items():
        logger.info(f"Extracting report: {report_name}")
        try:
            params = dict(config["default_params"])
            # Reports that use date ranges
            if report_name not in ("BalanceSheet",):
                params["start_date"] = start_date
                params["end_date"] = end_date
            else:
                # Balance sheet is point-in-time
                params["date"] = end_date
            report_data = qbo.get_report(config["path"], params)
            results[report_name] = report_data
        except Exception as e:
            logger.error(f"Failed to extract report {report_name}: {e}")
            results[report_name] = {}
    return results


def extract_report(
    qbo: QBOClient,
    report_name: str,
    start_date: str = None,
    end_date: str = None,
) -> dict:
    """Extract a single report from QBO."""
    if report_name not in REPORT_ENDPOINTS:
        raise ValueError(
            f"Unknown report: {report_name}. "
            f"Available: {list(REPORT_ENDPOINTS.keys())}"
        )

    today = date.today()
    if not start_date:
        start_date = f"{today.year}-01-01"
    if not end_date:
        end_date = today.isoformat()

    config = REPORT_ENDPOINTS[report_name]
    params = dict(config["default_params"])

    if report_name not in ("BalanceSheet",):
        params["start_date"] = start_date
        params["end_date"] = end_date
    else:
        params["date"] = end_date

    logger.info(f"Extracting report: {report_name} ({start_date} to {end_date})")
    return qbo.get_report(config["path"], params)


def flatten_report_rows(report_data: dict) -> list[dict]:
    """Flatten QBO report JSON into a list of row dicts.

    QBO reports have a nested structure with Header, Columns, and Rows.
    This flattens it into tabular format suitable for loading into SQL.
    """
    if not report_data:
        return []

    header = report_data.get("Header", {})
    report_name = header.get("ReportName", "Unknown")
    report_date = header.get("StartPeriod", "")
    end_date = header.get("EndPeriod", "")

    # Get column headers
    columns = report_data.get("Columns", {}).get("Column", [])
    col_names = [c.get("ColTitle", f"col_{i}") for i, c in enumerate(columns)]

    rows = []
    _extract_rows(
        report_data.get("Rows", {}).get("Row", []),
        rows,
        col_names,
        report_name,
        section="",
    )
    return rows


def _extract_rows(
    row_list: list,
    output: list[dict],
    col_names: list[str],
    report_name: str,
    section: str,
):
    """Recursively extract rows from QBO report nested structure."""
    for row in row_list:
        row_type = row.get("type", "")

        if row_type == "Section":
            section_header = row.get("Header", {})
            section_name = ""
            if section_header:
                col_data = section_header.get("ColData", [])
                if col_data:
                    section_name = col_data[0].get("value", "")

            # Recurse into section rows
            sub_rows = row.get("Rows", {}).get("Row", [])
            _extract_rows(sub_rows, output, col_names, report_name, section_name)

            # Also capture the Summary row if present
            summary = row.get("Summary", {})
            if summary:
                col_data = summary.get("ColData", [])
                values = {
                    col_names[i]: cd.get("value", "")
                    for i, cd in enumerate(col_data)
                    if i < len(col_names)
                }
                values["_section"] = section_name
                values["_row_type"] = "Summary"
                values["_report_name"] = report_name
                output.append(values)

        elif row_type == "Data" or "ColData" in row:
            col_data = row.get("ColData", [])
            values = {
                col_names[i]: cd.get("value", "")
                for i, cd in enumerate(col_data)
                if i < len(col_names)
            }
            values["_section"] = section
            values["_row_type"] = "Data"
            values["_report_name"] = report_name
            output.append(values)
