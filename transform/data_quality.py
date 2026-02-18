"""Data quality checks and type enforcement for QBO data."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def enforce_types(df: pd.DataFrame, type_map: dict[str, str]) -> pd.DataFrame:
    """Cast DataFrame columns to specified types.

    Args:
        df: Input DataFrame.
        type_map: Dict of {column_name: pandas_dtype}. Columns not in df are skipped.

    Example:
        enforce_types(df, {"amount": "float64", "is_active": "bool"})
    """
    for col, dtype in type_map.items():
        if col not in df.columns:
            continue
        try:
            if dtype == "bool":
                df[col] = df[col].map(
                    {True: True, False: False, "true": True, "false": False, "True": True, "False": False}
                ).fillna(False)
            elif dtype in ("float64", "float"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype == "date":
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif dtype == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype, errors="ignore")
        except Exception as e:
            logger.warning(f"Could not cast {col} to {dtype}: {e}")
    return df


def deduplicate(df: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Remove duplicate rows based on key columns, keeping the last occurrence."""
    if not key_columns or df.empty:
        return df
    before = len(df)
    df = df.drop_duplicates(subset=key_columns, keep="last")
    after = len(df)
    if before != after:
        logger.info(f"Removed {before - after} duplicate rows (keys: {key_columns})")
    return df


def add_date_key(df: pd.DataFrame, date_col: str, key_col: str) -> pd.DataFrame:
    """Add an integer date key column (YYYYMMDD) from a date column."""
    if date_col not in df.columns:
        df[key_col] = None
        return df
    dates = pd.to_datetime(df[date_col], errors="coerce")
    df[key_col] = dates.dt.strftime("%Y%m%d").astype("Int64")
    return df


# Standard type maps for each table
INVOICE_TYPES = {
    "total_amount": "float64",
    "balance": "float64",
    "total_tax": "float64",
    "txn_date": "date",
    "due_date": "date",
}

INVOICE_LINE_TYPES = {
    "amount": "float64",
    "quantity": "float64",
    "unit_price": "float64",
}

BILL_TYPES = {
    "total_amount": "float64",
    "balance": "float64",
    "txn_date": "date",
    "due_date": "date",
}

PAYMENT_TYPES = {
    "total_amount": "float64",
    "unapplied_amount": "float64",
    "txn_date": "date",
}

PURCHASE_TYPES = {
    "total_amount": "float64",
    "is_credit": "bool",
    "txn_date": "date",
}

ACCOUNT_TYPES = {
    "current_balance": "float64",
    "is_sub_account": "bool",
    "is_active": "bool",
}

CUSTOMER_TYPES = {
    "balance": "float64",
    "is_job": "bool",
    "is_active": "bool",
}
