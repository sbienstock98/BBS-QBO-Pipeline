"""Load transformed DataFrames into Azure SQL or local SQLite."""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class SQLLoader:
    """Upsert DataFrames into SQL database (Azure SQL or SQLite)."""

    def __init__(self, settings, client_id: str):
        self.settings = settings
        self.client_id = client_id
        self.backend = settings.DB_BACKEND

    def _get_sqlite_conn(self):
        db_path = Path(self.settings.LOCAL_DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(str(db_path))

    def _get_azure_sql_conn(self):
        import pyodbc

        return pyodbc.connect(self.settings.AZURE_SQL_CONNECTION_STRING)

    def upsert(self, table_name: str, df: pd.DataFrame):
        """Insert or replace rows into the target table.

        Adds client_id column automatically. For SQLite uses INSERT OR REPLACE.
        For Azure SQL uses MERGE.
        """
        if df.empty:
            logger.info(f"  {table_name}: no data to load")
            return

        # Add client_id to every row
        df = df.copy()
        df.insert(0, "client_id", self.client_id)

        if self.backend == "sqlite":
            self._upsert_sqlite(table_name, df)
        else:
            self._upsert_azure_sql(table_name, df)

        logger.info(f"  {table_name}: loaded {len(df)} rows")

    def _upsert_sqlite(self, table_name: str, df: pd.DataFrame):
        """Upsert into SQLite using INSERT OR REPLACE."""
        conn = self._get_sqlite_conn()
        try:
            # Create table if not exists (auto-infer schema from DataFrame)
            df.to_sql(
                table_name,
                conn,
                if_exists="replace",
                index=False,
                method="multi",
            )
        finally:
            conn.close()

    def _upsert_azure_sql(self, table_name: str, df: pd.DataFrame):
        """Upsert into Azure SQL using a staging table + MERGE."""
        conn = self._get_azure_sql_conn()
        cursor = conn.cursor()
        staging_table = f"staging_{table_name}"

        try:
            # 1. Create staging table (temp)
            columns = df.columns.tolist()
            col_defs = ", ".join(
                f"[{col}] NVARCHAR(MAX)" for col in columns
            )
            cursor.execute(
                f"IF OBJECT_ID('tempdb..#{staging_table}') IS NOT NULL "
                f"DROP TABLE #{staging_table}"
            )
            cursor.execute(f"CREATE TABLE #{staging_table} ({col_defs})")

            # 2. Bulk insert into staging
            placeholders = ", ".join(["?"] * len(columns))
            insert_sql = (
                f"INSERT INTO #{staging_table} "
                f"({', '.join(f'[{c}]' for c in columns)}) "
                f"VALUES ({placeholders})"
            )
            for _, row in df.iterrows():
                values = [
                    str(v) if pd.notna(v) else None for v in row.values
                ]
                cursor.execute(insert_sql, values)

            # 3. MERGE into target
            pk_cols = self._get_pk_columns(table_name)
            join_condition = " AND ".join(
                f"target.[{col}] = source.[{col}]" for col in pk_cols
            )
            update_cols = [c for c in columns if c not in pk_cols]
            update_set = ", ".join(
                f"target.[{col}] = source.[{col}]" for col in update_cols
            )
            insert_cols = ", ".join(f"[{c}]" for c in columns)
            insert_vals = ", ".join(f"source.[{c}]" for c in columns)

            merge_sql = f"""
                MERGE [{table_name}] AS target
                USING #{staging_table} AS source
                ON {join_condition}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_vals});
            """
            cursor.execute(merge_sql)
            conn.commit()

            # 4. Clean up staging
            cursor.execute(f"DROP TABLE #{staging_table}")
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def _get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary key columns for a table."""
        pk_map = {
            "dim_account": ["client_id", "account_id"],
            "dim_customer": ["client_id", "customer_id"],
            "dim_vendor": ["client_id", "vendor_id"],
            "dim_item": ["client_id", "item_id"],
            "dim_employee": ["client_id", "employee_id"],
            "dim_class": ["client_id", "id"],
            "dim_department": ["client_id", "id"],
            "dim_tax_code": ["client_id", "id"],
            "dim_tax_rate": ["client_id", "id"],
            "dim_term": ["client_id", "id"],
            "dim_payment_method": ["client_id", "id"],
            "dim_company_info": ["client_id", "id"],
            "fact_invoice": ["client_id", "invoice_id"],
            "fact_invoice_line": ["client_id", "invoice_id", "line_id"],
            "fact_bill": ["client_id", "bill_id"],
            "fact_bill_line": ["client_id", "bill_id", "line_id"],
            "fact_payment": ["client_id", "payment_id"],
            "fact_payment_line": ["client_id", "payment_id", "linked_invoice_id"],
            "fact_purchase": ["client_id", "purchase_id"],
            "fact_purchase_line": ["client_id", "purchase_id", "line_id"],
            "fact_estimate": ["client_id", "estimate_id"],
            "fact_bill_payment": ["client_id", "id"],
            "fact_deposit": ["client_id", "id"],
            "fact_credit_memo": ["client_id", "id"],
            "fact_refund_receipt": ["client_id", "id"],
            "fact_sales_receipt": ["client_id", "id"],
            "fact_journal_entry": ["client_id", "id"],
            "fact_transfer": ["client_id", "id"],
        }
        return pk_map.get(table_name, ["client_id", "id"])

    def execute_sql_file(self, sql_file_path: str):
        """Execute a .sql file against the database."""
        with open(sql_file_path) as f:
            sql = f.read()

        if self.backend == "sqlite":
            conn = self._get_sqlite_conn()
            try:
                conn.executescript(sql)
            finally:
                conn.close()
        else:
            conn = self._get_azure_sql_conn()
            cursor = conn.cursor()
            try:
                # Split on GO statements for Azure SQL
                for batch in sql.split("\nGO\n"):
                    batch = batch.strip()
                    if batch:
                        cursor.execute(batch)
                conn.commit()
            finally:
                cursor.close()
                conn.close()
