# QBO entity tables pulled via SELECT * FROM {Table}
ENTITY_TABLES = [
    # Original 12 tables from existing QB Pull.py
    "Customer",
    "Invoice",
    "Payment",
    "Vendor",
    "Employee",
    "Estimate",
    "Bill",
    "Purchase",
    "Item",
    "Account",
    "Department",
    "Class",
    # Additional tables for comprehensive CPA reporting
    "BillPayment",
    "Deposit",
    "CreditMemo",
    "RefundReceipt",
    "SalesReceipt",
    "JournalEntry",
    "Transfer",
    "TaxCode",
    "TaxRate",
    "Term",
    "PaymentMethod",
    "CompanyInfo",
]

# Tables whose Line arrays need flattening into separate fact tables
TABLES_WITH_LINE_ITEMS = {
    "Invoice": "SalesItemLineDetail",
    "Bill": ["AccountBasedExpenseLineDetail", "ItemBasedExpenseLineDetail"],
    "Purchase": ["AccountBasedExpenseLineDetail", "ItemBasedExpenseLineDetail"],
    "Estimate": "SalesItemLineDetail",
    "CreditMemo": "SalesItemLineDetail",
    "SalesReceipt": "SalesItemLineDetail",
    "JournalEntry": "JournalEntryLineDetail",
    "RefundReceipt": "SalesItemLineDetail",
}

# Report endpoints (GET requests with query parameters, different from entity queries)
REPORT_ENDPOINTS = {
    "ProfitAndLoss": {
        "path": "/reports/ProfitAndLoss",
        "default_params": {
            "accounting_method": "Accrual",
            "summarize_column_by": "Month",
        },
    },
    "ProfitAndLossCash": {
        "path": "/reports/ProfitAndLoss",
        "default_params": {
            "accounting_method": "Cash",
            "summarize_column_by": "Month",
        },
    },
    "BalanceSheet": {
        "path": "/reports/BalanceSheet",
        "default_params": {
            "accounting_method": "Accrual",
        },
    },
    "CashFlow": {
        "path": "/reports/CashFlow",
        "default_params": {
            "summarize_column_by": "Month",
        },
    },
    "AgedReceivables": {
        "path": "/reports/AgedReceivables",
        "default_params": {
            "aging_period": "30",
            "num_periods": "4",
        },
    },
    "AgedReceivableDetail": {
        "path": "/reports/AgedReceivableDetail",
        "default_params": {
            "aging_period": "30",
            "num_periods": "4",
        },
    },
    "AgedPayables": {
        "path": "/reports/AgedPayables",
        "default_params": {
            "aging_period": "30",
            "num_periods": "4",
        },
    },
    "AgedPayableDetail": {
        "path": "/reports/AgedPayableDetail",
        "default_params": {
            "aging_period": "30",
            "num_periods": "4",
        },
    },
    "TrialBalance": {
        "path": "/reports/TrialBalance",
        "default_params": {},
    },
}

# Mapping from QBO entity table name to star schema target table
ENTITY_TO_SCHEMA_MAP = {
    # Dimension tables
    "Account": "dim_account",
    "Customer": "dim_customer",
    "Vendor": "dim_vendor",
    "Item": "dim_item",
    "Employee": "dim_employee",
    "Class": "dim_class",
    "Department": "dim_department",
    "TaxCode": "dim_tax_code",
    "TaxRate": "dim_tax_rate",
    "Term": "dim_term",
    "PaymentMethod": "dim_payment_method",
    "CompanyInfo": "dim_company_info",
    # Fact tables (headers)
    "Invoice": "fact_invoice",
    "Bill": "fact_bill",
    "Payment": "fact_payment",
    "Purchase": "fact_purchase",
    "Estimate": "fact_estimate",
    "BillPayment": "fact_bill_payment",
    "Deposit": "fact_deposit",
    "CreditMemo": "fact_credit_memo",
    "RefundReceipt": "fact_refund_receipt",
    "SalesReceipt": "fact_sales_receipt",
    "JournalEntry": "fact_journal_entry",
    "Transfer": "fact_transfer",
}
