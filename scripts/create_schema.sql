-- ============================================================
-- Bayshore QBO Pipeline - Star Schema DDL
-- Supports both Azure SQL and SQLite (via compatible subset)
-- ============================================================

-- DIM_CLIENT: Master client registry
CREATE TABLE IF NOT EXISTS dim_client (
    client_id           TEXT PRIMARY KEY,
    client_name         TEXT NOT NULL,
    qbo_realm_id        TEXT NOT NULL,
    industry            TEXT,
    onboarded_date      TEXT,
    is_active           INTEGER DEFAULT 1
);

-- DIM_DATE: Shared calendar (populated separately)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INTEGER PRIMARY KEY,
    full_date           TEXT NOT NULL,
    day_of_week         INTEGER,
    day_name            TEXT,
    day_of_month        INTEGER,
    day_of_year         INTEGER,
    week_of_year        INTEGER,
    month_num           INTEGER,
    month_name          TEXT,
    month_short         TEXT,
    quarter_num         INTEGER,
    quarter_name        TEXT,
    year_num            INTEGER,
    fiscal_month        INTEGER,
    fiscal_quarter      INTEGER,
    fiscal_year         INTEGER,
    is_weekend          INTEGER,
    is_holiday          INTEGER DEFAULT 0
);

-- DIM_ACCOUNT: Chart of Accounts
CREATE TABLE IF NOT EXISTS dim_account (
    client_id           TEXT NOT NULL,
    account_id          TEXT NOT NULL,
    account_name        TEXT,
    fully_qualified_name TEXT,
    classification      TEXT,
    account_type        TEXT,
    account_sub_type    TEXT,
    is_sub_account      INTEGER,
    parent_account_id   TEXT,
    is_active           INTEGER,
    current_balance     REAL,
    currency_code       TEXT,
    PRIMARY KEY (client_id, account_id)
);

-- DIM_CUSTOMER
CREATE TABLE IF NOT EXISTS dim_customer (
    client_id           TEXT NOT NULL,
    customer_id         TEXT NOT NULL,
    display_name        TEXT,
    company_name        TEXT,
    given_name          TEXT,
    family_name         TEXT,
    email               TEXT,
    phone               TEXT,
    billing_city        TEXT,
    billing_state       TEXT,
    billing_postal_code TEXT,
    is_job              INTEGER,
    is_active           INTEGER,
    balance             REAL,
    parent_customer_id  TEXT,
    PRIMARY KEY (client_id, customer_id)
);

-- DIM_VENDOR
CREATE TABLE IF NOT EXISTS dim_vendor (
    client_id           TEXT NOT NULL,
    vendor_id           TEXT NOT NULL,
    display_name        TEXT,
    company_name        TEXT,
    email               TEXT,
    phone               TEXT,
    is_1099             INTEGER,
    is_active           INTEGER,
    balance             REAL,
    PRIMARY KEY (client_id, vendor_id)
);

-- DIM_ITEM: Products & Services
CREATE TABLE IF NOT EXISTS dim_item (
    client_id           TEXT NOT NULL,
    item_id             TEXT NOT NULL,
    item_name           TEXT,
    description         TEXT,
    item_type           TEXT,
    unit_price          REAL,
    purchase_cost       REAL,
    is_active           INTEGER,
    income_account_id   TEXT,
    expense_account_id  TEXT,
    track_qty           INTEGER,
    qty_on_hand         REAL,
    PRIMARY KEY (client_id, item_id)
);

-- DIM_EMPLOYEE
CREATE TABLE IF NOT EXISTS dim_employee (
    client_id           TEXT NOT NULL,
    employee_id         TEXT NOT NULL,
    display_name        TEXT,
    given_name          TEXT,
    family_name         TEXT,
    hired_date          TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, employee_id)
);

-- DIM_CLASS
CREATE TABLE IF NOT EXISTS dim_class (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- DIM_DEPARTMENT
CREATE TABLE IF NOT EXISTS dim_department (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- DIM_TAX_CODE
CREATE TABLE IF NOT EXISTS dim_tax_code (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- DIM_TAX_RATE
CREATE TABLE IF NOT EXISTS dim_tax_rate (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- DIM_TERM
CREATE TABLE IF NOT EXISTS dim_term (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- DIM_PAYMENT_METHOD
CREATE TABLE IF NOT EXISTS dim_payment_method (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- DIM_COMPANY_INFO
CREATE TABLE IF NOT EXISTS dim_company_info (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- FACT_INVOICE: One row per invoice (header)
CREATE TABLE IF NOT EXISTS fact_invoice (
    client_id           TEXT NOT NULL,
    invoice_id          TEXT NOT NULL,
    doc_number          TEXT,
    txn_date            TEXT,
    due_date            TEXT,
    customer_id         TEXT,
    total_amount        REAL,
    balance             REAL,
    total_tax           REAL,
    email_status        TEXT,
    print_status        TEXT,
    txn_date_key        INTEGER,
    due_date_key        INTEGER,
    PRIMARY KEY (client_id, invoice_id)
);

-- FACT_INVOICE_LINE: One row per invoice line item
CREATE TABLE IF NOT EXISTS fact_invoice_line (
    client_id           TEXT NOT NULL,
    invoice_id          TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    line_num            INTEGER,
    description         TEXT,
    amount              REAL,
    quantity            REAL,
    unit_price          REAL,
    item_id             TEXT,
    item_name           TEXT,
    account_id          TEXT,
    account_name        TEXT,
    tax_code_ref        TEXT,
    service_date        TEXT,
    detail_type         TEXT,
    linked_txn_id       TEXT,
    linked_txn_type     TEXT,
    PRIMARY KEY (client_id, invoice_id, line_id)
);

-- FACT_PAYMENT
CREATE TABLE IF NOT EXISTS fact_payment (
    client_id           TEXT NOT NULL,
    payment_id          TEXT NOT NULL,
    txn_date            TEXT,
    total_amount        REAL,
    customer_id         TEXT,
    deposit_to_account_id TEXT,
    payment_method_id   TEXT,
    unapplied_amount    REAL,
    txn_date_key        INTEGER,
    PRIMARY KEY (client_id, payment_id)
);

-- FACT_PAYMENT_LINE: One row per invoice payment application
CREATE TABLE IF NOT EXISTS fact_payment_line (
    client_id           TEXT NOT NULL,
    payment_id          TEXT NOT NULL,
    amount              REAL,
    linked_invoice_id   TEXT,
    linked_txn_type     TEXT,
    original_open_balance TEXT,
    invoice_doc_number  TEXT,
    PRIMARY KEY (client_id, payment_id, linked_invoice_id)
);

-- FACT_BILL: One row per bill (header)
CREATE TABLE IF NOT EXISTS fact_bill (
    client_id           TEXT NOT NULL,
    bill_id             TEXT NOT NULL,
    txn_date            TEXT,
    due_date            TEXT,
    vendor_id           TEXT,
    total_amount        REAL,
    balance             REAL,
    txn_date_key        INTEGER,
    due_date_key        INTEGER,
    PRIMARY KEY (client_id, bill_id)
);

-- FACT_BILL_LINE
CREATE TABLE IF NOT EXISTS fact_bill_line (
    client_id           TEXT NOT NULL,
    bill_id             TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    line_num            INTEGER,
    description         TEXT,
    amount              REAL,
    detail_type         TEXT,
    quantity            REAL,
    unit_price          REAL,
    item_id             TEXT,
    item_name           TEXT,
    account_id          TEXT,
    account_name        TEXT,
    billable_status     TEXT,
    customer_id         TEXT,
    customer_name       TEXT,
    tax_code_ref        TEXT,
    PRIMARY KEY (client_id, bill_id, line_id)
);

-- FACT_PURCHASE
CREATE TABLE IF NOT EXISTS fact_purchase (
    client_id           TEXT NOT NULL,
    purchase_id         TEXT NOT NULL,
    txn_date            TEXT,
    payment_type        TEXT,
    total_amount        REAL,
    account_id          TEXT,
    vendor_id           TEXT,
    vendor_type         TEXT,
    is_credit           INTEGER,
    doc_number          TEXT,
    txn_date_key        INTEGER,
    PRIMARY KEY (client_id, purchase_id)
);

-- FACT_PURCHASE_LINE
CREATE TABLE IF NOT EXISTS fact_purchase_line (
    client_id           TEXT NOT NULL,
    purchase_id         TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    description         TEXT,
    amount              REAL,
    detail_type         TEXT,
    item_id             TEXT,
    item_name           TEXT,
    account_id          TEXT,
    account_name        TEXT,
    billable_status     TEXT,
    customer_id         TEXT,
    tax_code_ref        TEXT,
    PRIMARY KEY (client_id, purchase_id, line_id)
);

-- FACT_ESTIMATE
CREATE TABLE IF NOT EXISTS fact_estimate (
    client_id           TEXT NOT NULL,
    estimate_id         TEXT NOT NULL,
    doc_number          TEXT,
    txn_date            TEXT,
    customer_id         TEXT,
    total_amount        REAL,
    txn_status          TEXT,
    linked_invoice_id   TEXT,
    txn_date_key        INTEGER,
    PRIMARY KEY (client_id, estimate_id)
);

-- FACT_ESTIMATE_LINE
CREATE TABLE IF NOT EXISTS fact_estimate_line (
    client_id           TEXT NOT NULL,
    estimate_id         TEXT NOT NULL,
    line_id             TEXT NOT NULL,
    line_num            INTEGER,
    description         TEXT,
    amount              REAL,
    quantity            REAL,
    unit_price          REAL,
    item_id             TEXT,
    item_name           TEXT,
    account_id          TEXT,
    account_name        TEXT,
    tax_code_ref        TEXT,
    detail_type         TEXT,
    PRIMARY KEY (client_id, estimate_id, line_id)
);

-- Generic fact tables for additional transaction types
CREATE TABLE IF NOT EXISTS fact_bill_payment (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

CREATE TABLE IF NOT EXISTS fact_deposit (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

CREATE TABLE IF NOT EXISTS fact_credit_memo (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

CREATE TABLE IF NOT EXISTS fact_refund_receipt (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

CREATE TABLE IF NOT EXISTS fact_sales_receipt (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

CREATE TABLE IF NOT EXISTS fact_journal_entry (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

CREATE TABLE IF NOT EXISTS fact_transfer (
    client_id           TEXT NOT NULL,
    id                  TEXT NOT NULL,
    name                TEXT,
    fully_qualified_name TEXT,
    is_active           INTEGER,
    PRIMARY KEY (client_id, id)
);

-- RLS user-to-client mapping for Power BI
CREATE TABLE IF NOT EXISTS security_user_client_map (
    user_email          TEXT NOT NULL,
    client_id           TEXT NOT NULL,
    PRIMARY KEY (user_email, client_id)
);
