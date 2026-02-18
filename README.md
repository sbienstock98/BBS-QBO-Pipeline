# BBS QBO Pipeline

Multi-client QuickBooks Online to Power BI ETL pipeline built on Azure Functions. Extracts entity and report data from the QBO API, transforms it into a star schema, and loads it into Azure SQL for Power BI reporting.

## Architecture

```
QuickBooks Online API
        |
   [Extract] ─── OAuth 2.0 token management (Azure Key Vault)
        |
   [Transform] ─ Flatten nested JSON, map to star schema, enforce types
        |
   [Load] ────── Azure SQL (prod) / SQLite (dev)
        |
   [Archive] ─── Raw JSON → Azure Blob Storage
        |
   Power BI ──── DirectQuery with row-level security per client
```

**Runtime:** Azure Functions (timer-triggered, daily at 6:00 AM ET)
**Target cost:** ~$15-35/month on Azure SQL Serverless + Functions Consumption plan

## Project Structure

```
bayshore-qbo-pipeline/
├── function_app.py              # Azure Functions entry point
├── host.json                    # Azure Functions host config
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment variable template
│
├── auth/
│   ├── oauth_manager.py         # Token refresh & Key Vault storage
│   └── qbo_auth_flow.py         # Interactive OAuth 2.0 consent flow
│
├── config/
│   ├── settings.py              # Pydantic settings (env vars)
│   └── tables.py                # Entity tables, report endpoints, schema mapping
│
├── extract/
│   ├── qbo_client.py            # QBO API client with rate limiting
│   ├── entity_extractor.py      # SELECT * pagination for 24 entity tables
│   └── report_extractor.py      # Financial report extraction (P&L, BS, etc.)
│
├── transform/
│   ├── flatteners.py            # Flatten nested line items (Invoice, Bill, etc.)
│   ├── schema_mapper.py         # Map raw QBO JSON → star schema columns
│   └── data_quality.py          # Type enforcement, dedup, date key generation
│
├── load/
│   ├── sql_loader.py            # Upsert into Azure SQL / SQLite
│   └── raw_archiver.py          # Archive raw JSON to Blob Storage
│
├── orchestrator/
│   └── pipeline.py              # Main ETL orchestrator (extract → transform → load)
│
├── scripts/
│   ├── create_schema.sql        # Star schema DDL (dims + facts)
│   ├── populate_dim_date.sql    # Date dimension population
│   ├── onboard_client.py        # Client onboarding (OAuth + registration)
│   └── backfill.py              # Historical data backfill
│
└── powerbi/
    ├── dax_measures.dax         # DAX measures for Power BI report
    ├── power_query_azure_sql.m  # Power Query for Azure SQL connection
    └── power_query_sqlite.m     # Power Query for local SQLite (dev)
```

## Data Model

### Dimensions (12 tables)
`dim_client` · `dim_date` · `dim_account` · `dim_customer` · `dim_vendor` · `dim_item` · `dim_employee` · `dim_class` · `dim_department` · `dim_tax_code` · `dim_tax_rate` · `dim_term` · `dim_payment_method` · `dim_company_info`

### Facts (8 header + 5 line-item tables)
- **Invoices:** `fact_invoice` + `fact_invoice_line`
- **Bills:** `fact_bill` + `fact_bill_line`
- **Purchases:** `fact_purchase` + `fact_purchase_line`
- **Payments:** `fact_payment` + `fact_payment_line`
- **Estimates:** `fact_estimate` + `fact_estimate_line`
- **Other:** `fact_bill_payment` · `fact_deposit` · `fact_credit_memo` · `fact_refund_receipt` · `fact_sales_receipt` · `fact_journal_entry` · `fact_transfer`

### Reports (9 endpoints)
P&L (Accrual) · P&L (Cash) · Balance Sheet · Cash Flow · AR Aging · AR Detail · AP Aging · AP Detail · Trial Balance

All tables are isolated by `client_id` for multi-tenant support with Power BI row-level security.

## Setup

### Prerequisites
- Python 3.11+
- An Intuit Developer account with a QuickBooks app ([developer.intuit.com](https://developer.intuit.com))
- Azure subscription (for production deployment)

### Local Development

1. **Clone and install dependencies:**
   ```bash
   git clone https://github.com/sbienstock98/BBS-QBO-Pipeline.git
   cd BBS-QBO-Pipeline
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your QBO app credentials
   ```

3. **Initialize the database and onboard a client:**
   ```bash
   python scripts/onboard_client.py \
     --client-id pilot_001 \
     --name "Client Name" \
     --init-db
   ```
   This will open a browser for the QBO OAuth consent flow, create the schema tables, and register the client.

4. **Run the pipeline:**
   ```bash
   python -m orchestrator.pipeline --client-id pilot_001
   ```

### Azure Deployment

1. Create an Azure Function App (Python 3.11, Consumption plan)
2. Create an Azure SQL Serverless database
3. Create an Azure Key Vault for OAuth tokens
4. Create a Storage Account for raw JSON archives
5. Set the environment variables from `.env.example` in the Function App configuration
6. Deploy the function app code

## Configuration

Key settings in `.env`:

| Variable | Description |
|---|---|
| `QBO_CLIENT_ID` | Intuit app OAuth client ID |
| `QBO_CLIENT_SECRET` | Intuit app OAuth client secret |
| `QBO_REDIRECT_URI` | OAuth callback URL (default: `http://localhost:8080/callback`) |
| `QBO_BASE_URL` | API base URL (sandbox or production) |
| `AZURE_SQL_CONNECTION_STRING` | Azure SQL connection string |
| `AZURE_KEY_VAULT_URL` | Key Vault URL for token storage |
| `AZURE_STORAGE_CONNECTION_STRING` | Blob Storage for raw JSON archives |
| `DB_BACKEND` | `sqlite` (local dev) or `azure_sql` (production) |
| `TOKEN_STORAGE` | `local` (file-based) or `keyvault` (Azure Key Vault) |

## License

Proprietary - Bayshore Business Solutions
