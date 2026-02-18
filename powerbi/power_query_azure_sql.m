// ============================================================
// Power Query M - Azure SQL Connection (Production)
// ============================================================
// In Power BI Desktop: Home > Get Data > Azure SQL Database
// Server: bayshore-qbo-sql.database.windows.net
// Database: qbo-warehouse
// Then replace each table's source with the queries below.
// ============================================================

// --- dim_date ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    dim_date = Source{[Schema="dbo", Item="dim_date"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(dim_date, {
        {"full_date", type date},
        {"date_key", Int64.Type}
    })
in
    #"Changed Types"

// --- dim_account ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    dim_account = Source{[Schema="dbo", Item="dim_account"]}[Data]
in
    dim_account

// --- dim_customer ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    dim_customer = Source{[Schema="dbo", Item="dim_customer"]}[Data]
in
    dim_customer

// --- dim_vendor ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    dim_vendor = Source{[Schema="dbo", Item="dim_vendor"]}[Data]
in
    dim_vendor

// --- dim_item ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    dim_item = Source{[Schema="dbo", Item="dim_item"]}[Data]
in
    dim_item

// --- fact_invoice ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_invoice = Source{[Schema="dbo", Item="fact_invoice"]}[Data]
in
    fact_invoice

// --- fact_invoice_line ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_invoice_line = Source{[Schema="dbo", Item="fact_invoice_line"]}[Data]
in
    fact_invoice_line

// --- fact_payment ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_payment = Source{[Schema="dbo", Item="fact_payment"]}[Data]
in
    fact_payment

// --- fact_bill ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_bill = Source{[Schema="dbo", Item="fact_bill"]}[Data]
in
    fact_bill

// --- fact_bill_line ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_bill_line = Source{[Schema="dbo", Item="fact_bill_line"]}[Data]
in
    fact_bill_line

// --- fact_purchase ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_purchase = Source{[Schema="dbo", Item="fact_purchase"]}[Data]
in
    fact_purchase

// --- fact_estimate ---
let
    Source = Sql.Database("bayshore-qbo-sql.database.windows.net", "qbo-warehouse"),
    fact_estimate = Source{[Schema="dbo", Item="fact_estimate"]}[Data]
in
    fact_estimate
