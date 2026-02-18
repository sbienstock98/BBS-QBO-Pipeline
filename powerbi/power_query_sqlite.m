// ============================================================
// Power Query M - SQLite Connection (Local Development)
// ============================================================
// In Power BI Desktop: Home > Get Data > ODBC
// Use the SQLite ODBC driver pointed at data/dev.db
// Then replace each table's source with the queries below.
// ============================================================

// --- dim_date ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    dim_date = Source{[Name="dim_date"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(dim_date, {
        {"date_key", Int64.Type},
        {"full_date", type date},
        {"day_of_week", Int64.Type},
        {"day_of_month", Int64.Type},
        {"month_num", Int64.Type},
        {"quarter_num", Int64.Type},
        {"year_num", Int64.Type},
        {"is_weekend", Int64.Type}
    })
in
    #"Changed Types"

// --- dim_account ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    dim_account = Source{[Name="dim_account"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(dim_account, {
        {"current_balance", Currency.Type},
        {"is_active", Int64.Type},
        {"is_sub_account", Int64.Type}
    })
in
    #"Changed Types"

// --- dim_customer ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    dim_customer = Source{[Name="dim_customer"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(dim_customer, {
        {"balance", Currency.Type},
        {"is_active", Int64.Type}
    })
in
    #"Changed Types"

// --- dim_vendor ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    dim_vendor = Source{[Name="dim_vendor"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(dim_vendor, {
        {"balance", Currency.Type},
        {"is_active", Int64.Type}
    })
in
    #"Changed Types"

// --- dim_item ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    dim_item = Source{[Name="dim_item"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(dim_item, {
        {"unit_price", Currency.Type},
        {"purchase_cost", Currency.Type}
    })
in
    #"Changed Types"

// --- fact_invoice ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_invoice = Source{[Name="fact_invoice"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_invoice, {
        {"txn_date", type date},
        {"due_date", type date},
        {"total_amount", Currency.Type},
        {"balance", Currency.Type},
        {"total_tax", Currency.Type},
        {"txn_date_key", Int64.Type},
        {"due_date_key", Int64.Type}
    })
in
    #"Changed Types"

// --- fact_invoice_line ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_invoice_line = Source{[Name="fact_invoice_line"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_invoice_line, {
        {"amount", Currency.Type},
        {"quantity", type number},
        {"unit_price", Currency.Type}
    })
in
    #"Changed Types"

// --- fact_payment ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_payment = Source{[Name="fact_payment"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_payment, {
        {"txn_date", type date},
        {"total_amount", Currency.Type},
        {"unapplied_amount", Currency.Type},
        {"txn_date_key", Int64.Type}
    })
in
    #"Changed Types"

// --- fact_bill ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_bill = Source{[Name="fact_bill"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_bill, {
        {"txn_date", type date},
        {"due_date", type date},
        {"total_amount", Currency.Type},
        {"balance", Currency.Type},
        {"txn_date_key", Int64.Type},
        {"due_date_key", Int64.Type}
    })
in
    #"Changed Types"

// --- fact_bill_line ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_bill_line = Source{[Name="fact_bill_line"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_bill_line, {
        {"amount", Currency.Type},
        {"quantity", type number},
        {"unit_price", Currency.Type}
    })
in
    #"Changed Types"

// --- fact_purchase ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_purchase = Source{[Name="fact_purchase"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_purchase, {
        {"txn_date", type date},
        {"total_amount", Currency.Type},
        {"txn_date_key", Int64.Type}
    })
in
    #"Changed Types"

// --- fact_estimate ---
let
    Source = Odbc.DataSource("dsn=SQLite3 Datasource", [HierarchicalNavigation=true]),
    fact_estimate = Source{[Name="fact_estimate"]}[Data],
    #"Changed Types" = Table.TransformColumnTypes(fact_estimate, {
        {"txn_date", type date},
        {"total_amount", Currency.Type},
        {"txn_date_key", Int64.Type}
    })
in
    #"Changed Types"
