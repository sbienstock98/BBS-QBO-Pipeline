"""Map raw QBO API records to star-schema dimension and fact table columns."""

import pandas as pd


def map_account(records: list[dict]) -> pd.DataFrame:
    """Map Account records to dim_account schema."""
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    return pd.DataFrame({
        "account_id": df["Id"],
        "account_name": df["Name"],
        "fully_qualified_name": df["FullyQualifiedName"],
        "classification": df.get("Classification"),
        "account_type": df.get("AccountType"),
        "account_sub_type": df.get("AccountSubType"),
        "is_sub_account": df.get("SubAccount", False),
        "parent_account_id": df.get("ParentRef.value")
        if "ParentRef.value" in df.columns
        else df.apply(
            lambda r: r.get("ParentRef", {}).get("value") if isinstance(r.get("ParentRef"), dict) else None,
            axis=1,
        ),
        "is_active": df.get("Active", True),
        "current_balance": df.get("CurrentBalance", 0),
        "currency_code": df.apply(
            lambda r: r.get("CurrencyRef", {}).get("value", "USD") if isinstance(r.get("CurrencyRef"), dict) else "USD",
            axis=1,
        ),
    })


def map_customer(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        bill = r.get("BillAddr", {}) or {}
        rows.append({
            "customer_id": r.get("Id"),
            "display_name": r.get("DisplayName"),
            "company_name": r.get("CompanyName"),
            "given_name": r.get("GivenName"),
            "family_name": r.get("FamilyName"),
            "email": (r.get("PrimaryEmailAddr") or {}).get("Address"),
            "phone": (r.get("PrimaryPhone") or {}).get("FreeFormNumber"),
            "billing_city": bill.get("City"),
            "billing_state": bill.get("CountrySubDivisionCode"),
            "billing_postal_code": bill.get("PostalCode"),
            "is_job": r.get("Job", False),
            "is_active": r.get("Active", True),
            "balance": r.get("Balance", 0),
            "parent_customer_id": (r.get("ParentRef") or {}).get("value"),
        })
    return pd.DataFrame(rows)


def map_vendor(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "vendor_id": r.get("Id"),
            "display_name": r.get("DisplayName"),
            "company_name": r.get("CompanyName"),
            "email": (r.get("PrimaryEmailAddr") or {}).get("Address"),
            "phone": (r.get("PrimaryPhone") or {}).get("FreeFormNumber"),
            "is_1099": r.get("Vendor1099", False),
            "is_active": r.get("Active", True),
            "balance": r.get("Balance", 0),
        })
    return pd.DataFrame(rows)


def map_item(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "item_id": r.get("Id"),
            "item_name": r.get("Name"),
            "description": r.get("Description"),
            "item_type": r.get("Type"),
            "unit_price": r.get("UnitPrice"),
            "purchase_cost": r.get("PurchaseCost"),
            "is_active": r.get("Active", True),
            "income_account_id": (r.get("IncomeAccountRef") or {}).get("value"),
            "expense_account_id": (r.get("ExpenseAccountRef") or {}).get("value"),
            "track_qty": r.get("TrackQtyOnHand", False),
            "qty_on_hand": r.get("QtyOnHand"),
        })
    return pd.DataFrame(rows)


def map_employee(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "employee_id": r.get("Id"),
            "display_name": r.get("DisplayName"),
            "given_name": r.get("GivenName"),
            "family_name": r.get("FamilyName"),
            "hired_date": r.get("HiredDate"),
            "is_active": r.get("Active", True),
        })
    return pd.DataFrame(rows)


def map_invoice(records: list[dict]) -> pd.DataFrame:
    """Map Invoice records to fact_invoice (header level, one row per invoice)."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        tax_detail = r.get("TxnTaxDetail", {}) or {}
        rows.append({
            "invoice_id": r.get("Id"),
            "doc_number": r.get("DocNumber"),
            "txn_date": r.get("TxnDate"),
            "due_date": r.get("DueDate"),
            "customer_id": (r.get("CustomerRef") or {}).get("value"),
            "total_amount": r.get("TotalAmt"),
            "balance": r.get("Balance"),
            "total_tax": tax_detail.get("TotalTax", 0),
            "email_status": r.get("EmailStatus"),
            "print_status": r.get("PrintStatus"),
        })
    return pd.DataFrame(rows)


def map_bill(records: list[dict]) -> pd.DataFrame:
    """Map Bill records to fact_bill (header level)."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "bill_id": r.get("Id"),
            "txn_date": r.get("TxnDate"),
            "due_date": r.get("DueDate"),
            "vendor_id": (r.get("VendorRef") or {}).get("value"),
            "total_amount": r.get("TotalAmt"),
            "balance": r.get("Balance"),
        })
    return pd.DataFrame(rows)


def map_payment(records: list[dict]) -> pd.DataFrame:
    """Map Payment records to fact_payment (header level)."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "payment_id": r.get("Id"),
            "txn_date": r.get("TxnDate"),
            "total_amount": r.get("TotalAmt"),
            "customer_id": (r.get("CustomerRef") or {}).get("value"),
            "deposit_to_account_id": (r.get("DepositToAccountRef") or {}).get("value"),
            "payment_method_id": (r.get("PaymentMethodRef") or {}).get("value"),
            "unapplied_amount": r.get("UnappliedAmt", 0),
        })
    return pd.DataFrame(rows)


def map_purchase(records: list[dict]) -> pd.DataFrame:
    """Map Purchase records to fact_purchase (header level)."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "purchase_id": r.get("Id"),
            "txn_date": r.get("TxnDate"),
            "payment_type": r.get("PaymentType"),
            "total_amount": r.get("TotalAmt"),
            "account_id": (r.get("AccountRef") or {}).get("value"),
            "vendor_id": (r.get("EntityRef") or {}).get("value"),
            "vendor_type": (r.get("EntityRef") or {}).get("type"),
            "is_credit": r.get("Credit", False),
            "doc_number": r.get("DocNumber"),
        })
    return pd.DataFrame(rows)


def map_estimate(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        linked = r.get("LinkedTxn", [])
        rows.append({
            "estimate_id": r.get("Id"),
            "doc_number": r.get("DocNumber"),
            "txn_date": r.get("TxnDate"),
            "customer_id": (r.get("CustomerRef") or {}).get("value"),
            "total_amount": r.get("TotalAmt"),
            "txn_status": r.get("TxnStatus"),
            "linked_invoice_id": next(
                (t["TxnId"] for t in linked if t.get("TxnType") == "Invoice"),
                None,
            ),
        })
    return pd.DataFrame(rows)


def map_generic_dimension(records: list[dict], id_field: str = "Id", name_field: str = "Name") -> pd.DataFrame:
    """Generic mapper for simple dimension tables (Class, Department, Term, etc.)."""
    if not records:
        return pd.DataFrame()
    rows = []
    for r in records:
        rows.append({
            "id": r.get(id_field),
            "name": r.get(name_field),
            "fully_qualified_name": r.get("FullyQualifiedName", r.get(name_field)),
            "is_active": r.get("Active", True),
        })
    return pd.DataFrame(rows)


# Registry mapping table names to their mapper functions
MAPPER_REGISTRY = {
    "Account": map_account,
    "Customer": map_customer,
    "Vendor": map_vendor,
    "Item": map_item,
    "Employee": map_employee,
    "Invoice": map_invoice,
    "Bill": map_bill,
    "Payment": map_payment,
    "Purchase": map_purchase,
    "Estimate": map_estimate,
}


def map_to_schema(table: str, records: list[dict]) -> pd.DataFrame:
    """Map raw QBO records to star schema using the appropriate mapper."""
    mapper = MAPPER_REGISTRY.get(table)
    if mapper:
        return mapper(records)
    # Fallback to generic dimension mapping for simple tables
    return map_generic_dimension(records)
