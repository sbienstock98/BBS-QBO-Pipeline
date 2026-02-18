"""Flatten nested QBO JSON into tabular DataFrames.

Each QBO transaction type (Invoice, Bill, Purchase, etc.) contains a `Line`
array with nested detail objects. These flatteners produce one row per line
item, suitable for loading into star-schema fact tables.
"""

import pandas as pd


def flatten_invoice_lines(invoices: list[dict]) -> pd.DataFrame:
    """Flatten Invoice -> Line -> SalesItemLineDetail into one row per line item."""
    rows = []
    for inv in invoices:
        invoice_id = inv.get("Id")
        for line in inv.get("Line", []):
            detail_type = line.get("DetailType", "")
            if detail_type in ("SubTotalLineDetail", "DiscountLineDetail"):
                continue
            detail = line.get("SalesItemLineDetail", {})
            item_ref = detail.get("ItemRef", {})
            account_ref = detail.get("ItemAccountRef", {})
            linked = line.get("LinkedTxn", [])

            rows.append({
                "invoice_id": invoice_id,
                "line_id": line.get("Id"),
                "line_num": line.get("LineNum"),
                "description": line.get("Description"),
                "amount": line.get("Amount"),
                "quantity": detail.get("Qty"),
                "unit_price": detail.get("UnitPrice"),
                "item_id": item_ref.get("value"),
                "item_name": item_ref.get("name"),
                "account_id": account_ref.get("value"),
                "account_name": account_ref.get("name"),
                "tax_code_ref": detail.get("TaxCodeRef", {}).get("value"),
                "service_date": detail.get("ServiceDate"),
                "detail_type": detail_type,
                "linked_txn_id": linked[0]["TxnId"] if linked else None,
                "linked_txn_type": linked[0]["TxnType"] if linked else None,
            })
    return pd.DataFrame(rows)


def flatten_bill_lines(bills: list[dict]) -> pd.DataFrame:
    """Flatten Bill -> Line into one row per line item.

    Bills can have either AccountBasedExpenseLineDetail or ItemBasedExpenseLineDetail.
    """
    rows = []
    for bill in bills:
        bill_id = bill.get("Id")
        for line in bill.get("Line", []):
            detail_type = line.get("DetailType", "")
            row = {
                "bill_id": bill_id,
                "line_id": line.get("Id"),
                "line_num": line.get("LineNum"),
                "description": line.get("Description"),
                "amount": line.get("Amount"),
                "detail_type": detail_type,
                "quantity": None,
                "unit_price": None,
                "item_id": None,
                "item_name": None,
                "account_id": None,
                "account_name": None,
                "billable_status": None,
                "customer_id": None,
                "customer_name": None,
                "tax_code_ref": None,
            }

            if detail_type == "ItemBasedExpenseLineDetail":
                detail = line.get("ItemBasedExpenseLineDetail", {})
                item_ref = detail.get("ItemRef", {})
                row["quantity"] = detail.get("Qty")
                row["unit_price"] = detail.get("UnitPrice")
                row["item_id"] = item_ref.get("value")
                row["item_name"] = item_ref.get("name")
                row["billable_status"] = detail.get("BillableStatus")
                row["tax_code_ref"] = detail.get("TaxCodeRef", {}).get("value")
                cust = detail.get("CustomerRef", {})
                row["customer_id"] = cust.get("value")
                row["customer_name"] = cust.get("name")

            elif detail_type == "AccountBasedExpenseLineDetail":
                detail = line.get("AccountBasedExpenseLineDetail", {})
                acct_ref = detail.get("AccountRef", {})
                row["account_id"] = acct_ref.get("value")
                row["account_name"] = acct_ref.get("name")
                row["billable_status"] = detail.get("BillableStatus")
                row["tax_code_ref"] = detail.get("TaxCodeRef", {}).get("value")
                cust = detail.get("CustomerRef", {})
                row["customer_id"] = cust.get("value")
                row["customer_name"] = cust.get("name")
            else:
                continue  # Skip subtotals and other non-data lines

            rows.append(row)
    return pd.DataFrame(rows)


def flatten_purchase_lines(purchases: list[dict]) -> pd.DataFrame:
    """Flatten Purchase -> Line into one row per line item.

    Purchases (checks, credit card charges, cash) have the same line
    detail types as Bills.
    """
    rows = []
    for purchase in purchases:
        purchase_id = purchase.get("Id")
        for line in purchase.get("Line", []):
            detail_type = line.get("DetailType", "")
            row = {
                "purchase_id": purchase_id,
                "line_id": line.get("Id"),
                "description": line.get("Description"),
                "amount": line.get("Amount"),
                "detail_type": detail_type,
                "item_id": None,
                "item_name": None,
                "account_id": None,
                "account_name": None,
                "billable_status": None,
                "customer_id": None,
                "tax_code_ref": None,
            }

            if detail_type == "ItemBasedExpenseLineDetail":
                detail = line.get("ItemBasedExpenseLineDetail", {})
                item_ref = detail.get("ItemRef", {})
                row["item_id"] = item_ref.get("value")
                row["item_name"] = item_ref.get("name")
                row["billable_status"] = detail.get("BillableStatus")
                row["tax_code_ref"] = detail.get("TaxCodeRef", {}).get("value")
                cust = detail.get("CustomerRef", {})
                row["customer_id"] = cust.get("value")

            elif detail_type == "AccountBasedExpenseLineDetail":
                detail = line.get("AccountBasedExpenseLineDetail", {})
                acct_ref = detail.get("AccountRef", {})
                row["account_id"] = acct_ref.get("value")
                row["account_name"] = acct_ref.get("name")
                row["billable_status"] = detail.get("BillableStatus")
                row["tax_code_ref"] = detail.get("TaxCodeRef", {}).get("value")
                cust = detail.get("CustomerRef", {})
                row["customer_id"] = cust.get("value")
            else:
                continue

            rows.append(row)
    return pd.DataFrame(rows)


def flatten_payment_lines(payments: list[dict]) -> pd.DataFrame:
    """Flatten Payment -> Line into one row per linked invoice payment."""
    rows = []
    for pmt in payments:
        payment_id = pmt.get("Id")
        for line in pmt.get("Line", []):
            linked = line.get("LinkedTxn", [])
            # Extract metadata from LineEx NameValue pairs
            line_ex = line.get("LineEx", {}).get("any", [])
            ex_values = {}
            for item in line_ex:
                val = item.get("value", {})
                ex_values[val.get("Name", "")] = val.get("Value", "")

            rows.append({
                "payment_id": payment_id,
                "amount": line.get("Amount"),
                "linked_invoice_id": linked[0]["TxnId"] if linked else None,
                "linked_txn_type": linked[0]["TxnType"] if linked else None,
                "original_open_balance": ex_values.get("txnOpenBalance"),
                "invoice_doc_number": ex_values.get("txnReferenceNumber"),
            })
    return pd.DataFrame(rows)


def flatten_estimate_lines(estimates: list[dict]) -> pd.DataFrame:
    """Flatten Estimate -> Line -> SalesItemLineDetail."""
    rows = []
    for est in estimates:
        estimate_id = est.get("Id")
        for line in est.get("Line", []):
            detail_type = line.get("DetailType", "")
            if detail_type in ("SubTotalLineDetail", "DiscountLineDetail"):
                continue
            detail = line.get("SalesItemLineDetail", {})
            item_ref = detail.get("ItemRef", {})
            account_ref = detail.get("ItemAccountRef", {})

            rows.append({
                "estimate_id": estimate_id,
                "line_id": line.get("Id"),
                "line_num": line.get("LineNum"),
                "description": line.get("Description"),
                "amount": line.get("Amount"),
                "quantity": detail.get("Qty"),
                "unit_price": detail.get("UnitPrice"),
                "item_id": item_ref.get("value"),
                "item_name": item_ref.get("name"),
                "account_id": account_ref.get("value"),
                "account_name": account_ref.get("name"),
                "tax_code_ref": detail.get("TaxCodeRef", {}).get("value"),
                "detail_type": detail_type,
            })
    return pd.DataFrame(rows)
