import json
import time

import frappe
from frappe.utils.background_jobs import get_jobs

logger = frappe.logger("payment_reconciliation")
logger.setLevel("DEBUG")


from erpnext.accounts.doctype.payment_reconciliation.payment_reconciliation import (
    PaymentReconciliation,
)
from erpnext.accounts.party import get_party_account
from frappe.utils.scheduler import is_scheduler_disabled, is_scheduler_inactive


@frappe.whitelist()
def start_reconciliation(company="Esnad"):

    if is_scheduler_disabled() or is_scheduler_inactive():
        frappe.throw("Please Enable The Scheduler")

    # Check if already in progress
    progress = frappe.get_single("Reconciliation Progress")
    if progress.status == "In Progress":
        frappe.msgprint("Reconciliation already in progress.")
        return

    customers = get_customers_to_reconcile(company)
    if not customers:
        frappe.msgprint("No customers to reconcile.")
        return

    # Clear old queue (if any) and populate new
    progress.status = "In Progress"
    progress.total_customers = len(customers)
    progress.processed_customers = 0
    progress.current_queue = []  # clear child table
    for cust in customers:
        progress.append("current_queue", {"customer": cust, "status": "Pending"})
    progress.company = company
    progress.save(ignore_permissions=True)

    frappe.msgprint(f"Reconciliation started for {len(customers)} customers.")


def get_customers_to_reconcile(company):
    """
    Return list of customers having outstanding invoices or unallocated payments
    """
    customers = set()

    # TODO Let the user set the company
    oustanding_invoices = frappe.db.sql(
        """
        SELECT DISTINCT customer
        FROM `tabSales Invoice`
        WHERE docstatus = 1 AND outstanding_amount > 0 AND company = %s
        """,
        company,
    )

    # Unallocated Payments
    unallocated_payments = frappe.db.sql(
        """
        SELECT DISTINCT party
        FROM `tabPayment Entry`
        WHERE docstatus = 1 AND unallocated_amount > 0
        AND party_type = 'Customer' AND company = %s
        """,
        company,
    )
    for (cust,) in oustanding_invoices:
        customers.add(cust)
    for (party,) in unallocated_payments:
        customers.add(party)

    customers = list(customers)
    return customers


def process_batch():
    progress = frappe.get_single("Reconciliation Progress")
    if progress.status != "In Progress":
        return

    # Reset stuck "Processing" items older than 30 minutes
    frappe.db.sql(
        """
        UPDATE `tabReconciliation Queue`
        SET status = 'Pending'
        WHERE parent = %s AND status = 'Processing' AND modified < NOW() - INTERVAL 30 MINUTE
    """,
        progress.name,
    )
    frappe.db.commit()

    # Fetch next 50 pending items
    pending_items = frappe.get_all(
        "Reconciliation Queue",
        filters={"parent": progress.name, "status": "Pending"},
        fields=["name", "customer"],
        limit=50,
    )
    if not pending_items:
        progress.status = "Completed"
        progress.save(ignore_permissions=True)
        frappe.db.commit()
        return

    # Mark them as Processing via direct update
    for item in pending_items:
        frappe.db.set_value("Reconciliation Queue", item.name, "status", "Processing")
    frappe.db.commit()

    company = progress.company
    receivable_account = frappe.db.get_value(
        "Company", company, "default_receivable_account"
    )

    for item in pending_items:
        try:
            success, log_name = reconcile_customer(
                item.customer, company, receivable_account
            )
            new_status = "Completed" if success else "Failed"
            frappe.db.set_value(
                "Reconciliation Queue",
                item.name,
                {"status": new_status, "log_reference": log_name, "last_error": None},
            )
        except Exception as e:
            frappe.db.set_value(
                "Reconciliation Queue",
                item.name,
                {"status": "Failed", "last_error": str(e)[:140]},
            )
        # Increment parent's processed counter – use set_value on the Single doctype
        frappe.db.set_value(
            "Reconciliation Progress",
            "Reconciliation Progress",
            "processed_customers",
            progress.processed_customers + 1,
        )
        frappe.db.commit()
        time.sleep(1)


def reconcile_customer(customer, company, receivable_account, party_type="Customer"):
    time.sleep(0.2)  # gentle pacing
    n_payments = n_invoices = 0
    success = False
    log = None
    error = None
    try:
        pr = PaymentReconciliation(
            {
                "doctype": "Payment Reconciliation",
                "company": company,
                "party_type": party_type,
                "party": customer,
                "receivable_payable_account": receivable_account,
            }
        )
        pr.get_unreconciled_entries()
        n_payments = len(pr.payments)
        n_invoices = len(pr.invoices)

        if not pr.payments or not pr.invoices:
            logger.debug("No payments or invoices for %s", customer)
            log = log_customer(customer, False, n_invoices, n_payments)
            return False, log.name

        invoices_data = [
            {
                "invoice_type": inv.invoice_type,
                "invoice_number": inv.invoice_number,
                "outstanding_amount": inv.outstanding_amount,
                "invoice_date": inv.invoice_date,
                "currency": inv.currency,
            }
            for inv in pr.invoices
        ]

        payments_data = [
            {
                "reference_type": pay.reference_type,
                "reference_name": pay.reference_name,
                "reference_row": pay.reference_row,
                "amount": pay.amount,
                "posting_date": pay.posting_date,
                "currency": pay.currency,
                "is_advance": pay.is_advance,
                "cost_center": pay.cost_center,
            }
            for pay in pr.payments
        ]

        try:
            pr.allocate_entries(
                frappe._dict({"invoices": invoices_data, "payments": payments_data})
            )
            logger.info("Allocated entries for %s", customer)
            pr.reconcile_allocations()
            frappe.db.commit()
            success = True
        except Exception as e:
            frappe.db.rollback()
            logger.warning("Allocation failed for %s", customer)
            error = e
            success = False

        log = log_customer(customer, success, n_invoices, n_payments, error)
        return success, log.name

    except Exception as e:
        logger.error("Unexpected error for %s: %s", customer, e)
        log = log_customer(customer, False, 0, 0, e)
        return False, log.name

def get_recievable_payable_acc(party, company, party_type="Customer"):

    try:
        rec_pay_acc = get_party_account(party_type, party, company)
        logger.debug("Party Account %s", party)

    except Exception as e:
        logger.error(
            "Couldn't get the party account with party_type: %s. party %s. company %s.",
            party_type,
            party,
            company,
        )

    return rec_pay_acc


def log_customer(customer, success, n_invoices, n_payments, e=None):
    log = frappe.get_doc(
        {
            "doctype": "Bulk Payment Reconciliation Log",
            "customer": customer,
            "status": "Success" if success else "Failed",
            "error_message": str(e) if not success else None,
            "invoices_processed": n_invoices,
            "payments_processed": n_payments,
        }
    )
    log.insert(ignore_permissions=True)
    return log
