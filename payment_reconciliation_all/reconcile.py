"""
Bulk payment reconciliation for all customers.

Orchestrates the end-to-end process:
  1. `start_reconciliation` – collects eligible customers and seeds the queue.
  2. `process_batch`        – called by the scheduler every 5 min; works through
                              the queue in chunks of 50.
  3. `reconcile_customer`  – runs ERPNext's PaymentReconciliation for one customer
                              and writes a Bulk Payment Reconciliation Log entry.

TODO: ignore payments/invoices with a zero amount from reconciliation.
"""

import time

import frappe
from erpnext.accounts.doctype.payment_reconciliation.payment_reconciliation import (
    PaymentReconciliation,
)
from frappe.utils.scheduler import is_scheduler_disabled, is_scheduler_inactive

logger = frappe.logger("payment_reconciliation")

_BATCH_SIZE = 50
_STUCK_THRESHOLD_MINUTES = 30


@frappe.whitelist()
def start_reconciliation(company="Esnad"):
    """Seed the reconciliation queue and kick off processing.

    Guards against double-starts by checking the Reconciliation Progress
    singleton status. Clears any previous queue before populating a fresh one.

    Args:
        company (str): ERPNext Company name to reconcile. Defaults to "Esnad".
    """
    if is_scheduler_disabled() or is_scheduler_inactive():
        frappe.throw("Please enable the scheduler before starting reconciliation.")

    progress = frappe.get_single("Reconciliation Progress")
    if progress.status == "In Progress":
        frappe.msgprint("Reconciliation is already in progress.")
        return

    customers = get_customers_to_reconcile(company)
    if not customers:
        frappe.msgprint("No customers need reconciliation.")
        return

    # Reset the singleton and build a fresh queue.
    progress.status = "In Progress"
    progress.company = company
    progress.total_customers = len(customers)
    progress.processed_customers = 0
    progress.current_queue = []  # clears the child table
    for customer in customers:
        progress.append("current_queue", {"customer": customer, "status": "Pending"})
    progress.save(ignore_permissions=True)

    frappe.msgprint(f"Reconciliation started for {len(customers)} customers.")


def get_customers_to_reconcile(company):
    """Return a deduplicated list of customers that need reconciliation.

    A customer qualifies if they have both:
    - Submitted Sales Invoice with a positive outstanding amount, and
    - Submitted Payment Entry with a positive unallocated amount.

    Args:
        company (str): ERPNext Company name used to filter both queries.

    Returns:
        list[str]: Customer names (no duplicates, order not guaranteed).
    """
    outstanding_invoice_rows = frappe.db.sql(
        """
        SELECT DISTINCT customer
        FROM `tabSales Invoice`
        WHERE docstatus = 1
          AND outstanding_amount > 0
          AND company = %s
        """,
        company,
    )

    unallocated_payment_rows = frappe.db.sql(
        """
        SELECT DISTINCT party
        FROM `tabPayment Entry`
        WHERE docstatus = 1
          AND unallocated_amount > 0
          AND party_type = 'Customer'
          AND company = %s
        """,
        company,
    )

    customers_out = {row[0] for row in outstanding_invoice_rows}
    customers = {row[0] for row in unallocated_payment_rows if row[0] in customers_out}
    return list(customers)


def process_batch():
    """Process the next batch of customers from the reconciliation queue.

    Called by the scheduler every 5 minutes (see hooks.py). Each invocation:
      1. Resets queue items that have been stuck in "Processing" for more than
         `_STUCK_THRESHOLD_MINUTES`, so they are retried on the next run.
      2. Claims up to `_BATCH_SIZE` pending items by marking them "Processing".
      3. Reconciles each claimed customer and updates its queue row and the
         parent progress counter.
      4. Marks the overall run "Completed" when the queue is exhausted.

    Commits to the database after each individual customer so that partial
    progress is preserved even if the job is interrupted.
    """
    progress = frappe.get_single("Reconciliation Progress")
    if progress.status != "In Progress":
        return

    _reset_stuck_queue_items(progress.name)

    pending_queue_items = frappe.get_all(
        "Reconciliation Queue",
        filters={"parent": progress.name, "status": "Pending"},
        fields=["name", "customer"],
        limit=_BATCH_SIZE,
    )

    if not pending_queue_items:
        progress.status = "Completed"
        progress.save(ignore_permissions=True)
        frappe.db.commit()
        return

    # Claim the batch atomically before doing any work.
    for queue_item in pending_queue_items:
        frappe.db.set_value(
            "Reconciliation Queue", queue_item.name, "status", "Processing"
        )
    frappe.db.commit()

    company = progress.company
    receivable_account = frappe.db.get_value(
        "Company", company, "default_receivable_account"
    )

    for queue_item in pending_queue_items:
        _process_queue_item(queue_item, company, receivable_account)


def _reset_stuck_queue_items(progress_name):
    """Reset any queue items stuck in 'Processing' back to 'Pending'.

    This guards against items that were claimed but never finished (e.g. due to
    a worker crash) holding up the queue indefinitely.

    Args:
        progress_name (str): The `name` field of the parent Reconciliation Progress doc.
    """
    frappe.db.sql(
        """
        UPDATE `tabReconciliation Queue`
        SET status = 'Pending'
        WHERE parent = %s
          AND status = 'Processing'
          AND modified < NOW() - INTERVAL %s MINUTE
        """,
        (progress_name, _STUCK_THRESHOLD_MINUTES),
    )
    frappe.db.commit()


def _process_queue_item(queue_item, company, receivable_account):
    """Reconcile one customer queue item and persist the result.

    Args:
        queue_item: Frappe dict with `name` and `customer` fields.
        company (str): ERPNext Company name.
        receivable_account (str): Default receivable GL account for the company.
    """
    try:
        success, log_name = reconcile_customer(
            queue_item.customer, company, receivable_account
        )
        new_status = "Completed" if success else "Failed"
        frappe.db.set_value(
            "Reconciliation Queue",
            queue_item.name,
            {"status": new_status, "log_reference": log_name, "last_error": None},
        )
    except Exception as error:
        frappe.db.set_value(
            "Reconciliation Queue",
            queue_item.name,
            {"status": "Failed", "last_error": str(error)[:140]},
        )

    # Increment the processed counter. Single DocTypes are stored in tabSingles,
    # not in their own table, so we must use set_value (or the tabSingles ORM).
    # Sequential batch processing means there is no parallel stale-read risk.
    current = frappe.db.get_single_value("Reconciliation Progress", "processed_customers") or 0
    frappe.db.set_single_value("Reconciliation Progress", "processed_customers", current + 1)
    frappe.db.commit()
    time.sleep(1)  # gentle pacing to avoid overloading the database


def reconcile_customer(customer, company, receivable_account, party_type="Customer"):
    """Run ERPNext's payment reconciliation for a single customer.

    Fetches all unreconciled entries, allocates payments against invoices, and
    commits the reconciliation. Always writes a Bulk Payment Reconciliation Log
    entry regardless of outcome.

    Args:
        customer (str): Customer name (ERPNext party).
        company (str): ERPNext Company name.
        receivable_account (str): Receivable GL account for the company.
        party_type (str): ERPNext party type. Defaults to "Customer".

    Returns:
        tuple[bool, str]: (success, log_entry_name) where `success` is True
            only when allocations were committed without error.
    """
    time.sleep(0.2)  # gentle pacing

    invoice_count = payment_count = 0
    error = None

    try:
        reconciler = PaymentReconciliation(
            {
                "doctype": "Payment Reconciliation",
                "company": company,
                "party_type": party_type,
                "party": customer,
                "receivable_payable_account": receivable_account,
            }
        )
        reconciler.get_unreconciled_entries()

        invoice_count = len(reconciler.invoices)
        payment_count = len(reconciler.payments)

        if not reconciler.payments or not reconciler.invoices:
            logger.debug(
                "No payments or invoices for customer '%s' — skipping.", customer
            )
            log_entry = create_reconciliation_log(
                customer, False, invoice_count, payment_count
            )
            return False, log_entry.name

        invoices = [
            {
                "invoice_type": inv.invoice_type,
                "invoice_number": inv.invoice_number,
                "outstanding_amount": inv.outstanding_amount,
                "invoice_date": inv.invoice_date,
                "currency": inv.currency,
            }
            for inv in reconciler.invoices
        ]

        payments = [
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
            for pay in reconciler.payments
        ]

        try:
            reconciler.allocate_entries(
                frappe._dict({"invoices": invoices, "payments": payments})
            )
            logger.info("Entries allocated for customer '%s'.", customer)
            reconciler.reconcile_allocations()
            frappe.db.commit()
            success = True
        except Exception as alloc_error:
            frappe.db.rollback()
            logger.warning(
                "Allocation failed for customer '%s': %s", customer, alloc_error
            )
            error = alloc_error
            success = False

        log_entry = create_reconciliation_log(
            customer, success, invoice_count, payment_count, error
        )
        return success, log_entry.name

    except Exception as unexpected_error:
        logger.error(
            "Unexpected error for customer '%s': %s", customer, unexpected_error
        )
        log_entry = create_reconciliation_log(customer, False, 0, 0, unexpected_error)
        return False, log_entry.name


def create_reconciliation_log(
    customer, success, invoice_count, payment_count, error=None
):
    """Insert a Bulk Payment Reconciliation Log entry and return the document.

    Args:
        customer (str): Customer name.
        success (bool): Whether reconciliation succeeded.
        invoice_count (int): Number of invoices that were considered.
        payment_count (int): Number of payments that were considered.
        error (Exception | None): The exception to record on failure, if any.

    Returns:
        Document: The newly inserted Bulk Payment Reconciliation Log document.
    """
    log_entry = frappe.get_doc(
        {
            "doctype": "Bulk Payment Reconciliation Log",
            "customer": customer,
            "status": "Success" if success else "Failed",
            "error_message": str(error) if error else None,
            "invoices_processed": invoice_count,
            "payments_processed": payment_count,
        }
    )
    log_entry.insert(ignore_permissions=True)
    return log_entry
