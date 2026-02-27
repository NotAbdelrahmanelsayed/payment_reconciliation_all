"""
TODO:
1.
  - Create a table at the data base with reconciled customers and their data
  - Do a report for this table
2.
  -
  -

"""

import frappe

logger = frappe.logger("payment_reconciliation")
logger.setLevel("DEBUG")

from erpnext.accounts.doctype.payment_reconciliation.payment_reconciliation import (
    PaymentReconciliation,
)
from erpnext.accounts.party import get_party_account


@frappe.whitelist()
def bulk_reconcile_all(company="Esnad"):
    """
    Return list of customers having outstanding invoices or unallocated payments
    """
    customers = set()
    receivable_account = frappe.db.get_value(
        "Company", company, "default_receivable_account"
    )
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

    # TODO Log count of unique customers to the user
    for (cust,) in oustanding_invoices:
        customers.add(cust)
    for (party,) in unallocated_payments:
        customers.add(party)

    customers = list(customers)

    batch_size = 50
    batches = [
        customers[i : i + batch_size] for i in range(0, len(customers), batch_size)
    ]
    for batch in batches:
        frappe.enqueue(
            "payment_reconciliation_all.reconcile.reconcile_customer_batch",
            queue="long",
            customer_list=batch,
            company=company,
            receivable_account=receivable_account,
            party_type="Customer",
        )
    frappe.msgprint(f"Queued for reconciliation | Customers: {len(customers)}")
    logger.info("Queeud for reconciliation | batches: %s", len(batches))
    return "Done"


def reconcile_customer_batch(
    customer_list, company, receivable_account, party_type="Customer"
):
    for customer in customer_list:
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
                logger.debug(
                    "Failed  reconcile |customer: %s | payments: %s.| invoices: %s",
                    customer,
                    n_payments,
                    n_invoices,
                )
                log_customer(customer, False, n_invoices, n_payments)
                continue
            # Prepare data to Allocate entries
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
                logger.info("Successfully allocated entries | customer: %s ", customer)
                pr.reconcile_allocations()

                success = True
                frappe.db.commit()
            except Exception as e:
                frappe.db.rollback()
                logger.warning(
                    "Failed to allocate entries |customer: %s | payments: %s.| invoices: %s",
                    customer,
                    n_payments,
                    n_invoices,
                )
                success = False
            try:
                e = e
            except Exception:
                e = ""
            log = log_customer(customer, success, n_invoices, n_payments, e)
            logger.debug(
                "Successfully Inserted |log: %s | for customer %s", log, customer
            )

        except Exception as e:
            logger.error(
                "Failed to create payment reconciliation | party: %s. | exception: %s",
                customer,
                e,
            )
            pass

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
