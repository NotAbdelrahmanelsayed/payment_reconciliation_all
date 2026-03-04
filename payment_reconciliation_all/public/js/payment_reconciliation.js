frappe.ui.form.on("Payment Reconciliation", {
  refresh: function (frm) {
    frm.add_custom_button(__("Reconcile All"), function () {
      if (!frm.doc.company) {
        frappe.throw({ message: __("Please set the Company") });
      }
      frappe.call({
        method: "payment_reconciliation_all.reconcile.start_reconciliation",
        args: {
          company: frm.doc.company,
        },
        callback: function (r) {
          if (r.message) {
            frappe.msgprint(r.message);
          }
        },
      });
    }, __("Bulk Process"));

    frm.add_custom_button(__("Bulk Reconciliation Log"), () => {
      frappe.set_route("List", "Bulk Payment Reconciliation Log");
    }, __("Bulk Process"));
  },
});
