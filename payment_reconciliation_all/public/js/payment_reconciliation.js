frappe.ui.form.on("Payment Reconciliation", {
  refresh: function (frm) {
    frm.add_custom_button(__("Reconcile All"), function () {
      // call server method
      //   if (!frm.company) {
      //     frappe.throw({
      //       message: "Please set the Company",
      //     });
      //   }
      frappe.call({
        method: "payment_reconciliation_all.reconcile.start_reconciliation",
        args: {
          company: frm.company,
        },
        callback: function (r) {
          if (r.message) {
            frappe.message(r.message);
          }
        },
      });
    });
    frm.add_custom_button("Bulk Reconciliation Log", () => {
      frappe.set_route("List", "Bulk Payment Reconciliation Log");
    });
  },
});
