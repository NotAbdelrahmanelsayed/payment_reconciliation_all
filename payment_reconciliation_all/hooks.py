app_name = "payment_reconciliation_all"
app_title = "Payment Reconciliation All"
app_publisher = "Abdelrahman Elsayed"
app_description = "fetch outstanding invoices and match unallocated payments/JE across all parties and reconcile them"
app_email = "notabdelrahmanelsayed@gmail.com"
app_license = "MIT"

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/payment_reconciliation_all/css/payment_reconciliation_all.css"
# app_include_js = "/assets/payment_reconciliation_all/js/payment_reconciliation_all.js"

# include js, css files in header of web template
# web_include_css = "/assets/payment_reconciliation_all/css/payment_reconciliation_all.css"
# web_include_js = "/assets/payment_reconciliation_all/js/payment_reconciliation_all.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "payment_reconciliation_all/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
doctype_js = {"Payment Reconciliation": "public/js/payment_reconciliation.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "payment_reconciliation_all.utils.jinja_methods",
# 	"filters": "payment_reconciliation_all.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "payment_reconciliation_all.install.before_install"
# after_install = "payment_reconciliation_all.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "payment_reconciliation_all.uninstall.before_uninstall"
# after_uninstall = "payment_reconciliation_all.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "payment_reconciliation_all.utils.before_app_install"
# after_app_install = "payment_reconciliation_all.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "payment_reconciliation_all.utils.before_app_uninstall"
# after_app_uninstall = "payment_reconciliation_all.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "payment_reconciliation_all.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------
scheduler_events = {
    "cron": {"*/5 * * * *": ["payment_reconciliation_all.reconcile.process_batch"]}
}
# scheduler_events = {
# 	"all": [
# 		"payment_reconciliation_all.tasks.all"
# 	],
# 	"daily": [
# 		"payment_reconciliation_all.tasks.daily"
# 	],
# 	"hourly": [
# 		"payment_reconciliation_all.tasks.hourly"
# 	],
# 	"weekly": [
# 		"payment_reconciliation_all.tasks.weekly"
# 	],
# 	"monthly": [
# 		"payment_reconciliation_all.tasks.monthly"
# 	],
# }

# Testing
# -------

# before_tests = "payment_reconciliation_all.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "payment_reconciliation_all.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "payment_reconciliation_all.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["payment_reconciliation_all.utils.before_request"]
# after_request = ["payment_reconciliation_all.utils.after_request"]

# Job Events
# ----------
# before_job = ["payment_reconciliation_all.utils.before_job"]
# after_job = ["payment_reconciliation_all.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"payment_reconciliation_all.auth.validate"
# ]
