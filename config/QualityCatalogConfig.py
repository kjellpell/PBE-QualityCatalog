# Template / reference copy — deploy to /lakehouse/default/Files/Configs/ on Fabric.
# This file is NOT loaded at runtime; the Lakehouse copy is always used.
"""Shared configuration for PBE Quality Catalog runtime."""

DEFAULT_SCHEMA = "qualitycatalog"


RULES_DIR = "rules"

DQ_RESULTS_TABLE = "dq_run_results"
DQ_VIOLATIONS_TABLE = "dq_violations"
DQ_ENRICHED_TABLE = "dq_violations_enriched"
DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"

# Employee lookup — used by nb_dq_04_routing to resolve record owners for Power BI handler report
ANSATTE_TABLE = "saksbehandling.ansatte"
ANSATTE_KEY_COL = ""          # column in ansatte matching ownership_col values
ANSATTE_EMAIL_COL = ""        # email column in ansatte (used for Power BI RLS)
ANSATTE_NAME_COL = ""         # display name column in ansatte

# Notifications — used by nb_dq_06_notify
REPORT_URL     = ""           # https://app.powerbi.com/groups/.../reports/...
MANAGER_EMAIL  = ""           # manager@org.com