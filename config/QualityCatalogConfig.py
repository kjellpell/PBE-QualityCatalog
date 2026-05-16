# Template / reference copy — deploy to /lakehouse/default/Files/Configs/ on Fabric.
# This file is NOT loaded at runtime; the Lakehouse copy is always used.
"""Shared configuration for PBE Quality Catalog runtime."""

DEFAULT_SCHEMA = "qualitycatalog"


RULES_DIR = "rules"

DQ_RESULTS_TABLE = "dq_run_results"
DQ_VIOLATIONS_TABLE = "dq_violations"
DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"

# Employee lookup — used by nb_dq_04_routing to resolve record owners for individual notifications
ANSATTE_TABLE = "saksbehandling.ansatte"
ANSATTE_KEY_COL = ""          # column in ansatte matching ownership_col values
ANSATTE_EMAIL_COL = ""        # email column in ansatte
ANSATTE_NAME_COL = ""         # display name column in ansatte

# Power Automate webhook URLs — leave empty to disable that notification channel
POWER_AUTOMATE_ITOPS_URL = ""
POWER_AUTOMATE_INDIVIDUAL_URL = ""