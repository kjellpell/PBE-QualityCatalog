# Template / reference copy — deploy to /lakehouse/default/Files/Configs/ on Fabric.
# This file is NOT loaded at runtime; the Lakehouse copy is always used.
"""Shared configuration for PBE Quality Catalog runtime."""

DEFAULT_SCHEMA = "default"


RULES_DIR = "rules"

DQ_RESULTS_TABLE = "dq_run_results"
DQ_VIOLATIONS_TABLE = "dq_violations"
DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"

# ---------------------------------------------------------------------------
# Internal Control (IC) tables
# Unqualified names — resolve_targets() prepends DEFAULT_SCHEMA at runtime.
# ---------------------------------------------------------------------------
IC_RUN_RESULTS_TABLE      = "ic_run_results"
IC_EXCEPTIONS_TABLE       = "ic_exceptions"
IC_CONTROL_REGISTER_TABLE = "ic_control_register"
IC_ATTESTATIONS_TABLE     = "ic_manual_attestations"

# Power Automate HTTP endpoint for email notifications on new IC exceptions.
# Store the actual URL in the file below — not in source code.
IC_NOTIFY_URL_PATH = "/lakehouse/default/Files/Configs/pa_notify_url.txt"

# IC-only YAML fields passed through to ic_run_results and ic_exceptions rows.
IC_PASSTHROUGH_FIELDS = [
    "control_ref",
    "control_type",
    "risk_domain",
    "remediation_due_days",
]

# A rule is treated as IC if it carries at least one of these fields.
IC_IDENTIFIER_FIELDS = ["control_ref", "control_type", "risk_domain"]