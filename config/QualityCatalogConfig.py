"""Shared configuration for PBE Quality Catalog runtime."""

DEFAULT_SCHEMA = "default"


RULES_DIR = "rules"
GX_SAMPLE_SIZE = 50_000

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

# Evidence file base path — Fabric Lakehouse Files mounted path.
# Writable from Fabric Spark notebooks using standard open() / os.makedirs().
# The attestation notebook downloads from report_link and stores here.
IC_EVIDENCE_BASE_PATH = "/lakehouse/default/Files/ic_evidence"

# IC-only YAML fields passed through to ic_run_results and ic_exceptions rows.
IC_PASSTHROUGH_FIELDS = [
    "control_ref",
    "control_type",
    "risk_domain",
    "remediation_due_days",
]

# A rule is treated as IC if it carries at least one of these fields.
IC_IDENTIFIER_FIELDS = ["control_ref", "control_type", "risk_domain"]