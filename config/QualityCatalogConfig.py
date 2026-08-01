# Template / reference copy — deploy to /lakehouse/default/Files/Configs/ on Fabric.
# This file is NOT loaded at runtime; the Lakehouse copy is always used.
"""Shared configuration for PBE Quality Catalog runtime."""

DEFAULT_SCHEMA = "datakvalitet"


RULES_DIR = "rules"

DQ_RESULTS_TABLE = "dq_run_results"
DQ_VIOLATIONS_TABLE = "dq_violations"
DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"