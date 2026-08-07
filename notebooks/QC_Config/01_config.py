# =============================================================================
# QC_Config
#
# Configuration for the Quality Catalog. This notebook only defines values —
# run it via `%run QC_Config` from an entry-point notebook.
#
# Values are identical in every deployment stage, so this notebook is promoted
# unchanged by the deployment pipeline.
# =============================================================================

# Cell source for the QC_Config notebook.
#
# Values are identical in every deployment stage — the workspace's default
# lakehouse binding is what differs between dev, test and production, and that
# is set on the notebook item by the deployment pipeline, not here.

QUALITY_CATALOG_CONFIG = {
    # Schema the three output tables live in.
    "DEFAULT_SCHEMA": "datakvalitet",

    # Output tables. Created by QC_Setup_Tables from the engine's own schemas.
    "DQ_RESULTS_TABLE": "dq_run_results",
    "DQ_VIOLATIONS_TABLE": "dq_violations",
    "DQ_EXECUTION_METRICS_TABLE": "dq_execution_metrics",
}


# Cell source for the QC_Config notebook.
#
# Runtime controls for the Quality Catalog validation runner.

QUALITY_CATALOG_RUNTIME = {
    # Fail the run when a configured source table has no rows.
    "FAIL_ON_EMPTY_SOURCE": True,

    # -----------------------------------------------------------------------
    # Per-rule retry: maximum number of additional attempts after the first
    # failure, applied only when the error is classified as retryable by
    # RETRYABLE_ERROR_MARKERS below.  Use 0 to disable retries.
    # -----------------------------------------------------------------------
    "MAX_RULE_RETRIES": 2,

    # -----------------------------------------------------------------------
    # Per-rule timeout in seconds.  Each rule is executed in a background
    # thread; if it has not returned within this window, the rule is recorded
    # as ERROR ("Timed out after Ns") and the run continues.
    #
    # Sizing guidance (5 M rows, ~30 rules across 3 tables):
    #   - Simple predicate rules (check:, unique:):  < 30 s typical
    #   - Reference / aggregate / group-scoped rules on large tables:
    #     1–3 min typical
    #   - 300 s (5 min) gives ample headroom while catching genuinely hung
    #     rules.  Worst-case total run: 30 × 300 s = 150 min; realistic:
    #     15–30 min.
    # -----------------------------------------------------------------------
    "RULE_TIMEOUT_SECONDS": 300,

    # -----------------------------------------------------------------------
    # Retryable error classification.
    # RETRYABLE_ERROR_MARKERS drives two things:
    #   1. The is_retryable flag in dq_execution_metrics (for alerting
    #      dashboards).
    #   2. Whether a failed rule is retried inline (up to MAX_RULE_RETRIES
    #      times).
    # Add lowercase substrings that appear in transient Spark/network error
    # messages.
    # -----------------------------------------------------------------------
    "RETRYABLE_ERROR_MARKERS": [
        "timeout",
        "temporar",
        "connection",
        "unavailable",
        "throttle",
    ],

    # -----------------------------------------------------------------------
    # Optional overrides for a catalog's `where:` filter, keyed by rule_group.
    #
    # The value is a Spark SQL predicate string — the same thing you would
    # write under `where:` in the catalog itself.  A date window is an ordinary
    # predicate rather than a dedicated filter type, so a lookback is expressed
    # directly:
    #
    #   "CATALOG_FILTER_OVERRIDES": {
    #       "Faser":   "sist_endret >= date_sub(current_date(), 90) OR sist_endret IS NULL",
    #       "Faktura": "fakturadato >= date_sub(current_date(), 30)",
    #   }
    #
    # Set a rule_group override to None to disable that catalog's own `where:`.
    # -----------------------------------------------------------------------
    "CATALOG_FILTER_OVERRIDES": {},
}
