# Template / reference copy — deploy to /lakehouse/default/Files/Configs/ on Fabric.
"""Runtime controls for the Quality Catalog validation runner."""

DRY_RUN = False
FAIL_ON_EMPTY_RULES = True
FAIL_ON_EMPTY_SOURCE = True

# ---------------------------------------------------------------------------
# Per-rule retry: maximum number of additional attempts after the first
# failure, applied only when the error is classified as retryable by
# RETRYABLE_ERROR_MARKERS below.  Use 0 to disable retries.
# ---------------------------------------------------------------------------
MAX_RULE_RETRIES = 2

# ---------------------------------------------------------------------------
# Per-rule timeout in seconds.  Each validator.validate() call is executed in
# a background thread; if it has not returned within this window, the rule is
# recorded as ERROR ("Timed out after Ns") and the run continues.
#
# Sizing guidance (5 M rows, ~30 rules across 3 tables):
#   - Simple rules (not-null, comparison, count):  < 30 s typical
#   - FK / aggregate / group-match rules on large tables: 1–3 min typical
#   - 300 s (5 min) gives ample headroom while catching genuinely hung rules.
#     Worst-case total run: 30 × 300 s = 150 min; realistic: 15–30 min.
# ---------------------------------------------------------------------------
RULE_TIMEOUT_SECONDS = 300

# ---------------------------------------------------------------------------
# Retryable error classification.
# RETRYABLE_ERROR_MARKERS drives two things:
#   1. The is_retryable flag in dq_execution_metrics (for alerting dashboards).
#   2. Whether a failed rule is retried inline (up to MAX_RULE_RETRIES times).
# Add lowercase substrings that appear in transient Spark/network error messages.
# ---------------------------------------------------------------------------
RETRYABLE_ERROR_MARKERS = [
    "timeout",
    "temporar",
    "connection",
    "unavailable",
    "throttle",
]

# ---------------------------------------------------------------------------
# Optional catalog-level source filter overrides keyed by rule_group.
#
# Example:
#   CATALOG_FILTER_OVERRIDES = {
#       "Process": {
#           "type": "date_range",
#           "date_column": "ActualEndDate",
#           "lookback_days": 90,
#           "include_nulls": True,
#       },
#       "Invoice": {
#           "type": "custom",
#           "where_clause": "FakturaDato >= date_sub(current_date(), 30)",
#       },
#   }
#
# Set a rule_group override to None to disable its YAML catalog_filter.
# ---------------------------------------------------------------------------
CATALOG_FILTER_OVERRIDES = {}
