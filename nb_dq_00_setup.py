# =============================================================================
# NB_DQ_00_SETUP.py
# Creates all Delta tables used by the Quality Catalog data quality framework.
# Run once before the first validation run.
# Safe to re-run — all statements use CREATE TABLE IF NOT EXISTS, and
# ALTER TABLE ... ADD COLUMNS IF NOT EXISTS is used so the script can also
# be re-run against existing tables to add new columns without data loss.
# =============================================================================

# CELL 1 — Spark session + config
# -----------------------------------------------------------------------------
import importlib.util
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

CONFIG_DIR = Path("/lakehouse/default/Files/Configs")
_cfg_spec = importlib.util.spec_from_file_location(
    "QualityCatalogConfig", str(CONFIG_DIR / "QualityCatalogConfig.py")
)
_cfg_mod = importlib.util.module_from_spec(_cfg_spec)
_cfg_spec.loader.exec_module(_cfg_mod)

_schema = getattr(_cfg_mod, "DEFAULT_SCHEMA", "qualitycatalog")
_results_table        = f"{_schema}.{_cfg_mod.DQ_RESULTS_TABLE}"
_violations_table     = f"{_schema}.{_cfg_mod.DQ_VIOLATIONS_TABLE}"
_metrics_table        = f"{_schema}.{_cfg_mod.DQ_EXECUTION_METRICS_TABLE}"
_notifications_table  = f"{_schema}.{getattr(_cfg_mod, 'DQ_NOTIFICATIONS_TABLE', 'dq_notifications')}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema}")
print(f"Spark ready. Target schema: {_schema}")


# CELL 2 — dq_run_results
# One row per rule per validation run.
# Used by Power BI for the summary / scorecard view.
# Key fields for the semantic model:
#   rule_group       – Process | Milestone | Invoice
#   expectation      – the expectation type applied
#   table_name       – the source table validated
#   status           – PASSED | FAILED | ERROR
#   column_a         – left-hand column (validate_column_comparison only)
#   column_b         – right-hand column (validate_column_comparison only)
#   operator         – comparison operator (validate_column_comparison only)
#   sql_query        – the SQL string executed (sql / sql_validation only)
#   reference_table  – reference table name (validate_foreign_key only)
#   reference_column – reference column name (validate_foreign_key only)
# -----------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_results_table} (
    run_id          STRING,
    run_timestamp   TIMESTAMP,
    batch_date      DATE,
    rule_group      STRING,
    rule_id         STRING,
    rule_name       STRING,
    table_name      STRING,
    expectation     STRING,
    owner           STRING,
    owner_email          STRING,
    total_rows      BIGINT,
    passed_rows     BIGINT,
    failed_rows     BIGINT,
    success_pct     DOUBLE,
    status          STRING,
    details         STRING,
    column_a        STRING,
    column_b        STRING,
    operator        STRING,
    sql_query       STRING,
    reference_table      STRING,
    reference_column     STRING,
    rule_duration_seconds DOUBLE,
    control_ref          STRING,
    control_type         STRING,
    risk_domain          STRING,
    remediation_due_days INT
)
USING DELTA
""")

# Add new columns to the existing table if this script is re-run against an
# older deployment that pre-dates these fields.
for _col_def in [
    "column_a             STRING",
    "column_b             STRING",
    "operator             STRING",
    "sql_query            STRING",
    "reference_table      STRING",
    "reference_column     STRING",
    "rule_duration_seconds DOUBLE",
    "owner_email          STRING",
    "control_ref          STRING",
    "control_type         STRING",
    "risk_domain          STRING",
    "remediation_due_days INT",
]:
    try:
        spark.sql(
            f"ALTER TABLE {_results_table} ADD COLUMNS ({_col_def})"
        )
    except Exception as _exc:
        # Silently skip if the column already exists; surface all other errors.
        _msg = str(_exc).lower()
        if "already exists" not in _msg and "duplicate" not in _msg:
            print(f"  Warning: could not add column '{_col_def}': {_exc}")

print("dq_run_results ready.")


# CELL 3 — dq_violations
# One row per offending record per rule (current state — upserted each run).
# Used by Power BI for the drill-through / detail view.
# Key fields for the semantic model:
#   primary_key_value    – the PK value of the offending row in its own table
#   violated_column      – which column caused the violation
#   owner                – copied from the rule definition for easy filtering
#   issue_status         – 'Active' while the violation persists; 'Resolved' once fixed
#   resolution_timestamp – ISO timestamp of when the violation was resolved (NULL if still Active)
# -----------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_violations_table} (
    run_id               STRING,
    run_timestamp        TIMESTAMP,
    batch_date           DATE,
    rule_group           STRING,
    rule_id              STRING,
    rule_name            STRING,
    table_name           STRING,
    owner                STRING,
    primary_key_value    STRING,
    violated_column      STRING,
    actual_value         STRING,
    expected_condition   STRING,
    violation_detail     STRING,
    issue_status         STRING,
    resolution_timestamp STRING
)
USING DELTA
""")

# Add new columns to the existing table if this script is re-run against an
# older deployment that pre-dates these fields.
for _col_def in [
    "issue_status         STRING",
    "resolution_timestamp STRING",
]:
    try:
        spark.sql(
            f"ALTER TABLE {_violations_table} ADD COLUMNS ({_col_def})"
        )
    except Exception as _exc:
        _msg = str(_exc).lower()
        if "already exists" not in _msg and "duplicate" not in _msg:
            print(f"  Warning: could not add column '{_col_def}': {_exc}")

print("dq_violations ready.")

# Performance tip: after the first significant data load, run the following
# OPTIMIZE command to apply Z-order clustering on the columns most commonly
# used in WHERE / JOIN predicates.  This can dramatically reduce scan times
# for handler- and rule-level queries in Power BI.
#
#   spark.sql("OPTIMIZE dq_violations ZORDER BY (rule_id, primary_key_value)")
#
# Re-run periodically (e.g. weekly) or after large backfills.


# CELL 4 — dq_execution_metrics
# One row per runner execution for operator observability.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_metrics_table} (
    script_name        STRING,
    status             STRING,
    dry_run            BOOLEAN,
    output_target      STRING,
    artifact_target    STRING,
    row_count          BIGINT,
    started_at_utc     TIMESTAMP,
    finished_at_utc    TIMESTAMP,
    duration_seconds   DOUBLE,
    is_retryable       BOOLEAN,
    error_message      STRING
)
USING DELTA
""")

print("dq_execution_metrics ready.")


# CELL 5 — dq_notifications
# One row per notification attempt written by nb_dq_04_routing.py.
# Append-only — preserves the full history of what was sent, skipped, or failed.
# Key fields for Power BI:
#   notification_type  – 'it-ops' or 'individual'
#   recipient_email    – owner email for individual; NULL for it-ops (recipient is team alias in PA)
#   status             – 'sent', 'skipped', or 'failed'
#   error_message      – populated only on failure
# -----------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_notifications_table} (
    run_id              STRING,
    notified_at         TIMESTAMP,
    notification_type   STRING,
    recipient_email     STRING,
    violation_count     INT,
    status              STRING,
    error_message       STRING
)
USING DELTA
""")

print("dq_notifications ready.")


# CELL 6 — dry-run tmp tables
# Same schema as production tables. Created here so the validation runner
# never has to auto-create them at runtime.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_results_table}_tmp (
    run_id          STRING,
    run_timestamp   TIMESTAMP,
    batch_date      DATE,
    rule_group      STRING,
    rule_id         STRING,
    rule_name       STRING,
    table_name      STRING,
    expectation     STRING,
    owner           STRING,
    owner_email          STRING,
    total_rows      BIGINT,
    passed_rows     BIGINT,
    failed_rows     BIGINT,
    success_pct     DOUBLE,
    status          STRING,
    details         STRING,
    column_a        STRING,
    column_b        STRING,
    operator        STRING,
    sql_query       STRING,
    reference_table      STRING,
    reference_column     STRING,
    rule_duration_seconds DOUBLE,
    control_ref          STRING,
    control_type         STRING,
    risk_domain          STRING,
    remediation_due_days INT
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_violations_table}_tmp (
    run_id               STRING,
    run_timestamp        TIMESTAMP,
    batch_date           DATE,
    rule_group           STRING,
    rule_id              STRING,
    rule_name            STRING,
    table_name           STRING,
    owner                STRING,
    primary_key_value    STRING,
    violated_column      STRING,
    actual_value         STRING,
    expected_condition   STRING,
    violation_detail     STRING,
    issue_status         STRING,
    resolution_timestamp STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_metrics_table}_tmp (
    script_name        STRING,
    status             STRING,
    dry_run            BOOLEAN,
    output_target      STRING,
    artifact_target    STRING,
    row_count          BIGINT,
    started_at_utc     TIMESTAMP,
    finished_at_utc    TIMESTAMP,
    duration_seconds   DOUBLE,
    is_retryable       BOOLEAN,
    error_message      STRING
)
USING DELTA
""")

print("dry-run tmp tables ready.")

print("\n=== DQ SETUP COMPLETE ===")
print(f"Delta tables: {_results_table}, {_violations_table}, {_metrics_table}, {_notifications_table}")
print(f"Dry-run tmp: {_results_table}_tmp, {_violations_table}_tmp, {_metrics_table}_tmp")

