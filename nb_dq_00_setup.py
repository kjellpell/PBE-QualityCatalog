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
# -----------------------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_results_table} (
    run_id                STRING,
    run_timestamp         TIMESTAMP,
    batch_date            DATE,
    rule_group            STRING,
    rule_id               STRING,
    rule_name             STRING,
    table_name            STRING,
    expectation           STRING,
    total_rows            BIGINT,
    passed_rows           BIGINT,
    failed_rows           BIGINT,
    success_pct           DOUBLE,
    status                STRING,
    details               STRING,
    rule_duration_seconds DOUBLE,
    error_category        STRING
)
USING DELTA
""")

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
    primary_key_value    STRING,
    violated_column      STRING,
    actual_value         STRING,
    expected_condition   STRING,
    violation_detail     STRING,
    issue_status         STRING,
    resolution_timestamp STRING,
    first_seen_at        TIMESTAMP,
    violation_scope      STRING
)
USING DELTA
""")

# Add new columns to the existing table if this script is re-run against an
# older deployment that pre-dates these fields.
for _col_def in [
    "issue_status         STRING",
    "resolution_timestamp STRING",
    "first_seen_at        TIMESTAMP",
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

# Add error_category to dq_run_results for deployments pre-dating this field.
for _col_def in [
    "error_category STRING",
]:
    try:
        spark.sql(
            f"ALTER TABLE {_results_table} ADD COLUMNS ({_col_def})"
        )
    except Exception as _exc:
        _msg = str(_exc).lower()
        if "already exists" not in _msg and "duplicate" not in _msg:
            print(f"  Warning: could not add column '{_col_def}': {_exc}")

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


# CELL 5 — dry-run tmp tables
# Same schema as production tables. Created here so the validation runner
# never has to auto-create them at runtime.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {_results_table}_tmp (
    run_id                STRING,
    run_timestamp         TIMESTAMP,
    batch_date            DATE,
    rule_group            STRING,
    rule_id               STRING,
    rule_name             STRING,
    table_name            STRING,
    expectation           STRING,
    total_rows            BIGINT,
    passed_rows           BIGINT,
    failed_rows           BIGINT,
    success_pct           DOUBLE,
    status                STRING,
    details               STRING,
    rule_duration_seconds DOUBLE,
    error_category        STRING
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
    primary_key_value    STRING,
    violated_column      STRING,
    actual_value         STRING,
    expected_condition   STRING,
    violation_detail     STRING,
    issue_status         STRING,
    resolution_timestamp STRING,
    first_seen_at        TIMESTAMP,
    violation_scope      STRING
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
print(f"Delta tables: {_results_table}, {_violations_table}, {_metrics_table}")
print(f"Dry-run tmp: {_results_table}_tmp, {_violations_table}_tmp, {_metrics_table}_tmp")

