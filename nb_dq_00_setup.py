# =============================================================================
# NB_DQ_00_SETUP.py
# Creates all Delta tables used by the GX Core data quality framework.
# Run once before the first validation run.
# Safe to re-run — all statements use CREATE TABLE IF NOT EXISTS, and
# ALTER TABLE ... ADD COLUMNS IF NOT EXISTS is used so the script can also
# be re-run against existing tables to add new columns without data loss.
# =============================================================================

# CELL 1 — Spark session
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")
print("Spark ready.")


# CELL 2 — dq_run_results
# One row per rule per validation run.
# Used by Power BI for the summary / scorecard view.
# Key fields for the semantic model:
#   rule_group       – Process | Milestone | Invoice
#   expectation      – the expectation type applied
#   table_name       – the source table validated
#   severity         – critical | high | medium | low
#   status           – PASSED | FAILED | ERROR
#   column_a         – left-hand column (validate_column_comparison only)
#   column_b         – right-hand column (validate_column_comparison only)
#   operator         – comparison operator (validate_column_comparison only)
#   sql_query        – the SQL string executed (sql / sql_validation only)
#   rule_category    – business category of the rule (e.g. "Referential Integrity")
#   reference_table  – reference table name (validate_foreign_key only)
#   reference_column – reference column name (validate_foreign_key only)
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_run_results (
    run_id          STRING,
    run_timestamp   TIMESTAMP,
    batch_date      DATE,
    rule_group      STRING,
    rule_id         STRING,
    rule_name       STRING,
    table_name      STRING,
    expectation     STRING,
    severity        STRING,
    owner           STRING,
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
    rule_category   STRING,
    reference_table STRING,
    reference_column STRING
)
USING DELTA
""")

# Add new columns to the existing table if this script is re-run against an
# older deployment that pre-dates these fields.
for _col_def in [
    "column_a        STRING",
    "column_b        STRING",
    "operator        STRING",
    "sql_query       STRING",
    "rule_category   STRING",
    "reference_table STRING",
    "reference_column STRING",
]:
    try:
        spark.sql(
            f"ALTER TABLE dq_run_results ADD COLUMNS ({_col_def})"
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
#   prosess_id           – links the violation back to Saksbehandling.Prosesser
#   primary_key_value    – the PK value of the offending row in its own table
#   violated_column      – which column caused the violation
#   severity / owner     – copied from the rule definition for easy filtering
#   issue_status         – 'Active' while the violation persists; 'Resolved' once fixed
#   resolution_timestamp – ISO timestamp of when the violation was resolved (NULL if still Active)
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_violations (
    run_id               STRING,
    run_timestamp        TIMESTAMP,
    batch_date           DATE,
    rule_group           STRING,
    rule_id              STRING,
    rule_name            STRING,
    table_name           STRING,
    severity             STRING,
    owner                STRING,
    failure_type         STRING,
    prosess_id           STRING,
    primary_key_value    STRING,
    violated_column      STRING,
    actual_value         STRING,
    expected_condition   STRING,
    violation_detail     STRING,
    saksbehandler_kode   STRING,
    issue_status         STRING,
    resolution_timestamp STRING
)
USING DELTA
""")

# Add new columns to the existing table if this script is re-run against an
# older deployment that pre-dates these fields.
for _col_def in [
    "failure_type         STRING",
    "issue_status         STRING",
    "resolution_timestamp STRING",
]:
    try:
        spark.sql(
            f"ALTER TABLE dq_violations ADD COLUMNS ({_col_def})"
        )
    except Exception as _exc:
        _msg = str(_exc).lower()
        if "already exists" not in _msg and "duplicate" not in _msg:
            print(f"  Warning: could not add column '{_col_def}': {_exc}")

print("dq_violations ready.")


print("\n=== DQ SETUP COMPLETE ===")
print("Delta tables: dq_run_results, dq_violations")
