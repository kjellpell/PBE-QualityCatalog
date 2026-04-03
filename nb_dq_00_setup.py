# =============================================================================
# NB_DQ_00_SETUP.py
# Creates all Delta tables used by the GX Core data quality framework.
# Run once before the first validation run.
# Safe to re-run — all statements use CREATE TABLE IF NOT EXISTS.
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
#   rule_group    – Process | Milestone | Invoice
#   expectation   – the expectation type applied
#   table_name    – the source table validated
#   severity      – critical | high | medium | low
#   category      – Completeness | Ordering | Validation
#   status        – PASSED | FAILED | ERROR
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
    category        STRING,
    owner           STRING,
    total_rows      BIGINT,
    passed_rows     BIGINT,
    failed_rows     BIGINT,
    success_pct     DOUBLE,
    status          STRING,
    details         STRING
)
USING DELTA
""")
print("dq_run_results ready.")


# CELL 3 — dq_violations
# One row per offending record per rule per run.
# Used by Power BI for the drill-through / detail view.
# Key fields for the semantic model:
#   prosess_id          – links the violation back to Saksbehandling.Prosesser
#   primary_key_value   – the PK value of the offending row in its own table
#   violated_column     – which column caused the violation
#   severity / category / owner – copied from the rule definition for easy filtering
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_violations (
    run_id              STRING,
    run_timestamp       TIMESTAMP,
    batch_date          DATE,
    rule_group          STRING,
    rule_id             STRING,
    rule_name           STRING,
    table_name          STRING,
    severity            STRING,
    category            STRING,
    owner               STRING,
    prosess_id          STRING,
    primary_key_value   STRING,
    violated_column     STRING,
    actual_value        STRING,
    expected_condition  STRING,
    violation_detail    STRING,
    saksbehandler_kode  STRING
)
USING DELTA
""")
print("dq_violations ready.")


print("\n=== DQ SETUP COMPLETE ===")
print("Delta tables: dq_run_results, dq_violations")
