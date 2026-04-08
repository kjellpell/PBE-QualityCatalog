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
    rule_category        STRING,
    reference_table      STRING,
    reference_column     STRING,
    rule_duration_seconds DOUBLE
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
    "rule_category        STRING",
    "reference_table      STRING",
    "reference_column     STRING",
    "rule_duration_seconds DOUBLE",
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
spark.sql("""
CREATE TABLE IF NOT EXISTS dq_execution_metrics (
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


print("\n=== DQ SETUP COMPLETE ===")
print("Delta tables: dq_run_results, dq_violations, dq_execution_metrics")


# =============================================================================
# IC TABLE SETUP — Cells 5–8
# Internal Control tables.  Rerunnable — all CREATE TABLE statements use
# IF NOT EXISTS, and ALTER TABLE uses try/except to skip already-present cols.
# =============================================================================

# CELL 5 — ic_run_results
# One row per IC rule per validation run (mirrors dq_run_results + IC fields).
spark.sql("""
CREATE TABLE IF NOT EXISTS ic_run_results (
    run_id                STRING NOT NULL,
    run_timestamp         TIMESTAMP NOT NULL,
    batch_date            DATE NOT NULL,
    rule_group            STRING NOT NULL,
    rule_id               STRING NOT NULL,
    rule_name             STRING NOT NULL,
    table_name            STRING NOT NULL,
    expectation           STRING NOT NULL,
    severity              STRING NOT NULL,
    owner                 STRING,
    total_rows            BIGINT,
    passed_rows           BIGINT,
    failed_rows           BIGINT,
    success_pct           DOUBLE,
    status                STRING NOT NULL,
    details               STRING,
    control_ref           STRING,
    control_type          STRING,
    risk_domain           STRING,
    remediation_due_days  INTEGER,
    rule_duration_seconds DOUBLE
)
USING DELTA
""")

for _col_def in [
    "column_a             STRING",
    "column_b             STRING",
    "operator             STRING",
    "sql_query            STRING",
    "rule_category        STRING",
    "reference_table      STRING",
    "reference_column     STRING",
]:
    try:
        spark.sql(f"ALTER TABLE ic_run_results ADD COLUMNS ({_col_def})")
    except Exception as _exc:
        _msg = str(_exc).lower()
        if "already exists" not in _msg and "duplicate" not in _msg:
            print(f"  Warning: could not add column '{_col_def}': {_exc}")

print("ic_run_results ready.")


# CELL 6 — ic_exceptions
# One row per IC violation, persisted with a 4-state lifecycle:
#   Open → Remediated → Verified
#   Open → Waived
# Human-set fields are written only by nb_ic_01_manage_exceptions.
spark.sql("""
CREATE TABLE IF NOT EXISTS ic_exceptions (
    run_id                STRING NOT NULL,
    run_timestamp         TIMESTAMP NOT NULL,
    batch_date            DATE NOT NULL,
    rule_group            STRING NOT NULL,
    rule_id               STRING NOT NULL,
    rule_name             STRING NOT NULL,
    table_name            STRING NOT NULL,
    severity              STRING NOT NULL,
    owner                 STRING,
    failure_type          STRING,
    primary_key_value     STRING,
    violated_column       STRING,
    actual_value          STRING,
    expected_condition    STRING,
    violation_detail      STRING,
    ic_status             STRING NOT NULL,
    control_ref           STRING,
    control_type          STRING,
    risk_domain           STRING,
    remediation_due_days  INTEGER,
    remediation_due_date  DATE,
    first_seen_at         TIMESTAMP,
    remediated_by         STRING,
    remediated_at         TIMESTAMP,
    verified_by           STRING,
    verified_at           TIMESTAMP,
    waived_by             STRING,
    waived_at             TIMESTAMP,
    waiver_reason         STRING
)
USING DELTA
""")

for _col_def in [
    "remediated_by        STRING",
    "remediated_at        TIMESTAMP",
    "verified_by          STRING",
    "verified_at          TIMESTAMP",
    "waived_by            STRING",
    "waived_at            TIMESTAMP",
    "waiver_reason        STRING",
]:
    try:
        spark.sql(f"ALTER TABLE ic_exceptions ADD COLUMNS ({_col_def})")
    except Exception as _exc:
        _msg = str(_exc).lower()
        if "already exists" not in _msg and "duplicate" not in _msg:
            print(f"  Warning: could not add column '{_col_def}': {_exc}")

print("ic_exceptions ready.")


# CELL 7 — ic_control_register
# Manually maintained register of all controls (Automated and Manual).
# The engine does not write to this table.
spark.sql("""
CREATE TABLE IF NOT EXISTS ic_control_register (
    control_id              STRING NOT NULL,
    name                    STRING NOT NULL,
    description             STRING,
    control_ref             STRING,
    execution_type          STRING NOT NULL,
    control_type            STRING,
    risk_domain             STRING,
    inherent_risk           STRING,
    control_owner           STRING,
    review_frequency        STRING,
    attestation_frequency   STRING,
    last_design_review_at   DATE,
    active                  BOOLEAN NOT NULL,
    created_at              TIMESTAMP NOT NULL,
    updated_at              TIMESTAMP NOT NULL
)
USING DELTA
""")

print("ic_control_register ready.")


# CELL 8 — ic_manual_attestations
# Written exclusively by nb_ic_02_attest_manual_control.
# report_link  : URL to evidence document (SharePoint share link, etc.)
# evidence_path: Lakehouse Files path if the file was successfully downloaded
#                from report_link; NULL if download failed or link not provided.
spark.sql("""
CREATE TABLE IF NOT EXISTS ic_manual_attestations (
    attestation_id  STRING NOT NULL,
    control_id      STRING NOT NULL,
    attested_by     STRING NOT NULL,
    attested_at     TIMESTAMP NOT NULL,
    period_covered  STRING NOT NULL,
    report_link     STRING,
    evidence_path   STRING,
    notes           STRING,
    next_due_date   DATE
)
USING DELTA
""")

print("ic_manual_attestations ready.")


print("\n=== IC SETUP COMPLETE ===")
print("IC Delta tables: ic_run_results, ic_exceptions, ic_control_register, ic_manual_attestations")
