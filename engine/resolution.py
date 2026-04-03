# =============================================================================
# engine/resolution.py
# Resolution-tracking helpers for the Delta Table violation log.
#
# This module is kept separate from validation_runner.py so that its logic
# can be imported and tested without triggering the runner's module-level
# Spark / Great Expectations bootstrap code.
#
# Public API
# ----------
# VIOLATION_SCHEMA          – canonical Spark schema for dq_violations rows
# _find_stale_violations()  – pure-DataFrame helper (no I/O)
# _apply_resolution_tracking() – MERGE-based persistence (uses Delta Lake)
# =============================================================================

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Canonical schema for dq_violations
# ---------------------------------------------------------------------------

VIOLATION_SCHEMA = StructType([
    StructField("run_id",              StringType(),    False),
    StructField("run_timestamp",       TimestampType(), False),
    StructField("batch_date",          DateType(),      False),
    StructField("rule_group",          StringType(),    False),
    StructField("rule_id",             StringType(),    False),
    StructField("rule_name",           StringType(),    False),
    StructField("table_name",          StringType(),    False),
    StructField("severity",            StringType(),    False),
    StructField("owner",               StringType(),    False),
    StructField("prosess_id",          StringType(),    True),
    StructField("primary_key_value",   StringType(),    True),
    StructField("violated_column",     StringType(),    True),
    StructField("actual_value",        StringType(),    True),
    StructField("expected_condition",  StringType(),    True),
    StructField("violation_detail",    StringType(),    True),
    StructField("saksbehandler_kode",  StringType(),    True),
    StructField("issue_status",        StringType(),    False),
    # Stored as an ISO-8601 string ("2026-04-03T10:00:00") so that the value
    # can be read in environments without full Delta/Spark type coercion.
    StructField("resolution_timestamp", StringType(),   True),
])


# ---------------------------------------------------------------------------
# Pure-DataFrame helpers (no I/O — fully unit-testable)
# ---------------------------------------------------------------------------

def _find_stale_violations(
    existing_active_df: DataFrame,
    current_violations_df: DataFrame,
) -> DataFrame:
    """
    Identify previously Active violations that are no longer present in the
    current run (i.e. the underlying data issue has been fixed).

    Parameters
    ----------
    existing_active_df    : DataFrame with at least (rule_id, primary_key_value)
                            representing the currently Active violations in the
                            Delta table.
    current_violations_df : DataFrame with at least (rule_id, primary_key_value)
                            representing violations detected in this run.

    Returns
    -------
    DataFrame with columns (rule_id, primary_key_value) for violations that
    should be transitioned to Resolved.
    """
    current_keys = current_violations_df.select(
        "rule_id", "primary_key_value"
    ).distinct()
    return existing_active_df.join(
        current_keys, on=["rule_id", "primary_key_value"], how="left_anti"
    ).select("rule_id", "primary_key_value")


# ---------------------------------------------------------------------------
# Delta MERGE persistence
# ---------------------------------------------------------------------------

def _apply_resolution_tracking(
    current_violations_df: DataFrame,
    spark_session,
    violations_table: str = "dq_violations",
    run_timestamp: datetime | None = None,
) -> None:
    """
    Persist violations using MERGE-based resolution tracking:

    1. Previously Active violations whose (rule_id, primary_key_value) is
       **not** in the current run are marked as Resolved with a
       resolution_timestamp.
    2. Violations still present have their run metadata refreshed in-place
       (run_id, run_timestamp, batch_date, violation_detail, actual_value).
    3. Brand-new violations are inserted with issue_status = 'Active' and
       resolution_timestamp = NULL.

    Falls back to a plain append if the table does not yet exist or if the
    Delta MERGE is unavailable (e.g. unit-test environments without a
    Spark metastore).  In that case a warning is printed and the caller
    is advised to re-run nb_dq_00_setup.py.

    Parameters
    ----------
    current_violations_df : Spark DataFrame matching VIOLATION_SCHEMA
    spark_session         : active SparkSession
    violations_table      : fully-qualified Delta table name
    run_timestamp         : timestamp to record for resolutions
                            (defaults to datetime.utcnow())
    """
    ts = (run_timestamp or datetime.utcnow()).isoformat()

    try:
        current_violations_df.createOrReplaceTempView("_dq_current_violations")

        # Step 1 — mark previously Active violations as Resolved if they do
        # not appear in the current run's violation set.
        spark_session.sql(f"""
            MERGE INTO {violations_table} AS t
            USING (
                SELECT DISTINCT a.rule_id, a.primary_key_value
                FROM {violations_table} AS a
                WHERE a.issue_status = 'Active'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM _dq_current_violations cv
                      WHERE cv.rule_id           = a.rule_id
                        AND cv.primary_key_value  = a.primary_key_value
                  )
            ) AS stale
            ON  t.rule_id            = stale.rule_id
            AND t.primary_key_value   = stale.primary_key_value
            AND t.issue_status        = 'Active'
            WHEN MATCHED THEN UPDATE SET
                t.issue_status          = 'Resolved',
                t.resolution_timestamp  = '{ts}'
        """)

        # Step 2 — upsert current violations: refresh run metadata for
        # still-Active rows and insert new violations.
        spark_session.sql(f"""
            MERGE INTO {violations_table} AS t
            USING _dq_current_violations AS s
            ON  t.rule_id            = s.rule_id
            AND t.primary_key_value   = s.primary_key_value
            AND t.issue_status        = 'Active'
            WHEN MATCHED THEN UPDATE SET
                t.run_id             = s.run_id,
                t.run_timestamp      = s.run_timestamp,
                t.batch_date         = s.batch_date,
                t.violation_detail   = s.violation_detail,
                t.actual_value       = s.actual_value
            WHEN NOT MATCHED THEN INSERT *
        """)

        print(f"  Resolution tracking applied via MERGE on '{violations_table}'.")

    except Exception as exc:
        print(f"  Warning: MERGE-based resolution tracking failed ({exc}).")
        print("  Falling back to plain append — run nb_dq_00_setup.py to enable MERGE.")
        current_violations_df.write.mode("append").saveAsTable(violations_table)
