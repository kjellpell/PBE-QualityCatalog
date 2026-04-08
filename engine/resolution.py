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
    IntegerType,
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
    StructField("failure_type",        StringType(),    True),
    StructField("primary_key_value",   StringType(),    True),
    StructField("violated_column",     StringType(),    True),
    StructField("actual_value",        StringType(),    True),
    StructField("expected_condition",  StringType(),    True),
    StructField("violation_detail",    StringType(),    True),
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

    The three cases are handled in a single atomic MERGE statement using the
    ``NOT MATCHED BY SOURCE`` clause (Delta Lake 2.0+ / Fabric Spark 3.3+).
    This eliminates the former two-step approach where a failure between the
    two statements could permanently lose violation records.

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
    # Validate that the input DataFrame has the columns required by the MERGE
    # key predicates.  Catching this early avoids a cryptic SQL error later.
    _MERGE_KEY_COLUMNS = {"rule_id", "primary_key_value", "issue_status"}
    missing = _MERGE_KEY_COLUMNS - set(current_violations_df.columns)
    if missing:
        raise ValueError(
            f"_apply_resolution_tracking: input DataFrame is missing required "
            f"columns: {sorted(missing)}"
        )

    ts = (run_timestamp or datetime.utcnow()).isoformat()

    try:
        current_violations_df.createOrReplaceTempView("_dq_current_violations")
        # Register the resolution timestamp as a single-row view so it is
        # passed as a typed value rather than interpolated directly into SQL.
        ts_df = spark_session.createDataFrame([(ts,)], ["_ts"])
        ts_df.createOrReplaceTempView("_dq_run_ts")

        # Single atomic MERGE (Delta Lake 2.0+ / Fabric Spark 3.3+):
        #   WHEN MATCHED               → refresh still-active violations
        #   WHEN NOT MATCHED BY SOURCE → mark stale violations as Resolved
        #   WHEN NOT MATCHED BY TARGET → insert brand-new violations
        spark_session.sql(f"""
            MERGE INTO {violations_table} AS t
            USING _dq_current_violations AS s
            ON  t.rule_id           = s.rule_id
            AND t.primary_key_value  = s.primary_key_value
            AND t.issue_status       = 'Active'
            WHEN MATCHED THEN UPDATE SET
                t.run_id             = s.run_id,
                t.run_timestamp      = s.run_timestamp,
                t.batch_date         = s.batch_date,
                t.violation_detail   = s.violation_detail,
                t.actual_value       = s.actual_value
            WHEN NOT MATCHED BY SOURCE
              AND t.issue_status = 'Active'
              THEN UPDATE SET
                t.issue_status          = 'Resolved',
                t.resolution_timestamp  = (SELECT _ts FROM _dq_run_ts)
            WHEN NOT MATCHED BY TARGET THEN INSERT *
        """)

        print(f"  Resolution tracking applied via MERGE on '{violations_table}'.")

    except Exception as exc:
        print(f"  Warning: MERGE-based resolution tracking failed ({exc}).")
        print("  Falling back to plain append — run nb_dq_00_setup.py to enable MERGE.")
        current_violations_df.write.mode("append").saveAsTable(violations_table)
    finally:
        for _view in ("_dq_current_violations", "_dq_run_ts"):
            try:
                spark_session.catalog.dropTempView(_view)
            except Exception:
                pass  # best-effort cleanup; non-fatal


# ---------------------------------------------------------------------------
# IC-specific schema for ic_exceptions
# ---------------------------------------------------------------------------

IC_EXCEPTION_SCHEMA = StructType([
    StructField("run_id",                StringType(),    False),
    StructField("run_timestamp",         TimestampType(), False),
    StructField("batch_date",            DateType(),      False),
    StructField("rule_group",            StringType(),    False),
    StructField("rule_id",               StringType(),    False),
    StructField("rule_name",             StringType(),    False),
    StructField("table_name",            StringType(),    False),
    StructField("severity",              StringType(),    False),
    StructField("owner",                 StringType(),    False),
    StructField("failure_type",          StringType(),    True),
    StructField("primary_key_value",     StringType(),    True),
    StructField("violated_column",       StringType(),    True),
    StructField("actual_value",          StringType(),    True),
    StructField("expected_condition",    StringType(),    True),
    StructField("violation_detail",      StringType(),    True),
    # IC lifecycle
    StructField("ic_status",             StringType(),    False),   # Open | Remediated | Verified | Waived
    # IC metadata from rule definition
    StructField("control_ref",           StringType(),    True),
    StructField("control_type",          StringType(),    True),
    StructField("risk_domain",           StringType(),    True),
    StructField("remediation_due_days",  IntegerType(),   True),
    StructField("remediation_due_date",  DateType(),      True),    # first_seen_at + remediation_due_days
    # Timestamps
    StructField("first_seen_at",         TimestampType(), True),    # set on INSERT, never updated by engine
    # Human-set workflow fields — written only by nb_ic_01_manage_exceptions
    StructField("remediated_by",         StringType(),    True),
    StructField("remediated_at",         TimestampType(), True),
    StructField("verified_by",           StringType(),    True),
    StructField("verified_at",           TimestampType(), True),
    StructField("waived_by",             StringType(),    True),
    StructField("waived_at",             TimestampType(), True),
    StructField("waiver_reason",         StringType(),    True),
])


def _apply_ic_resolution_tracking(
    current_ic_violations_df: DataFrame,
    spark_session,
    ic_exceptions_table: str = "default.ic_exceptions",
    run_timestamp: datetime | None = None,  # noqa: F841 — reserved for future use
) -> None:
    """
    Persist IC violations using a MERGE with 4-state lifecycle semantics.

    Key differences from _apply_resolution_tracking():

    1. WHEN MATCHED (ic_status = 'Open' and violation still present):
         Refresh run metadata only.  Do NOT change ic_status.
    2. WHEN NOT MATCHED BY SOURCE (Open violation gone from current run):
         INTENTIONALLY OMITTED.  IC exceptions must NOT be auto-resolved
         when a violation disappears — they require human sign-off via
         nb_ic_01_manage_exceptions.  The row stays Open.
    3. WHEN NOT MATCHED BY TARGET (new violation):
         INSERT with ic_status = 'Open'.  first_seen_at and
         remediation_due_date are populated in the caller before this call.

    Terminal states (Verified, Waived):
         The ON condition matches only ic_status = 'Open', so Verified and
         Waived rows are never touched by the engine.  If the same
         (rule_id, primary_key_value) re-appears after being Verified or
         Waived, it will NOT match an Open row and is therefore inserted as
         a brand-new Open exception.

    Falls back to plain append if MERGE is unavailable (e.g. unit-test
    environments without a Spark metastore).
    """
    _IC_MERGE_KEY_COLUMNS = {"rule_id", "primary_key_value", "ic_status"}
    missing = _IC_MERGE_KEY_COLUMNS - set(current_ic_violations_df.columns)
    if missing:
        raise ValueError(
            f"_apply_ic_resolution_tracking: input DataFrame is missing "
            f"required columns: {sorted(missing)}"
        )

    try:
        current_ic_violations_df.createOrReplaceTempView("_ic_current_violations")

        spark_session.sql(f"""
            MERGE INTO {ic_exceptions_table} AS t
            USING _ic_current_violations AS s
            ON  t.rule_id           = s.rule_id
            AND t.primary_key_value  = s.primary_key_value
            AND t.ic_status          = 'Open'
            WHEN MATCHED THEN UPDATE SET
                t.run_id           = s.run_id,
                t.run_timestamp    = s.run_timestamp,
                t.batch_date       = s.batch_date,
                t.violation_detail = s.violation_detail,
                t.actual_value     = s.actual_value
            WHEN NOT MATCHED BY TARGET THEN INSERT *
        """)
        # NOT MATCHED BY SOURCE is intentionally omitted — see docstring.

        print(f"  IC resolution tracking applied via MERGE on '{ic_exceptions_table}'.")

    except Exception as exc:
        print(f"  Warning: IC MERGE failed ({exc}). Falling back to plain append.")
        current_ic_violations_df.write.mode("append").saveAsTable(ic_exceptions_table)
    finally:
        try:
            spark_session.catalog.dropTempView("_ic_current_violations")
        except Exception:
            pass  # best-effort cleanup; non-fatal
