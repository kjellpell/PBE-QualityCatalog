# =============================================================================
# engine/resolution.py
# Resolution-tracking helpers for the Delta Table violation log.
#
# This module is kept separate from validation_runner.py so that its logic
# can be imported and tested without triggering the runner's module-level
# Spark bootstrap code.
#
# Public API
# ----------
# VIOLATION_SCHEMA               – canonical Spark schema for dq_violations rows
# _apply_resolution_tracking()   – MERGE-based persistence (uses Delta Lake)
# IC_EXCEPTION_SCHEMA            – canonical Spark schema for ic_exceptions rows
# _apply_ic_resolution_tracking() – MERGE-based IC persistence
# =============================================================================

import re
from datetime import datetime, timezone

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


def _safe_table_name(name: str) -> str:
    """Validate and return a bare table name.  See engine/runtime.py for docs."""
    bare = name.strip()
    if not re.match(r"^[a-zA-Z0-9_]+$", bare):
        raise ValueError(
            f"Invalid table name '{bare}': only letters, digits, and underscores "
            f"are allowed.  Check table name settings in QualityCatalogConfig.py."
        )
    return bare


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

     1. Violations still present have their run metadata refreshed in-place
         (run_id, run_timestamp, batch_date, violation_detail, actual_value).
     2. Brand-new violations are inserted with issue_status = 'Active' and
         resolution_timestamp = NULL.
     3. Previously Active violations whose (rule_id, primary_key_value) is
         **not** in the current run are marked as Resolved with a
         resolution_timestamp via a follow-up UPDATE.

     The stale-row resolve step is implemented as a separate UPDATE to stay
     compatible with Fabric SQL dialects that do not support
     ``WHEN NOT MATCHED BY SOURCE THEN UPDATE`` in MERGE.

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
                            (defaults to datetime.now(timezone.utc))
    """
    # Validate that the input DataFrame has the columns required by the MERGE
    # key predicates.  Catching this early avoids a cryptic SQL error later.
    _MERGE_KEY_COLUMNS = {"rule_id", "primary_key_value", "violated_column", "issue_status"}
    missing = _MERGE_KEY_COLUMNS - set(current_violations_df.columns)
    if missing:
        raise ValueError(
            f"_apply_resolution_tracking: input DataFrame is missing required "
            f"columns: {sorted(missing)}"
        )

    ts = (run_timestamp or datetime.now(timezone.utc)).isoformat()

    # In Fabric, schema-qualified names like "dbo.dq_violations" are resolved
    # by the Lakehouse connector for DataFrame API calls, but Spark SQL
    # (MERGE, CREATE TEMP VIEW) only sees the Spark metastore where tables are
    # registered under their bare names.  Always use the bare name for SQL.
    _sql_table_name = _safe_table_name(violations_table.split(".")[-1])
    _quoted_violations_table = f"`{_sql_table_name}`"

    try:
        # Deduplicate on the MERGE key before registering the temp view.
        # Rules such as expect_unique_combination_of_columns emit one violation
        # row per duplicate occurrence of the primary key, so the same
        # (rule_id, primary_key_value, violated_column) tuple can appear many
        # times.  Delta MERGE requires source rows to be unique on the join key;
        # keeping one representative row per key is correct here because the
        # MERGE tracks existence, not individual row identity.
        _merge_key = ["rule_id", "primary_key_value", "violated_column"]
        current_violations_df = current_violations_df.dropDuplicates(_merge_key)

        current_violations_df.createOrReplaceTempView("_dq_current_violations")

        # Step 1: MERGE active violations that are still present in this run
        # and insert brand-new active violations.
        # violated_column is included in the ON key (NULL-safe) so that rules
        # producing multiple per-column violations for the same primary key do
        # not cause DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE.
        spark_session.sql(f"""
            MERGE INTO {_quoted_violations_table} AS t
            USING _dq_current_violations AS s
            ON  t.rule_id                          = s.rule_id
            AND t.primary_key_value                = s.primary_key_value
            AND COALESCE(t.violated_column, '')    = COALESCE(s.violated_column, '')
            AND t.issue_status                     = 'Active'
            WHEN MATCHED THEN UPDATE SET
                t.run_id             = s.run_id,
                t.run_timestamp      = s.run_timestamp,
                t.batch_date         = s.batch_date,
                t.violation_detail   = s.violation_detail,
                t.actual_value       = s.actual_value
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Step 2: Mark previously active violations as resolved when they are
        # absent from the current violation set (matched on the same composite key).
        # Fabric does not support subqueries in UPDATE conditions, so we first
        # compute stale active keys with a LEFT ANTI join and then MERGE-update.
        spark_session.sql(f"""
            CREATE OR REPLACE TEMP VIEW _dq_stale_active_keys AS
            SELECT DISTINCT
                t.rule_id,
                t.primary_key_value,
                t.violated_column
            FROM {_quoted_violations_table} AS t
            LEFT ANTI JOIN _dq_current_violations AS s
              ON s.rule_id                       = t.rule_id
             AND s.primary_key_value             = t.primary_key_value
             AND COALESCE(s.violated_column, '') = COALESCE(t.violated_column, '')
            WHERE t.issue_status = 'Active'
        """)

        # Pass the resolution timestamp via a single-row temp view rather than
        # string interpolation, eliminating any SQL injection risk from the value.
        spark_session.createDataFrame(
            [(ts,)], StructType([StructField("resolution_ts", StringType(), False)])
        ).createOrReplaceTempView("_dq_resolution_ts")

        spark_session.sql(f"""
            MERGE INTO {_quoted_violations_table} AS t
            USING _dq_stale_active_keys AS s
            ON  t.rule_id                          = s.rule_id
            AND t.primary_key_value                = s.primary_key_value
            AND COALESCE(t.violated_column, '')    = COALESCE(s.violated_column, '')
            AND t.issue_status                     = 'Active'
            WHEN MATCHED THEN UPDATE SET
                t.issue_status          = 'Resolved',
                t.resolution_timestamp  = (SELECT resolution_ts FROM _dq_resolution_ts)
        """)

        print(f"  Resolution tracking applied via MERGE on '{_sql_table_name}'.")

    except Exception as exc:
        raise RuntimeError(
            f"MERGE failed — violations not written. Check Delta table locks and schema drift. "
            f"Original error: {exc}"
        ) from exc
    finally:
        for _view in ("_dq_current_violations", "_dq_stale_active_keys", "_dq_resolution_ts"):
            try:
                spark_session.catalog.dropTempView(_view)
            except Exception:
                pass  # best-effort cleanup; non-fatal
