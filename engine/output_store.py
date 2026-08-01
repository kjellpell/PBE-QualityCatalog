# =============================================================================
# engine/output_store.py
# Canonical output schemas, and resolution tracking for the violation log.
#
# This module is kept separate from engine/runner.py so that it can be
# imported without triggering the runner's module-level Spark bootstrap.  The
# table schemas live here for the same reason: scripts/setup_dq_tables.py generates its
# DDL from them, so the column lists are defined once rather than restated in
# the setup script.
#
# Public API
# ----------
# RESULT_SCHEMA                – canonical Spark schema for dq_run_results rows
# VIOLATION_SCHEMA             – canonical Spark schema for dq_violations rows
# _apply_resolution_tracking() – DataFrame-based persistence
# =============================================================================

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Canonical schema for dq_run_results
# ---------------------------------------------------------------------------

RESULT_SCHEMA = StructType([
    StructField("run_id",               StringType(),    False),
    StructField("run_timestamp",        TimestampType(), False),
    StructField("batch_date",           DateType(),      False),
    StructField("rule_group",           StringType(),    False),
    StructField("rule_id",              StringType(),    False),
    StructField("rule_name",            StringType(),    False),
    StructField("table_name",           StringType(),    False),
    StructField("expectation",          StringType(),    False),
    StructField("total_rows",           LongType(),      True),
    StructField("passed_rows",          LongType(),      True),
    StructField("failed_rows",          LongType(),      True),
    StructField("success_pct",          DoubleType(),    True),
    StructField("status",               StringType(),    False),
    StructField("details",              StringType(),    True),
    StructField("rule_duration_seconds", DoubleType(),   True),
    # Populated only when status = 'ERROR'; NULL for PASSED/FAILED rules.
    # Values: 'infrastructure' | 'configuration' | 'source_data'
    StructField("error_category",       StringType(),    True),
])


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
    StructField("primary_key_value",   StringType(),    True),
    StructField("violated_column",     StringType(),    True),
    StructField("actual_value",        StringType(),    True),
    StructField("expected_condition",  StringType(),    True),
    StructField("violation_detail",    StringType(),    True),
    StructField("issue_status",        StringType(),    False),
    # Stored as an ISO-8601 string ("2026-04-03T10:00:00") so that the value
    # can be read in environments without full Delta/Spark type coercion.
    StructField("resolution_timestamp", StringType(),   True),
    # Set once when the violation is first detected; preserved on every subsequent
    # run so violation age can be calculated as (now - first_seen_at).
    StructField("first_seen_at",       TimestampType(), True),
    # "row" — primary_key_value is a PK in table_name; "group" — it's a group key
    # (event_flow, required_event, aggregate_matches).
    StructField("violation_scope",     StringType(),    True),
])


# ---------------------------------------------------------------------------
# Delta DataFrame persistence
# ---------------------------------------------------------------------------

def _apply_resolution_tracking(
    current_violations_df: DataFrame,
    spark_session,
    violations_table: str = "dq_violations",
    run_timestamp: datetime | None = None,
) -> None:
    """
    Persist violations with resolution tracking using pure DataFrame operations.

    Fabric's SQL engine cannot resolve schema-qualified Hive metastore table
    names inside MERGE statements, so all persistence uses the DataFrame API
    (spark.table / DataFrame.write.saveAsTable) which bypasses that limitation.

    Logic applied:
      1. Violations still present → row replaced with current run metadata
         (run_id, run_timestamp, batch_date, violation_detail, actual_value);
         issue_status stays 'Active'.
      2. Brand-new violations → inserted with issue_status = 'Active' and
         resolution_timestamp = NULL.
      3. Previously Active violations absent from this run → issue_status set
         to 'Resolved', resolution_timestamp set to the run timestamp.
      4. Already-Resolved historical rows → kept unchanged.

    Parameters
    ----------
    current_violations_df : Spark DataFrame matching VIOLATION_SCHEMA
    spark_session         : active SparkSession
    violations_table      : fully-qualified table name (e.g. "datakvalitet.dq_violations")
    run_timestamp         : timestamp to record for resolutions
                            (defaults to datetime.now(timezone.utc))
    """
    _REQUIRED_COLUMNS = {
        "rule_id", "primary_key_value", "violated_column",
        "expected_condition", "issue_status",
    }
    missing = _REQUIRED_COLUMNS - set(current_violations_df.columns)
    if missing:
        raise ValueError(
            f"_apply_resolution_tracking: input DataFrame is missing required "
            f"columns: {sorted(missing)}"
        )

    ts = (run_timestamp or datetime.now(timezone.utc)).isoformat()

    # violated_column and expected_condition are nullable; replace NULL with a
    # sentinel for joining so two NULL values are treated as the same key.
    #
    # expected_condition is part of the key because group-style expectations
    # (e.g. event_flow) emit several distinct violations for the same
    # (rule_id, primary_key_value, violated_column) — one per required pair —
    # differing only in expected_condition.  Without it those rows would collapse
    # to one under dropDuplicates/left-anti and the extra violations would be
    # lost.  expected_condition is a deterministic rule/pair-level string (it
    # never contains per-row data), so keying on it keeps resolution stable.
    _SENTINEL = "__NULL__"
    _jk = ["rule_id", "primary_key_value", "_vk", "_ek"]

    def _with_join_key(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_vk", F.coalesce(F.col("violated_column"), F.lit(_SENTINEL)))
            .withColumn("_ek", F.coalesce(F.col("expected_condition"), F.lit(_SENTINEL)))
        )

    try:
        merge_key = ["rule_id", "primary_key_value", "violated_column", "expected_condition"]
        current_violations_df = current_violations_df.dropDuplicates(merge_key)

        # Break the read's lineage to violations_table before the final write
        # targets the same table, otherwise Spark's analyzer rejects the write
        # with UNSUPPORTED_OVERWRITE.TABLE ("can't overwrite the target that is
        # also being read from") even though the write only happens afterwards.
        existing_df     = spark_session.table(violations_table).localCheckpoint(eager=True)
        existing_active = existing_df.filter(F.col("issue_status") == "Active")
        # Everything that is not Active is carried through unchanged.  Use a
        # NULL-safe negation so legacy rows with a NULL issue_status (e.g. rows
        # predating the column) are preserved rather than dropped on rewrite.
        existing_other  = existing_df.filter(
            ~(F.col("issue_status") == "Active") | F.col("issue_status").isNull()
        )

        curr_jk = _with_join_key(current_violations_df)
        act_jk  = _with_join_key(existing_active)

        # Violations not previously active → insert as Active
        brand_new = (
            curr_jk
            .join(act_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk", "_ek")
        )

        # Still-active violations → refresh run metadata but preserve first_seen_at
        # from the existing row so violation age is measured from initial detection.
        _orig_first_seen = act_jk.select(_jk + ["first_seen_at"]).withColumnRenamed(
            "first_seen_at", "_orig_first_seen_at"
        )
        still_active = (
            curr_jk
            .join(_orig_first_seen, on=_jk, how="inner")
            .withColumn("first_seen_at", F.col("_orig_first_seen_at"))
            .drop("_orig_first_seen_at", "_vk", "_ek")
        )

        # Previously active, absent from current run → mark Resolved
        stale_active = (
            act_jk
            .join(curr_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk", "_ek")
            .withColumn("issue_status", F.lit("Resolved"))
            .withColumn("resolution_timestamp", F.lit(ts))
        )

        final_df = (
            existing_other
            .unionByName(stale_active)
            .unionByName(still_active)
            .unionByName(brand_new)
        )

        final_df.write.mode("overwrite").saveAsTable(violations_table)
        print(f"  Resolution tracking applied on '{violations_table}'.")

    except Exception as exc:
        raise RuntimeError(
            f"Violations not written. Original error: {exc}"
        ) from exc
