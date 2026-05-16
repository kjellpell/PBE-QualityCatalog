# =============================================================================
# engine/resolution.py
# Resolution-tracking helpers for the violation log.
#
# This module is kept separate from validation_runner.py so that its logic
# can be imported and tested without triggering the runner's module-level
# Spark bootstrap code.
#
# Public API
# ----------
# VIOLATION_SCHEMA             – canonical Spark schema for dq_violations rows
# _apply_resolution_tracking() – DataFrame-based persistence
# =============================================================================

from datetime import datetime, timezone

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
    StructField("owner",               StringType(),    False),
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
    violations_table      : fully-qualified table name (e.g. "qualitycatalog.dq_violations")
    run_timestamp         : timestamp to record for resolutions
                            (defaults to datetime.now(timezone.utc))
    """
    _REQUIRED_COLUMNS = {"rule_id", "primary_key_value", "violated_column", "issue_status"}
    missing = _REQUIRED_COLUMNS - set(current_violations_df.columns)
    if missing:
        raise ValueError(
            f"_apply_resolution_tracking: input DataFrame is missing required "
            f"columns: {sorted(missing)}"
        )

    ts = (run_timestamp or datetime.now(timezone.utc)).isoformat()

    # violated_column is nullable; replace NULL with a sentinel for joining so
    # that two NULL violated_columns are treated as the same key.
    _SENTINEL = "__NULL__"
    _jk = ["rule_id", "primary_key_value", "_vk"]

    def _with_join_key(df: DataFrame) -> DataFrame:
        return df.withColumn("_vk", F.coalesce(F.col("violated_column"), F.lit(_SENTINEL)))

    try:
        merge_key = ["rule_id", "primary_key_value", "violated_column"]
        current_violations_df = current_violations_df.dropDuplicates(merge_key)

        existing_df     = spark_session.table(violations_table)
        existing_active = existing_df.filter(F.col("issue_status") == "Active")
        existing_other  = existing_df.filter(F.col("issue_status") != "Active")

        curr_jk = _with_join_key(current_violations_df)
        act_jk  = _with_join_key(existing_active)

        # Violations not previously active → insert as Active
        brand_new = (
            curr_jk
            .join(act_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk")
        )

        # Still-active violations → refresh run metadata using current row
        still_active = (
            curr_jk
            .join(act_jk.select(_jk), on=_jk, how="inner")
            .drop("_vk")
        )

        # Previously active, absent from current run → mark Resolved
        stale_active = (
            act_jk
            .join(curr_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk")
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
