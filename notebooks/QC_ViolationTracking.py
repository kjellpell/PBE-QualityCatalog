# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# =============================================================================
# QC_ViolationTracking
#
# A standalone extraction of QC_Engine's violation lifecycle wrapper (see
# `_apply_resolution_tracking` in QC_Engine.py, "Violation persistence" in
# ARCHITECTURE.md, and "Resolution Tracking" in README.md).
#
# This is the piece that answers "when did this error first appear, and when
# did it go away?" It is completely independent of *what* decided a row or
# group is wrong — QC_Engine's rule types, QC_EventFlow's check_event_flow(),
# a Great Expectations validation, a one-off SQL query — any of them can feed
# it, as long as their output is the same six-column shape:
#
#   primaernoekkel_verdi, identifikator_verdi, avvikende_kolonne,
#   faktisk_verdi, forventet_betingelse, avviksdetaljer
#
# (check_event_flow() in QC_EventFlow.py already returns exactly this.)
#
# Two functions:
#   stamp_violations()        - attach run/rule metadata to a check's raw
#                                output, producing a full VIOLATION_SCHEMA row.
#   apply_resolution_tracking() - diff a run's stamped violations against the
#                                persisted table and rewrite it:
#     - new violation                 -> inserted, avviksstatus = 'Aktiv'
#     - still present                 -> metadata refreshed, but
#                                         foerst_observert_tidspunkt (first
#                                         seen) is carried over unchanged
#     - previously active, now absent -> avviksstatus = 'Løst',
#                                         loest_tidspunkt (closed) stamped
#     - already 'Løst'                -> carried through unchanged
#
# Example — running one check end to end:
#
#     violations = check_event_flow(df, event_column=..., group_column=..., ...)
#     stamped = stamp_violations(
#         violations,
#         kjoert_id=run_id, kjoert_tidspunkt=run_started_at,
#         regelgruppe="Milepæler", regel_id="MIL-004", regelnavn="...",
#         tabellnavn="milepaeler", avviksomfang="Gruppe",
#     )
#     apply_resolution_tracking(stamped, spark, violations_table="datakvalitet.avvik")
#
# Violations are keyed on (regel_id, primaernoekkel_verdi, avvikende_kolonne,
# forventet_betingelse) — forventet_betingelse is part of the key because a
# group-scoped check can emit several distinct violations for one group that
# differ only in which part of the check failed (e.g. check_event_flow always
# emits at most one, but a hand-written check covering several conditions at
# once might not).
#
# Requires a Delta-backed violations table: this does a read-then-overwrite,
# which plain parquet rejects.
# =============================================================================

from datetime import date, datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VIOLATION_SCHEMA = StructType([
    StructField("kjoert_id",             StringType(),    False),
    StructField("kjoert_tidspunkt",      TimestampType(), False),
    StructField("kjoert_dato",           DateType(),      False),
    StructField("regelgruppe",           StringType(),    False),
    StructField("regel_id",              StringType(),    False),
    StructField("regelnavn",             StringType(),    False),
    StructField("tabellnavn",            StringType(),    False),
    StructField("primaernoekkel_verdi",  StringType(),    True),
    StructField("identifikator_verdi",   StringType(),    True),
    StructField("avvikende_kolonne",     StringType(),    True),
    StructField("faktisk_verdi",         StringType(),    True),
    StructField("forventet_betingelse",  StringType(),    True),
    StructField("avviksdetaljer",        StringType(),    True),
    StructField("avviksstatus",          StringType(),    False),
    # Stored as an ISO-8601 string ("2026-04-03T10:00:00") so the value can be
    # read in environments without full Delta/Spark type coercion.
    StructField("loest_tidspunkt",       StringType(),    True),
    # Set once when the violation is first detected; preserved on every
    # subsequent run so violation age can be calculated as
    # (now - foerst_observert_tidspunkt).
    StructField("foerst_observert_tidspunkt", TimestampType(), True),
    # "Rad" - primaernoekkel_verdi is a row's primary key; "Gruppe" - it's a
    # group key (e.g. event_flow); "Tabell" - the whole table is the unit.
    StructField("avviksomfang",          StringType(),    True),
])


def stamp_violations(
    violations_df: DataFrame,
    *,
    kjoert_id: str,
    kjoert_tidspunkt: datetime,
    regelgruppe: str,
    regel_id: str,
    regelnavn: str,
    tabellnavn: str,
    avviksomfang: str = "Rad",
    kjoert_dato: date | None = None,
) -> DataFrame:
    """
    Attach run/rule metadata to a check's raw six-column output, producing a
    DataFrame matching VIOLATION_SCHEMA — ready for apply_resolution_tracking().

    `violations_df` must have exactly: primaernoekkel_verdi, identifikator_verdi,
    avvikende_kolonne, faktisk_verdi, forventet_betingelse, avviksdetaljer.
    """
    kjoert_dato = kjoert_dato or kjoert_tidspunkt.date()
    return violations_df.select(
        F.lit(kjoert_id).alias("kjoert_id"),
        F.lit(kjoert_tidspunkt).alias("kjoert_tidspunkt"),
        F.lit(str(kjoert_dato)).cast("date").alias("kjoert_dato"),
        F.lit(regelgruppe).alias("regelgruppe"),
        F.lit(regel_id).alias("regel_id"),
        F.lit(regelnavn).alias("regelnavn"),
        F.lit(tabellnavn).alias("tabellnavn"),
        F.col("primaernoekkel_verdi"),
        F.col("identifikator_verdi"),
        F.col("avvikende_kolonne"),
        F.col("faktisk_verdi"),
        F.col("forventet_betingelse"),
        F.col("avviksdetaljer"),
        F.lit("Aktiv").alias("avviksstatus"),
        F.lit(None).cast("string").alias("loest_tidspunkt"),
        # apply_resolution_tracking() preserves this value for still-active
        # violations; a brand-new violation uses this run as its origin.
        F.lit(kjoert_tidspunkt).alias("foerst_observert_tidspunkt"),
        F.lit(avviksomfang).alias("avviksomfang"),
    )


def apply_resolution_tracking(
    current_violations_df: DataFrame,
    spark_session,
    violations_table: str = "avvik",
    run_timestamp: datetime | None = None,
) -> None:
    """
    Persist violations with resolution tracking using pure DataFrame operations.

    Fabric's SQL engine cannot resolve schema-qualified Hive metastore table
    names inside MERGE statements, so this uses the DataFrame API
    (spark.table / DataFrame.write.saveAsTable), which does not have that
    limitation.

    Logic applied:
      1. Violations still present -> row replaced with current run metadata
         (kjoert_id, kjoert_tidspunkt, kjoert_dato, avviksdetaljer, faktisk_verdi);
         avviksstatus stays 'Aktiv'.
      2. Brand-new violations -> inserted with avviksstatus = 'Aktiv' and
         loest_tidspunkt = NULL.
      3. Previously Aktiv violations absent from this run -> avviksstatus set
         to 'Løst', loest_tidspunkt set to the run timestamp.
      4. Already-Løst historical rows -> kept unchanged.

    Parameters
    ----------
    current_violations_df : Spark DataFrame matching VIOLATION_SCHEMA
    spark_session          : active SparkSession
    violations_table       : fully-qualified table name (e.g. "datakvalitet.avvik")
    run_timestamp          : timestamp to record for resolutions
                              (defaults to datetime.now(timezone.utc))
    """
    _REQUIRED_COLUMNS = {
        "regel_id", "primaernoekkel_verdi", "avvikende_kolonne",
        "forventet_betingelse", "avviksstatus",
    }
    missing = _REQUIRED_COLUMNS - set(current_violations_df.columns)
    if missing:
        raise ValueError(
            f"apply_resolution_tracking: input DataFrame is missing required "
            f"columns: {sorted(missing)}"
        )

    ts = (run_timestamp or datetime.now(timezone.utc)).isoformat()

    # avvikende_kolonne and forventet_betingelse are nullable; replace NULL with
    # a sentinel for joining so two NULL values are treated as the same key.
    #
    # forventet_betingelse is part of the key because a group-scoped check can
    # emit several distinct violations for the same (regel_id,
    # primaernoekkel_verdi, avvikende_kolonne) — one per failing condition —
    # differing only in forventet_betingelse. Without it those rows would
    # collapse to one under dropDuplicates/left-anti and the extra violations
    # would be lost. forventet_betingelse is a deterministic rule-level string
    # (it never contains per-row data), so keying on it keeps resolution stable.
    _SENTINEL = "__NULL__"
    _jk = ["regel_id", "primaernoekkel_verdi", "_vk", "_ek"]

    def _with_join_key(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_vk", F.coalesce(F.col("avvikende_kolonne"), F.lit(_SENTINEL)))
            .withColumn("_ek", F.coalesce(F.col("forventet_betingelse"), F.lit(_SENTINEL)))
        )

    try:
        merge_key = ["regel_id", "primaernoekkel_verdi", "avvikende_kolonne", "forventet_betingelse"]
        current_violations_df = current_violations_df.dropDuplicates(merge_key)

        # Break the read's lineage to violations_table before the final write
        # targets the same table, otherwise Spark's analyzer rejects the write
        # with UNSUPPORTED_OVERWRITE.TABLE ("can't overwrite the target that is
        # also being read from") even though the write only happens afterwards.
        existing_df     = spark_session.table(violations_table).localCheckpoint(eager=True)
        existing_active = existing_df.filter(F.col("avviksstatus") == "Aktiv")
        # Everything that is not Aktiv is carried through unchanged. Use a
        # NULL-safe negation so legacy rows with a NULL avviksstatus (e.g. rows
        # predating the column) are preserved rather than dropped on rewrite.
        existing_other  = existing_df.filter(
            ~(F.col("avviksstatus") == "Aktiv") | F.col("avviksstatus").isNull()
        )

        curr_jk = _with_join_key(current_violations_df)
        act_jk  = _with_join_key(existing_active)

        # Violations not previously active -> insert as Active
        brand_new = (
            curr_jk
            .join(act_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk", "_ek")
        )

        # Still-active violations -> refresh run metadata but preserve
        # foerst_observert_tidspunkt from the existing row so violation age is
        # measured from initial detection.
        _orig_first_seen = act_jk.select(_jk + ["foerst_observert_tidspunkt"]).withColumnRenamed(
            "foerst_observert_tidspunkt", "_orig_first_seen_at"
        )
        still_active = (
            curr_jk
            .join(_orig_first_seen, on=_jk, how="inner")
            .withColumn("foerst_observert_tidspunkt", F.col("_orig_first_seen_at"))
            .drop("_orig_first_seen_at", "_vk", "_ek")
        )

        # Previously active, absent from current run -> mark Løst
        stale_active = (
            act_jk
            .join(curr_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk", "_ek")
            .withColumn("avviksstatus", F.lit("Løst"))
            .withColumn("loest_tidspunkt", F.lit(ts))
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
