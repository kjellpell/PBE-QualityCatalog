# =============================================================================
# dq_expectations/milestone_expectations.py
#
# Custom Great Expectations–style validators for Milestone data
# (Saksbehandling.Milepel table).
#
# The Milepel table has one row per milestone per process:
#   prosess_id  – foreign key to Saksbehandling.Prosesser
#   Milepel     – milestone type / name (e.g. "Startbehandling")
#
# All column references are driven by YAML parameters, keeping Python logic
# completely decoupled from specific column or value names.
#
# Each class implements:
#   validate(df, rule, spark) → (result_dict, violations_df | None)
#
#   result_dict keys (GX-compatible):
#     total_rows    int   – rows / groups evaluated
#     passed_rows   int   – rows / groups that satisfy the rule
#     failed_rows   int   – rows / groups that violate the rule
#     success_pct   float – passed_rows / total_rows * 100
#     status        str   – "PASSED" | "FAILED"
#     details       str   – human-readable summary
#
#   violations_df  Spark DataFrame | None
#     Columns: primary_key_value, violated_column,
#              actual_value, expected_condition, violation_detail
#
# Adding a new expectation:
#   1. Create a class with a validate() method following the contract above.
#   2. Register it in dq_expectations/__init__.py.
#   3. Reference it by name in the YAML rule file.
# =============================================================================

from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame


def _empty_violations(spark):
    """Return an empty violations DataFrame with the canonical schema."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("primary_key_value",   StringType(), True),
        StructField("violated_column",     StringType(), True),
        StructField("actual_value",        StringType(), True),
        StructField("expected_condition",  StringType(), True),
        StructField("violation_detail",    StringType(), True),
    ])
    return spark.createDataFrame([], schema)


def _passed_result(total: int, label: str = "rows") -> dict:
    return {
        "total_rows":  total,
        "passed_rows": total,
        "failed_rows": 0,
        "success_pct": 100.0,
        "status":      "PASSED",
        "details":     f"All {total} evaluated {label} passed.",
    }


# -----------------------------------------------------------------------------
# MIL-003
# expect_no_duplicate_milestones
# For each group (prosess_id), the milestone_column must not repeat the same
# value.  Duplicate milestone types within one process indicate data entry
# errors or source-system idempotency failures.
# -----------------------------------------------------------------------------
class MilestoneNoDuplicatesExpectation:
    """
    Detects duplicate milestone type values within the same process group.

    YAML parameters:
      milestone_column – column holding the milestone type name (e.g. "Milepel")
      group_column     – column that identifies the process (e.g. "prosess_id")
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params           = rule.get("parameters", {})
        milestone_col    = params["milestone_column"]
        group_col        = params["group_column"]

        total = df.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        # Count occurrences of each (group, milestone) combination
        counts = df.filter(
            F.col(group_col).isNotNull() & F.col(milestone_col).isNotNull()
        ).groupBy(group_col, milestone_col).agg(
            F.count("*").alias("_cnt")
        )

        duplicates = counts.filter(F.col("_cnt") > 1)
        failed = duplicates.count()

        if failed == 0:
            return _passed_result(total), _empty_violations(spark)

        violations_out = duplicates.select(
            F.col(group_col).cast("string").alias("primary_key_value"),
            F.lit(milestone_col).alias("violated_column"),
            F.col(milestone_col).cast("string").alias("actual_value"),
            F.lit(
                f"Each value of {milestone_col} must appear at most once "
                f"per {group_col}"
            ).alias("expected_condition"),
            F.concat(
                F.lit(f"Milestone '{milestone_col}' = '"),
                F.col(milestone_col).cast("string"),
                F.lit("' appears "),
                F.col("_cnt").cast("string"),
                F.lit(f" times for {group_col} = '"),
                F.col(group_col).cast("string"),
                F.lit("'"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": total - failed,
            "failed_rows": failed,
            "success_pct": round((total - failed) / total * 100, 2),
            "status":      "FAILED",
            "details": (
                f"{failed} duplicate {milestone_col} value(s) found within "
                f"their {group_col} group."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# MIL-004
# expect_milestone_sequence
# Validates that, within each process group, milestones listed in
# expected_sequence actually appear in that order (sorted by date_column).
# Only groups that contain at least two milestones from the sequence are
# evaluated; groups with zero or one relevant milestone are skipped.
# -----------------------------------------------------------------------------
class MilestoneSequenceExpectation:
    """
    Checks that milestones within expected_sequence appear in the correct
    chronological order for each process group.

    YAML parameters:
      milestone_column  – column holding milestone type names (e.g. "Milepel")
      group_column      – column identifying the process (e.g. "prosess_id")
      date_column       – date/timestamp column used to determine order
                          (e.g. "Milepel_dato")
      expected_sequence – ordered list of milestone type values
                          (e.g. ["Startbehandling", "Behandling", "Stoppbehandling"])
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params            = rule.get("parameters", {})
        milestone_col     = params["milestone_column"]
        group_col         = params["group_column"]
        date_col          = params["date_column"]
        expected_sequence = params.get("expected_sequence", [])

        total = df.count()
        if total == 0 or len(expected_sequence) < 2:
            return _passed_result(total), _empty_violations(spark)

        if date_col not in df.columns:
            result = {
                "total_rows":  total,
                "passed_rows": total,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details": (
                    f"Date column '{date_col}' not found in dataframe — "
                    f"sequence check skipped."
                ),
            }
            return result, _empty_violations(spark)

        # Assign an integer rank to each milestone type in the expected order
        seq_map = {m: i for i, m in enumerate(expected_sequence)}

        # Build a mapping expression: CASE WHEN milestone = v THEN rank ELSE NULL
        rank_expr = F.when(F.lit(False), F.lit(None).cast("int"))
        for milestone_val, rank in seq_map.items():
            rank_expr = rank_expr.when(
                F.col(milestone_col) == milestone_val, F.lit(rank)
            )
        rank_expr = rank_expr.otherwise(F.lit(None).cast("int"))

        # Filter to only rows with a recognised milestone and a non-null date
        relevant = df.filter(
            F.col(milestone_col).isin(list(seq_map.keys()))
            & F.col(date_col).isNotNull()
        ).withColumn("_seq_rank", rank_expr)

        # For each group pick the milestone with the earliest date
        # then the one with the latest date and check their ranks are ordered
        window_asc  = Window.partitionBy(group_col).orderBy(F.col(date_col).asc())
        window_desc = Window.partitionBy(group_col).orderBy(F.col(date_col).desc())

        ranked = relevant.withColumn(
            "_row_asc",  F.row_number().over(window_asc)
        ).withColumn(
            "_row_desc", F.row_number().over(window_desc)
        )

        first_milestone = (
            ranked.filter(F.col("_row_asc") == 1)
            .select(group_col, F.col("_seq_rank").alias("_first_rank"),
                    F.col(milestone_col).alias("_first_milestone"),
                    F.col(date_col).alias("_first_date"))
        )
        last_milestone = (
            ranked.filter(F.col("_row_desc") == 1)
            .select(group_col, F.col("_seq_rank").alias("_last_rank"),
                    F.col(milestone_col).alias("_last_milestone"),
                    F.col(date_col).alias("_last_date"))
        )

        # Groups evaluated = those with at least 2 relevant milestones
        group_counts = relevant.groupBy(group_col).agg(
            F.count("*").alias("_relevant_cnt")
        ).filter(F.col("_relevant_cnt") >= 2)

        evaluated = (
            group_counts
            .join(first_milestone, on=group_col, how="inner")
            .join(last_milestone,  on=group_col, how="inner")
        )

        evaluated_total = evaluated.count()
        if evaluated_total == 0:
            return _passed_result(total), _empty_violations(spark)

        # Violation: first milestone rank > last milestone rank
        # (i.e. chronologically first milestone should have a lower seq_rank)
        violations_df = evaluated.filter(
            F.col("_first_rank") > F.col("_last_rank")
        )
        failed = violations_df.count()

        if failed == 0:
            return _passed_result(total), _empty_violations(spark)

        seq_str = " → ".join(expected_sequence)
        violations_out = violations_df.select(
            F.col(group_col).cast("string").alias("primary_key_value"),
            F.lit(milestone_col).alias("violated_column"),
            F.concat(
                F.lit("first="), F.col("_first_milestone").cast("string"),
                F.lit(", last="),  F.col("_last_milestone").cast("string"),
            ).alias("actual_value"),
            F.lit(
                f"Milestones must appear in sequence: {seq_str}"
            ).alias("expected_condition"),
            F.concat(
                F.lit(f"For {group_col}='"),
                F.col(group_col).cast("string"),
                F.lit(f"', earliest {milestone_col} is '"),
                F.col("_first_milestone").cast("string"),
                F.lit(f"' ({date_col}="),
                F.col("_first_date").cast("string"),
                F.lit(") which comes after '"),
                F.col("_last_milestone").cast("string"),
                F.lit(") in the expected sequence"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": total - failed,
            "failed_rows": failed,
            "success_pct": round((total - failed) / total * 100, 2),
            "status":      "FAILED",
            "details": (
                f"{failed} process group(s) have out-of-order milestones "
                f"(expected: {seq_str})."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# MIL-005
# expect_milestone_pairs_complete
# For each required pair [start_type, stop_type], both milestone types must
# be present for a given process.  A process that has only one side of the
# pair is flagged as a violation.
# -----------------------------------------------------------------------------
class MilestonePairsCompleteExpectation:
    """
    Validates that paired milestone types both exist within the same process.

    YAML parameters:
      milestone_column – column holding milestone type names (e.g. "Milepel")
      group_column     – column identifying the process (e.g. "prosess_id")
      required_pairs   – list of [start_type, stop_type] pairs
                         (e.g. [["Startbehandling", "Stoppbehandling"]])
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        milestone_col  = params["milestone_column"]
        group_col      = params["group_column"]
        required_pairs = params.get("required_pairs", [])

        total = df.count()
        if total == 0 or not required_pairs:
            return _passed_result(total), _empty_violations(spark)

        # Collect all milestone types involved in any pair
        all_types = list({t for pair in required_pairs for t in pair})

        # Pivot: one row per group, one boolean column per milestone type
        pivot_df = (
            df.filter(F.col(group_col).isNotNull())
            .withColumn(
                "_in_pair",
                F.col(milestone_col).isin(all_types),
            )
            .filter(F.col("_in_pair"))
            .groupBy(group_col)
            .agg(*[
                F.max(
                    F.when(F.col(milestone_col) == t, F.lit(True)).otherwise(F.lit(False))
                ).alias(f"_has_{i}")
                for i, t in enumerate(all_types)
            ])
        )
        type_to_col = {t: f"_has_{i}" for i, t in enumerate(all_types)}

        all_violations = _empty_violations(spark)
        total_failed = 0

        for pair in required_pairs:
            start_type, stop_type = pair[0], pair[1]
            has_start = type_to_col.get(start_type)
            has_stop  = type_to_col.get(stop_type)
            if not has_start or not has_stop:
                continue

            # Violation: has one but not both
            pair_viols = pivot_df.filter(
                F.col(has_start) != F.col(has_stop)
            ).select(
                F.col(group_col).cast("string").alias("primary_key_value"),
                F.lit(milestone_col).alias("violated_column"),
                F.when(F.col(has_start), F.lit(start_type))
                 .otherwise(F.lit(stop_type)).alias("actual_value"),
                F.lit(
                    f"Both '{start_type}' and '{stop_type}' must exist for "
                    f"the same {group_col}"
                ).alias("expected_condition"),
                F.when(
                    F.col(has_start) & ~F.col(has_stop),
                    F.concat(
                        F.lit(f"'{start_type}' present but '{stop_type}' missing "
                              f"for {group_col}='"),
                        F.col(group_col).cast("string"), F.lit("'"),
                    ),
                ).otherwise(
                    F.concat(
                        F.lit(f"'{stop_type}' present but '{start_type}' missing "
                              f"for {group_col}='"),
                        F.col(group_col).cast("string"), F.lit("'"),
                    ),
                ).alias("violation_detail"),
            )
            cnt = pair_viols.count()
            total_failed += cnt
            if cnt > 0:
                all_violations = all_violations.unionByName(pair_viols)

        if total_failed == 0:
            return _passed_result(total), _empty_violations(spark)

        result = {
            "total_rows":  total,
            "passed_rows": total - total_failed,
            "failed_rows": total_failed,
            "success_pct": round((total - total_failed) / total * 100, 2),
            "status":      "FAILED",
            "details": (
                f"{total_failed} process group(s) have incomplete milestone pairs."
            ),
        }
        return result, all_violations


# -----------------------------------------------------------------------------
# MIL-006
# expect_no_orphan_milestones
# For each pair [start_type, stop_type], if a stop milestone exists for a
# process then the start milestone must also exist.  A stop without a start
# is an impossible lifecycle state.
# -----------------------------------------------------------------------------
class MilestoneNoOrphanExpectation:
    """
    Detects processes where a stop-type milestone exists without the
    corresponding start-type milestone.

    YAML parameters:
      milestone_column – column holding milestone type names (e.g. "Milepel")
      group_column     – column identifying the process (e.g. "prosess_id")
      pairs            – list of [start_type, stop_type] pairs
                         (e.g. [["Startbehandling", "Stoppbehandling"]])
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params        = rule.get("parameters", {})
        milestone_col = params["milestone_column"]
        group_col     = params["group_column"]
        pairs         = params.get("pairs", [])

        total = df.count()
        if total == 0 or not pairs:
            return _passed_result(total), _empty_violations(spark)

        all_types = list({t for pair in pairs for t in pair})

        pivot_df = (
            df.filter(F.col(group_col).isNotNull())
            .filter(F.col(milestone_col).isin(all_types))
            .groupBy(group_col)
            .agg(*[
                F.max(
                    F.when(F.col(milestone_col) == t, F.lit(True)).otherwise(F.lit(False))
                ).alias(f"_has_{i}")
                for i, t in enumerate(all_types)
            ])
        )
        type_to_col = {t: f"_has_{i}" for i, t in enumerate(all_types)}

        all_violations = _empty_violations(spark)
        total_failed = 0

        for pair in pairs:
            start_type, stop_type = pair[0], pair[1]
            has_start = type_to_col.get(start_type)
            has_stop  = type_to_col.get(stop_type)
            if not has_start or not has_stop:
                continue

            # Violation: stop exists but start does not
            pair_viols = pivot_df.filter(
                F.col(has_stop) & ~F.col(has_start)
            ).select(
                F.col(group_col).cast("string").alias("primary_key_value"),
                F.lit(milestone_col).alias("violated_column"),
                F.lit(stop_type).alias("actual_value"),
                F.lit(
                    f"'{start_type}' must exist when '{stop_type}' is present"
                ).alias("expected_condition"),
                F.concat(
                    F.lit(f"'{stop_type}' exists but '{start_type}' is missing "
                          f"for {group_col}='"),
                    F.col(group_col).cast("string"), F.lit("'"),
                ).alias("violation_detail"),
            )
            cnt = pair_viols.count()
            total_failed += cnt
            if cnt > 0:
                all_violations = all_violations.unionByName(pair_viols)

        if total_failed == 0:
            return _passed_result(total), _empty_violations(spark)

        result = {
            "total_rows":  total,
            "passed_rows": total - total_failed,
            "failed_rows": total_failed,
            "success_pct": round((total - total_failed) / total * 100, 2),
            "status":      "FAILED",
            "details": (
                f"{total_failed} process group(s) have a stop milestone "
                f"without a corresponding start milestone."
            ),
        }
        return result, all_violations
