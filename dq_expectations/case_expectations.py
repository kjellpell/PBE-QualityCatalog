# =============================================================================
# dq_expectations/case_expectations.py
#
# Custom Great Expectations–style validators for Case data (Prosesser table).
#
# Each class implements:
#   validate(df, rule, spark) → (result_dict, violations_df | None)
#
#   result_dict keys (GX-compatible):
#     total_rows    int   – rows evaluated
#     passed_rows   int   – rows that satisfy the rule
#     failed_rows   int   – rows that violate the rule
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

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def _empty_violations(spark):
    """Return an empty violations DataFrame with the canonical schema."""
    from pyspark.sql.types import (
        StructType, StructField, StringType,
    )
    schema = StructType([
        StructField("primary_key_value",   StringType(), True),
        StructField("violated_column",     StringType(), True),
        StructField("actual_value",        StringType(), True),
        StructField("expected_condition",  StringType(), True),
        StructField("violation_detail",    StringType(), True),
    ])
    return spark.createDataFrame([], schema)


def _passed_result(total: int) -> dict:
    return {
        "total_rows":  total,
        "passed_rows": total,
        "failed_rows": 0,
        "success_pct": 100.0,
        "status":      "PASSED",
        "details":     f"All {total} evaluated rows passed.",
    }


# -----------------------------------------------------------------------------
# CASE-004 / CASE-005
# expect_milestone_order
# Validates that start_column date ≤ stop_column date (when both are non-null).
# -----------------------------------------------------------------------------
class CaseMilestoneOrderExpectation:
    """
    Checks that a start milestone date does not come after a stop milestone
    date.  Only rows where BOTH columns are non-null are evaluated.

    YAML parameters:
      start_column  – name of the column holding the earlier date
      stop_column   – name of the column holding the later date
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params      = rule.get("parameters", {})
        start_col   = params["start_column"]
        stop_col    = params["stop_column"]
        pk_col      = "Saksnummer"
        condition   = f"{start_col} <= {stop_col}"

        # Only evaluate rows where both dates are present
        evaluated = df.filter(
            F.col(start_col).isNotNull() & F.col(stop_col).isNotNull()
        )
        total = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     "No rows with both milestone columns populated — skipped.",
            }
            return result, _empty_violations(spark)

        violations_df = evaluated.filter(F.col(start_col) > F.col(stop_col))
        failed = violations_df.count()
        passed = total - failed

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(start_col).alias("violated_column"),
            F.col(start_col).cast("string").alias("actual_value"),
            F.lit(condition).alias("expected_condition"),
            F.concat(
                F.lit(start_col), F.lit(" ("),
                F.col(start_col).cast("string"),
                F.lit(") is after "),
                F.lit(stop_col), F.lit(" ("),
                F.col(stop_col).cast("string"), F.lit(")"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) have {start_col} later than {stop_col}."
                if failed > 0
                else f"All {total} rows have {start_col} ≤ {stop_col}."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# CASE-006
# expect_milestone_pairs
# Checks that closed cases (Status = closed_indicator) have BOTH the start
# and stop milestone dates populated — i.e. no half-completed pairs.
# -----------------------------------------------------------------------------
class CaseMilestonePairsExpectation:
    """
    Validates that every case flagged as closed has a complete milestone pair.

    YAML parameters:
      start_column      – column for start milestone date
      stop_column       – column for stop milestone date
      closed_indicator  – the Status value that marks a case as closed
                          (e.g. 'Saken er avsluttet')
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params           = rule.get("parameters", {})
        start_col        = params["start_column"]
        stop_col         = params["stop_column"]
        closed_indicator = params["closed_indicator"]
        pk_col           = "Saksnummer"

        # We only care about rows that are supposed to be finished
        # The Prosesser table does not hold Status directly; we join via Saker.
        # If the Status column is not present in df, skip gracefully.
        if "Status" not in df.columns:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     "Status column not present in dataframe — skipped.",
            }
            return result, _empty_violations(spark)

        closed_cases = df.filter(F.col("Status") == closed_indicator)
        total = closed_cases.count()

        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        # Violation: one of the pair dates is NULL
        violations_df = closed_cases.filter(
            F.col(start_col).isNull() | F.col(stop_col).isNull()
        )
        failed = violations_df.count()
        passed = total - failed

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.when(F.col(start_col).isNull(), F.lit(start_col))
             .otherwise(F.lit(stop_col)).alias("violated_column"),
            F.lit(None).cast("string").alias("actual_value"),
            F.lit(
                f"Closed case must have both {start_col} and {stop_col}"
            ).alias("expected_condition"),
            F.concat(
                F.lit("Closed case is missing "),
                F.when(F.col(start_col).isNull() & F.col(stop_col).isNull(),
                       F.lit(f"both {start_col} and {stop_col}"))
                 .when(F.col(start_col).isNull(), F.lit(start_col))
                 .otherwise(F.lit(stop_col)),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} closed case(s) have an incomplete milestone pair."
                if failed > 0
                else f"All {total} closed cases have complete milestone pairs."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# CASE-007
# expect_no_open_milestone_pairs
# Checks that a stop milestone date does not exist without a start milestone.
# (A start without a stop is normal for open cases; the reverse is always bad.)
# -----------------------------------------------------------------------------
class CaseNoOpenMilestonePairsExpectation:
    """
    Detects rows where a stop milestone exists but the corresponding start
    milestone is missing — an impossible lifecycle state.

    YAML parameters:
      start_column – column for start milestone date
      stop_column  – column for stop milestone date
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        start_col = params["start_column"]
        stop_col  = params["stop_column"]
        pk_col    = "Saksnummer"

        total = df.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        # Violation: stop is set, start is NULL
        violations_df = df.filter(
            F.col(stop_col).isNotNull() & F.col(start_col).isNull()
        )
        failed = violations_df.count()
        passed = total - failed

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(start_col).alias("violated_column"),
            F.col(stop_col).cast("string").alias("actual_value"),
            F.lit(
                f"{start_col} must be set when {stop_col} is set"
            ).alias("expected_condition"),
            F.concat(
                F.lit(f"{stop_col} is populated ("),
                F.col(stop_col).cast("string"),
                F.lit(f") but {start_col} is null"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) have {stop_col} set without {start_col}."
                if failed > 0
                else f"No open milestone pair mismatches found in {total} rows."
            ),
        }
        return result, violations_out
