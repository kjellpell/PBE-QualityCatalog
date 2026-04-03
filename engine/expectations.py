# =============================================================================
# engine/expectations.py
#
# Consolidated generic expectations library for the PBE Quality Catalog.
# All validation logic — generic cross-table, domain-specific (Process,
# Milestone, Invoice) — lives here.  No expectation logic is scattered across
# multiple files.
#
# Each class implements:
#   validate(df, rule, spark) → (result_dict, violations_df | None)
#
#   result_dict keys (GX-compatible):
#     total_rows    int   – rows evaluated
#     passed_rows   int   – rows that satisfy the rule
#     failed_rows   int   – rows that violate the rule
#     success_pct   float – passed_rows / total_rows * 100
#     status        str   – "PASSED" | "FAILED" | "ERROR"
#     details       str   – human-readable summary
#
#   violations_df  Spark DataFrame | None
#     Columns: primary_key_value, violated_column,
#              actual_value, expected_condition, violation_detail
#
# CUSTOM_EXPECTATION_REGISTRY maps YAML expectation names to validator classes.
# Add new entries when creating new custom validators — no other file needs
# to be changed to register a new expectation.
#
# Adding a new expectation:
#   1. Create a class with a validate() method following the contract above.
#   2. Add it to CUSTOM_EXPECTATION_REGISTRY at the bottom of this file.
#   3. Reference it by name in any YAML rule file.
# =============================================================================

from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame


# =============================================================================
# Shared helpers
# =============================================================================

def _empty_violations(spark):
    """Return an empty violations DataFrame with the canonical schema."""
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("primary_key_value",  StringType(), True),
        StructField("violated_column",    StringType(), True),
        StructField("actual_value",       StringType(), True),
        StructField("expected_condition", StringType(), True),
        StructField("violation_detail",   StringType(), True),
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


# =============================================================================
# Generic / cross-table expectations
# =============================================================================

# -----------------------------------------------------------------------------
# validate_column_comparison
# Validates that column_A <operator> column_B is TRUE for every row.
# Rows where either column is NULL are excluded from evaluation.
# -----------------------------------------------------------------------------
class ColumnComparisonExpectation:
    """
    Compares two columns using a configurable operator and fails if any row
    does not satisfy the condition.  Only rows where both columns are non-null
    are evaluated.

    YAML parameters:
      column_A  – name of the left-hand column
      column_B  – name of the right-hand column
      operator  – comparison operator: >, <, >=, <=, ==, !=
      pk_column – primary key column used to identify violating rows
                  (default: "Saksnummer")
    """

    _VIOLATION_FILTERS = {
        ">":  lambda a, b: F.col(a) <= F.col(b),
        "<":  lambda a, b: F.col(a) >= F.col(b),
        ">=": lambda a, b: F.col(a) < F.col(b),
        "<=": lambda a, b: F.col(a) > F.col(b),
        "==": lambda a, b: F.col(a) != F.col(b),
        "!=": lambda a, b: F.col(a) == F.col(b),
    }

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params   = rule.get("parameters", {})
        col_a    = params["column_A"]
        col_b    = params["column_B"]
        operator = params["operator"]
        pk_col   = params.get("pk_column", "Saksnummer")

        if operator not in self._VIOLATION_FILTERS:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details": (
                    f"Unsupported operator '{operator}'. "
                    f"Allowed: {sorted(self._VIOLATION_FILTERS)}"
                ),
            }
            return result, _empty_violations(spark)

        condition = f"{col_a} {operator} {col_b}"

        evaluated = df.filter(
            F.col(col_a).isNotNull() & F.col(col_b).isNotNull()
        )
        total = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details": (
                    f"No rows with both {col_a} and {col_b} populated — skipped."
                ),
            }
            return result, _empty_violations(spark)

        violation_filter = self._VIOLATION_FILTERS[operator](col_a, col_b)
        violations_df    = evaluated.filter(violation_filter)
        failed           = violations_df.count()
        passed           = total - failed

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(condition).alias("violated_column"),
            F.concat(
                F.col(col_a).cast("string"),
                F.lit(f" {operator} "),
                F.col(col_b).cast("string"),
            ).alias("actual_value"),
            F.lit(condition).alias("expected_condition"),
            F.concat(
                F.lit(f"{col_a} = "),
                F.col(col_a).cast("string"),
                F.lit(f", {col_b} = "),
                F.col(col_b).cast("string"),
                F.lit(f": condition '{condition}' not satisfied"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) violate {condition}."
                if failed > 0
                else f"All {total} rows satisfy {condition}."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# sql_validation / sql
# Executes a user-supplied SQL query.  Any row returned by the query is treated
# as a violation — the query should be written to SELECT only offending rows.
# -----------------------------------------------------------------------------
class SqlValidationExpectation:
    """
    Runs an arbitrary SQL query against the Spark metastore.  If the query
    returns zero rows the validation passes; otherwise each returned row is
    recorded as a violation.

    This acts as a fallback for complex rules that cannot be expressed as
    standard GX expectations or simple two-column comparisons.

    YAML parameters:
      sql       – the SQL query to execute (every returned row = one violation)
      pk_column – optional; column in the SQL result to use as the
                  primary_key_value in violations (default: row index)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        sql_query = params.get("sql") or rule.get("sql", "")
        sql_query = sql_query.strip() if sql_query else ""
        pk_col    = params.get("pk_column", None)

        if not sql_query:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     "No SQL query provided in parameters.sql.",
            }
            return result, _empty_violations(spark)

        try:
            result_df = spark.sql(sql_query)
            failed    = result_df.count()
        except Exception as exc:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"SQL execution error: {exc}",
            }
            return result, _empty_violations(spark)

        if failed == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     "SQL query returned 0 rows — validation passed.",
            }
            return result, _empty_violations(spark)

        col_names   = result_df.columns
        detail_expr = F.to_json(F.struct(*[F.col(c) for c in col_names]))

        if pk_col and pk_col in col_names:
            pk_expr = F.col(pk_col).cast("string")
        else:
            result_df = result_df.withColumn(
                "_row_num", F.monotonically_increasing_id()
            )
            pk_expr = F.col("_row_num").cast("string")

        violations_out = result_df.select(
            pk_expr.alias("primary_key_value"),
            F.lit("SQL_VALIDATION").alias("violated_column"),
            F.lit(None).cast("string").alias("actual_value"),
            F.lit("SQL query should return 0 rows").alias("expected_condition"),
            detail_expr.alias("violation_detail"),
        )

        result = {
            "total_rows":  failed,
            "passed_rows": 0,
            "failed_rows": failed,
            "success_pct": 0.0,
            "status":      "FAILED",
            "details":     f"SQL query returned {failed} violation row(s).",
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# expect_column_sum_to_equal
# Validates that SUM(column) == expected_value within an optional tolerance.
# Aggregate / table-level check — no per-row violations are produced.
# -----------------------------------------------------------------------------
class ColumnSumExpectation:
    """
    Validates that SUM(column) equals expected_value (within tolerance).

    YAML parameters:
      column         – numeric column to sum
      expected_value – the value the sum must equal
      tolerance      – allowed absolute deviation (default: 0.01)
      pk_column      – (unused; kept for schema consistency)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        column         = params.get("column")
        expected_value = params.get("expected_value")
        tolerance      = float(params.get("tolerance", 0.01))

        if not column or expected_value is None:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     "Parameters 'column' and 'expected_value' are required.",
            }
            return result, _empty_violations(spark)

        if column not in df.columns:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"Column '{column}' not found in DataFrame.",
            }
            return result, _empty_violations(spark)

        total     = df.count()
        actual    = df.agg(F.sum(F.col(column).cast("double"))).collect()[0][0]
        actual    = float(actual) if actual is not None else 0.0
        expected  = float(expected_value)
        deviation = abs(actual - expected)
        passed    = deviation <= tolerance

        condition = f"SUM({column}) == {expected} (±{tolerance})"

        result = {
            "total_rows":  total,
            "passed_rows": total if passed else 0,
            "failed_rows": 0 if passed else total,
            "success_pct": 100.0 if passed else 0.0,
            "status":      "PASSED" if passed else "FAILED",
            "details": (
                f"SUM({column}) = {actual:.4f}, expected {expected} ±{tolerance}."
                if not passed
                else f"SUM({column}) = {actual:.4f} satisfies {condition}."
            ),
        }
        return result, _empty_violations(spark)


# -----------------------------------------------------------------------------
# expect_row_count_to_be_between
# Validates that the table row count falls within [min_value, max_value].
# PySpark aggregate check against the full dataset.
# -----------------------------------------------------------------------------
class RowCountExpectation:
    """
    Validates that the table row count is between min_value and max_value
    (inclusive).  Produces no per-row violations — only a pass/fail result.

    YAML parameters:
      min_value – minimum acceptable row count (inclusive)
      max_value – maximum acceptable row count (inclusive)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        min_value = params.get("min_value")
        max_value = params.get("max_value")

        if min_value is None or max_value is None:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     "Parameters 'min_value' and 'max_value' are required.",
            }
            return result, _empty_violations(spark)

        total  = df.count()
        passed = int(min_value) <= total <= int(max_value)

        result = {
            "total_rows":  total,
            "passed_rows": total if passed else 0,
            "failed_rows": 0 if passed else total,
            "success_pct": 100.0 if passed else 0.0,
            "status":      "PASSED" if passed else "FAILED",
            "details": (
                f"Row count {total:,} is outside expected range "
                f"[{min_value:,}, {max_value:,}]."
                if not passed
                else f"Row count {total:,} is within [{min_value:,}, {max_value:,}]."
            ),
        }
        return result, _empty_violations(spark)


# -----------------------------------------------------------------------------
# expect_unique_combination_of_columns
# Validates that the combination of specified columns is unique across the table.
# -----------------------------------------------------------------------------
class UniqueColumnCombinationExpectation:
    """
    Validates that the combination of 'columns' is unique per row.
    Duplicate combinations (groups with count > 1) are violations.

    YAML parameters:
      columns   – list of column names that must form a unique key
      pk_column – primary key column used to identify violating rows
                  (default: first column in 'columns')
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params  = rule.get("parameters", {})
        columns = params.get("columns", [])
        pk_col  = params.get("pk_column") or (columns[0] if columns else None)

        if not columns:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     "Parameter 'columns' must be a non-empty list.",
            }
            return result, _empty_violations(spark)

        missing = [c for c in columns if c not in df.columns]
        if missing:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"Column(s) not found in DataFrame: {missing}",
            }
            return result, _empty_violations(spark)

        total = df.count()

        dup_groups = (
            df.groupBy(*columns)
            .agg(F.count("*").alias("_cnt"))
            .filter(F.col("_cnt") > 1)
        )

        violations_df = df.join(dup_groups.drop("_cnt"), on=columns, how="inner")
        failed = violations_df.count()
        passed = total - failed

        col_combo = ", ".join(columns)
        condition  = f"UNIQUE({col_combo})"

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(col_combo).alias("violated_column"),
            F.concat_ws("|", *[F.col(c).cast("string") for c in columns]).alias("actual_value"),
            F.lit(condition).alias("expected_condition"),
            F.lit(f"Duplicate combination of ({col_combo})").alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2) if total else 100.0,
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) are part of duplicate ({col_combo}) combinations."
                if failed > 0
                else f"All {total} rows have a unique ({col_combo}) combination."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# validate_foreign_key
# Validates referential integrity: every non-null value in 'column' must exist
# in 'reference.column' of 'reference.table'.
# -----------------------------------------------------------------------------
class ForeignKeyExpectation:
    """
    Validates that all non-null values in 'column' exist in the reference table.

    YAML parameters:
      column              – column in the source table to check
      pk_column           – primary key column used to identify violating rows
                            (default: same as 'column')
      reference:
        table             – fully-qualified reference table (e.g. Saksbehandling.Handlers)
        column            – column in the reference table that holds valid values
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        column    = params.get("column")
        pk_col    = params.get("pk_column") or column
        ref_block = params.get("reference", {})
        ref_table = ref_block.get("table")
        ref_col   = ref_block.get("column")

        if not column or not ref_table or not ref_col:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details": (
                    "Parameters 'column', 'reference.table', and "
                    "'reference.column' are all required."
                ),
            }
            return result, _empty_violations(spark)

        if column not in df.columns:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"Column '{column}' not found in source DataFrame.",
            }
            return result, _empty_violations(spark)

        try:
            ref_df = spark.read.table(ref_table).select(
                F.col(ref_col).cast("string").alias("_ref_key")
            ).distinct()
        except Exception as exc:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"Could not load reference table '{ref_table}': {exc}",
            }
            return result, _empty_violations(spark)

        evaluated = df.filter(F.col(column).isNotNull())
        total     = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details": f"No non-null values in '{column}' — skipped.",
            }
            return result, _empty_violations(spark)

        violations_df = evaluated.alias("src").join(
            ref_df,
            F.col("src." + column).cast("string") == F.col("_ref_key"),
            how="left_anti",
        )
        failed = violations_df.count()
        passed = total - failed

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(column).alias("violated_column"),
            F.col(column).cast("string").alias("actual_value"),
            F.lit(
                f"{column} must exist in {ref_table}.{ref_col}"
            ).alias("expected_condition"),
            F.concat(
                F.lit(f"Value '"),
                F.col(column).cast("string"),
                F.lit(f"' not found in {ref_table}.{ref_col}"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} value(s) in '{column}' not found in "
                f"{ref_table}.{ref_col}."
                if failed > 0
                else (
                    f"All {total} non-null '{column}' values exist in "
                    f"{ref_table}.{ref_col}."
                )
            ),
        }
        return result, violations_out


# =============================================================================
# Process (Case) expectations — Saksbehandling.Prosesser
# =============================================================================

# -----------------------------------------------------------------------------
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
        pk_col      = params.get("pk_column", "Saksnummer")
        condition   = f"{start_col} <= {stop_col}"

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
# expect_milestone_pairs
# Checks that closed cases have BOTH start and stop milestone dates populated.
# -----------------------------------------------------------------------------
class CaseMilestonePairsExpectation:
    """
    Validates that every case flagged as closed has a complete milestone pair.

    YAML parameters:
      start_column      – column for start milestone date
      stop_column       – column for stop milestone date
      closed_indicator  – the Status value that marks a case as closed
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params           = rule.get("parameters", {})
        start_col        = params["start_column"]
        stop_col         = params["stop_column"]
        closed_indicator = params["closed_indicator"]
        pk_col           = params.get("pk_column", "Saksnummer")

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
# expect_no_open_milestone_pairs
# Checks that a stop milestone does not exist without a start milestone.
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
        pk_col    = params.get("pk_column", "Saksnummer")

        total = df.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

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


# =============================================================================
# Milestone expectations — Saksbehandling.Milepel
# =============================================================================

# -----------------------------------------------------------------------------
# expect_no_duplicate_milestones
# For each group (prosess_id), the milestone_column must not repeat.
# -----------------------------------------------------------------------------
class MilestoneNoDuplicatesExpectation:
    """
    Detects duplicate milestone type values within the same process group.

    YAML parameters:
      milestone_column – column holding the milestone type name (e.g. "Milepel")
      group_column     – column that identifies the process (e.g. "prosess_id")
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params        = rule.get("parameters", {})
        milestone_col = params["milestone_column"]
        group_col     = params["group_column"]

        total = df.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

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
# expect_milestone_sequence
# Validates that milestones appear in the correct chronological order.
# -----------------------------------------------------------------------------
class MilestoneSequenceExpectation:
    """
    Checks that milestones within expected_sequence appear in the correct
    chronological order for each process group.

    YAML parameters:
      milestone_column  – column holding milestone type names
      group_column      – column identifying the process
      date_column       – date/timestamp column used to determine order
      expected_sequence – ordered list of milestone type values
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

        seq_map   = {m: i for i, m in enumerate(expected_sequence)}
        seq_items = list(seq_map.items())

        rank_expr = F.when(
            F.col(milestone_col) == seq_items[0][0], F.lit(seq_items[0][1])
        )
        for milestone_val, rank in seq_items[1:]:
            rank_expr = rank_expr.when(
                F.col(milestone_col) == milestone_val, F.lit(rank)
            )
        rank_expr = rank_expr.otherwise(F.lit(None).cast("int"))

        relevant = df.filter(
            F.col(milestone_col).isin(list(seq_map.keys()))
            & F.col(date_col).isNotNull()
        ).withColumn("_seq_rank", rank_expr)

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
                F.lit(") which in the expected sequence comes after '"),
                F.col("_last_milestone").cast("string"),
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
                f"{failed} process group(s) have out-of-order milestones "
                f"(expected: {seq_str})."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# expect_milestone_pairs_complete
# For each required pair [start_type, stop_type], both types must be present.
# -----------------------------------------------------------------------------
class MilestonePairsCompleteExpectation:
    """
    Validates that paired milestone types both exist within the same process.

    YAML parameters:
      milestone_column – column holding milestone type names
      group_column     – column identifying the process
      required_pairs   – list of [start_type, stop_type] pairs
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        milestone_col  = params["milestone_column"]
        group_col      = params["group_column"]
        required_pairs = params.get("required_pairs", [])

        total = df.count()
        if total == 0 or not required_pairs:
            return _passed_result(total), _empty_violations(spark)

        all_types = list({t for pair in required_pairs for t in pair})

        pivot_df = (
            df.filter(F.col(group_col).isNotNull())
            .withColumn("_in_pair", F.col(milestone_col).isin(all_types))
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
# expect_no_orphan_milestones
# A stop milestone must not exist without a corresponding start milestone.
# -----------------------------------------------------------------------------
class MilestoneNoOrphanExpectation:
    """
    Detects processes where a stop-type milestone exists without the
    corresponding start-type milestone.

    YAML parameters:
      milestone_column – column holding milestone type names
      group_column     – column identifying the process
      pairs            – list of [start_type, stop_type] pairs
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


# =============================================================================
# Invoice expectations — Saksbehandling.Fakturalinjer
# =============================================================================

# -----------------------------------------------------------------------------
# expect_refund_validation
# Negative line amounts are only valid when the invoice type is a credit note.
# -----------------------------------------------------------------------------
class InvoiceRefundValidationExpectation:
    """
    Checks that negative linje_belop values are only present on rows where
    Faktura_type equals the configured credit_type (e.g. 'Kreditnota').

    YAML parameters:
      amount_column – column containing the line amount
      type_column   – column containing the invoice type
      credit_type   – the type value that permits negative amounts
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params      = rule.get("parameters", {})
        amount_col  = params["amount_column"]
        type_col    = params["type_column"]
        credit_type = params["credit_type"]
        pk_col      = params.get("pk_column", "Fakturanr")

        evaluated = df.filter(
            F.col(amount_col).isNotNull() & (F.col(amount_col) < 0)
        )
        total = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  df.count(),
                "passed_rows": df.count(),
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     "No negative amounts found — refund rule not triggered.",
            }
            return result, _empty_violations(spark)

        violations_df = evaluated.filter(
            F.col(type_col).isNull() | (F.col(type_col) != credit_type)
        )
        failed = violations_df.count()
        passed = total - failed

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(type_col).alias("violated_column"),
            F.col(type_col).cast("string").alias("actual_value"),
            F.lit(f"{type_col} must be '{credit_type}' when {amount_col} < 0")
             .alias("expected_condition"),
            F.concat(
                F.lit("Negative amount ("),
                F.col(amount_col).cast("string"),
                F.lit(f") but {type_col} is '"),
                F.coalesce(F.col(type_col).cast("string"), F.lit("NULL")),
                F.lit(f"' instead of '{credit_type}'"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} negative-amount row(s) are not classified as '{credit_type}'."
                if failed > 0
                else (
                    f"All {total} negative-amount rows are correctly "
                    f"classified as '{credit_type}'."
                )
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# expect_invoice_total_consistency
# The sum of line amounts per invoice must match the header total (± tolerance).
# -----------------------------------------------------------------------------
class InvoiceTotalConsistencyExpectation:
    """
    Validates that the sum of all linje_belop values for a given invoice
    matches the Faktura_belop recorded in the header, within a given tolerance.

    YAML parameters:
      invoice_id_column    – column that groups lines into one invoice
      line_amount_column   – the per-line amount column
      header_amount_column – column holding the expected invoice total
      tolerance            – maximum allowed absolute difference (default 0.01)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params             = rule.get("parameters", {})
        invoice_id_col     = params["invoice_id_column"]
        line_amount_col    = params["line_amount_column"]
        header_amount_col  = params["header_amount_column"]
        tolerance          = float(params.get("tolerance", 0.01))

        if header_amount_col not in df.columns:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details": (
                    f"Column '{header_amount_col}' not found in dataframe "
                    f"— skipped.  Join the header table to enable this rule."
                ),
            }
            return result, _empty_violations(spark)

        invoice_totals = df.filter(
            F.col(invoice_id_col).isNotNull()
            & F.col(line_amount_col).isNotNull()
            & F.col(header_amount_col).isNotNull()
        ).groupBy(invoice_id_col, header_amount_col).agg(
            F.sum(line_amount_col).alias("line_sum")
        ).withColumn(
            "diff", F.abs(F.col("line_sum") - F.col(header_amount_col))
        )

        total = invoice_totals.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        violations_df = invoice_totals.filter(F.col("diff") > tolerance)
        failed = violations_df.count()
        passed = total - failed

        violations_out = violations_df.select(
            F.col(invoice_id_col).cast("string").alias("primary_key_value"),
            F.lit(line_amount_col).alias("violated_column"),
            F.col("line_sum").cast("string").alias("actual_value"),
            F.lit(
                f"ABS(SUM({line_amount_col}) - {header_amount_col}) <= {tolerance}"
            ).alias("expected_condition"),
            F.concat(
                F.lit("Invoice "), F.col(invoice_id_col).cast("string"),
                F.lit(": line sum = "), F.col("line_sum").cast("string"),
                F.lit(f", header {header_amount_col} = "),
                F.col(header_amount_col).cast("string"),
                F.lit(", diff = "), F.col("diff").cast("string"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} invoice(s) have a line-sum mismatch exceeding "
                f"tolerance {tolerance}."
                if failed > 0
                else (
                    f"All {total} invoices have consistent line sums "
                    f"(tolerance {tolerance})."
                )
            ),
        }
        return result, violations_out


# =============================================================================
# Registry — maps YAML expectation names to validator classes.
# Add new entries here when creating new custom validators.
# =============================================================================
CUSTOM_EXPECTATION_REGISTRY = {
    # Generic cross-table validators (any table)
    "validate_column_comparison":           ColumnComparisonExpectation,
    "sql_validation":                       SqlValidationExpectation,
    # sql is a shorthand alias (top-level sql: key)
    "sql":                                  SqlValidationExpectation,
    # Aggregate-level validators
    "expect_column_sum_to_equal":           ColumnSumExpectation,
    "expect_row_count_to_be_between":       RowCountExpectation,
    "expect_unique_combination_of_columns": UniqueColumnCombinationExpectation,
    # Cross-table / referential integrity
    "validate_foreign_key":                 ForeignKeyExpectation,
    # Process / Case validators (Saksbehandling.Prosesser)
    "expect_milestone_order":               CaseMilestoneOrderExpectation,
    "expect_milestone_pairs":               CaseMilestonePairsExpectation,
    "expect_no_open_milestone_pairs":       CaseNoOpenMilestonePairsExpectation,
    # Milestone validators (Saksbehandling.Milepel)
    "expect_no_duplicate_milestones":       MilestoneNoDuplicatesExpectation,
    "expect_milestone_sequence":            MilestoneSequenceExpectation,
    "expect_milestone_pairs_complete":      MilestonePairsCompleteExpectation,
    "expect_no_orphan_milestones":          MilestoneNoOrphanExpectation,
    # Invoice validators (Saksbehandling.Fakturalinjer)
    "expect_refund_validation":             InvoiceRefundValidationExpectation,
    "expect_invoice_total_consistency":     InvoiceTotalConsistencyExpectation,
}
