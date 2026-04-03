# =============================================================================
# dq_expectations/common_expectations.py
#
# Generic / cross-table custom expectations that are not specific to a single
# domain (Process, Milestone, Invoice).
#
# Provides:
#   ColumnComparisonExpectation       – validates column_A <op> column_B per row
#   SqlValidationExpectation          – executes a SQL query; any returned row is
#                                       treated as a violation
#   ColumnSumExpectation              – validates that SUM(column) equals an expected value
#   RowCountExpectation               – validates that table row count is within a range
#   UniqueColumnCombinationExpectation– validates that a set of columns forms a unique key
#   ForeignKeyExpectation             – validates referential integrity between tables
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
# Adding a new expectation:
#   1. Create a class with a validate() method following the contract above.
#   2. Register it in dq_expectations/__init__.py.
#   3. Reference it by name in the YAML rule file.
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


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

    # Map operator strings to the negated Spark column filter expression used
    # to detect VIOLATIONS (i.e. rows where the condition is NOT met).
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

        # Only evaluate rows where both columns are non-null
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
# sql_validation
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
        # Support both nested (parameters.sql) and top-level (rule.sql) formats
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

        # Serialise each returned row as JSON for violation_detail
        col_names  = result_df.columns
        detail_expr = F.to_json(F.struct(*[F.col(c) for c in col_names]))

        if pk_col and pk_col in col_names:
            pk_expr  = F.col(pk_col).cast("string")
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
# Validates that the aggregate SUM of a numeric column equals an expected value
# (within an optional tolerance).  This is an aggregate / table-level check:
# no per-row violations are produced — only a single pass/fail result.
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
# Validates that the total row count of the table falls within [min_value,
# max_value].  This is a PySpark aggregate check that runs against the full
# dataset (unlike the GX native expect_table_row_count_to_be_between which
# operates on a sampled Pandas frame).
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
# Validates that the combination of the specified columns is unique across the
# table.  Rows that share an identical combination with at least one other row
# are reported as violations.
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

        # Find groups that have duplicates
        dup_groups = (
            df.groupBy(*columns)
            .agg(F.count("*").alias("_cnt"))
            .filter(F.col("_cnt") > 1)
        )

        # Join back to get all violating rows
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
# Validates referential integrity between tables: every non-null value in
# 'column' must exist in 'reference.column' of 'reference.table'.
# The reference table is loaded dynamically from the Spark metastore.
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

        # Only evaluate non-null source values
        evaluated = df.filter(F.col(column).isNotNull())
        total     = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     f"No non-null values in '{column}' to validate.",
            }
            return result, _empty_violations(spark)

        src_keyed = evaluated.withColumn("_src_key", F.col(column).cast("string"))

        violations_df = src_keyed.join(ref_df, src_keyed["_src_key"] == ref_df["_ref_key"], how="left_anti")
        failed = violations_df.count()
        passed = total - failed

        condition = f"{column} EXISTS IN {ref_table}.{ref_col}"

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(column).alias("violated_column"),
            F.col(column).cast("string").alias("actual_value"),
            F.lit(condition).alias("expected_condition"),
            F.concat(
                F.lit(f"{column} = "),
                F.col(column).cast("string"),
                F.lit(f" not found in {ref_table}.{ref_col}"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2) if total else 100.0,
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} value(s) in '{column}' not found in "
                f"{ref_table}.{ref_col}."
                if failed > 0
                else f"All {total} non-null '{column}' values exist in "
                     f"{ref_table}.{ref_col}."
            ),
        }
        return result, violations_out
