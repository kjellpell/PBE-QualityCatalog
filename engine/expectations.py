# =============================================================================
# engine/expectations.py
#
# Consolidated generic expectations library for the PBE Quality Catalog.
# All validation logic lives here as fully parameterised, table-agnostic
# validators.  No expectation is hard-coded to a specific table or domain.
#
# Each class implements:
#   validate(df, rule, spark) -> (result_dict, violations_df | None)
#
#   result_dict keys:
#     total_rows    int   - rows evaluated
#     passed_rows   int   - rows that satisfy the rule
#     failed_rows   int   - rows that violate the rule
#     success_pct   float - passed_rows / total_rows * 100
#     status        str   - "PASSED" | "FAILED" | "ERROR"
#     details       str   - human-readable summary
#
#   violations_df  Spark DataFrame | None
#     Columns: primary_key_value, violated_column,
#              actual_value, expected_condition, violation_detail
#
# CUSTOM_EXPECTATION_REGISTRY maps expectation names to validator classes.
# Add new entries when creating new custom validators - no other file needs
# to be changed to register a new expectation.
#
# Adding a new expectation:
#   1. Create a class with a validate() method following the contract above.
#   2. Add it to CUSTOM_EXPECTATION_REGISTRY at the bottom of this file.
#   3. Reference it by name in YAML rule files.
# =============================================================================

from functools import reduce

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


def _error_result(message: str) -> dict:
    return {
        "total_rows":  0,
        "passed_rows": 0,
        "failed_rows": 0,
        "success_pct": 0.0,
        "status":      "ERROR",
        "details":     message,
    }


def _safe_pct(passed: int, total: int) -> float:
    """Return passed/total as a percentage, guarding against division by zero."""
    return round(passed / total * 100, 2) if total else 100.0


def _normalise_stops(raw_stop) -> list:
    """
    Normalise the stop-slot of a pair to a list.

    Allows the YAML pair syntax to use either a scalar stop value or a list of
    acceptable stop values:
      - ``[A, B]``       → stops = [B]       (single stop, scalar form)
      - ``[A, [B, C]]``  → stops = [B, C]    (one-to-many, list form)
    """
    return list(raw_stop) if isinstance(raw_stop, list) else [raw_stop]


def _resolve_gate_groups(df: DataFrame, gate: dict, group_col: str):
    """
    Return the subset of *df* whose group_col values belong to groups that
    have passed the completion gate.

    A group passes the gate when at least one of its rows satisfies:
      - event_column == value  (the designated completion marker)
      - order_column IS NOT NULL (optional; when provided the marker row must
        have a non-null value in this column to be considered done)

    Parameters (from the YAML ``completion_gate`` block):
      event_column - column holding the marker value (canonical name)
      value        - the value that signals group completion
      order_column - (optional) column that must be non-null on the marker row; (canonical name)
                     when omitted any row with the marker value closes the group

    If the gate block is absent or empty, the original DataFrame is returned
    unchanged so that the caller validates all groups.
    """
    if not gate:
        return df

    gate_value_col = gate.get("event_column")
    gate_value     = gate.get("value")
    gate_sort_col  = gate.get("order_column")

    if not gate_value_col or gate_value is None:
        return df

    gate_filter = F.col(gate_value_col) == gate_value
    if gate_sort_col:
        gate_filter = gate_filter & F.col(gate_sort_col).isNotNull()

    completed_groups = (
        df.filter(gate_filter)
        .select(group_col)
        .distinct()
    )
    return df.join(completed_groups, on=group_col, how="inner")


# =============================================================================
# Generic / cross-table expectations
# =============================================================================

# -----------------------------------------------------------------------------
# expect_column_values_to_not_be_null
# Generic not-null validator used by YAML rules with top-level 'column'.
# -----------------------------------------------------------------------------
class ExpectColumnValuesToNotBeNullExpectation:
    """
    Validates that every row has a non-null value in the target column.

    YAML fields:
      column                 - target column (top-level or parameters.column)
      parameters.pk_column   - optional PK column for violations (default: id)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params = rule.get("parameters", {})
        column = rule.get("column") or params.get("column")
        pk_col = params.get("pk_column", "id")

        if not column:
            return (
                _error_result("Missing required field 'column' for not-null expectation."),
                _empty_violations(spark),
            )

        if column not in df.columns:
            return (
                _error_result(f"Column '{column}' not found in DataFrame."),
                _empty_violations(spark),
            )

        total = df.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        violations_df = df.filter(F.col(column).isNull())
        failed = violations_df.count()
        passed = total - failed

        pk_expr = (
            F.col(pk_col).cast("string")
            if pk_col in df.columns
            else F.lit(None).cast("string")
        )

        violations_out = violations_df.select(
            pk_expr.alias("primary_key_value"),
            F.lit(column).alias("violated_column"),
            F.lit(None).cast("string").alias("actual_value"),
            F.lit("NOT NULL").alias("expected_condition"),
            F.concat(F.lit("Column '"), F.lit(column), F.lit("' is NULL")).alias("violation_detail"),
        )

        result = {
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status": "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) have NULL in '{column}'."
                if failed > 0
                else f"All {total} rows have non-null '{column}'."
            ),
        }
        return result, violations_out

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

    YAML parameters (canonical names):
      left_column   - name of the left-hand column (was: column_A)
      right_column  - name of the right-hand column (was: column_B)
      operator      - comparison operator: >, <, >=, <=, ==, !=
      pk_column     - primary key column used to identify violating rows
                      (default: "id")
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
        col_a    = params.get("left_column")
        col_b    = params.get("right_column")
        operator = params.get("operator")
        pk_col   = params.get("pk_column", "id")

        if not col_a or not col_b or not operator:
            return (
                _error_result(
                    "Parameters 'left_column', 'right_column', and 'operator' are all required."
                ),
                _empty_violations(spark),
            )

        missing = [c for c in (col_a, col_b) if c not in df.columns]
        if missing:
            return (
                _error_result(f"Column(s) not found in DataFrame: {missing}"),
                _empty_violations(spark),
            )

        if operator not in self._VIOLATION_FILTERS:
            return (
                _error_result(
                    f"Unsupported operator '{operator}'. "
                    f"Allowed: {sorted(self._VIOLATION_FILTERS)}"
                ),
                _empty_violations(spark),
            )

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
                    f"No rows with both {col_a} and {col_b} populated - skipped."
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
            "success_pct": _safe_pct(passed, total),
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
# as a violation - the query should be written to SELECT only offending rows.
# -----------------------------------------------------------------------------
class SqlValidationExpectation:
    """
    Runs an arbitrary SQL query against the Spark metastore.  If the query
    returns zero rows the validation passes; otherwise each returned row is
    recorded as a violation.

    YAML parameters:
      sql       - the SQL query to execute (every returned row = one violation)
      pk_column - optional; column in the SQL result to use as the
                  primary_key_value in violations (default: row index)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        sql_query = params.get("sql") or rule.get("sql", "")
        sql_query = sql_query.strip() if sql_query else ""
        pk_col    = params.get("pk_column", None)

        if not sql_query:
            return (
                _error_result("No SQL query provided in parameters.sql."),
                _empty_violations(spark),
            )

        try:
            result_df = spark.sql(sql_query)
            failed    = result_df.count()
        except Exception as exc:
            return (
                _error_result(f"SQL execution error: {exc}"),
                _empty_violations(spark),
            )

        if failed == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     "SQL query returned 0 rows - validation passed.",
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
# expect_row_count_to_be_between
# Validates that the table row count falls within [min_value, max_value].
# -----------------------------------------------------------------------------
class RowCountExpectation:
    """
    Validates that the table row count is between min_value and max_value
    (inclusive).

    YAML parameters:
      min_value - minimum acceptable row count (inclusive)
      max_value - maximum acceptable row count (inclusive)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        min_value = params.get("min_value")
        max_value = params.get("max_value")

        if min_value is None or max_value is None:
            return (
                _error_result(
                    "Parameters 'min_value' and 'max_value' are required."
                ),
                _empty_violations(spark),
            )

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

    YAML parameters:
      columns   - list of column names that must form a unique key
      pk_column - primary key column used to identify violating rows
                  (default: first column in 'columns')
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params  = rule.get("parameters", {})
        columns = params.get("columns", [])
        pk_col  = params.get("pk_column") or (columns[0] if columns else None)

        if not columns:
            return (
                _error_result("Parameter 'columns' must be a non-empty list."),
                _empty_violations(spark),
            )

        missing = [c for c in columns if c not in df.columns]
        if missing:
            return (
                _error_result(f"Column(s) not found in DataFrame: {missing}"),
                _empty_violations(spark),
            )

        total = df.count()

        dup_groups = (
            df.groupBy(*columns)
            .agg(F.count("*").alias("_cnt"))
            .filter(F.col("_cnt") > 1)
        )

        # Get failed count from the already-computed group aggregation rather
        # than joining back and counting, saving one full-table scan.
        failed_agg = dup_groups.agg(F.sum("_cnt")).collect()[0][0]
        failed = int(failed_agg) if failed_agg else 0
        passed = total - failed

        violations_df = df.join(dup_groups.drop("_cnt"), on=columns, how="inner")

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

    YAML parameters (canonical names):
      column               - column in the source table to check
      pk_column            - primary key column (default: same as 'column')
      reference_table      - fully-qualified reference table (was: reference.table)
      reference_column     - column in the reference table holding valid values (was: reference.column)
    """

    def validate(self, df: DataFrame, rule: dict, spark, ref_cache: dict = None) -> tuple:
        params     = rule.get("parameters", {})
        column     = params.get("column")
        pk_col     = params.get("pk_column") or column
        ref_table  = params.get("reference_table")
        ref_col    = params.get("reference_column")

        if not column or not ref_table or not ref_col:
            return (
                _error_result(
                    "Parameters 'column', 'reference_table', and "
                    "'reference_column' are all required."
                ),
                _empty_violations(spark),
            )

        if column not in df.columns:
            return (
                _error_result(f"Column '{column}' not found in source DataFrame."),
                _empty_violations(spark),
            )

        # Use pre-loaded reference DataFrame from cache when available,
        # avoiding redundant table reads when multiple rules share a ref table.
        cache_key = (ref_table, ref_col)
        if ref_cache is not None and cache_key in ref_cache:
            ref_df = ref_cache[cache_key]
        else:
            try:
                ref_df = spark.read.table(ref_table).select(
                    F.col(ref_col).cast("string").alias("_ref_key")
                ).distinct()
            except Exception as exc:
                return (
                    _error_result(f"Could not load reference table '{ref_table}': {exc}"),
                    _empty_violations(spark),
                )
            if ref_cache is not None:
                ref_cache[cache_key] = ref_df

        evaluated = df.filter(F.col(column).isNotNull())
        total     = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details":     f"No non-null values in '{column}' - skipped.",
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
            F.lit(f"{column} must exist in {ref_table}.{ref_col}").alias("expected_condition"),
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
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} value(s) in '{column}' not found in {ref_table}.{ref_col}."
                if failed > 0
                else f"All {total} non-null '{column}' values exist in {ref_table}.{ref_col}."
            ),
        }
        return result, violations_out


# =============================================================================
# Module-level constants shared across multiple expectation classes
# =============================================================================

# Aggregate function map used by ValidateAggregateRuleExpectation and
# ValidateGroupAggregateMatchExpectation.  Defined once here to avoid
# duplication and ensure both classes stay in sync.
_AGGREGATE_FUNCTIONS = {
    "sum":   F.sum,
    "count": F.count,
    "avg":   F.avg,
    "min":   F.min,
    "max":   F.max,
}


# =============================================================================
# New generic expectations (Phase 1 additions)
# =============================================================================

# -----------------------------------------------------------------------------
# validate_aggregate_rule
# Generic aggregate validation: applies any aggregate function to a column and
# compares the result to a threshold using any comparison operator.
# More flexible replacement for expect_column_sum_to_equal.
# -----------------------------------------------------------------------------
class ValidateAggregateRuleExpectation:
    """
    Computes an aggregate over a column and validates it against a threshold.

    YAML parameters:
      column    - numeric column to aggregate (omit or use count with no column)
      aggregate - aggregation function: sum, count, avg, min, max
      operator  - comparison operator: >, <, >=, <=, ==, !=
      threshold - the value the aggregate must satisfy
    """

    _OPS = {
        ">":  lambda a, b: a > b,
        "<":  lambda a, b: a < b,
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        column    = params.get("column")
        aggregate = str(params.get("aggregate", "sum")).lower()
        operator  = params.get("operator", ">=")
        threshold = params.get("threshold")

        if threshold is None:
            return (
                _error_result("Parameter 'threshold' is required."),
                _empty_violations(spark),
            )

        if aggregate not in _AGGREGATE_FUNCTIONS:
            return (
                _error_result(
                    f"Unsupported aggregate '{aggregate}'. "
                    f"Allowed: {sorted(_AGGREGATE_FUNCTIONS)}"
                ),
                _empty_violations(spark),
            )

        if operator not in self._OPS:
            return (
                _error_result(
                    f"Unsupported operator '{operator}'. "
                    f"Allowed: {sorted(self._OPS)}"
                ),
                _empty_violations(spark),
            )

        if aggregate != "count":
            if not column or column not in df.columns:
                return (
                    _error_result(
                        f"Column '{column}' not found in DataFrame."
                        if column
                        else "Parameter 'column' is required when aggregate != 'count'."
                    ),
                    _empty_violations(spark),
                )
            agg_row = df.agg(
                F.count(F.lit(1)).alias("_total"),
                _AGGREGATE_FUNCTIONS[aggregate](F.col(column).cast("double")).alias("_agg"),
            ).collect()[0]
            total  = agg_row["_total"]
            actual = float(agg_row["_agg"]) if agg_row["_agg"] is not None else 0.0
        else:
            total  = df.count()
            actual = float(total)

        threshold_f = float(threshold)
        passed      = self._OPS[operator](actual, threshold_f)
        col_label   = column if column else "*"
        condition   = f"{aggregate.upper()}({col_label}) {operator} {threshold_f}"

        result = {
            "total_rows":  total,
            "passed_rows": total if passed else 0,
            "failed_rows": 0 if passed else total,
            "success_pct": 100.0 if passed else 0.0,
            "status":      "PASSED" if passed else "FAILED",
            "details": (
                f"{aggregate.upper()}({col_label}) = {actual} satisfies {condition}."
                if passed
                else f"{aggregate.upper()}({col_label}) = {actual}; expected {condition}."
            ),
        }
        return result, _empty_violations(spark)


# -----------------------------------------------------------------------------
# validate_not_null_when
# Validates that checked_columns are NOT NULL whenever trigger_column satisfies
# the configured condition.
#
# This single generic expectation replaces the former table-specific:
#   expect_milestone_pairs        (closed case => both dates required)
#   expect_no_open_milestone_pairs (stop set => start must also be set)
# -----------------------------------------------------------------------------
class ValidateNotNullWhenExpectation:
    """
    Validates that all checked_columns are not null when when_column
    satisfies the trigger condition.

    YAML parameters (canonical names):
      when_column        - column that triggers the check (was: trigger_column, condition_column)
      operator           - "==" (equality), "IS NOT NULL", or "IS NULL"
      value              - required when operator is "=="
      checked_columns    - list of columns that must be NOT NULL when triggered (was: check_columns)
      pk_column          - primary key column
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params        = rule.get("parameters", {})
        condition_col = params.get("when_column")
        condition_op  = params.get("operator", "==")
        condition_val = params.get("value")
        check_cols    = params.get("checked_columns", [])
        pk_col        = params.get("pk_column", "id")

        if not condition_col or not check_cols:
            return (
                _error_result(
                    "Parameters 'when_column' and 'checked_columns' "
                    "(non-empty list) are required."
                ),
                _empty_violations(spark),
            )

        if condition_op not in ("==", "IS NOT NULL", "IS NULL"):
            return (
                _error_result(
                    f"Unsupported operator '{condition_op}'. "
                    "Allowed: '==', 'IS NOT NULL', 'IS NULL'"
                ),
                _empty_violations(spark),
            )

        if condition_op == "==" and condition_val is None:
            return (
                _error_result("Parameter 'value' is required when operator is '=='."),
                _empty_violations(spark),
            )

        if condition_op == "IS NOT NULL":
            conditional_rows = df.filter(F.col(condition_col).isNotNull())
            cond_label = f"{condition_col} IS NOT NULL"
        elif condition_op == "IS NULL":
            conditional_rows = df.filter(F.col(condition_col).isNull())
            cond_label = f"{condition_col} IS NULL"
        else:
            conditional_rows = df.filter(F.col(condition_col) == condition_val)
            cond_label = f"{condition_col} == '{condition_val}'"

        total = conditional_rows.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        null_filter = F.col(check_cols[0]).isNull()
        for col in check_cols[1:]:
            null_filter = null_filter | F.col(col).isNull()

        violations_df = conditional_rows.filter(null_filter)
        failed = violations_df.count()
        passed = total - failed

        check_cols_str = ", ".join(check_cols)
        expected_cond  = (
            f"When {cond_label}, all of [{check_cols_str}] must be NOT NULL"
        )

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(check_cols_str).alias("violated_column"),
            F.lit(None).cast("string").alias("actual_value"),
            F.lit(expected_cond).alias("expected_condition"),
            F.concat(
                F.lit(f"Condition '{cond_label}' is met but "),
                F.lit(check_cols_str),
                F.lit(" contain NULL value(s)"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) where {cond_label} have NULL in [{check_cols_str}]."
                if failed > 0
                else (
                    f"All {total} row(s) where {cond_label} "
                    f"have [{check_cols_str}] populated."
                )
            ),
        }
        return result, violations_out


# =============================================================================
# Generic sequence / ordering expectations
# (previously milestone-specific; now fully parameterised for any table)
# =============================================================================

# -----------------------------------------------------------------------------
# validate_sequence_order
# Validates that values appear in an expected order within each group,
# as determined by a date/sort column.
# Replaces the former table-specific expect_milestone_sequence.
# -----------------------------------------------------------------------------
class ValidateSequenceOrderExpectation:
    """
    Checks that values in event_column appear in the specified expected_sequence
    order for each group identified by group_column.

    YAML parameters (canonical names):
      event_column       - column holding the sequence value names (was: value_column)
      group_column       - column that identifies the group
      order_column       - column used to determine the order of rows within each
                          group; can be any sortable type (date, numeric, string) (was: sort_column)
      expected_sequence  - ordered list of sequence steps; two formats supported:

          Simple string list (all steps are strict):
              expected_sequence: ["Start", "Middle", "End"]

          Dict list with optional flexible flag (allows repetition of a step):
              expected_sequence:
                - value: "Start"
                - value: "Middle"
                  flexible: true   # consecutive repeats of this step are allowed
                - value: "End"

          When a step is marked flexible: true, consecutive rows with that value
          are permitted before the sequence advances to the next step.  Strict
          steps (flexible omitted or false) must not repeat.

      completion_gate    - (optional) only evaluate groups that are "done":
          event_column   - column holding the completion marker value
          value         - the value that signals the group is closed
          order_column   - (optional) column that must be non-null on the marker row
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params       = rule.get("parameters", {})
        value_col    = params.get("event_column")
        group_col    = params.get("group_column")
        sort_col     = params.get("order_column")
        raw_sequence = params.get("expected_sequence", [])
        gate         = params.get("completion_gate", {})

        df = _resolve_gate_groups(df, gate, group_col)

        if not value_col:
            return (
                _error_result(
                    "Parameter 'value_column' is required for validate_sequence_order."
                ),
                _empty_violations(spark),
            )

        total = df.count()

        # Parse expected_sequence: support both plain string list and dict list
        # with optional flexible flag.
        seq_values   = []
        flexible_set = set()
        for item in raw_sequence:
            if isinstance(item, dict):
                val = item.get("value", "")
                seq_values.append(val)
                if item.get("flexible", False):
                    flexible_set.add(val)
            else:
                seq_values.append(str(item))

        allow_loops = params.get("allow_loops", False)
        if allow_loops:
            flexible_set = set(seq_values)  # every step is flexible when allow_loops is True

        if total == 0:
            return _passed_result(total), _empty_violations(spark)
        if len(seq_values) < 2:
            return (
                _error_result(
                    "Parameter 'expected_sequence' must contain at least 2 values "
                    "to define an ordering relationship."
                ),
                _empty_violations(spark),
            )

        if not sort_col or sort_col not in df.columns:
            return (
                _error_result(
                    f"Parameter 'sort_column' ('{sort_col}') is required and must "
                    f"exist in the dataframe for validate_sequence_order."
                ),
                _empty_violations(spark),
            )

        seq_map   = {v: i for i, v in enumerate(seq_values)}
        seq_items = list(seq_map.items())

        rank_expr = F.when(F.col(value_col) == seq_items[0][0], F.lit(seq_items[0][1]))
        for val, rank in seq_items[1:]:
            rank_expr = rank_expr.when(F.col(value_col) == val, F.lit(rank))
        rank_expr = rank_expr.otherwise(F.lit(None).cast("int"))

        relevant = df.filter(
            F.col(value_col).isin(list(seq_map.keys()))
            & F.col(sort_col).isNotNull()
        ).withColumn("_seq_rank", rank_expr)

        group_counts = relevant.groupBy(group_col).agg(
            F.count("*").alias("_relevant_cnt")
        ).filter(F.col("_relevant_cnt") >= 2)

        evaluated_total = group_counts.count()
        if evaluated_total == 0:
            return _passed_result(total), _empty_violations(spark)

        relevant_filtered = relevant.join(
            group_counts.select(group_col), on=group_col, how="inner"
        )

        # Check ordering row-by-row within each group (sorted by sort_column).
        # A violation occurs when:
        #   1. The sequence rank decreases (out-of-order value), or
        #   2. The rank is unchanged (repeated value) and the step is not flexible.
        window_grp = Window.partitionBy(group_col).orderBy(F.col(sort_col).asc())

        ranked = relevant_filtered.withColumn(
            "_prev_rank", F.lag("_seq_rank").over(window_grp)
        )

        is_out_of_order   = F.col("_seq_rank") < F.col("_prev_rank")
        if flexible_set:
            is_illegal_repeat = (
                (F.col("_seq_rank") == F.col("_prev_rank"))
                & ~F.col(value_col).isin(list(flexible_set))
            )
        else:
            is_illegal_repeat = F.col("_seq_rank") == F.col("_prev_rank")

        violation_row_expr = (
            F.col("_prev_rank").isNotNull()
            & (is_out_of_order | is_illegal_repeat)
        )

        violation_groups = (
            ranked
            .withColumn("_row_violation", violation_row_expr.cast("int"))
            .groupBy(group_col)
            .agg(F.max("_row_violation").alias("_has_violation"))
            .filter(F.col("_has_violation") == 1)
        )

        failed = violation_groups.count()

        if failed == 0:
            return _passed_result(total), _empty_violations(spark)

        # Gather first/last values per violated group for violation reporting.
        window_asc  = Window.partitionBy(group_col).orderBy(F.col(sort_col).asc())
        window_desc = Window.partitionBy(group_col).orderBy(F.col(sort_col).desc())

        relevant_rn = relevant_filtered.withColumn(
            "_row_asc",  F.row_number().over(window_asc)
        ).withColumn(
            "_row_desc", F.row_number().over(window_desc)
        )

        first_val = (
            relevant_rn.filter(F.col("_row_asc") == 1)
            .select(group_col, F.col("_seq_rank").alias("_first_rank"),
                    F.col(value_col).alias("_first_val"),
                    F.col(sort_col).alias("_first_sort"))
        )
        last_val = (
            relevant_rn.filter(F.col("_row_desc") == 1)
            .select(group_col, F.col("_seq_rank").alias("_last_rank"),
                    F.col(value_col).alias("_last_val"),
                    F.col(sort_col).alias("_last_sort"))
        )

        violations_info = (
            violation_groups.select(group_col)
            .join(first_val, on=group_col, how="inner")
            .join(last_val,  on=group_col, how="inner")
        )
        seq_str = " \u2192 ".join(seq_values)
        if allow_loops:
            flexible_note = " (allow_loops: true)"
        elif flexible_set:
            flexible_note = f" (flexible: {', '.join(sorted(flexible_set))})"
        else:
            flexible_note = ""

        violations_out = violations_info.select(
            F.col(group_col).cast("string").alias("primary_key_value"),
            F.lit(value_col).alias("violated_column"),
            F.concat(
                F.lit("first="), F.col("_first_val").cast("string"),
                F.lit(", last="), F.col("_last_val").cast("string"),
            ).alias("actual_value"),
            F.lit(f"Values must appear in sequence: {seq_str}{flexible_note}").alias("expected_condition"),
            F.concat(
                F.lit(f"For {group_col}='"),
                F.col(group_col).cast("string"),
                F.lit(f"', {value_col} sequence order violated"),
                F.lit(f" (first seen: '"),
                F.col("_first_val").cast("string"),
                F.lit(f"' ({sort_col}="),
                F.col("_first_sort").cast("string"),
                F.lit(f", last seen: '"),
                F.col("_last_val").cast("string"),
                F.lit("')"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": total - failed,
            "failed_rows": failed,
            "success_pct": _safe_pct(total - failed, total),
            "status":      "FAILED",
            "details": (
                f"{failed} group(s) in {group_col} have out-of-order "
                f"{value_col} values (expected: {seq_str}{flexible_note})."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# validate_paired_presence
# For each required pair, both values must exist within the same group.
# Replaces the former table-specific expect_milestone_pairs_complete.
# -----------------------------------------------------------------------------
class ValidatePairedPresenceExpectation:
    """
    Validates that each required pair of values both exist within the same group.

    YAML parameters (canonical names):
      event_column       - column holding the values to check (was: value_column)
      group_column       - column that identifies the group
      required_pairs     - list of pairs; each pair is [start_marker, stop_marker] or
                           [start_marker, [stop_marker1, stop_marker2, ...]].
                           In the multi-stop form the pair is satisfied when at least
                           one of the stop values exists in the group alongside the
                           start value.
      completion_gate    - (optional) only evaluate groups that are "done":
          event_column   - column holding the completion marker value
          value         - the value that signals the group is closed
          order_column   - (optional) column that must be non-null on the marker row
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        value_col      = params.get("event_column")
        group_col      = params.get("group_column")
        required_pairs = params.get("required_pairs", [])
        gate           = params.get("completion_gate", {})

        df = _resolve_gate_groups(df, gate, group_col)

        total = df.count()
        if total == 0 or not required_pairs:
            return _passed_result(total), _empty_violations(spark)

        # Validate pair structure and normalise stop-slots to lists.
        normalised = []
        for pi, pair in enumerate(required_pairs):
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                return (
                    _error_result(
                        f"Parameter 'required_pairs[{pi}]' must be a 2-element list "
                        f"[start_value, stop_value] or [start_value, [stop1, stop2, ...]]."
                    ),
                    _empty_violations(spark),
                )
            stops = _normalise_stops(pair[1])
            if not stops:
                return (
                    _error_result(
                        f"Parameter 'required_pairs[{pi}]': stop slot must not be empty."
                    ),
                    _empty_violations(spark),
                )
            normalised.append((pair[0], stops))

        # Collect every distinct value referenced across all pairs for the pivot.
        all_types = list({
            t
            for start, stops in normalised
            for t in ([start] + stops)
        })

        pivot_df = (
            df.filter(F.col(group_col).isNotNull())
            .filter(F.col(value_col).isin(all_types))
            .groupBy(group_col)
            .agg(*[
                F.max(
                    F.when(F.col(value_col) == t, F.lit(True)).otherwise(F.lit(False))
                ).alias(f"_has_{i}")
                for i, t in enumerate(all_types)
            ])
        )
        type_to_col = {t: f"_has_{i}" for i, t in enumerate(all_types)}

        # Build all pair violation DataFrames as lazy transformations first,
        # then union them in one go and count once — avoids per-pair actions.
        pair_violation_dfs = []

        for start_type, stop_types in normalised:
            has_start = type_to_col.get(start_type)
            if not has_start:
                continue

            # Build OR expression across all stop columns.
            has_any_stop_expr = F.col(type_to_col[stop_types[0]])
            for st in stop_types[1:]:
                has_any_stop_expr = has_any_stop_expr | F.col(type_to_col[st])

            pivot_with_stop = pivot_df.withColumn("_has_any_stop", has_any_stop_expr)

            stop_label = " or ".join(f"'{s}'" for s in stop_types)
            expected = (
                f"Both '{start_type}' and ({stop_label}) must exist "
                f"for the same {group_col}"
            )

            pair_viols = pivot_with_stop.filter(
                F.col(has_start) != F.col("_has_any_stop")
            ).select(
                F.col(group_col).cast("string").alias("primary_key_value"),
                F.lit(value_col).alias("violated_column"),
                F.when(F.col(has_start), F.lit(start_type))
                 .otherwise(F.lit(stop_label)).alias("actual_value"),
                F.lit(expected).alias("expected_condition"),
                F.when(
                    F.col(has_start) & ~F.col("_has_any_stop"),
                    F.concat(
                        F.lit(f"'{start_type}' present but none of ({stop_label}) found "
                              f"for {group_col}='"),
                        F.col(group_col).cast("string"), F.lit("'"),
                    ),
                ).otherwise(
                    F.concat(
                        F.lit(f"One of ({stop_label}) present but '{start_type}' missing "
                              f"for {group_col}='"),
                        F.col(group_col).cast("string"), F.lit("'"),
                    ),
                ).alias("violation_detail"),
            )
            pair_violation_dfs.append(pair_viols)

        if not pair_violation_dfs:
            return _passed_result(total), _empty_violations(spark)

        all_violations = reduce(lambda a, b: a.unionByName(b), pair_violation_dfs)
        total_failed = all_violations.count()

        if total_failed == 0:
            return _passed_result(total), _empty_violations(spark)

        result = {
            "total_rows":  total,
            "passed_rows": total - total_failed,
            "failed_rows": total_failed,
            "success_pct": _safe_pct(total - total_failed, total),
            "status":      "FAILED",
            "details": f"{total_failed} group(s) in {group_col} have incomplete pairs.",
        }
        return result, all_violations


# -----------------------------------------------------------------------------
# validate_gate
# Validates that every group contains at least one row where value_column
# equals value_to_check.  Groups that lack the required value are violations.
#
# This is the standalone companion to the completion_gate filter used inside
# validate_sequence_order and validate_paired_presence.  Use it to assert that
# every group has completed a gating step (e.g. received an approval).
# -----------------------------------------------------------------------------
class ValidateGateExpectation:
    """
    Validates that each group has at least one row where event_column equals
    value_to_check.

    YAML parameters (canonical names):
      event_column   - column holding the value to check (was: value_column)
      group_column   - column that identifies the group
      value_to_check - the required value that must be present in each group
      order_column   - (optional) when provided, only rows where order_column
                       IS NOT NULL are considered for the check (was: sort_column)
      trigger        - (optional) human-readable label for the gate; used in
                       violation details (default: "Approval completed")
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        value_col      = params.get("event_column")
        group_col      = params.get("group_column")
        value_to_check = params.get("value_to_check")
        sort_col       = params.get("order_column")
        trigger        = params.get("trigger", "Approval completed")

        if not value_col:
            return (
                _error_result(
                    "Parameter 'value_column' is required for validate_gate."
                ),
                _empty_violations(spark),
            )
        if not group_col:
            return (
                _error_result(
                    "Parameter 'group_column' is required for validate_gate."
                ),
                _empty_violations(spark),
            )
        if value_to_check is None:
            return (
                _error_result(
                    "Parameter 'value_to_check' is required for validate_gate."
                ),
                _empty_violations(spark),
            )

        total = df.count()
        if total == 0:
            return _passed_result(total), _empty_violations(spark)

        # Build the filter for rows that satisfy the gate condition.
        gate_filter = F.col(value_col) == value_to_check
        if sort_col:
            gate_filter = gate_filter & F.col(sort_col).isNotNull()

        # Groups that have at least one qualifying row are considered "passed".
        passed_groups = (
            df.filter(gate_filter)
            .select(group_col)
            .distinct()
        )

        # Violation: any group in the dataset that did NOT pass the gate.
        all_groups = df.select(group_col).distinct()
        failed_groups = all_groups.join(passed_groups, on=group_col, how="left_anti")
        failed = failed_groups.count()

        if failed == 0:
            return _passed_result(total), _empty_violations(spark)

        expected_cond = (
            f"Group must contain at least one row where "
            f"{value_col} = '{value_to_check}'"
            + (f" and {sort_col} IS NOT NULL" if sort_col else "")
        )

        violations_out = failed_groups.select(
            F.col(group_col).cast("string").alias("primary_key_value"),
            F.lit(value_col).alias("violated_column"),
            F.lit(None).cast("string").alias("actual_value"),
            F.lit(expected_cond).alias("expected_condition"),
            F.concat(
                F.lit(f"Gate '{trigger}': {group_col}='"),
                F.col(group_col).cast("string"),
                F.lit(
                    f"' does not have a row where {value_col} = '{value_to_check}'"
                ),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": total - failed,
            "failed_rows": failed,
            "success_pct": _safe_pct(total - failed, total),
            "status":      "FAILED",
            "details": (
                f"{failed} group(s) in {group_col} do not have "
                f"'{value_to_check}' in {value_col} (gate: '{trigger}')."
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# validate_no_orphan
# A stop-type value must not exist without the corresponding start-type value.
# Replaces the former table-specific expect_no_orphan_milestones.
# -----------------------------------------------------------------------------
class ValidateNoOrphanExpectation:
    """
    Detects groups where a stop-type value exists without its start-type pair.

    YAML parameters (canonical names):
      event_column   - column holding the type/name values (was: value_column)
      group_column   - column identifying the group
      pairs          - list of pairs; each pair is [start_marker, stop_marker] or
                       [start_marker, [stop_marker1, stop_marker2, ...]].
                       In the multi-stop form, the presence of ANY stop value
                       without the corresponding start value is a violation.
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        value_col = params.get("event_column") or params.get("milestone_column")
        group_col = params.get("group_column")
        pairs     = params.get("pairs", [])

        total = df.count()
        if total == 0 or not pairs:
            return _passed_result(total), _empty_violations(spark)

        # Normalise stop-slots to lists and collect all referenced values.
        normalised = [(pair[0], _normalise_stops(pair[1])) for pair in pairs]
        all_types  = list({
            t
            for start, stops in normalised
            for t in ([start] + stops)
        })

        pivot_df = (
            df.filter(F.col(group_col).isNotNull())
            .filter(F.col(value_col).isin(all_types))
            .groupBy(group_col)
            .agg(*[
                F.max(
                    F.when(F.col(value_col) == t, F.lit(True)).otherwise(F.lit(False))
                ).alias(f"_has_{i}")
                for i, t in enumerate(all_types)
            ])
        )
        type_to_col = {t: f"_has_{i}" for i, t in enumerate(all_types)}

        # Build all pair violation DataFrames as lazy transformations first,
        # then union them in one go and count once — avoids per-pair actions.
        pair_violation_dfs = []

        for start_type, stop_types in normalised:
            has_start = type_to_col.get(start_type)
            if not has_start:
                continue

            # Build OR expression: any stop column is True.
            has_any_stop_expr = F.col(type_to_col[stop_types[0]])
            for st in stop_types[1:]:
                has_any_stop_expr = has_any_stop_expr | F.col(type_to_col[st])

            stop_label = " or ".join(f"'{s}'" for s in stop_types)
            pivot_with_stop = pivot_df.withColumn("_has_any_stop", has_any_stop_expr)

            pair_viols = pivot_with_stop.filter(
                F.col("_has_any_stop") & ~F.col(has_start)
            ).select(
                F.col(group_col).cast("string").alias("primary_key_value"),
                F.lit(value_col).alias("violated_column"),
                F.lit(stop_label).alias("actual_value"),
                F.lit(
                    f"'{start_type}' must exist when any of ({stop_label}) is present"
                ).alias("expected_condition"),
                F.concat(
                    F.lit(f"One of ({stop_label}) exists but '{start_type}' is missing "
                          f"for {group_col}='"),
                    F.col(group_col).cast("string"), F.lit("'"),
                ).alias("violation_detail"),
            )
            pair_violation_dfs.append(pair_viols)

        if not pair_violation_dfs:
            return _passed_result(total), _empty_violations(spark)

        all_violations = reduce(lambda a, b: a.unionByName(b), pair_violation_dfs)
        total_failed = all_violations.count()

        if total_failed == 0:
            return _passed_result(total), _empty_violations(spark)

        result = {
            "total_rows":  total,
            "passed_rows": total - total_failed,
            "failed_rows": total_failed,
            "success_pct": _safe_pct(total - total_failed, total),
            "status":      "FAILED",
            "details": (
                f"{total_failed} group(s) in {group_col} have a stop-type "
                f"value without a corresponding start-type value."
            ),
        }
        return result, all_violations


# =============================================================================
# Generic conditional / consistency expectations
# (previously invoice-specific; now fully parameterised for any table)
# =============================================================================

# -----------------------------------------------------------------------------
# validate_conditional_column_value
# When trigger_column satisfies a condition, required_column must equal
# required_value.  Replaces the former table-specific expect_refund_validation.
# -----------------------------------------------------------------------------
class ValidateConditionalColumnValueExpectation:
    """
    Validates that required_column equals required_value on all rows where
    when_column satisfies the configured condition.

    YAML parameters (canonical names):
      when_column        - column checked for the trigger condition (was: trigger_column, condition_column)
      operator           - comparison operator: <, >, <=, >=, ==, !=
      value              - threshold value for the trigger condition
      required_column    - column that must equal required_value when triggered
      required_value     - the required value for required_column
      pk_column          - primary key column
    """

    _CONDITIONS = {
        "<":  lambda col, val: F.col(col) < val,
        ">":  lambda col, val: F.col(col) > val,
        "<=": lambda col, val: F.col(col) <= val,
        ">=": lambda col, val: F.col(col) >= val,
        "==": lambda col, val: F.col(col) == val,
        "!=": lambda col, val: F.col(col) != val,
    }

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params = rule.get("parameters", {})

        condition_col = params.get("when_column")
        condition_op  = params.get("operator", "<")
        condition_val = params.get("value", 0)
        required_col  = params.get("required_column")
        required_val  = params.get("required_value")
        pk_col        = params.get("pk_column", "id")

        if not condition_col or not required_col or required_val is None:
            return (
                _error_result(
                    "Parameters 'when_column', 'required_column', and "
                    "'required_value' are all required."
                ),
                _empty_violations(spark),
            )

        if condition_op not in self._CONDITIONS:
            return (
                _error_result(
                    f"Unsupported operator '{condition_op}'. "
                    f"Allowed: {sorted(self._CONDITIONS)}"
                ),
                _empty_violations(spark),
            )

        cond_filter = self._CONDITIONS[condition_op](condition_col, condition_val)
        evaluated   = df.filter(F.col(condition_col).isNotNull() & cond_filter)
        total       = evaluated.count()

        if total == 0:
            result = {
                "total_rows":  df.count(),
                "passed_rows": df.count(),
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details": (
                    f"No rows satisfy condition "
                    f"{condition_col} {condition_op} {condition_val} - skipped."
                ),
            }
            return result, _empty_violations(spark)

        violations_df = evaluated.filter(
            F.col(required_col).isNull()
            | (F.col(required_col).cast("string") != str(required_val))
        )
        failed = violations_df.count()
        passed = total - failed

        cond_label = f"{condition_col} {condition_op} {condition_val}"
        expected   = f"When {cond_label}, {required_col} must be '{required_val}'"

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(required_col).alias("violated_column"),
            F.coalesce(F.col(required_col).cast("string"), F.lit("NULL")).alias("actual_value"),
            F.lit(expected).alias("expected_condition"),
            F.concat(
                F.lit(f"Condition '{cond_label}' is met but {required_col} is '"),
                F.coalesce(F.col(required_col).cast("string"), F.lit("NULL")),
                F.lit(f"' instead of '{required_val}'"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) where {cond_label} do not have "
                f"{required_col} = '{required_val}'."
                if failed > 0
                else (
                    f"All {total} row(s) where {cond_label} correctly "
                    f"have {required_col} = '{required_val}'."
                )
            ),
        }
        return result, violations_out


# -----------------------------------------------------------------------------
# validate_group_aggregate_match
# The aggregate (e.g. SUM) of a column per group must match a reference column
# value within a given tolerance.
# Replaces the former table-specific expect_invoice_total_consistency.
# -----------------------------------------------------------------------------
class ValidateGroupAggregateMatchExpectation:
    """
    Validates that the aggregate of aggregate_column per group equals the value
    in reference_column (within tolerance).

    YAML parameters:
      group_column     - column identifying each group
      aggregate_column - numeric column to aggregate within each group
      reference_column - column holding the expected group total
      aggregate        - aggregation function: sum, count, avg, min, max (default: sum)
      tolerance        - maximum allowed absolute difference (default: 0.01)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params = rule.get("parameters", {})

        group_col = params.get("group_column")
        agg_col   = params.get("aggregate_column")
        ref_col   = params.get("reference_column")
        aggregate = str(params.get("aggregate", "sum")).lower()
        tolerance = float(params.get("tolerance", 0.01))

        if not group_col or not agg_col or not ref_col:
            return (
                _error_result(
                    "Parameters 'group_column', 'aggregate_column', and "
                    "'reference_column' are all required."
                ),
                _empty_violations(spark),
            )

        if aggregate not in _AGGREGATE_FUNCTIONS:
            return (
                _error_result(
                    f"Unsupported aggregate '{aggregate}'. "
                    f"Allowed: {sorted(_AGGREGATE_FUNCTIONS)}"
                ),
                _empty_violations(spark),
            )

        if ref_col not in df.columns:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 100.0,
                "status":      "PASSED",
                "details": (
                    f"Column '{ref_col}' not found in dataframe - skipped. "
                    "Join the reference table to enable this rule."
                ),
            }
            return result, _empty_violations(spark)

        group_totals = df.filter(
            F.col(group_col).isNotNull()
            & F.col(agg_col).isNotNull()
            & F.col(ref_col).isNotNull()
        ).groupBy(group_col, ref_col).agg(
            _AGGREGATE_FUNCTIONS[aggregate](agg_col).alias("_agg_val")
        ).withColumn(
            "diff", F.abs(F.col("_agg_val") - F.col(ref_col))
        )

        total = group_totals.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        violations_df = group_totals.filter(F.col("diff") > tolerance)
        failed = violations_df.count()
        passed = total - failed

        expected_cond = (
            f"ABS({aggregate.upper()}({agg_col}) - {ref_col}) <= {tolerance}"
        )

        violations_out = violations_df.select(
            F.col(group_col).cast("string").alias("primary_key_value"),
            F.lit(agg_col).alias("violated_column"),
            F.col("_agg_val").cast("string").alias("actual_value"),
            F.lit(expected_cond).alias("expected_condition"),
            F.concat(
                F.lit(f"{group_col} '"), F.col(group_col).cast("string"),
                F.lit(f"': {aggregate.upper()}({agg_col}) = "),
                F.col("_agg_val").cast("string"),
                F.lit(f", {ref_col} = "),
                F.col(ref_col).cast("string"),
                F.lit(", diff = "), F.col("diff").cast("string"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} group(s) in {group_col} have a mismatch "
                f"exceeding tolerance {tolerance}."
                if failed > 0
                else (
                    f"All {total} group(s) in {group_col} have consistent "
                    f"{aggregate.upper()}({agg_col}) values (tolerance {tolerance})."
                )
            ),
        }
        return result, violations_out


# =============================================================================
# Negative / forbidden-state expectations
# =============================================================================

# -----------------------------------------------------------------------------
# validate_column_exclusions
# Enforces forbidden states by asserting that a given combination of column
# values must NEVER occur together.  Any row that satisfies the condition is
# treated as a violation.
#
# This is the negation counterpart to conditional validators: instead of
# "column X must be set when Y is true", this says
# "it must NEVER be the case that (condition)".
# -----------------------------------------------------------------------------
class ValidateColumnExclusionsExpectation:
    """
    Asserts that no row satisfies the given forbidden-state condition.
    A violation is recorded for every row where the condition holds true.

    YAML parameters:
      condition   - Spark SQL expression that identifies forbidden rows.
                    Any row matching this filter is a violation.
                    Example: "ColumnA IS NULL AND ColumnB IS NULL"
      pk_column   - primary key column used to identify violating rows
                    (default: "id")

    Example YAML rule:
      - rule: "Columns A and B cannot both be NULL"
        expectation: "validate_column_exclusions"
        parameters:
          condition: "ColumnA IS NULL AND ColumnB IS NULL"
          pk_column: "Saksnummer"
          severity:  "Critical"
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        condition = params.get("condition")
        pk_col    = params.get("pk_column", "id")

        if not condition:
            return (
                _error_result("Parameter 'condition' is required."),
                _empty_violations(spark),
            )

        total = df.count()
        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        try:
            violations_df = df.filter(condition)
        except Exception as exc:
            return (
                _error_result(f"Invalid condition expression: {exc}"),
                _empty_violations(spark),
            )

        failed = violations_df.count()
        passed = total - failed

        pk_in_df      = pk_col in df.columns
        pk_expr       = (
            F.col(pk_col).cast("string")
            if pk_in_df
            else F.lit(None).cast("string")
        )
        expected_cond = f"NOT ({condition})"
        detail_expr   = F.concat(
            F.lit("Forbidden condition satisfied: "),
            F.lit(condition),
        )

        violations_out = violations_df.select(
            pk_expr.alias("primary_key_value"),
            F.lit("forbidden_state").alias("violated_column"),
            F.lit(condition).alias("actual_value"),
            F.lit(expected_cond).alias("expected_condition"),
            detail_expr.alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) satisfy the forbidden condition: {condition}."
                if failed > 0
                else (
                    f"No rows satisfy the forbidden condition. "
                    f"All {total} rows are valid."
                )
            ),
        }
        return result, violations_out


# =============================================================================
# Active-reference validator
# =============================================================================

# -----------------------------------------------------------------------------
# validate_active_reference
# Validates that every non-null value in source_column exists in a reference
# table AND that the matching row in the reference table is "active" according
# to a configurable flag column and value.
# Generalised form of "handler must be an active employee".
# -----------------------------------------------------------------------------
class ValidateActiveReferenceExpectation:
    """
    Validates that all non-null values in 'source_column' exist in the
    reference table and match a row where active_column equals active_value.

    YAML parameters (canonical names):
      source_column            - column in the source table whose values are checked
      pk_column                - primary key column for violation reporting
      reference_table          - fully-qualified reference table name (was: reference.table)
      reference_column         - column in the reference table to join on (was: reference.column)
      reference_active_column  - column in the reference table holding the active flag (was: reference.active_column)
      reference_active_value   - value that means "active" (string or boolean) (was: reference.active_value)
    """

    def validate(self, df: DataFrame, rule: dict, spark, ref_cache: dict = None) -> tuple:
        params     = rule.get("parameters", {})
        src_col    = params.get("source_column")
        pk_col     = params.get("pk_column") or src_col
        ref_table  = params.get("reference_table")
        ref_col    = params.get("reference_column")
        active_col = params.get("reference_active_column")
        active_val = params.get("reference_active_value")

        if not src_col or not ref_table or not ref_col or not active_col or active_val is None:
            return (
                _error_result(
                    "Parameters 'source_column', 'reference_table', 'reference_column', "
                    "'reference_active_column', and 'reference_active_value' are all required."
                ),
                _empty_violations(spark),
            )

        if src_col not in df.columns:
            return (
                _error_result(f"Column '{src_col}' not found in source DataFrame."),
                _empty_violations(spark),
            )

        # Build active reference set, using cache when available.
        cache_key = (ref_table, ref_col, active_col, str(active_val))
        if ref_cache is not None and cache_key in ref_cache:
            active_ref_df = ref_cache[cache_key]
        else:
            try:
                raw_ref = spark.read.table(ref_table)
                missing_ref_cols = [c for c in (ref_col, active_col) if c is not None and c not in raw_ref.columns]
                if missing_ref_cols:
                    return (
                        _error_result(
                            f"Column(s) {missing_ref_cols} not found in reference table '{ref_table}'."
                        ),
                        _empty_violations(spark),
                    )
            except Exception as exc:
                return (
                    _error_result(f"Could not load reference table '{ref_table}': {exc}"),
                    _empty_violations(spark),
                )

            # Build the active-row filter: support boolean, numeric, and
            # case-insensitive string comparison.
            if isinstance(active_val, str):
                # Case-insensitive string comparison
                active_filter = (
                    F.lower(F.col(active_col).cast("string"))
                    == F.lower(F.lit(active_val))
                )
            else:
                # bool (must be checked before int since bool is a subclass of int),
                # int, float — all use direct equality
                active_filter = F.col(active_col) == active_val

            active_ref_df = (
                raw_ref.filter(active_filter)
                .select(F.col(ref_col).cast("string").alias("_active_ref_key"))
                .distinct()
            )
            if ref_cache is not None:
                ref_cache[cache_key] = active_ref_df

        evaluated = df.filter(F.col(src_col).isNotNull())
        total     = evaluated.count()

        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        violations_df = evaluated.alias("src").join(
            active_ref_df,
            F.col("src." + src_col).cast("string") == F.col("_active_ref_key"),
            how="left_anti",
        )
        failed = violations_df.count()
        passed = total - failed

        expected_cond = (
            f"{src_col} must reference an active row in "
            f"{ref_table}.{ref_col} where {active_col} = {active_val}"
        )

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(src_col).alias("violated_column"),
            F.col(src_col).cast("string").alias("actual_value"),
            F.lit(expected_cond).alias("expected_condition"),
            F.concat(
                F.lit("Value '"),
                F.col(src_col).cast("string"),
                F.lit(f"' not found or inactive in {ref_table}.{ref_col}"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} value(s) in '{src_col}' not found or inactive "
                f"in {ref_table}.{ref_col} (active filter: {active_col} = {active_val})."
                if failed > 0
                else (
                    f"All {total} non-null '{src_col}' values reference an active row "
                    f"in {ref_table}.{ref_col}."
                )
            ),
        }
        return result, violations_out


# =============================================================================
# Time-in-state validator
# =============================================================================

# -----------------------------------------------------------------------------
# validate_time_in_state
# Flags rows that have been in an "open" state for too long.
# "Open" is defined as open_when_column IS NULL (the common case) or equals
# a specific open_when_value.
# Generalised form of "open case must not exceed 30 days" and
# "unpaid invoice must not exceed 60 days".
# -----------------------------------------------------------------------------
class ValidateTimeInStateExpectation:
    """
    Flags rows that have been in an open state for more than max_days days.

    YAML parameters (canonical names):
      start_column         - date/timestamp column marking when the state began
      open_state_column    - column checked to decide if the row is still open (was: open_when_column)
      open_state_value     - value that means "open"; use "null" (or Python None)
                            to mean IS NULL (default behaviour) (was: open_when_value)
      pk_column            - primary key column for violation reporting
      max_days             - maximum number of days allowed in the open state
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        start_col      = params.get("start_column")
        open_when_col  = params.get("open_state_column")
        open_when_val  = params.get("open_state_value")
        pk_col         = params.get("pk_column")
        max_days       = params.get("max_days")

        if not start_col or not open_when_col or not pk_col or max_days is None:
            return (
                _error_result(
                    "Parameters 'start_column', 'open_state_column', 'pk_column', "
                    "and 'max_days' are all required."
                ),
                _empty_violations(spark),
            )

        if start_col not in df.columns:
            return (
                _error_result(f"Column '{start_col}' not found in source DataFrame."),
                _empty_violations(spark),
            )

        if open_when_col not in df.columns:
            return (
                _error_result(f"Column '{open_when_col}' not found in source DataFrame."),
                _empty_violations(spark),
            )

        try:
            max_days = int(max_days)
        except (TypeError, ValueError):
            return (
                _error_result(
                    f"Parameter 'max_days' must be a valid integer, got: {max_days!r}."
                ),
                _empty_violations(spark),
            )

        if max_days < 0:
            return (
                _error_result(
                    f"Parameter 'max_days' must be a non-negative integer, got: {max_days}."
                ),
                _empty_violations(spark),
            )

        # Build the "open" filter: IS NULL or matches specific value.
        is_open_when_null = open_when_val is None or (
            isinstance(open_when_val, str) and open_when_val.lower() == "null"
        )
        if is_open_when_null:
            open_filter = F.col(open_when_col).isNull()
        else:
            open_filter = F.col(open_when_col) == open_when_val

        open_rows = df.filter(open_filter & F.col(start_col).isNotNull())
        total     = open_rows.count()

        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        evaluated = open_rows.withColumn(
            "_days_open", F.datediff(F.current_date(), F.col(start_col))
        )

        violations_df = evaluated.filter(F.col("_days_open") > max_days)
        failed = violations_df.count()
        passed = total - failed

        if failed == 0:
            return _passed_result(total), _empty_violations(spark)

        expected_cond = f"Days in open state must not exceed {max_days}"

        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(start_col).alias("violated_column"),
            F.col("_days_open").cast("string").alias("actual_value"),
            F.lit(expected_cond).alias("expected_condition"),
            F.concat(
                F.lit("Open for "),
                F.col("_days_open").cast("string"),
                F.lit(" days (started "),
                F.col(start_col).cast("string"),
                F.lit(f"), exceeds limit of {max_days} days"),
            ).alias("violation_detail"),
        )

        result = {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) have been in the open state for more than "
                f"{max_days} day(s) (open filter: {open_when_col} "
                + ("IS NULL" if is_open_when_null else f"= {open_when_val}")
                + ")."
            ),
        }
        return result, violations_out


class ValueInListExpectation:
    """
    Validates that all non-null values in 'column' belong to 'allowed_values'.

    YAML parameters:
      column         - column to check
      allowed_values - list of permitted values (compared as strings)
      pk_column      - primary key column for violation reporting (default: column)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params        = rule.get("parameters", {})
        column        = params.get("column") or rule.get("column")
        pk_col        = params.get("pk_column") or column
        allowed_values = params.get("allowed_values", [])

        if not column:
            return _error_result("Parameter 'column' is required."), _empty_violations(spark)
        if not allowed_values or not isinstance(allowed_values, list):
            return (
                _error_result("Parameter 'allowed_values' must be a non-empty list."),
                _empty_violations(spark),
            )
        if column not in df.columns:
            return (
                _error_result(f"Column '{column}' not found in DataFrame."),
                _empty_violations(spark),
            )

        allowed_strs = [str(v) for v in allowed_values]
        evaluated    = df.filter(F.col(column).isNotNull())
        total        = evaluated.count()

        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        violations_df = evaluated.filter(~F.col(column).cast("string").isin(allowed_strs))
        failed        = violations_df.count()
        passed        = total - failed

        condition = f"{column} IN ({', '.join(allowed_strs)})"
        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(column).alias("violated_column"),
            F.col(column).cast("string").alias("actual_value"),
            F.lit(condition).alias("expected_condition"),
            F.concat(
                F.lit("Value '"),
                F.col(column).cast("string"),
                F.lit(f"' not in allowed list: {allowed_strs}"),
            ).alias("violation_detail"),
        )

        return {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) in '{column}' have values not in the allowed list."
                if failed > 0
                else f"All {total} evaluated rows in '{column}' have an allowed value."
            ),
        }, violations_out if failed > 0 else _empty_violations(spark)


class GreaterThanExpectation:
    """
    Validates that all non-null values in 'column' are strictly greater than 'threshold'.

    YAML parameters:
      column    - column to check (must be numeric)
      threshold - numeric lower bound (exclusive); value must be > threshold
      pk_column - primary key column for violation reporting (default: column)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        column    = params.get("column") or rule.get("column")
        pk_col    = params.get("pk_column") or column
        threshold = params.get("threshold")

        if not column:
            return _error_result("Parameter 'column' is required."), _empty_violations(spark)
        if threshold is None:
            return _error_result("Parameter 'threshold' is required."), _empty_violations(spark)
        if column not in df.columns:
            return (
                _error_result(f"Column '{column}' not found in DataFrame."),
                _empty_violations(spark),
            )

        try:
            threshold_val = float(threshold)
        except (TypeError, ValueError):
            return (
                _error_result(
                    f"Parameter 'threshold' must be numeric, got {threshold!r}."
                ),
                _empty_violations(spark),
            )

        evaluated = df.filter(F.col(column).isNotNull())
        total     = evaluated.count()

        if total == 0:
            return _passed_result(0), _empty_violations(spark)

        violations_df = evaluated.filter(
            ~(F.col(column).cast("double") > F.lit(threshold_val))
        )
        failed = violations_df.count()
        passed = total - failed

        condition = f"{column} > {threshold_val}"
        violations_out = violations_df.select(
            F.col(pk_col).cast("string").alias("primary_key_value"),
            F.lit(column).alias("violated_column"),
            F.col(column).cast("string").alias("actual_value"),
            F.lit(condition).alias("expected_condition"),
            F.concat(
                F.lit("Value '"),
                F.col(column).cast("string"),
                F.lit(f"' is not greater than {threshold_val}"),
            ).alias("violation_detail"),
        )

        return {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} row(s) in '{column}' are not greater than {threshold_val}."
                if failed > 0
                else f"All {total} evaluated rows in '{column}' are greater than {threshold_val}."
            ),
        }, violations_out if failed > 0 else _empty_violations(spark)


# =============================================================================
# Registry — maps YAML expectation names to validator classes.
#
# Only generic canonical names are registered here.  No table-specific
# aliases are retained; all YAML rule files must use the generic names.
# =============================================================================
CUSTOM_EXPECTATION_REGISTRY = {

    # -------------------------------------------------------------------------
    # Generic cross-table validators (work on any table)
    # Canonical names effective 2026-05-02 — old names not supported
    # -------------------------------------------------------------------------
    "comparison":                           ColumnComparisonExpectation,
    "not_null":                             ExpectColumnValuesToNotBeNullExpectation,
    "sql_violations":                       SqlValidationExpectation,
    "value_in_list":                        ValueInListExpectation,
    "greater_than":                         GreaterThanExpectation,

    # -------------------------------------------------------------------------
    # Aggregate validators
    # -------------------------------------------------------------------------
    "aggregate_threshold":                  ValidateAggregateRuleExpectation,
    "row_count_in_range":                   RowCountExpectation,
    "combination_unique":                   UniqueColumnCombinationExpectation,

    # -------------------------------------------------------------------------
    # Referential integrity
    # -------------------------------------------------------------------------
    "reference_exists":                     ForeignKeyExpectation,

    # -------------------------------------------------------------------------
    # Conditional / dependency validators
    # -------------------------------------------------------------------------
    "not_null_when":                        ValidateNotNullWhenExpectation,

    # -------------------------------------------------------------------------
    # Negative / forbidden-state validators
    # -------------------------------------------------------------------------
    "columns_excluded":                     ValidateColumnExclusionsExpectation,

    # -------------------------------------------------------------------------
    # Sequence / ordering validators
    # -------------------------------------------------------------------------
    "sequence_ordered":                     ValidateSequenceOrderExpectation,

    # -------------------------------------------------------------------------
    # Gate / completion validators
    # -------------------------------------------------------------------------
    "gate_complete":                        ValidateGateExpectation,

    # -------------------------------------------------------------------------
    # Paired-presence validators
    # -------------------------------------------------------------------------
    "pairs_present":                        ValidatePairedPresenceExpectation,

    # -------------------------------------------------------------------------
    # Orphan / stop-without-start validators
    # -------------------------------------------------------------------------
    "stops_paired_with_starts":             ValidateNoOrphanExpectation,

    # -------------------------------------------------------------------------
    # Conditional column-value validators
    # -------------------------------------------------------------------------
    "value_when":                           ValidateConditionalColumnValueExpectation,

    # -------------------------------------------------------------------------
    # Group aggregate match validators
    # -------------------------------------------------------------------------
    "group_aggregate_matches":              ValidateGroupAggregateMatchExpectation,

    # -------------------------------------------------------------------------
    # Active reference validators
    # -------------------------------------------------------------------------
    "reference_active":                     ValidateActiveReferenceExpectation,

    # -------------------------------------------------------------------------
    # Time-in-state validators
    # -------------------------------------------------------------------------
    "state_duration_within_limit":          ValidateTimeInStateExpectation,
}
