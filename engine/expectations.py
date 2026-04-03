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
#   result_dict keys (GX-compatible):
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
# CUSTOM_EXPECTATION_REGISTRY maps YAML expectation names to validator classes.
# Add new entries when creating new custom validators - no other file needs
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


def _error_result(message: str) -> dict:
    return {
        "total_rows":  0,
        "passed_rows": 0,
        "failed_rows": 0,
        "success_pct": 0.0,
        "status":      "ERROR",
        "details":     message,
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
      column_A  - name of the left-hand column
      column_B  - name of the right-hand column
      operator  - comparison operator: >, <, >=, <=, ==, !=
      pk_column - primary key column used to identify violating rows
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
# expect_column_sum_to_equal
# Validates that SUM(column) == expected_value within an optional tolerance.
# Kept for backward compatibility; prefer validate_aggregate_rule for new rules.
# -----------------------------------------------------------------------------
class ColumnSumExpectation:
    """
    Validates that SUM(column) equals expected_value (within tolerance).

    YAML parameters:
      column         - numeric column to sum
      expected_value - the value the sum must equal
      tolerance      - allowed absolute deviation (default: 0.01)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        column         = params.get("column")
        expected_value = params.get("expected_value")
        tolerance      = float(params.get("tolerance", 0.01))

        if not column or expected_value is None:
            return (
                _error_result(
                    "Parameters 'column' and 'expected_value' are required."
                ),
                _empty_violations(spark),
            )

        if column not in df.columns:
            return (
                _error_result(f"Column '{column}' not found in DataFrame."),
                _empty_violations(spark),
            )

        total     = df.count()
        actual    = df.agg(F.sum(F.col(column).cast("double"))).collect()[0][0]
        actual    = float(actual) if actual is not None else 0.0
        expected  = float(expected_value)
        deviation = abs(actual - expected)
        passed    = deviation <= tolerance

        condition = f"SUM({column}) == {expected} (\u00b1{tolerance})"

        result = {
            "total_rows":  total,
            "passed_rows": total if passed else 0,
            "failed_rows": 0 if passed else total,
            "success_pct": 100.0 if passed else 0.0,
            "status":      "PASSED" if passed else "FAILED",
            "details": (
                f"SUM({column}) = {actual:.4f}, expected {expected} \u00b1{tolerance}."
                if not passed
                else f"SUM({column}) = {actual:.4f} satisfies {condition}."
            ),
        }
        return result, _empty_violations(spark)


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
      column           - column in the source table to check
      pk_column        - primary key column (default: same as 'column')
      reference:
        table          - fully-qualified reference table
        column         - column in the reference table holding valid values
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        column    = params.get("column")
        pk_col    = params.get("pk_column") or column
        ref_block = params.get("reference", {})
        ref_table = ref_block.get("table")
        ref_col   = ref_block.get("column")

        if not column or not ref_table or not ref_col:
            return (
                _error_result(
                    "Parameters 'column', 'reference.table', and "
                    "'reference.column' are all required."
                ),
                _empty_violations(spark),
            )

        if column not in df.columns:
            return (
                _error_result(f"Column '{column}' not found in source DataFrame."),
                _empty_violations(spark),
            )

        try:
            ref_df = spark.read.table(ref_table).select(
                F.col(ref_col).cast("string").alias("_ref_key")
            ).distinct()
        except Exception as exc:
            return (
                _error_result(f"Could not load reference table '{ref_table}': {exc}"),
                _empty_violations(spark),
            )

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
            "success_pct": round(passed / total * 100, 2),
            "status":      "PASSED" if failed == 0 else "FAILED",
            "details": (
                f"{failed} value(s) in '{column}' not found in {ref_table}.{ref_col}."
                if failed > 0
                else f"All {total} non-null '{column}' values exist in {ref_table}.{ref_col}."
            ),
        }
        return result, violations_out


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

    _AGGREGATES = {
        "sum":   F.sum,
        "count": F.count,
        "avg":   F.avg,
        "min":   F.min,
        "max":   F.max,
    }

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

        if aggregate not in self._AGGREGATES:
            return (
                _error_result(
                    f"Unsupported aggregate '{aggregate}'. "
                    f"Allowed: {sorted(self._AGGREGATES)}"
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

        total = df.count()

        if aggregate == "count":
            actual = float(total)
        else:
            if not column or column not in df.columns:
                return (
                    _error_result(
                        f"Column '{column}' not found in DataFrame."
                        if column
                        else "Parameter 'column' is required when aggregate != 'count'."
                    ),
                    _empty_violations(spark),
                )
            agg_val = df.agg(
                self._AGGREGATES[aggregate](F.col(column).cast("double"))
            ).collect()[0][0]
            actual = float(agg_val) if agg_val is not None else 0.0

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
# Validates that check_columns are NOT NULL whenever condition_column satisfies
# the configured condition.
#
# This single generic expectation replaces the former table-specific:
#   expect_milestone_pairs        (closed case => both dates required)
#   expect_no_open_milestone_pairs (stop set => start must also be set)
# -----------------------------------------------------------------------------
class ValidateNotNullWhenExpectation:
    """
    Validates that all check_columns are not null when condition_column
    satisfies the trigger condition.

    YAML parameters:
      condition_column   - column that triggers the check
      condition_operator - "==" (equality) or "IS NOT NULL"
      condition_value    - required when operator is "=="
      check_columns      - list of columns that must be NOT NULL when triggered
      pk_column          - primary key column
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params        = rule.get("parameters", {})
        condition_col = params.get("condition_column")
        condition_op  = params.get("condition_operator", "==")
        condition_val = params.get("condition_value")
        check_cols    = params.get("check_columns", [])
        pk_col        = params.get("pk_column", "id")

        if not condition_col or not check_cols:
            return (
                _error_result(
                    "Parameters 'condition_column' and 'check_columns' "
                    "(non-empty list) are required."
                ),
                _empty_violations(spark),
            )

        if condition_op not in ("==", "IS NOT NULL"):
            return (
                _error_result(
                    f"Unsupported condition_operator '{condition_op}'. "
                    "Allowed: '==', 'IS NOT NULL'"
                ),
                _empty_violations(spark),
            )

        if condition_op == "IS NOT NULL":
            conditional_rows = df.filter(F.col(condition_col).isNotNull())
            cond_label = f"{condition_col} IS NOT NULL"
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
            "success_pct": round(passed / total * 100, 2),
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
    Checks that values in value_column appear in the specified expected_sequence
    order for each group identified by group_column.

    YAML parameters:
      value_column      - column holding the sequence value names
      group_column      - column that identifies the group
      date_column       - date/timestamp column used to determine order
      expected_sequence - ordered list of value names
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params            = rule.get("parameters", {})
        value_col         = params.get("value_column") or params.get("milestone_column")
        group_col         = params.get("group_column")
        date_col          = params.get("date_column")
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
                    f"Date column '{date_col}' not found in dataframe - "
                    f"sequence check skipped."
                ),
            }
            return result, _empty_violations(spark)

        seq_map   = {m: i for i, m in enumerate(expected_sequence)}
        seq_items = list(seq_map.items())

        rank_expr = F.when(F.col(value_col) == seq_items[0][0], F.lit(seq_items[0][1]))
        for val, rank in seq_items[1:]:
            rank_expr = rank_expr.when(F.col(value_col) == val, F.lit(rank))
        rank_expr = rank_expr.otherwise(F.lit(None).cast("int"))

        relevant = df.filter(
            F.col(value_col).isin(list(seq_map.keys()))
            & F.col(date_col).isNotNull()
        ).withColumn("_seq_rank", rank_expr)

        window_asc  = Window.partitionBy(group_col).orderBy(F.col(date_col).asc())
        window_desc = Window.partitionBy(group_col).orderBy(F.col(date_col).desc())

        ranked = relevant.withColumn(
            "_row_asc",  F.row_number().over(window_asc)
        ).withColumn(
            "_row_desc", F.row_number().over(window_desc)
        )

        first_val = (
            ranked.filter(F.col("_row_asc") == 1)
            .select(group_col, F.col("_seq_rank").alias("_first_rank"),
                    F.col(value_col).alias("_first_val"),
                    F.col(date_col).alias("_first_date"))
        )
        last_val = (
            ranked.filter(F.col("_row_desc") == 1)
            .select(group_col, F.col("_seq_rank").alias("_last_rank"),
                    F.col(value_col).alias("_last_val"),
                    F.col(date_col).alias("_last_date"))
        )

        group_counts = relevant.groupBy(group_col).agg(
            F.count("*").alias("_relevant_cnt")
        ).filter(F.col("_relevant_cnt") >= 2)

        evaluated = (
            group_counts
            .join(first_val, on=group_col, how="inner")
            .join(last_val,  on=group_col, how="inner")
        )

        evaluated_total = evaluated.count()
        if evaluated_total == 0:
            return _passed_result(total), _empty_violations(spark)

        violations_df = evaluated.filter(F.col("_first_rank") > F.col("_last_rank"))
        failed = violations_df.count()

        if failed == 0:
            return _passed_result(total), _empty_violations(spark)

        seq_str = " \u2192 ".join(expected_sequence)
        violations_out = violations_df.select(
            F.col(group_col).cast("string").alias("primary_key_value"),
            F.lit(value_col).alias("violated_column"),
            F.concat(
                F.lit("first="), F.col("_first_val").cast("string"),
                F.lit(", last="), F.col("_last_val").cast("string"),
            ).alias("actual_value"),
            F.lit(f"Values must appear in sequence: {seq_str}").alias("expected_condition"),
            F.concat(
                F.lit(f"For {group_col}='"),
                F.col(group_col).cast("string"),
                F.lit(f"', earliest {value_col} is '"),
                F.col("_first_val").cast("string"),
                F.lit(f"' ({date_col}="),
                F.col("_first_date").cast("string"),
                F.lit(") which in the expected sequence comes after '"),
                F.col("_last_val").cast("string"),
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
                f"{failed} group(s) in {group_col} have out-of-order "
                f"{value_col} values (expected: {seq_str})."
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

    YAML parameters:
      value_column   - column holding the values to check
      group_column   - column that identifies the group
      required_pairs - list of [start_value, stop_value] pairs
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params         = rule.get("parameters", {})
        value_col      = params.get("value_column") or params.get("milestone_column")
        group_col      = params.get("group_column")
        required_pairs = params.get("required_pairs", [])

        total = df.count()
        if total == 0 or not required_pairs:
            return _passed_result(total), _empty_violations(spark)

        all_types = list({t for pair in required_pairs for t in pair})

        pivot_df = (
            df.filter(F.col(group_col).isNotNull())
            .withColumn("_in_pair", F.col(value_col).isin(all_types))
            .filter(F.col("_in_pair"))
            .groupBy(group_col)
            .agg(*[
                F.max(
                    F.when(F.col(value_col) == t, F.lit(True)).otherwise(F.lit(False))
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
                F.lit(value_col).alias("violated_column"),
                F.when(F.col(has_start), F.lit(start_type))
                 .otherwise(F.lit(stop_type)).alias("actual_value"),
                F.lit(
                    f"Both '{start_type}' and '{stop_type}' must exist "
                    f"for the same {group_col}"
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
            "details": f"{total_failed} group(s) in {group_col} have incomplete pairs.",
        }
        return result, all_violations


# -----------------------------------------------------------------------------
# validate_no_orphan
# A stop-type value must not exist without the corresponding start-type value.
# Replaces the former table-specific expect_no_orphan_milestones.
# -----------------------------------------------------------------------------
class ValidateNoOrphanExpectation:
    """
    Detects groups where a stop-type value exists without its start-type pair.

    YAML parameters:
      value_column - column holding the type/name values
      group_column - column identifying the group
      pairs        - list of [start_type, stop_type] pairs to check
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params    = rule.get("parameters", {})
        value_col = params.get("value_column") or params.get("milestone_column")
        group_col = params.get("group_column")
        pairs     = params.get("pairs", [])

        total = df.count()
        if total == 0 or not pairs:
            return _passed_result(total), _empty_violations(spark)

        all_types = list({t for pair in pairs for t in pair})

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
                F.lit(value_col).alias("violated_column"),
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
# When condition_column satisfies a condition, required_column must equal
# required_value.  Replaces the former table-specific expect_refund_validation.
# -----------------------------------------------------------------------------
class ValidateConditionalColumnValueExpectation:
    """
    Validates that required_column equals required_value on all rows where
    condition_column satisfies the configured condition.

    YAML parameters (generic form):
      condition_column   - column checked for the trigger condition
      condition_operator - comparison operator: <, >, <=, >=, ==, !=
      condition_value    - threshold value for the trigger condition
      required_column    - column that must equal required_value when triggered
      required_value     - the required value for required_column
      pk_column          - primary key column

    Backward-compatible aliases (old expect_refund_validation form):
      amount_column -> condition_column  (condition_operator defaults to "<")
      type_column   -> required_column
      credit_type   -> required_value
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

        condition_col = params.get("condition_column") or params.get("amount_column")
        condition_op  = params.get("condition_operator", "<")
        condition_val = params.get("condition_value", 0)
        required_col  = params.get("required_column") or params.get("type_column")
        required_val  = params.get("required_value") or params.get("credit_type")
        pk_col        = params.get("pk_column", "id")

        if not condition_col or not required_col or required_val is None:
            return (
                _error_result(
                    "Parameters 'condition_column', 'required_column', and "
                    "'required_value' are all required."
                ),
                _empty_violations(spark),
            )

        if condition_op not in self._CONDITIONS:
            return (
                _error_result(
                    f"Unsupported condition_operator '{condition_op}'. "
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
            F.col(required_col).isNull() | (F.col(required_col) != str(required_val))
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
            "success_pct": round(passed / total * 100, 2),
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

    YAML parameters (generic form):
      group_column     - column identifying each group
      aggregate_column - numeric column to aggregate within each group
      reference_column - column holding the expected group total
      aggregate        - aggregation function: sum, count, avg, min, max (default: sum)
      tolerance        - maximum allowed absolute difference (default: 0.01)

    Backward-compatible aliases (old expect_invoice_total_consistency form):
      invoice_id_column    -> group_column
      line_amount_column   -> aggregate_column
      header_amount_column -> reference_column
    """

    _AGGREGATES = {
        "sum":   F.sum,
        "count": F.count,
        "avg":   F.avg,
        "min":   F.min,
        "max":   F.max,
    }

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params = rule.get("parameters", {})

        group_col = params.get("group_column") or params.get("invoice_id_column")
        agg_col   = params.get("aggregate_column") or params.get("line_amount_column")
        ref_col   = params.get("reference_column") or params.get("header_amount_column")
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

        if aggregate not in self._AGGREGATES:
            return (
                _error_result(
                    f"Unsupported aggregate '{aggregate}'. "
                    f"Allowed: {sorted(self._AGGREGATES)}"
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
            self._AGGREGATES[aggregate](agg_col).alias("_agg_val")
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
            "success_pct": round(passed / total * 100, 2),
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
# Registry — maps YAML expectation names to validator classes.
#
# Generic names are the canonical forms.  Old table-specific names are kept
# as backward-compatible aliases so existing YAML files continue to work
# without modification.
# =============================================================================
CUSTOM_EXPECTATION_REGISTRY = {

    # -------------------------------------------------------------------------
    # Generic cross-table validators (work on any table)
    # -------------------------------------------------------------------------
    "validate_column_comparison":           ColumnComparisonExpectation,
    "sql_validation":                       SqlValidationExpectation,
    "sql":                                  SqlValidationExpectation,    # shorthand

    # -------------------------------------------------------------------------
    # Aggregate validators
    # validate_aggregate_rule is the new generic form;
    # expect_column_sum_to_equal is kept for backward compatibility.
    # -------------------------------------------------------------------------
    "validate_aggregate_rule":              ValidateAggregateRuleExpectation,
    "expect_column_sum_to_equal":           ColumnSumExpectation,
    "expect_row_count_to_be_between":       RowCountExpectation,
    "expect_unique_combination_of_columns": UniqueColumnCombinationExpectation,

    # -------------------------------------------------------------------------
    # Referential integrity
    # -------------------------------------------------------------------------
    "validate_foreign_key":                 ForeignKeyExpectation,

    # -------------------------------------------------------------------------
    # Conditional / dependency validators
    # -------------------------------------------------------------------------
    "validate_not_null_when":               ValidateNotNullWhenExpectation,

    # -------------------------------------------------------------------------
    # Sequence / ordering validators
    # validate_sequence_order is the generic name;
    # expect_milestone_sequence is kept as a backward-compatible alias.
    # -------------------------------------------------------------------------
    "validate_sequence_order":              ValidateSequenceOrderExpectation,
    "expect_milestone_sequence":            ValidateSequenceOrderExpectation,

    # -------------------------------------------------------------------------
    # Paired-presence validators
    # validate_paired_presence is the generic name;
    # expect_milestone_pairs_complete is kept as a backward-compatible alias.
    # -------------------------------------------------------------------------
    "validate_paired_presence":             ValidatePairedPresenceExpectation,
    "expect_milestone_pairs_complete":      ValidatePairedPresenceExpectation,

    # -------------------------------------------------------------------------
    # Orphan / stop-without-start validators
    # validate_no_orphan is the generic name;
    # expect_no_orphan_milestones is kept as a backward-compatible alias.
    # -------------------------------------------------------------------------
    "validate_no_orphan":                   ValidateNoOrphanExpectation,
    "expect_no_orphan_milestones":          ValidateNoOrphanExpectation,

    # -------------------------------------------------------------------------
    # Conditional column-value validators
    # validate_conditional_column_value is the generic name;
    # expect_refund_validation is kept as a backward-compatible alias
    # (also supports old parameter names: amount_column, type_column, credit_type).
    # -------------------------------------------------------------------------
    "validate_conditional_column_value":    ValidateConditionalColumnValueExpectation,
    "expect_refund_validation":             ValidateConditionalColumnValueExpectation,

    # -------------------------------------------------------------------------
    # Group aggregate match validators
    # validate_group_aggregate_match is the generic name;
    # expect_invoice_total_consistency is kept as a backward-compatible alias
    # (also supports old parameter names: invoice_id_column, line_amount_column,
    # header_amount_column).
    # -------------------------------------------------------------------------
    "validate_group_aggregate_match":       ValidateGroupAggregateMatchExpectation,
    "expect_invoice_total_consistency":     ValidateGroupAggregateMatchExpectation,
}
