# =============================================================================
# dq_expectations/common_expectations.py
#
# Generic / cross-table custom expectations that are not specific to a single
# domain (Process, Milestone, Invoice).
#
# Currently provides:
#   ColumnComparisonExpectation  – validates column_A <op> column_B per row
#   SqlValidationExpectation     – executes a SQL query; any returned row is
#                                  treated as a violation
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
        sql_query = params.get("sql", "").strip()
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
