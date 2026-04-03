# =============================================================================
# dq_expectations/invoice_expectations.py
#
# Custom Great Expectations–style validators for Invoice data (Fakturalinjer).
#
# Each class implements:
#   validate(df, rule, spark) → (result_dict, violations_df | None)
#
# Same result_dict / violations_df contract as case_expectations.py.
# See that module's docstring for the full schema description.
# =============================================================================

from pyspark.sql import functions as F
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
# INV-004
# expect_refund_validation
# Negative line amounts are only valid when the invoice type is a credit note.
# -----------------------------------------------------------------------------
class InvoiceRefundValidationExpectation:
    """
    Checks that negative linje_belop values are only present on rows where
    Faktura_type equals the configured credit_type (e.g. 'Kreditnota').

    YAML parameters:
      amount_column – column containing the line amount (e.g. linje_belop)
      type_column   – column containing the invoice type (e.g. Faktura_type)
      credit_type   – the type value that permits negative amounts
                      (e.g. 'Kreditnota')
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params      = rule.get("parameters", {})
        amount_col  = params["amount_column"]
        type_col    = params["type_column"]
        credit_type = params["credit_type"]
        pk_col      = "Fakturanr"

        # Only evaluate rows where the amount column is present and negative
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

        # Violation: negative amount but NOT a credit note
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
# INV-005
# expect_invoice_total_consistency
# The sum of line amounts per invoice must match the header total amount
# (within a configurable tolerance).
# -----------------------------------------------------------------------------
class InvoiceTotalConsistencyExpectation:
    """
    Validates that the sum of all linje_belop values for a given invoice
    matches the Faktura_belop recorded in the header, within a given tolerance.

    YAML parameters:
      invoice_id_column    – column that groups lines into one invoice
                             (e.g. Fakturanr)
      line_amount_column   – the per-line amount column (e.g. linje_belop)
      header_amount_column – column holding the expected invoice total
                             (e.g. Faktura_belop)
      tolerance            – maximum allowed absolute difference (default 0.01)
    """

    def validate(self, df: DataFrame, rule: dict, spark) -> tuple:
        params             = rule.get("parameters", {})
        invoice_id_col     = params["invoice_id_column"]
        line_amount_col    = params["line_amount_column"]
        header_amount_col  = params["header_amount_column"]
        tolerance          = float(params.get("tolerance", 0.01))

        # Check that header_amount_column actually exists in the dataframe.
        # In some schemas it lives only at header level; if absent, skip rule.
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

        # Aggregate line amounts per invoice and compare to header total
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
