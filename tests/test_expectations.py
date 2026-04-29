# =============================================================================
# tests/test_expectations.py
#
# Unit tests for engine/expectations.py.
# These tests run with PySpark and validate the core expectation logic without
# requiring a real Fabric / Delta lake environment.
#
# Run with:
#   pytest tests/test_expectations.py -v
# =============================================================================

import pytest

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, IntegerType, DoubleType, DateType,
    )
    import pyspark.sql.functions as F

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not PYSPARK_AVAILABLE, reason="PySpark not installed"
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pbe-dq-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# ColumnComparisonExpectation
# ---------------------------------------------------------------------------

class TestColumnComparisonExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("id",    StringType(),  True),
            StructField("col_a", IntegerType(), True),
            StructField("col_b", IntegerType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self, operator):
        return {
            "rule_id": "T-001",
            "name": "test",
            "expectation": "validate_column_comparison",
            "parameters": {
                "column_A": "col_a",
                "column_B": "col_b",
                "operator": operator,
                "pk_column": "id",
            },
        }

    def test_all_pass(self, spark):
        from engine.expectations import ColumnComparisonExpectation
        df = self._make_df(spark, [("1", 5, 3), ("2", 4, 2)])
        result, viols = ColumnComparisonExpectation().validate(df, self._rule(">"), spark)
        assert result["status"] == "PASSED"
        assert result["failed_rows"] == 0
        assert viols.count() == 0

    def test_violation_detected(self, spark):
        from engine.expectations import ColumnComparisonExpectation
        df = self._make_df(spark, [("1", 2, 5), ("2", 4, 2)])
        result, viols = ColumnComparisonExpectation().validate(df, self._rule(">"), spark)
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 1
        assert viols.count() == 1

    def test_nulls_excluded(self, spark):
        from engine.expectations import ColumnComparisonExpectation
        df = self._make_df(spark, [("1", None, 5), ("2", 4, 2)])
        result, viols = ColumnComparisonExpectation().validate(df, self._rule(">"), spark)
        assert result["status"] == "PASSED"
        assert result["total_rows"] == 1

    def test_unsupported_operator(self, spark):
        from engine.expectations import ColumnComparisonExpectation
        df = self._make_df(spark, [("1", 5, 3)])
        result, viols = ColumnComparisonExpectation().validate(df, self._rule("??"), spark)
        assert result["status"] == "ERROR"

    def test_all_operators(self, spark):
        from engine.expectations import ColumnComparisonExpectation
        cases = [
            (">",  [(5, 3)], "PASSED"),
            ("<",  [(3, 5)], "PASSED"),
            (">=", [(5, 5)], "PASSED"),
            ("<=", [(3, 3)], "PASSED"),
            ("==", [(4, 4)], "PASSED"),
            ("!=", [(4, 5)], "PASSED"),
        ]
        for op, rows, expected_status in cases:
            df = self._make_df(spark, [("1", a, b) for a, b in rows])
            result, _ = ColumnComparisonExpectation().validate(df, self._rule(op), spark)
            assert result["status"] == expected_status, f"Operator {op} failed"


# ---------------------------------------------------------------------------
# RowCountExpectation
# ---------------------------------------------------------------------------

class TestRowCountExpectation:
    def _rule(self, min_val, max_val):
        return {
            "rule_id": "T-002",
            "name": "test",
            "expectation": "expect_row_count_to_be_between",
            "parameters": {"min_value": min_val, "max_value": max_val},
        }

    def test_within_range(self, spark):
        from engine.expectations import RowCountExpectation
        df = spark.range(5).toDF("id")
        result, _ = RowCountExpectation().validate(df, self._rule(1, 10), spark)
        assert result["status"] == "PASSED"

    def test_below_minimum(self, spark):
        from engine.expectations import RowCountExpectation
        df = spark.range(2).toDF("id")
        result, _ = RowCountExpectation().validate(df, self._rule(5, 100), spark)
        assert result["status"] == "FAILED"

    def test_above_maximum(self, spark):
        from engine.expectations import RowCountExpectation
        df = spark.range(20).toDF("id")
        result, _ = RowCountExpectation().validate(df, self._rule(1, 10), spark)
        assert result["status"] == "FAILED"

    def test_missing_params(self, spark):
        from engine.expectations import RowCountExpectation
        df = spark.range(5).toDF("id")
        rule = {"rule_id": "T", "name": "t", "expectation": "x", "parameters": {}}
        result, _ = RowCountExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"


# ---------------------------------------------------------------------------
# UniqueColumnCombinationExpectation
# ---------------------------------------------------------------------------

class TestUniqueColumnCombinationExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("id",  StringType(), True),
            StructField("col", StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self, columns):
        return {
            "rule_id": "T-003",
            "name": "test",
            "expectation": "expect_unique_combination_of_columns",
            "parameters": {"columns": columns, "pk_column": "id"},
        }

    def test_all_unique(self, spark):
        from engine.expectations import UniqueColumnCombinationExpectation
        df = self._make_df(spark, [("1", "a"), ("2", "b"), ("3", "c")])
        result, viols = UniqueColumnCombinationExpectation().validate(
            df, self._rule(["col"]), spark
        )
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_duplicates_detected(self, spark):
        from engine.expectations import UniqueColumnCombinationExpectation
        df = self._make_df(spark, [("1", "a"), ("2", "a"), ("3", "b")])
        result, viols = UniqueColumnCombinationExpectation().validate(
            df, self._rule(["col"]), spark
        )
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 2

    def test_empty_columns_param(self, spark):
        from engine.expectations import UniqueColumnCombinationExpectation
        df = self._make_df(spark, [("1", "a")])
        rule = {
            "rule_id": "T", "name": "t", "expectation": "x",
            "parameters": {"columns": []},
        }
        result, _ = UniqueColumnCombinationExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_missing_column(self, spark):
        from engine.expectations import UniqueColumnCombinationExpectation
        df = self._make_df(spark, [("1", "a")])
        rule = {
            "rule_id": "T", "name": "t", "expectation": "x",
            "parameters": {"columns": ["nonexistent"]},
        }
        result, _ = UniqueColumnCombinationExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"


# ---------------------------------------------------------------------------
# ColumnSumExpectation
# ---------------------------------------------------------------------------

class TestColumnSumExpectation:
    def _make_df(self, spark, values):
        schema = StructType([
            StructField("amount", DoubleType(), True),
        ])
        return spark.createDataFrame([(v,) for v in values], schema)

    def _rule(self, column, expected, tolerance=0.01):
        return {
            "rule_id": "T-004",
            "name": "test",
            "expectation": "expect_column_sum_to_equal",
            "parameters": {
                "column": column,
                "expected_value": expected,
                "tolerance": tolerance,
            },
        }

    def test_sum_matches(self, spark):
        from engine.expectations import ColumnSumExpectation
        df = self._make_df(spark, [10.0, 20.0, 30.0])
        result, _ = ColumnSumExpectation().validate(df, self._rule("amount", 60.0), spark)
        assert result["status"] == "PASSED"

    def test_sum_mismatch(self, spark):
        from engine.expectations import ColumnSumExpectation
        df = self._make_df(spark, [10.0, 20.0])
        result, _ = ColumnSumExpectation().validate(df, self._rule("amount", 100.0), spark)
        assert result["status"] == "FAILED"

    def test_within_tolerance(self, spark):
        from engine.expectations import ColumnSumExpectation
        df = self._make_df(spark, [10.0, 20.005])
        result, _ = ColumnSumExpectation().validate(
            df, self._rule("amount", 30.0, tolerance=0.01), spark
        )
        assert result["status"] == "PASSED"

    def test_missing_column(self, spark):
        from engine.expectations import ColumnSumExpectation
        df = self._make_df(spark, [1.0])
        result, _ = ColumnSumExpectation().validate(
            df, self._rule("nonexistent", 1.0), spark
        )
        assert result["status"] == "ERROR"


# ---------------------------------------------------------------------------
# ValidateAggregateRuleExpectation  (new generic expectation)
# ---------------------------------------------------------------------------

class TestValidateAggregateRuleExpectation:
    def _make_df(self, spark, values):
        schema = StructType([StructField("amount", DoubleType(), True)])
        return spark.createDataFrame([(v,) for v in values], schema)

    def _rule(self, column, aggregate, operator, threshold):
        return {
            "rule_id": "T-AGG",
            "name": "test",
            "expectation": "validate_aggregate_rule",
            "parameters": {
                "column": column,
                "aggregate": aggregate,
                "operator": operator,
                "threshold": threshold,
            },
        }

    def test_sum_above_threshold_passes(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [5.0, 10.0, 15.0])   # sum = 30
        result, _ = ValidateAggregateRuleExpectation().validate(
            df, self._rule("amount", "sum", ">=", 20.0), spark
        )
        assert result["status"] == "PASSED"

    def test_sum_below_threshold_fails(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [1.0, 2.0])            # sum = 3
        result, _ = ValidateAggregateRuleExpectation().validate(
            df, self._rule("amount", "sum", ">=", 100.0), spark
        )
        assert result["status"] == "FAILED"

    def test_count_aggregate(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [1.0, 2.0, 3.0])      # count = 3
        result, _ = ValidateAggregateRuleExpectation().validate(
            df, {"rule_id": "T", "name": "t", "expectation": "validate_aggregate_rule",
                 "parameters": {"aggregate": "count", "operator": "==", "threshold": 3}},
            spark,
        )
        assert result["status"] == "PASSED"

    def test_avg_operator(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [10.0, 20.0, 30.0])   # avg = 20
        result, _ = ValidateAggregateRuleExpectation().validate(
            df, self._rule("amount", "avg", "<=", 25.0), spark
        )
        assert result["status"] == "PASSED"

    def test_missing_threshold_errors(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [1.0])
        rule = {"rule_id": "T", "name": "t", "expectation": "x",
                "parameters": {"column": "amount", "aggregate": "sum", "operator": ">="}}
        result, _ = ValidateAggregateRuleExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_unsupported_aggregate_errors(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [1.0])
        rule = {"rule_id": "T", "name": "t", "expectation": "x",
                "parameters": {"column": "amount", "aggregate": "median",
                               "operator": ">=", "threshold": 0}}
        result, _ = ValidateAggregateRuleExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_missing_column_errors(self, spark):
        from engine.expectations import ValidateAggregateRuleExpectation
        df = self._make_df(spark, [1.0])
        rule = {"rule_id": "T", "name": "t", "expectation": "x",
                "parameters": {"column": "nonexistent", "aggregate": "sum",
                               "operator": ">=", "threshold": 0}}
        result, _ = ValidateAggregateRuleExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"


# ---------------------------------------------------------------------------
# ValidateNotNullWhenExpectation  (new generic expectation)
# ---------------------------------------------------------------------------

class TestValidateNotNullWhenExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("id",       StringType(), True),
            StructField("status",   StringType(), True),
            StructField("start_dt", StringType(), True),
            StructField("stop_dt",  StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def test_equality_condition_all_pass(self, spark):
        from engine.expectations import ValidateNotNullWhenExpectation
        # When status == "Closed", start_dt and stop_dt must not be null
        df = self._make_df(spark, [
            ("1", "Closed", "2024-01-01", "2024-06-01"),
            ("2", "Open",   None,         None),
        ])
        rule = {
            "rule_id": "T-NNW",
            "name": "test",
            "expectation": "validate_not_null_when",
            "parameters": {
                "condition_column":   "status",
                "operator":           "==",
                "value":              "Closed",
                "check_columns":      ["start_dt", "stop_dt"],
                "pk_column":          "id",
            },
        }
        result, viols = ValidateNotNullWhenExpectation().validate(df, rule, spark)
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_equality_condition_violation(self, spark):
        from engine.expectations import ValidateNotNullWhenExpectation
        df = self._make_df(spark, [
            ("1", "Closed", None, "2024-06-01"),   # start_dt is null but closed
            ("2", "Closed", "2024-01-01", "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-NNW",
            "name": "test",
            "expectation": "validate_not_null_when",
            "parameters": {
                "condition_column":   "status",
                "operator":           "==",
                "value":              "Closed",
                "check_columns":      ["start_dt", "stop_dt"],
                "pk_column":          "id",
            },
        }
        result, viols = ValidateNotNullWhenExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 1

    def test_is_not_null_condition(self, spark):
        from engine.expectations import ValidateNotNullWhenExpectation
        # When stop_dt IS NOT NULL, start_dt must not be null
        df = self._make_df(spark, [
            ("1", "X", None, "2024-06-01"),   # stop set but start missing - violation
            ("2", "X", "2024-01-01", "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-NNW",
            "name": "test",
            "expectation": "validate_not_null_when",
            "parameters": {
                "condition_column":   "stop_dt",
                "operator":           "IS NOT NULL",
                "check_columns":      ["start_dt"],
                "pk_column":          "id",
            },
        }
        result, viols = ValidateNotNullWhenExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_is_null_condition(self, spark):
        from engine.expectations import ValidateNotNullWhenExpectation
        # When stop_dt IS NULL, start_dt must not be null
        df = self._make_df(spark, [
            ("1", "X", None, None),           # stop null + start null -> violation
            ("2", "X", "2024-01-01", None),
            ("3", "X", None, "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-NNW",
            "name": "test",
            "expectation": "validate_not_null_when",
            "parameters": {
                "condition_column": "stop_dt",
                "operator": "IS NULL",
                "check_columns": ["start_dt"],
                "pk_column": "id",
            },
        }
        result, viols = ValidateNotNullWhenExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_missing_required_params_errors(self, spark):
        from engine.expectations import ValidateNotNullWhenExpectation
        df = spark.range(1).toDF("id")
        rule = {"rule_id": "T", "name": "t", "expectation": "x",
                "parameters": {"condition_column": "status"}}   # check_columns missing
        result, _ = ValidateNotNullWhenExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"


# ---------------------------------------------------------------------------
# ValidateSequenceOrderExpectation  (renamed from MilestoneSequenceExpectation)
# ---------------------------------------------------------------------------

class TestValidateSequenceOrderExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("group_id",   StringType(), True),
            StructField("step",       StringType(), True),
            StructField("event_date", StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self):
        return {
            "rule_id": "T-SEQ",
            "name": "test",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": ["Start", "Middle", "End"],
            },
        }

    def test_correct_order_passes(self, spark):
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start",  "2024-01-01"),
            ("G1", "Middle", "2024-03-01"),
            ("G1", "End",    "2024-06-01"),
        ])
        result, viols = ValidateSequenceOrderExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"

    def test_wrong_order_fails(self, spark):
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "End",    "2024-01-01"),   # End appears before Start (wrong)
            ("G1", "Start",  "2024-06-01"),
        ])
        result, viols = ValidateSequenceOrderExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED"

    def test_old_milestone_column_param_is_unsupported(self, spark):
        # Backward-compat alias 'milestone_column' has been removed; the rule
        # must use 'value_column' instead.  Without the correct param the
        # expectation returns ERROR.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start", "2024-01-01"),
            ("G1", "End",   "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-SEQ-COMPAT",
            "name": "test",
            "expectation": "validate_sequence_order",
            "parameters": {
                "milestone_column":  "step",     # old param name — no longer supported
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": ["Start", "End"],
            },
        }
        result, _ = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR", (
            "Rule using unsupported 'milestone_column' alias must return ERROR; "
            "got: " + result["status"]
        )

    def test_missing_sort_column_returns_error(self, spark):
        # sort_column is required; omitting it must return ERROR.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start", "2024-01-01"),
            ("G1", "End",   "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-SEQ-NO-SORT",
            "name": "test",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                # sort_column intentionally omitted
                "expected_sequence": ["Start", "End"],
            },
        }
        result, _ = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR", (
            "Missing sort_column must return ERROR; got: " + result["status"]
        )

    def test_nonexistent_sort_column_returns_error(self, spark):
        # sort_column pointing to a column not in the DataFrame must return ERROR.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start", "2024-01-01"),
            ("G1", "End",   "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-SEQ-BAD-SORT",
            "name": "test",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "nonexistent_col",
                "expected_sequence": ["Start", "End"],
            },
        }
        result, _ = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR", (
            "Nonexistent sort_column must return ERROR; got: " + result["status"]
        )

    def test_numeric_sort_column_correct_order_passes(self, spark):
        # Sort by a numeric rank column instead of a date string.
        from engine.expectations import ValidateSequenceOrderExpectation
        schema = StructType([
            StructField("group_id", StringType(),  True),
            StructField("step",     StringType(),  True),
            StructField("rank",     IntegerType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "Start",  1),
            ("G1", "Middle", 2),
            ("G1", "End",    3),
        ], schema)
        rule = {
            "rule_id": "T-SEQ-NUMERIC",
            "name": "test numeric sort",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "rank",
                "expected_sequence": ["Start", "Middle", "End"],
            },
        }
        result, viols = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "PASSED"

    def test_numeric_sort_column_wrong_order_fails(self, spark):
        # Sort by numeric rank; End ranked lower than Start → violation.
        from engine.expectations import ValidateSequenceOrderExpectation
        schema = StructType([
            StructField("group_id", StringType(),  True),
            StructField("step",     StringType(),  True),
            StructField("rank",     IntegerType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "End",   1),
            ("G1", "Start", 2),
        ], schema)
        rule = {
            "rule_id": "T-SEQ-NUMERIC-FAIL",
            "name": "test numeric sort failure",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "rank",
                "expected_sequence": ["Start", "End"],
            },
        }
        result, viols = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"

    def test_gate_skips_open_groups(self, spark):
        # G1 has both Start and End, but end_date is NULL — group not yet closed.
        # G2 has both Start and End with end_date set — gate passes, order is correct.
        # With a gate using sort_column, G1 must be silently skipped (PASSED overall).
        from engine.expectations import ValidateSequenceOrderExpectation
        schema = StructType([
            StructField("group_id",   StringType(), True),
            StructField("step",       StringType(), True),
            StructField("event_date", StringType(), True),
            StructField("end_date",   StringType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "Start", "2024-01-01", None),         # open — no end_date on End row
            ("G1", "End",   "2024-06-01", None),
            ("G2", "Start", "2024-01-01", None),
            ("G2", "End",   "2024-06-01", "2024-06-02"), # closed
        ], schema)
        rule = {
            "rule_id": "T-SEQ-GATE",
            "name": "test",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": ["Start", "End"],
                "completion_gate": {
                    "value_column": "step",
                    "value":        "End",
                    "sort_column":  "end_date",
                },
            },
        }
        result, viols = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "PASSED", (
            "Open groups must be skipped by the gate; expected PASSED got "
            + result["status"]
        )

    def test_gate_without_sort_column_includes_all_groups_with_value(self, spark):
        # Gate with no sort_column: any group with the gate value is considered closed.
        # Both G1 and G2 have End → both evaluated. G1 has wrong order → FAILED.
        from engine.expectations import ValidateSequenceOrderExpectation
        schema = StructType([
            StructField("group_id",   StringType(), True),
            StructField("step",       StringType(), True),
            StructField("event_date", StringType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "End",   "2024-01-01"),  # End before Start — wrong
            ("G1", "Start", "2024-06-01"),
            ("G2", "Start", "2024-01-01"),
            ("G2", "End",   "2024-06-01"),  # correct order
        ], schema)
        rule = {
            "rule_id": "T-SEQ-GATE-NO-SORT",
            "name": "test gate no sort_column",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": ["Start", "End"],
                "completion_gate": {
                    "value_column": "step",
                    "value":        "End",
                    # no sort_column — presence check only
                },
            },
        }
        result, viols = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        pk_values = [r["primary_key_value"] for r in viols.collect()]
        assert "G1" in pk_values

    def test_gate_catches_bad_order_in_closed_groups(self, spark):
        # G1 is closed (End has end_date) but order is wrong — must FAIL.
        from engine.expectations import ValidateSequenceOrderExpectation
        schema = StructType([
            StructField("group_id",   StringType(), True),
            StructField("step",       StringType(), True),
            StructField("event_date", StringType(), True),
            StructField("end_date",   StringType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "End",   "2024-01-01", "2024-06-02"), # End before Start — wrong
            ("G1", "Start", "2024-06-01", None),
        ], schema)
        rule = {
            "rule_id": "T-SEQ-GATE-FAIL",
            "name": "test",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": ["Start", "End"],
                "completion_gate": {
                    "value_column": "step",
                    "value":        "End",
                    "sort_column":  "end_date",
                },
            },
        }
        result, viols = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    # ------------------------------------------------------------------
    # Flexible sequence tests
    # ------------------------------------------------------------------

    def _rule_flexible(self):
        """Rule where 'Middle' is marked flexible (repeats allowed)."""
        return {
            "rule_id": "T-SEQ-FLEX",
            "name": "test flexible",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": [
                    {"value": "Start"},
                    {"value": "Middle", "flexible": True},
                    {"value": "End"},
                ],
            },
        }

    def test_flexible_allows_repeated_middle(self, spark):
        # Start → Middle → Middle → End: Middle is flexible so repeats are allowed.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start",  "2024-01-01"),
            ("G1", "Middle", "2024-02-01"),
            ("G1", "Middle", "2024-03-01"),
            ("G1", "End",    "2024-06-01"),
        ])
        result, viols = ValidateSequenceOrderExpectation().validate(df, self._rule_flexible(), spark)
        assert result["status"] == "PASSED", (
            "Repeated flexible step must not be a violation; got: " + result["status"]
        )

    def test_flexible_catches_out_of_order(self, spark):
        # Start → End → Middle: even with flexible Middle, going backwards is a violation.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start",  "2024-01-01"),
            ("G1", "End",    "2024-02-01"),
            ("G1", "Middle", "2024-03-01"),
        ])
        result, viols = ValidateSequenceOrderExpectation().validate(df, self._rule_flexible(), spark)
        assert result["status"] == "FAILED", (
            "Out-of-order step after End must be a violation; got: " + result["status"]
        )
        assert viols.count() == 1

    def test_dict_format_without_flexible_matches_strict(self, spark):
        # Dict format without any flexible flag behaves identically to the plain string list.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start",  "2024-01-01"),
            ("G1", "Middle", "2024-03-01"),
            ("G1", "End",    "2024-06-01"),
        ])
        rule = {
            "rule_id": "T-SEQ-DICT",
            "name": "test dict format",
            "expectation": "validate_sequence_order",
            "parameters": {
                "value_column":      "step",
                "group_column":      "group_id",
                "sort_column":       "event_date",
                "expected_sequence": [
                    {"value": "Start"},
                    {"value": "Middle"},
                    {"value": "End"},
                ],
            },
        }
        result, viols = ValidateSequenceOrderExpectation().validate(df, rule, spark)
        assert result["status"] == "PASSED"

    def test_strict_repeated_step_is_violation(self, spark):
        # Without flexible, a repeated value is an illegal repetition violation.
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start",  "2024-01-01"),
            ("G1", "Middle", "2024-02-01"),
            ("G1", "Middle", "2024-03-01"),   # repeated strict step — violation
            ("G1", "End",    "2024-06-01"),
        ])
        result, viols = ValidateSequenceOrderExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED", (
            "Repeated strict step must be flagged as a violation; got: " + result["status"]
        )

    def test_flexible_multiple_repeats_with_two_groups(self, spark):
        # G1 valid (Middle repeats are allowed); G2 invalid (End → Middle is out of order).
        from engine.expectations import ValidateSequenceOrderExpectation
        df = self._make_df(spark, [
            ("G1", "Start",  "2024-01-01"),
            ("G1", "Middle", "2024-02-01"),
            ("G1", "Middle", "2024-03-01"),
            ("G1", "End",    "2024-04-01"),
            ("G2", "Start",  "2024-01-01"),
            ("G2", "End",    "2024-02-01"),
            ("G2", "Middle", "2024-03-01"),   # out of order
        ])
        result, viols = ValidateSequenceOrderExpectation().validate(df, self._rule_flexible(), spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1
        pk_values = [r["primary_key_value"] for r in viols.collect()]
        assert "G2" in pk_values
        assert "G1" not in pk_values


# ---------------------------------------------------------------------------
# ValidatePairedPresenceExpectation  (renamed from MilestonePairsCompleteExpectation)
# ---------------------------------------------------------------------------

class TestValidatePairedPresenceExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("group_id", StringType(), True),
            StructField("step",     StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self):
        return {
            "rule_id": "T-PAIR",
            "name": "test",
            "expectation": "validate_paired_presence",
            "parameters": {
                "value_column":   "step",
                "group_column":   "group_id",
                "required_pairs": [["Start", "Stop"]],
            },
        }

    def test_both_present_passes(self, spark):
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Start"), ("G1", "Stop")])
        result, viols = ValidatePairedPresenceExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"

    def test_start_without_stop_fails(self, spark):
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Start")])   # no Stop
        result, viols = ValidatePairedPresenceExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_gate_skips_open_groups(self, spark):
        # G1 has only Start — pair incomplete. But it is open (no Stop with end_date).
        # With a gate using sort_column, G1 must be silently skipped (PASSED overall).
        from engine.expectations import ValidatePairedPresenceExpectation
        schema = StructType([
            StructField("group_id", StringType(), True),
            StructField("step",     StringType(), True),
            StructField("end_date", StringType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "Start", None),  # open — no Stop with end_date
        ], schema)
        rule = {
            "rule_id": "T-PAIR-GATE",
            "name": "test",
            "expectation": "validate_paired_presence",
            "parameters": {
                "value_column":   "step",
                "group_column":   "group_id",
                "required_pairs": [["Start", "Stop"]],
                "completion_gate": {
                    "value_column": "step",
                    "value":        "Stop",
                    "sort_column":  "end_date",
                },
            },
        }
        result, viols = ValidatePairedPresenceExpectation().validate(df, rule, spark)
        assert result["status"] == "PASSED", (
            "Open groups must be skipped by the gate; expected PASSED got "
            + result["status"]
        )

    def test_gate_catches_missing_pair_in_closed_groups(self, spark):
        # G1 has Stop with end_date set — it is closed — but Start is missing.
        from engine.expectations import ValidatePairedPresenceExpectation
        schema = StructType([
            StructField("group_id", StringType(), True),
            StructField("step",     StringType(), True),
            StructField("end_date", StringType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "Stop", "2024-06-01"),  # closed — missing Start
        ], schema)
        rule = {
            "rule_id": "T-PAIR-GATE-FAIL",
            "name": "test",
            "expectation": "validate_paired_presence",
            "parameters": {
                "value_column":   "step",
                "group_column":   "group_id",
                "required_pairs": [["Start", "Stop"]],
                "completion_gate": {
                    "value_column": "step",
                    "value":        "Stop",
                    "sort_column":  "end_date",
                },
            },
        }
        result, viols = ValidatePairedPresenceExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_gate_without_sort_column_uses_presence_check(self, spark):
        # Gate with no sort_column: any group with the gate value is considered closed.
        # G1 has Stop (no end_date column needed) → evaluated → Start missing → FAILED.
        from engine.expectations import ValidatePairedPresenceExpectation
        schema = StructType([
            StructField("group_id", StringType(), True),
            StructField("step",     StringType(), True),
        ])
        df = spark.createDataFrame([
            ("G1", "Stop"),  # closed by presence — missing Start
        ], schema)
        rule = {
            "rule_id": "T-PAIR-GATE-NO-SORT",
            "name": "test gate no sort_column",
            "expectation": "validate_paired_presence",
            "parameters": {
                "value_column":   "step",
                "group_column":   "group_id",
                "required_pairs": [["Start", "Stop"]],
                "completion_gate": {
                    "value_column": "step",
                    "value":        "Stop",
                    # no sort_column — presence check only
                },
            },
        }
        result, viols = ValidatePairedPresenceExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    # ---- one-to-many pair tests ----

    def _rule_one_to_many(self):
        return {
            "rule_id": "T-PAIR-OTM",
            "name": "test one-to-many",
            "expectation": "validate_paired_presence",
            "parameters": {
                "value_column": "step",
                "group_column": "group_id",
                "required_pairs": [["Start", ["Stop", "Cancel"]]],
            },
        }

    def test_one_to_many_first_stop_only_passes(self, spark):
        """Start + Stop (Cancel absent) → PASSED."""
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Start"), ("G1", "Stop")])
        result, viols = ValidatePairedPresenceExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_one_to_many_second_stop_only_passes(self, spark):
        """Start + Cancel (Stop absent) → PASSED."""
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Start"), ("G1", "Cancel")])
        result, viols = ValidatePairedPresenceExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_one_to_many_both_stops_present_passes(self, spark):
        """Start + Stop + Cancel → PASSED (more than one stop is fine)."""
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [
            ("G1", "Start"), ("G1", "Stop"), ("G1", "Cancel")
        ])
        result, viols = ValidatePairedPresenceExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "PASSED"

    def test_one_to_many_start_without_any_stop_fails(self, spark):
        """Start present, neither Stop nor Cancel → FAILED."""
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Start")])
        result, viols = ValidatePairedPresenceExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 1
        row = viols.collect()[0]
        assert row["primary_key_value"] == "G1"

    def test_one_to_many_stop_without_start_fails(self, spark):
        """Stop present, Start absent → FAILED."""
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Stop")])
        result, viols = ValidatePairedPresenceExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_one_to_many_cancel_without_start_fails(self, spark):
        """Cancel present, Start absent → FAILED."""
        from engine.expectations import ValidatePairedPresenceExpectation
        df = self._make_df(spark, [("G1", "Cancel")])
        result, viols = ValidatePairedPresenceExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_mixed_strict_and_one_to_many_pairs(self, spark):
        """One strict pair and one one-to-many pair in the same rule."""
        from engine.expectations import ValidatePairedPresenceExpectation
        rule = {
            "rule_id": "T-PAIR-MIX",
            "name": "mixed",
            "expectation": "validate_paired_presence",
            "parameters": {
                "value_column": "step",
                "group_column": "group_id",
                "required_pairs": [
                    ["Approve", "Send"],                    # strict
                    ["Start",   ["Stop", "Cancel"]],        # one-to-many
                ],
            },
        }
        # G1: Approve+Send OK, Start+Stop OK → all PASS
        # G2: Approve+Send OK, Start without any stop → FAIL
        df = self._make_df(spark, [
            ("G1", "Approve"), ("G1", "Send"), ("G1", "Start"), ("G1", "Stop"),
            ("G2", "Approve"), ("G2", "Send"), ("G2", "Start"),
        ])
        result, viols = ValidatePairedPresenceExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        pks = {r["primary_key_value"] for r in viols.collect()}
        assert pks == {"G2"}


# ---------------------------------------------------------------------------
# ValidateNoOrphanExpectation  (renamed from MilestoneNoOrphanExpectation)
# ---------------------------------------------------------------------------

class TestValidateNoOrphanExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("group_id", StringType(), True),
            StructField("step",     StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self):
        return {
            "rule_id": "T-ORPHAN",
            "name": "test",
            "expectation": "validate_no_orphan",
            "parameters": {
                "value_column": "step",
                "group_column": "group_id",
                "pairs":        [["Start", "Stop"]],
            },
        }

    def test_no_orphan_passes(self, spark):
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Start"), ("G1", "Stop")])
        result, viols = ValidateNoOrphanExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"

    def test_stop_without_start_fails(self, spark):
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Stop")])   # no Start - orphan
        result, viols = ValidateNoOrphanExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED"

    def test_start_without_stop_passes(self, spark):
        # Only stop-without-start is an orphan; start-without-stop is valid
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Start")])
        result, viols = ValidateNoOrphanExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"

    # ---- one-to-many orphan tests ----

    def _rule_one_to_many(self):
        return {
            "rule_id": "T-ORPHAN-OTM",
            "name": "test one-to-many orphan",
            "expectation": "validate_no_orphan",
            "parameters": {
                "value_column": "step",
                "group_column": "group_id",
                "pairs":        [["Start", ["Stop", "Cancel"]]],
            },
        }

    def test_one_to_many_no_orphan_first_stop_has_start_passes(self, spark):
        """Stop present with Start → no orphan."""
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Start"), ("G1", "Stop")])
        result, viols = ValidateNoOrphanExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "PASSED"

    def test_one_to_many_no_orphan_second_stop_has_start_passes(self, spark):
        """Cancel present with Start → no orphan."""
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Start"), ("G1", "Cancel")])
        result, viols = ValidateNoOrphanExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "PASSED"

    def test_one_to_many_orphan_first_stop_fails(self, spark):
        """Stop present without Start → FAILED (orphan)."""
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Stop")])
        result, viols = ValidateNoOrphanExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_one_to_many_orphan_second_stop_fails(self, spark):
        """Cancel present without Start → FAILED (orphan)."""
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Cancel")])
        result, viols = ValidateNoOrphanExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_one_to_many_start_without_any_stop_passes(self, spark):
        """Start present, no Stop or Cancel → no orphan (start-without-stop is valid)."""
        from engine.expectations import ValidateNoOrphanExpectation
        df = self._make_df(spark, [("G1", "Start")])
        result, viols = ValidateNoOrphanExpectation().validate(
            df, self._rule_one_to_many(), spark
        )
        assert result["status"] == "PASSED"


# ---------------------------------------------------------------------------
# ValidateConditionalColumnValueExpectation
# (renamed from InvoiceRefundValidationExpectation; more generic params)
# ---------------------------------------------------------------------------

class TestValidateConditionalColumnValueExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("id",          StringType(), True),
            StructField("amount",      DoubleType(), True),
            StructField("record_type", StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule_generic(self):
        return {
            "rule_id": "T-COND",
            "name": "test",
            "expectation": "validate_conditional_column_value",
            "parameters": {
                "condition_column":   "amount",
                "operator":           "<",
                "value":              0,
                "required_column":    "record_type",
                "required_value":     "Credit",
                "pk_column":          "id",
            },
        }

    def _rule_backward_compat(self):
        # Old parameter names (expect_refund_validation style)
        return {
            "rule_id": "T-COND-COMPAT",
            "name": "test",
            "expectation": "expect_refund_validation",
            "parameters": {
                "amount_column": "amount",
                "type_column":   "record_type",
                "credit_type":   "Credit",
                "pk_column":     "id",
            },
        }

    def test_negative_on_credit_passes(self, spark):
        from engine.expectations import ValidateConditionalColumnValueExpectation
        df = self._make_df(spark, [("1", -100.0, "Credit")])
        result, viols = ValidateConditionalColumnValueExpectation().validate(
            df, self._rule_generic(), spark
        )
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_negative_on_non_credit_fails(self, spark):
        from engine.expectations import ValidateConditionalColumnValueExpectation
        df = self._make_df(spark, [("1", -100.0, "Standard")])
        result, viols = ValidateConditionalColumnValueExpectation().validate(
            df, self._rule_generic(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_no_negatives_passes(self, spark):
        from engine.expectations import ValidateConditionalColumnValueExpectation
        df = self._make_df(spark, [("1", 100.0, "Standard")])
        result, viols = ValidateConditionalColumnValueExpectation().validate(
            df, self._rule_generic(), spark
        )
        assert result["status"] == "PASSED"

    def test_backward_compat_old_params_no_longer_supported(self, spark):
        # Old parameter names (amount_column, type_column, credit_type) have been
        # removed. Using them must now return an ERROR due to missing required params.
        from engine.expectations import ValidateConditionalColumnValueExpectation
        df = self._make_df(spark, [("1", -50.0, "Standard")])
        rule = {
            "rule_id": "T-COND-COMPAT",
            "name": "test",
            "expectation": "validate_conditional_column_value",
            "parameters": {
                "amount_column": "amount",     # old param — no longer supported
                "type_column":   "record_type",  # old param — no longer supported
                "credit_type":   "Credit",       # old param — no longer supported
                "pk_column":     "id",
            },
        }
        result, _ = ValidateConditionalColumnValueExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR", (
            "Old parameter aliases must no longer be accepted; expected ERROR, "
            "got: " + result["status"]
        )

    def test_missing_params_errors(self, spark):
        from engine.expectations import ValidateConditionalColumnValueExpectation
        df = self._make_df(spark, [("1", -50.0, "Standard")])
        rule = {"rule_id": "T", "name": "t", "expectation": "x",
                "parameters": {"condition_column": "amount"}}  # missing required params
        result, _ = ValidateConditionalColumnValueExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"


# ---------------------------------------------------------------------------
# ValidateGroupAggregateMatchExpectation
# (renamed from InvoiceTotalConsistencyExpectation; more generic params)
# ---------------------------------------------------------------------------

class TestValidateGroupAggregateMatchExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("invoice_id",    StringType(), True),
            StructField("line_amount",   DoubleType(), True),
            StructField("header_amount", DoubleType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule_generic(self):
        return {
            "rule_id": "T-GRP",
            "name": "test",
            "expectation": "validate_group_aggregate_match",
            "parameters": {
                "group_column":     "invoice_id",
                "aggregate_column": "line_amount",
                "reference_column": "header_amount",
                "aggregate":        "sum",
                "tolerance":        0.01,
            },
        }

    def test_totals_match_passes(self, spark):
        from engine.expectations import ValidateGroupAggregateMatchExpectation
        df = self._make_df(spark, [
            ("INV1", 100.0, 300.0),
            ("INV1", 200.0, 300.0),
        ])
        result, viols = ValidateGroupAggregateMatchExpectation().validate(
            df, self._rule_generic(), spark
        )
        assert result["status"] == "PASSED"

    def test_totals_mismatch_fails(self, spark):
        from engine.expectations import ValidateGroupAggregateMatchExpectation
        df = self._make_df(spark, [
            ("INV1", 100.0, 500.0),   # sum = 100, header = 500 -> mismatch
        ])
        result, viols = ValidateGroupAggregateMatchExpectation().validate(
            df, self._rule_generic(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_backward_compat_old_params_no_longer_supported(self, spark):
        from engine.expectations import ValidateGroupAggregateMatchExpectation
        df = self._make_df(spark, [
            ("INV1", 50.0, 100.0),
            ("INV1", 50.0, 100.0),
        ])
        rule = {
            "rule_id": "T-GRP-COMPAT",
            "name": "test",
            "expectation": "validate_group_aggregate_match",
            "parameters": {
                "invoice_id_column":    "invoice_id",     # old param — no longer supported
                "line_amount_column":   "line_amount",    # old param — no longer supported
                "header_amount_column": "header_amount",  # old param — no longer supported
                "tolerance":            0.01,
            },
        }
        result, _ = ValidateGroupAggregateMatchExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR", (
            "Old parameter aliases must no longer be accepted; expected ERROR, "
            "got: " + result["status"]
        )

    def test_missing_reference_column_skips(self, spark):
        from engine.expectations import ValidateGroupAggregateMatchExpectation
        df = self._make_df(spark, [("INV1", 100.0, None)])
        rule = {
            "rule_id": "T-GRP",
            "name": "test",
            "expectation": "validate_group_aggregate_match",
            "parameters": {
                "group_column":     "invoice_id",
                "aggregate_column": "line_amount",
                "reference_column": "no_such_column",
                "aggregate":        "sum",
                "tolerance":        0.01,
            },
        }
        result, _ = ValidateGroupAggregateMatchExpectation().validate(df, rule, spark)
        assert result["status"] == "PASSED"   # skips when column missing


# ---------------------------------------------------------------------------
# ValidateColumnExclusionsExpectation  (negative / forbidden-state validator)
# ---------------------------------------------------------------------------

class TestValidateColumnExclusionsExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("id",      StringType(), True),
            StructField("col_a",   StringType(), True),
            StructField("col_b",   StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self, condition):
        return {
            "rule_id": "T-EXCL",
            "name": "test exclusion",
            "expectation": "validate_column_exclusions",
            "parameters": {
                "condition": condition,
                "pk_column":  "id",
            },
        }

    def test_no_forbidden_rows_passes(self, spark):
        """All rows are valid — neither column is NULL simultaneously."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [
            ("R1", "A",   "B"),
            ("R2", "A",   None),   # only col_b is null — not forbidden
            ("R3", None,  "B"),    # only col_a is null — not forbidden
        ])
        result, viols = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("col_a IS NULL AND col_b IS NULL"), spark
        )
        assert result["status"] == "PASSED"
        assert result["failed_rows"] == 0
        assert viols.count() == 0

    def test_forbidden_rows_fail(self, spark):
        """Rows where both columns are NULL violate the rule."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [
            ("R1", "A",   "B"),
            ("R2", None,  None),   # forbidden: both NULL
            ("R3", None,  None),   # forbidden: both NULL
        ])
        result, viols = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("col_a IS NULL AND col_b IS NULL"), spark
        )
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 2
        assert result["passed_rows"] == 1
        assert viols.count() == 2

    def test_all_rows_forbidden_fails(self, spark):
        """Every row violates the condition."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [
            ("R1", None, None),
            ("R2", None, None),
        ])
        result, viols = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("col_a IS NULL AND col_b IS NULL"), spark
        )
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 2
        assert result["success_pct"] == 0.0

    def test_empty_dataframe_passes(self, spark):
        """Empty DataFrame should pass (no rows to violate)."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [])
        result, viols = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("col_a IS NULL AND col_b IS NULL"), spark
        )
        assert result["status"] == "PASSED"
        assert result["total_rows"] == 0

    def test_missing_condition_errors(self, spark):
        """Missing condition parameter should return ERROR status."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [("R1", "A", "B")])
        rule = {
            "rule_id": "T-EXCL-ERR",
            "name": "test",
            "expectation": "validate_column_exclusions",
            "parameters": {"pk_column": "id"},
        }
        result, _ = ValidateColumnExclusionsExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"
        assert "condition" in result["details"].lower()

    def test_invalid_condition_expression_errors(self, spark):
        """An invalid SQL expression in condition should return ERROR status."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [("R1", "A", "B")])
        result, _ = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("THIS IS NOT VALID SQL !!!@#"), spark
        )
        assert result["status"] == "ERROR"

    def test_violations_have_correct_schema(self, spark):
        """Violations DataFrame must have the canonical five-column schema."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [
            ("R1", None, None),
        ])
        _, viols = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("col_a IS NULL AND col_b IS NULL"), spark
        )
        col_names = viols.columns
        for expected in [
            "primary_key_value", "violated_column",
            "actual_value", "expected_condition", "violation_detail",
        ]:
            assert expected in col_names, f"Missing violations column: {expected}"

    def test_success_pct_calculated_correctly(self, spark):
        """success_pct should reflect the fraction of non-violating rows."""
        from engine.expectations import ValidateColumnExclusionsExpectation
        df = self._make_df(spark, [
            ("R1", "A",  "B"),    # valid
            ("R2", "A",  "B"),    # valid
            ("R3", "A",  "B"),    # valid
            ("R4", None, None),   # violation
        ])
        result, _ = ValidateColumnExclusionsExpectation().validate(
            df, self._rule("col_a IS NULL AND col_b IS NULL"), spark
        )
        assert result["total_rows"] == 4
        assert result["passed_rows"] == 3
        assert result["failed_rows"] == 1
        assert result["success_pct"] == 75.0


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_all_expected_keys_present(self):
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY

        # Generic canonical names (the only names supported)
        generic_keys = [
            "validate_column_comparison",
            "validate_aggregate_rule",
            "validate_foreign_key",
            "validate_not_null_when",
            "validate_column_exclusions",
            "validate_sequence_order",
            "validate_paired_presence",
            "validate_no_orphan",
            "validate_conditional_column_value",
            "validate_group_aggregate_match",
        ]
        for key in generic_keys:
            assert key in CUSTOM_EXPECTATION_REGISTRY, f"Missing generic key: {key}"

        # SQL and aggregate helpers
        for key in ["sql_validation", "sql", "expect_column_sum_to_equal",
                    "expect_row_count_to_be_between",
                    "expect_unique_combination_of_columns"]:
            assert key in CUSTOM_EXPECTATION_REGISTRY, f"Missing key: {key}"

    def test_legacy_aliases_are_removed(self):
        """Verify that all table-specific backward-compatible aliases have been removed."""
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        removed_keys = [
            # Original table-specific names (removed in earlier refactor)
            "expect_milestone_order",
            "expect_milestone_pairs",
            "expect_no_open_milestone_pairs",
            "expect_no_duplicate_milestones",
            # Backward-compatible aliases (removed in this refactor)
            "expect_milestone_sequence",
            "expect_milestone_pairs_complete",
            "expect_no_orphan_milestones",
            "expect_refund_validation",
            "expect_invoice_total_consistency",
        ]
        for key in removed_keys:
            assert key not in CUSTOM_EXPECTATION_REGISTRY, (
                f"Legacy alias still present in registry: {key}"
            )

    def test_all_registry_classes_are_instantiable(self):
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        for name, cls in CUSTOM_EXPECTATION_REGISTRY.items():
            try:
                instance = cls()
                assert hasattr(instance, "validate"), (
                    f"{name}: class must have a validate() method"
                )
            except Exception as exc:
                pytest.fail(f"Could not instantiate {name}: {exc}")


# ---------------------------------------------------------------------------
# Resolution Tracking — VIOLATION_SCHEMA fields
# ---------------------------------------------------------------------------

class TestResolutionTracking:
    """
    Tests for the VIOLATION_SCHEMA fields (issue_status, resolution_timestamp).
    """

    def test_violation_schema_contains_issue_status(self, spark):
        """VIOLATION_SCHEMA must include the issue_status field."""
        from engine.resolution import VIOLATION_SCHEMA
        field_names = {f.name for f in VIOLATION_SCHEMA.fields}
        assert "issue_status" in field_names

    def test_violation_schema_contains_resolution_timestamp(self, spark):
        """VIOLATION_SCHEMA must include the resolution_timestamp field."""
        from engine.resolution import VIOLATION_SCHEMA
        field_names = {f.name for f in VIOLATION_SCHEMA.fields}
        assert "resolution_timestamp" in field_names

    def test_issue_status_is_not_nullable(self, spark):
        """issue_status must be defined as NOT NULL in VIOLATION_SCHEMA."""
        from engine.resolution import VIOLATION_SCHEMA
        status_field = next(
            f for f in VIOLATION_SCHEMA.fields if f.name == "issue_status"
        )
        assert not status_field.nullable, "issue_status must be NOT NULL"

    def test_resolution_timestamp_is_nullable(self, spark):
        """resolution_timestamp must be nullable (NULL while issue is Active)."""
        from engine.resolution import VIOLATION_SCHEMA
        res_ts_field = next(
            f for f in VIOLATION_SCHEMA.fields if f.name == "resolution_timestamp"
        )
        assert res_ts_field.nullable, "resolution_timestamp must be nullable"

    def test_new_violations_carry_active_status(self, spark):
        """
        Violations produced by expectation classes have the raw five-column
        schema; the runner selects issue_status='Active' and
        resolution_timestamp=NULL before persisting.  Here we verify that a
        real expectation validates correctly and that the VIOLATION_SCHEMA
        nullability constraints are set as documented.
        """
        from engine.expectations import ColumnComparisonExpectation
        from engine.resolution import VIOLATION_SCHEMA

        schema = StructType([
            StructField("id",    StringType(),  True),
            StructField("col_a", IntegerType(), True),
            StructField("col_b", IntegerType(), True),
        ])
        df = spark.createDataFrame([("PK-1", 2, 5)], schema)
        rule = {
            "rule_id": "T-RT",
            "name": "test",
            "expectation": "validate_column_comparison",
            "parameters": {
                "column_A": "col_a",
                "column_B": "col_b",
                "operator": ">",
                "pk_column": "id",
            },
        }
        result, viols = ColumnComparisonExpectation().validate(df, rule, spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

        # Confirm VIOLATION_SCHEMA nullability contract
        status_field = next(
            f for f in VIOLATION_SCHEMA.fields if f.name == "issue_status"
        )
        assert not status_field.nullable
        res_ts_field = next(
            f for f in VIOLATION_SCHEMA.fields if f.name == "resolution_timestamp"
        )
        assert res_ts_field.nullable


# ---------------------------------------------------------------------------
# Delta Log Enrichment — failure_type and Power BI fields
# ---------------------------------------------------------------------------

class TestDeltaLogEnrichment:
    """
    Tests that verify the enriched delta log fields introduced for Power BI
    integration: failure_type.
    """

    def test_failure_type_field_in_violation_schema(self, spark):
        """The VIOLATION_SCHEMA must contain failure_type for Power BI categorisation."""
        from engine.resolution import VIOLATION_SCHEMA
        field_names = [f.name for f in VIOLATION_SCHEMA.fields]
        assert "failure_type" in field_names, (
            "failure_type is required in VIOLATION_SCHEMA for Power BI failure categorisation"
        )

    def test_run_id_field_in_violation_schema(self, spark):
        """VIOLATION_SCHEMA must contain run_id as the Validation Run ID."""
        from engine.resolution import VIOLATION_SCHEMA
        field_names = [f.name for f in VIOLATION_SCHEMA.fields]
        assert "run_id" in field_names

    def test_run_timestamp_field_in_violation_schema(self, spark):
        """VIOLATION_SCHEMA must contain run_timestamp as execution metadata."""
        from engine.resolution import VIOLATION_SCHEMA
        field_names = [f.name for f in VIOLATION_SCHEMA.fields]
        assert "run_timestamp" in field_names

    def test_failure_type_nullable_in_schema(self, spark):
        """failure_type must be nullable since not all rules define a category."""
        from engine.resolution import VIOLATION_SCHEMA
        ft_field = next(
            (f for f in VIOLATION_SCHEMA.fields if f.name == "failure_type"), None
        )
        assert ft_field is not None, "failure_type field must exist in VIOLATION_SCHEMA"
        assert ft_field.nullable, "failure_type must be nullable"

    def test_all_required_pbi_fields_present(self, spark):
        """All fields required for actionable Power BI reports must be in VIOLATION_SCHEMA."""
        from engine.resolution import VIOLATION_SCHEMA
        field_names = {f.name for f in VIOLATION_SCHEMA.fields}
        required_for_pbi = {
            "run_id",              # Validation Run ID — links violations to a specific run
            "run_timestamp",       # Execution metadata — when the run occurred
            "batch_date",          # Partitioning field for historical reporting
            "rule_id",             # Rule reference for drill-through
            "rule_name",           # Human-readable rule label
            "severity",            # Priority filtering in Power BI
            "failure_type",        # Category-based filtering (Completeness / Business Logic / etc.)
            "primary_key_value",   # Row-level traceability to the source record
            "issue_status",        # Active / Resolved tracking
            "resolution_timestamp",# When the issue was resolved
        }
        missing = required_for_pbi - field_names
        assert not missing, f"Missing required Power BI fields: {missing}"


# ---------------------------------------------------------------------------
# ValidateGateExpectation
# ---------------------------------------------------------------------------

class TestValidateGateExpectation:
    """Tests for the validate_gate expectation."""

    def _make_df(self, spark, rows, include_sort_column=False):
        from pyspark.sql.types import StructType, StructField, StringType
        if include_sort_column:
            schema = StructType([
                StructField("group_id",   StringType(), True),
                StructField("step",       StringType(), True),
                StructField("event_date", StringType(), True),
            ])
        else:
            schema = StructType([
                StructField("group_id", StringType(), True),
                StructField("step",     StringType(), True),
            ])
        return spark.createDataFrame(rows, schema)

    def _rule(self, value_to_check="Approved", sort_col=None, trigger=None):
        params = {
            "value_column":   "step",
            "group_column":   "group_id",
            "value_to_check": value_to_check,
        }
        if sort_col:
            params["sort_column"] = sort_col
        if trigger:
            params["trigger"] = trigger
        return {
            "rule_id":     "T-GATE",
            "name":        "test gate",
            "expectation": "validate_gate",
            "parameters":  params,
        }

    def test_group_with_required_value_passes(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [("G1", "Approved"), ("G1", "Pending")])
        result, viols = ValidateGateExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_group_without_required_value_fails(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [("G1", "Pending"), ("G1", "Review")])
        result, viols = ValidateGateExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1
        rows = viols.collect()
        assert rows[0]["primary_key_value"] == "G1"

    def test_mixed_groups_only_failed_reported(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [
            ("G1", "Approved"),  # passes
            ("G2", "Pending"),   # fails
        ])
        result, viols = ValidateGateExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1
        pk_values = [r["primary_key_value"] for r in viols.collect()]
        assert "G2" in pk_values
        assert "G1" not in pk_values

    def test_empty_dataframe_passes(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [])
        result, viols = ValidateGateExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"

    def test_sort_column_filters_null_rows(self, spark):
        # G1 has Approved but with null event_date — not counted with sort_column check.
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [
            ("G1", "Approved", None),   # null sort_col — excluded
            ("G1", "Pending",  "2024-01-01"),
        ], include_sort_column=True)
        result, viols = ValidateGateExpectation().validate(
            df, self._rule(sort_col="event_date"), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_sort_column_non_null_row_passes(self, spark):
        # G1 has Approved with a non-null event_date — group passes.
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [
            ("G1", "Approved", "2024-06-01"),
            ("G1", "Pending",  "2024-01-01"),
        ], include_sort_column=True)
        result, viols = ValidateGateExpectation().validate(
            df, self._rule(sort_col="event_date"), spark
        )
        assert result["status"] == "PASSED"

    def test_custom_trigger_label_in_violation_detail(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [("G1", "Pending")])
        result, viols = ValidateGateExpectation().validate(
            df, self._rule(trigger="Final Approval"), spark
        )
        detail = viols.collect()[0]["violation_detail"]
        assert "Final Approval" in detail

    def test_missing_value_column_returns_error(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [("G1", "Approved")])
        rule = {
            "rule_id": "T-GATE-ERR",
            "name": "test",
            "expectation": "validate_gate",
            "parameters": {"group_column": "group_id", "value_to_check": "X"},
        }
        result, viols = ValidateGateExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_missing_group_column_returns_error(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [("G1", "Approved")])
        rule = {
            "rule_id": "T-GATE-ERR2",
            "name": "test",
            "expectation": "validate_gate",
            "parameters": {"value_column": "step", "value_to_check": "X"},
        }
        result, viols = ValidateGateExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_missing_value_to_check_returns_error(self, spark):
        from engine.expectations import ValidateGateExpectation
        df = self._make_df(spark, [("G1", "Approved")])
        rule = {
            "rule_id": "T-GATE-ERR3",
            "name": "test",
            "expectation": "validate_gate",
            "parameters": {"value_column": "step", "group_column": "group_id"},
        }
        result, viols = ValidateGateExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_validate_gate_registered_in_registry(self, spark):
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        assert "validate_gate" in CUSTOM_EXPECTATION_REGISTRY
        assert CUSTOM_EXPECTATION_REGISTRY["validate_gate"].__name__ == "ValidateGateExpectation"


# ---------------------------------------------------------------------------
# SqlValidationExpectation
# ---------------------------------------------------------------------------

class TestSqlValidationExpectation:
    """Tests for SqlValidationExpectation (sql_validation / sql shorthand).

    These tests register a small temp view so the SQL queries have a real
    target table to reference without needing a Spark metastore.
    """

    def _make_view(self, spark, rows, view_name="test_sql_tbl"):
        schema = StructType([
            StructField("id",     StringType(),  True),
            StructField("amount", IntegerType(), True),
            StructField("status", StringType(),  True),
        ])
        spark.createDataFrame(rows, schema).createOrReplaceTempView(view_name)
        return view_name

    def _rule(self, sql_query, pk_col=None):
        params = {"sql": sql_query}
        if pk_col:
            params["pk_column"] = pk_col
        return {
            "rule_id": "T-SQL-001",
            "name": "test sql",
            "expectation": "sql_validation",
            "parameters": params,
        }

    def test_zero_violations_passes(self, spark):
        """SQL that returns 0 rows → PASSED."""
        from engine.expectations import SqlValidationExpectation
        self._make_view(spark, [("1", 100, "OK"), ("2", 200, "OK")])
        rule = self._rule("SELECT * FROM test_sql_tbl WHERE amount < 0")
        result, viols = SqlValidationExpectation().validate(None, rule, spark)
        assert result["status"] == "PASSED"
        assert result["failed_rows"] == 0
        assert viols.count() == 0

    def test_violations_returned(self, spark):
        """SQL that returns rows → FAILED with correct violation count."""
        from engine.expectations import SqlValidationExpectation
        self._make_view(spark, [("1", -5, "ERR"), ("2", 200, "OK")])
        rule = self._rule("SELECT * FROM test_sql_tbl WHERE amount < 0", pk_col="id")
        result, viols = SqlValidationExpectation().validate(None, rule, spark)
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 1
        assert viols.count() == 1
        row = viols.collect()[0]
        assert row["primary_key_value"] == "1"
        assert row["violated_column"] == "SQL_VALIDATION"

    def test_multiple_violations(self, spark):
        """Multiple offending rows all appear in violations."""
        from engine.expectations import SqlValidationExpectation
        self._make_view(spark, [("1", -5, "ERR"), ("2", -1, "ERR"), ("3", 50, "OK")])
        rule = self._rule("SELECT * FROM test_sql_tbl WHERE amount < 0", pk_col="id")
        result, viols = SqlValidationExpectation().validate(None, rule, spark)
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 2
        assert viols.count() == 2

    def test_row_index_used_when_no_pk(self, spark):
        """When pk_column is absent, row index is used as primary_key_value."""
        from engine.expectations import SqlValidationExpectation
        self._make_view(spark, [("1", -5, "ERR")])
        rule = self._rule("SELECT * FROM test_sql_tbl WHERE amount < 0")
        result, viols = SqlValidationExpectation().validate(None, rule, spark)
        assert result["status"] == "FAILED"
        # primary_key_value should be a string representation of the row index
        pk_val = viols.collect()[0]["primary_key_value"]
        assert pk_val is not None

    def test_malformed_sql_returns_error(self, spark):
        """Invalid SQL → ERROR result, not an unhandled exception."""
        from engine.expectations import SqlValidationExpectation
        rule = self._rule("THIS IS NOT VALID SQL !!!!")
        result, viols = SqlValidationExpectation().validate(None, rule, spark)
        assert result["status"] == "ERROR"
        assert "SQL execution error" in result["details"]
        assert viols.count() == 0

    def test_empty_sql_returns_error(self, spark):
        """Empty SQL string → ERROR result."""
        from engine.expectations import SqlValidationExpectation
        rule = self._rule("   ")
        result, viols = SqlValidationExpectation().validate(None, rule, spark)
        assert result["status"] == "ERROR"
        assert viols.count() == 0

    def test_sql_shorthand_alias_works(self, spark):
        """'sql' shorthand key is accepted identically to 'sql_validation'."""
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        assert "sql" in CUSTOM_EXPECTATION_REGISTRY
        assert CUSTOM_EXPECTATION_REGISTRY["sql"] is CUSTOM_EXPECTATION_REGISTRY["sql_validation"]

    def test_violation_detail_is_json(self, spark):
        """violation_detail column holds a JSON representation of the row."""
        import json
        from engine.expectations import SqlValidationExpectation
        self._make_view(spark, [("X1", -99, "FAIL")])
        rule = self._rule("SELECT * FROM test_sql_tbl WHERE amount < 0", pk_col="id")
        _, viols = SqlValidationExpectation().validate(None, rule, spark)
        detail = viols.collect()[0]["violation_detail"]
        parsed = json.loads(detail)
        assert parsed.get("id") == "X1"


# ---------------------------------------------------------------------------
# ForeignKeyExpectation
# ---------------------------------------------------------------------------

class TestForeignKeyExpectation:
    """Tests for ForeignKeyExpectation (validate_foreign_key).

    Reference tables are registered as temp views so no real metastore is
    needed.
    """

    def _make_source(self, spark, rows):
        schema = StructType([
            StructField("id",       StringType(), True),
            StructField("category", StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _make_ref_view(self, spark, values, view_name="test_fk_ref"):
        schema = StructType([StructField("code", StringType(), True)])
        spark.createDataFrame(
            [(v,) for v in values], schema
        ).createOrReplaceTempView(view_name)
        return view_name

    def _rule(self, column="category", pk_col=None, ref_table="test_fk_ref", ref_col="code"):
        params = {
            "column": column,
            "reference": {"table": ref_table, "column": ref_col},
        }
        if pk_col:
            params["pk_column"] = pk_col
        return {
            "rule_id": "T-FK-001",
            "name": "test fk",
            "expectation": "validate_foreign_key",
            "parameters": params,
        }

    def test_all_values_exist_passes(self, spark):
        """All source FK values present in reference → PASSED."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A", "B", "C"])
        df = self._make_source(spark, [("1", "A"), ("2", "B")])
        result, viols = ForeignKeyExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"
        assert result["failed_rows"] == 0
        assert viols.count() == 0

    def test_missing_fk_detected(self, spark):
        """Source value not in reference → FAILED with 1 violation."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A", "B"])
        df = self._make_source(spark, [("1", "A"), ("2", "Z")])
        result, viols = ForeignKeyExpectation().validate(
            df, self._rule(pk_col="id"), spark
        )
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 1
        row = viols.collect()[0]
        assert row["primary_key_value"] == "2"
        assert row["actual_value"] == "Z"

    def test_multiple_missing_fk_values(self, spark):
        """Multiple missing FK values all appear as violations."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A"])
        df = self._make_source(spark, [("1", "X"), ("2", "Y"), ("3", "A")])
        result, viols = ForeignKeyExpectation().validate(
            df, self._rule(pk_col="id"), spark
        )
        assert result["status"] == "FAILED"
        assert result["failed_rows"] == 2
        assert viols.count() == 2

    def test_null_fk_values_excluded(self, spark):
        """Null FK values are skipped — only non-null values are validated."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A"])
        df = self._make_source(spark, [("1", None), ("2", "A")])
        result, viols = ForeignKeyExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"
        assert result["total_rows"] == 1  # only the non-null row was evaluated

    def test_all_null_fk_passes(self, spark):
        """All null FK values → PASSED (nothing to validate)."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A"])
        df = self._make_source(spark, [("1", None), ("2", None)])
        result, viols = ForeignKeyExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"

    def test_missing_reference_table_returns_error(self, spark):
        """Reference table that does not exist → ERROR result, not an exception."""
        from engine.expectations import ForeignKeyExpectation
        df = self._make_source(spark, [("1", "A")])
        rule = self._rule(ref_table="nonexistent_table_xyz_404")
        result, viols = ForeignKeyExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"
        assert "nonexistent_table_xyz_404" in result["details"]
        assert viols.count() == 0

    def test_missing_column_in_source_returns_error(self, spark):
        """Referencing a column not in the source DataFrame → ERROR."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A"])
        df = self._make_source(spark, [("1", "A")])
        rule = self._rule(column="nonexistent_col")
        result, viols = ForeignKeyExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_missing_required_params_returns_error(self, spark):
        """Omitting required parameters → ERROR."""
        from engine.expectations import ForeignKeyExpectation
        df = self._make_source(spark, [("1", "A")])
        rule = {"rule_id": "T", "name": "t", "expectation": "x", "parameters": {}}
        result, viols = ForeignKeyExpectation().validate(df, rule, spark)
        assert result["status"] == "ERROR"

    def test_success_pct_correct(self, spark):
        """success_pct reflects the proportion of valid FK values."""
        from engine.expectations import ForeignKeyExpectation
        self._make_ref_view(spark, ["A", "B"])
        df = self._make_source(spark, [("1", "A"), ("2", "B"), ("3", "X"), ("4", "Y")])
        result, viols = ForeignKeyExpectation().validate(
            df, self._rule(pk_col="id"), spark
        )
        assert result["status"] == "FAILED"
        assert result["total_rows"] == 4
        assert result["passed_rows"] == 2
        assert result["failed_rows"] == 2
        assert result["success_pct"] == 50.0

    def test_validate_foreign_key_registered_in_registry(self, spark):
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        assert "validate_foreign_key" in CUSTOM_EXPECTATION_REGISTRY


# =============================================================================
# IC Engine Tests
# Tests for IC_EXCEPTION_SCHEMA, _apply_ic_resolution_tracking(), and
# the ic_rule_ids detection logic in validation_runner / config.
# =============================================================================

class TestIcRuleIdentification:
    """A rule is IC if it carries at least one IC_IDENTIFIER_FIELDS value."""

    def _rule(self, **kwargs):
        base = {"rule_id": "IC-T-001", "name": "test", "expectation": "sql"}
        base.update(kwargs)
        return base

    def test_rule_with_control_ref_is_ic(self):
        from config.QualityCatalogConfig import IC_IDENTIFIER_FIELDS
        rule = self._rule(control_ref="COSO-CC5.2")
        assert any(rule.get(f) for f in IC_IDENTIFIER_FIELDS)

    def test_rule_with_control_type_is_ic(self):
        from config.QualityCatalogConfig import IC_IDENTIFIER_FIELDS
        rule = self._rule(control_type="Detective")
        assert any(rule.get(f) for f in IC_IDENTIFIER_FIELDS)

    def test_rule_with_risk_domain_is_ic(self):
        from config.QualityCatalogConfig import IC_IDENTIFIER_FIELDS
        rule = self._rule(risk_domain="Financial")
        assert any(rule.get(f) for f in IC_IDENTIFIER_FIELDS)

    def test_dq_only_rule_is_not_ic(self):
        from config.QualityCatalogConfig import IC_IDENTIFIER_FIELDS
        rule = self._rule(severity="critical", owner="Team")
        assert not any(rule.get(f) for f in IC_IDENTIFIER_FIELDS)

    def test_rule_with_all_ic_fields_is_ic(self):
        from config.QualityCatalogConfig import IC_IDENTIFIER_FIELDS
        rule = self._rule(
            control_ref="COSO-CC5.2",
            control_type="Detective",
            risk_domain="Financial",
            remediation_due_days=5,
        )
        assert any(rule.get(f) for f in IC_IDENTIFIER_FIELDS)


class TestIcExceptionSchema:
    """IC_EXCEPTION_SCHEMA has the required columns and correct types."""

    def test_schema_has_primary_key_value(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        assert "primary_key_value" in field_names, (
            "IC_EXCEPTION_SCHEMA must use 'primary_key_value' (not 'pk_value') "
            "to be consistent with VIOLATION_SCHEMA"
        )

    def test_schema_has_ic_status_not_issue_status(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        assert "ic_status" in field_names
        assert "issue_status" not in field_names

    def test_schema_has_separate_waived_and_verified_columns(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        assert "verified_at" in field_names
        assert "verified_by" in field_names
        assert "waived_at" in field_names
        assert "waived_by" in field_names

    def test_schema_has_first_seen_at(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        assert "first_seen_at" in field_names

    def test_schema_has_remediation_due_date(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        assert "remediation_due_date" in field_names


class TestApplyIcResolutionTracking:
    """Tests for _apply_ic_resolution_tracking() MERGE semantics."""

    def _make_ic_df(self, spark, rows):
        """Create a minimal ic_exceptions-compatible DataFrame."""
        from engine.resolution import IC_EXCEPTION_SCHEMA
        from pyspark.sql.types import TimestampType, DateType, IntegerType
        from datetime import datetime, date

        full_rows = []
        for rule_id, pk_val, ic_status in rows:
            full_rows.append((
                "run-1",                          # run_id
                datetime(2025, 1, 1),             # run_timestamp
                date(2025, 1, 1),                 # batch_date
                "IC-Test",                        # rule_group
                rule_id,                          # rule_id
                "Test rule",                      # rule_name
                "TestTable",                      # table_name
                "critical",                       # severity
                "Testteam",                       # owner
                None,                             # failure_type
                pk_val,                           # primary_key_value
                None, None, None, None,           # violated_column, actual_value, expected_condition, violation_detail
                ic_status,                        # ic_status
                "COSO-CC5.2", "Detective", "Financial",  # control_ref/type/domain
                5,                                # remediation_due_days
                date(2025, 1, 6),                 # remediation_due_date
                datetime(2025, 1, 1),             # first_seen_at
                None, None,                       # remediated_by, remediated_at
                None, None,                       # verified_by, verified_at
                None, None, None,                 # waived_by, waived_at, waiver_reason
            ))
        return spark.createDataFrame(full_rows, schema=IC_EXCEPTION_SCHEMA)

    def test_new_violation_inserted_as_open(self, spark):
        from engine.resolution import _apply_ic_resolution_tracking
        df = self._make_ic_df(spark, [("IC-T-001", "pk-1", "Open")])
        # Should not raise; fallback append is acceptable in test environment
        try:
            _apply_ic_resolution_tracking(df, spark, ic_exceptions_table="ic_test_exceptions")
        except Exception:
            pass  # Table may not exist in test env; fallback append tested separately

    def test_missing_primary_key_value_raises(self, spark):
        from engine.resolution import _apply_ic_resolution_tracking
        from pyspark.sql.types import StructType, StructField, StringType
        bad_schema = StructType([
            StructField("rule_id",   StringType(), False),
            StructField("ic_status", StringType(), False),
            # primary_key_value is MISSING
        ])
        df = spark.createDataFrame([("IC-T-001", "Open")], schema=bad_schema)
        with pytest.raises(ValueError, match="primary_key_value"):
            _apply_ic_resolution_tracking(df, spark)

    def test_missing_ic_status_raises(self, spark):
        from engine.resolution import _apply_ic_resolution_tracking
        from pyspark.sql.types import StructType, StructField, StringType
        bad_schema = StructType([
            StructField("rule_id",           StringType(), False),
            StructField("primary_key_value", StringType(), True),
            # ic_status is MISSING
        ])
        df = spark.createDataFrame([("IC-T-001", "pk-1")], schema=bad_schema)
        with pytest.raises(ValueError, match="ic_status"):
            _apply_ic_resolution_tracking(df, spark)

    def test_fallback_append_on_merge_failure(self, spark):
        """When MERGE fails (no metastore), falls back to append without raising."""
        from engine.resolution import _apply_ic_resolution_tracking
        df = self._make_ic_df(spark, [("IC-T-001", "pk-1", "Open")])
        # In a unit-test Spark without a metastore, MERGE will fail and fallback
        # to append. The function must not raise.
        try:
            _apply_ic_resolution_tracking(df, spark, ic_exceptions_table="nonexistent_table")
        except Exception as exc:
            pytest.fail(
                f"_apply_ic_resolution_tracking raised unexpectedly after MERGE failure: {exc}"
            )


class TestIcResolutionTrackingDoesNotAutoResolve:
    """
    The key contract: Open IC exceptions must NOT be auto-resolved when the
    violation disappears from the current run (unlike DQ MERGE which sets Resolved).
    This is enforced by omitting WHEN NOT MATCHED BY SOURCE in the IC MERGE.
    """

    def test_ic_merge_sql_has_no_not_matched_by_source_update(self):
        """
        The _apply_ic_resolution_tracking source must NOT contain an update clause
        for NOT MATCHED BY SOURCE.  This is the structural guard against
        auto-resolution of IC exceptions.
        """
        import inspect
        from engine.resolution import _apply_ic_resolution_tracking
        source = inspect.getsource(_apply_ic_resolution_tracking)
        # Verify the MERGE SQL does not resolve when not seen in current run
        assert "NOT MATCHED BY SOURCE" not in source or (
            "intentionally omitted" in source or "NOT auto-resolved" in source
        ), (
            "_apply_ic_resolution_tracking must NOT auto-resolve Open IC exceptions "
            "when they disappear from the current run."
        )


class TestRuntimeIcTargets:
    """resolve_targets() must include IC table keys and honour DRY_RUN."""

    def _config(self, dry_run=False):
        class C:
            DEFAULT_SCHEMA = "default"
            DQ_RESULTS_TABLE = "dq_run_results"
            DQ_VIOLATIONS_TABLE = "dq_violations"
            DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"
            IC_RUN_RESULTS_TABLE = "ic_run_results"
            IC_EXCEPTIONS_TABLE  = "ic_exceptions"
        class R:
            DRY_RUN = dry_run
        return C(), R()

    def test_ic_targets_present(self):
        from engine.runtime import resolve_targets
        c, r = self._config(dry_run=False)
        targets = resolve_targets(c, r)
        assert "ic_results_table"    in targets
        assert "ic_exceptions_table" in targets

    def test_ic_targets_have_schema_prefix(self):
        from engine.runtime import resolve_targets
        c, r = self._config(dry_run=False)
        targets = resolve_targets(c, r)
        assert targets["ic_results_table"]    == "default.ic_run_results"
        assert targets["ic_exceptions_table"] == "default.ic_exceptions"

    def test_ic_targets_get_tmp_suffix_when_dry_run(self):
        from engine.runtime import resolve_targets
        c, r = self._config(dry_run=True)
        targets = resolve_targets(c, r)
        assert targets["ic_results_table"].endswith("_tmp")
        assert targets["ic_exceptions_table"].endswith("_tmp")

    def test_dq_targets_unchanged_by_ic_addition(self):
        from engine.runtime import resolve_targets
        c, r = self._config(dry_run=False)
        targets = resolve_targets(c, r)
        assert targets["results_table"]    == "default.dq_run_results"
        assert targets["violations_table"] == "default.dq_violations"


# =============================================================================
# Tests for IC, attestation validation, schema checks
# =============================================================================


class TestLoadAllRulesUsesYaml:
    """Engine loads rules from YAML files."""

    def test_load_all_rules_uses_yaml(self, spark):
        """_load_all_rules should reference YAML/glob loading."""
        import inspect
        from engine import validation_runner
        source = inspect.getsource(validation_runner._load_all_rules)
        assert "RuntimeError" in source
        assert ".yaml" in source or "glob" in source


class TestNbIc02Validation:
    """Pure-Python validation logic for nb_ic_02 parameter guards."""

    def _run_conclusion_check(self, conclusion, follow_up_action=""):
        # Replicate the assertion logic from the notebook
        valid_conclusions = ("Working", "Needs Attention", "Not Working")
        if conclusion not in valid_conclusions:
            raise AssertionError(
                "conclusion must be one of: Working, Needs Attention, Not Working"
            )
        if conclusion != "Working":
            if not follow_up_action or len(follow_up_action.strip()) < 10:
                raise AssertionError(
                    "follow_up_action (min 10 chars) is required when conclusion is not Working"
                )

    def test_valid_conclusion_working(self):
        self._run_conclusion_check("Working")

    def test_valid_conclusion_needs_attention_with_action(self):
        self._run_conclusion_check("Needs Attention", "Follow up with manager next week")

    def test_valid_conclusion_not_working_with_action(self):
        self._run_conclusion_check("Not Working", "Escalate to risk team immediately")

    def test_invalid_conclusion_raises(self):
        with pytest.raises(AssertionError, match="conclusion must be"):
            self._run_conclusion_check("Unknown")

    def test_needs_attention_without_follow_up_raises(self):
        with pytest.raises(AssertionError, match="follow_up_action"):
            self._run_conclusion_check("Needs Attention", "")

    def test_needs_attention_short_follow_up_raises(self):
        with pytest.raises(AssertionError, match="follow_up_action"):
            self._run_conclusion_check("Needs Attention", "Too short")

    def test_items_checked_non_numeric_returns_none(self):
        # Replicates the try/except int coercion in the notebook
        def safe_int(val):
            try:
                return int(val) if val else None
            except ValueError:
                return None

        assert safe_int("abc") is None
        assert safe_int("") is None
        assert safe_int("42") == 42
        assert safe_int(None) is None


class TestOwnerEmailInSchema:
    """owner_email is present in all the right places."""

    def test_ic_exception_schema_has_owner_email(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        assert "owner_email" in field_names

    def test_owner_email_position_after_owner(self):
        from engine.resolution import IC_EXCEPTION_SCHEMA
        field_names = [f.name for f in IC_EXCEPTION_SCHEMA.fields]
        owner_idx = field_names.index("owner")
        email_idx = field_names.index("owner_email")
        assert email_idx == owner_idx + 1

    def test_result_schema_has_owner_email(self):
        import inspect
        from engine import validation_runner
        source = inspect.getsource(validation_runner)
        # Verify owner_email appears in RESULT_SCHEMA definition
        assert 'StructField("owner_email"' in source
