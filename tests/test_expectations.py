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
            StructField("id",  StringType(),  True),
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
# MilestoneNoDuplicatesExpectation
# ---------------------------------------------------------------------------

class TestMilestoneNoDuplicatesExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("prosess_id", StringType(), True),
            StructField("Milepel",    StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self):
        return {
            "rule_id": "T-005",
            "name": "test",
            "expectation": "expect_no_duplicate_milestones",
            "parameters": {
                "milestone_column": "Milepel",
                "group_column": "prosess_id",
            },
        }

    def test_no_duplicates(self, spark):
        from engine.expectations import MilestoneNoDuplicatesExpectation
        df = self._make_df(spark, [
            ("P1", "Start"), ("P1", "Stop"), ("P2", "Start"),
        ])
        result, viols = MilestoneNoDuplicatesExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_duplicates_detected(self, spark):
        from engine.expectations import MilestoneNoDuplicatesExpectation
        df = self._make_df(spark, [
            ("P1", "Start"), ("P1", "Start"), ("P1", "Stop"),
        ])
        result, viols = MilestoneNoDuplicatesExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_empty_df(self, spark):
        from engine.expectations import MilestoneNoDuplicatesExpectation
        schema = StructType([
            StructField("prosess_id", StringType(), True),
            StructField("Milepel",    StringType(), True),
        ])
        df = spark.createDataFrame([], schema)
        result, viols = MilestoneNoDuplicatesExpectation().validate(df, self._rule(), spark)
        assert result["status"] == "PASSED"


# ---------------------------------------------------------------------------
# InvoiceRefundValidationExpectation
# ---------------------------------------------------------------------------

class TestInvoiceRefundValidationExpectation:
    def _make_df(self, spark, rows):
        schema = StructType([
            StructField("Fakturanr",    StringType(), True),
            StructField("linje_belop",  DoubleType(), True),
            StructField("Faktura_type", StringType(), True),
        ])
        return spark.createDataFrame(rows, schema)

    def _rule(self):
        return {
            "rule_id": "T-006",
            "name": "test",
            "expectation": "expect_refund_validation",
            "parameters": {
                "amount_column": "linje_belop",
                "type_column": "Faktura_type",
                "credit_type": "Kreditnota",
                "pk_column": "Fakturanr",
            },
        }

    def test_negative_on_credit_note_passes(self, spark):
        from engine.expectations import InvoiceRefundValidationExpectation
        df = self._make_df(spark, [("INV1", -100.0, "Kreditnota")])
        result, viols = InvoiceRefundValidationExpectation().validate(
            df, self._rule(), spark
        )
        assert result["status"] == "PASSED"
        assert viols.count() == 0

    def test_negative_on_non_credit_fails(self, spark):
        from engine.expectations import InvoiceRefundValidationExpectation
        df = self._make_df(spark, [("INV1", -100.0, "Standard")])
        result, viols = InvoiceRefundValidationExpectation().validate(
            df, self._rule(), spark
        )
        assert result["status"] == "FAILED"
        assert viols.count() == 1

    def test_no_negatives_passes(self, spark):
        from engine.expectations import InvoiceRefundValidationExpectation
        df = self._make_df(spark, [("INV1", 100.0, "Standard")])
        result, viols = InvoiceRefundValidationExpectation().validate(
            df, self._rule(), spark
        )
        assert result["status"] == "PASSED"


# ---------------------------------------------------------------------------
# Registry completeness
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_all_expected_keys_present(self):
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        expected_keys = [
            "validate_column_comparison",
            "sql_validation",
            "sql",
            "expect_column_sum_to_equal",
            "expect_row_count_to_be_between",
            "expect_unique_combination_of_columns",
            "validate_foreign_key",
            "expect_milestone_order",
            "expect_milestone_pairs",
            "expect_no_open_milestone_pairs",
            "expect_no_duplicate_milestones",
            "expect_milestone_sequence",
            "expect_milestone_pairs_complete",
            "expect_no_orphan_milestones",
            "expect_refund_validation",
            "expect_invoice_total_consistency",
        ]
        for key in expected_keys:
            assert key in CUSTOM_EXPECTATION_REGISTRY, f"Missing key: {key}"

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
