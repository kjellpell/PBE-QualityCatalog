"""
Golden tests for the 6 actively-used expectation types (per rules/*.yaml usage:
comparison x6, not_null x4, pairs_present x3, not_null_when x3,
group_aggregate_matches x1, combination_unique x1), plus the two other classes
touched by the A3 output-contract fix (sql_violations, combination_unique).

Each test builds a small synthetic DataFrame with both a passing and a failing
case and asserts on the result dict and the violations DataFrame's
primary_key_value / violated_column / actual_value / expected_condition.
"""

from engine.expectations import (
    ColumnComparisonExpectation,
    ExpectColumnValuesToNotBeNullExpectation,
    SqlValidationExpectation,
    UniqueColumnCombinationExpectation,
    ValidateGroupAggregateMatchExpectation,
    ValidateNotNullWhenExpectation,
    ValidatePairedPresenceExpectation,
)


def _rule(parameters):
    return {"parameters": parameters}


def test_not_null(spark):
    df = spark.createDataFrame(
        [(1, "a", "x"), (2, None, "y"), (3, "c", None)],
        ["id", "a", "b"],
    )
    rule = _rule({"columns": ["a", "b"], "pk_column": "id"})
    result, violations = ExpectColumnValuesToNotBeNullExpectation().validate(df, rule, spark)

    assert result["total_rows"] == 6  # 3 rows x 2 columns
    assert result["failed_rows"] == 2
    assert result["passed_rows"] == 4
    assert result["status"] == "FAILED"

    rows = {(r.primary_key_value, r.violated_column) for r in violations.collect()}
    assert rows == {("2", "a"), ("3", "b")}
    for r in violations.collect():
        assert r.actual_value is None
        assert r.expected_condition == "NOT NULL"


def test_comparison(spark):
    df = spark.createDataFrame(
        [(1, 10, 5), (2, 3, 5)],
        ["id", "a", "b"],
    )
    rule = _rule({
        "left_column": "a",
        "right_column": "b",
        "operator": ">=",
        "pk_column": "id",
    })
    result, violations = ColumnComparisonExpectation().validate(df, rule, spark)

    assert result["total_rows"] == 2
    assert result["failed_rows"] == 1
    assert result["passed_rows"] == 1
    assert result["status"] == "FAILED"

    rows = violations.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.primary_key_value == "2"
    # A3 fix: violated_column must be the left column name, not the expression.
    assert row.violated_column == "a"
    # A3 fix: actual_value must be the left column's own value, not "3 >= 5".
    assert row.actual_value == "3"
    # expected_condition still carries the full expression.
    assert row.expected_condition == "a >= b"


def test_comparison_scalar_mode_unaffected(spark):
    df = spark.createDataFrame([(1, 10), (2, -1)], ["id", "a"])
    rule = _rule({"left_column": "a", "right_value": 0, "operator": ">=", "pk_column": "id"})
    result, violations = ColumnComparisonExpectation().validate(df, rule, spark)

    assert result["failed_rows"] == 1
    row = violations.collect()[0]
    assert row.violated_column == "a"
    assert row.actual_value == "-1"
    assert row.expected_condition == "a >= 0.0"


def test_pairs_present(spark):
    df = spark.createDataFrame(
        [
            ("case-1", "start"),
            ("case-1", "stop"),
            ("case-2", "start"),
        ],
        ["case_id", "milestone"],
    )
    rule = _rule({
        "event_column": "milestone",
        "group_column": "case_id",
        "required_pairs": [["start", "stop"]],
    })
    result, violations = ValidatePairedPresenceExpectation().validate(df, rule, spark)

    # Known latent bug (documented, deliberately out of scope for Track A):
    # total_rows here is a row count (3) while failed_rows is a group count (1)
    # -- mixed counting units. Fixing this is deferred to Track B's unit-scoped
    # counting rework. This assertion pins today's actual behavior so a future
    # fix shows up as an intentional, visible test change rather than a silent
    # regression.
    assert result["total_rows"] == 3
    assert result["failed_rows"] == 1
    assert result["passed_rows"] == 2
    assert result["status"] == "FAILED"

    rows = violations.collect()
    assert len(rows) == 1
    assert rows[0].primary_key_value == "case-2"
    assert rows[0].violated_column == "milestone"


def test_not_null_when(spark):
    df = spark.createDataFrame(
        [
            (1, "OPEN", None),
            (2, "OPEN", "alice"),
            (3, "CLOSED", None),
        ],
        ["id", "status", "owner"],
    )
    rule = _rule({
        "when_column": "status",
        "operator": "==",
        "value": "OPEN",
        "columns": ["owner"],
        "pk_column": "id",
    })
    result, violations = ValidateNotNullWhenExpectation().validate(df, rule, spark)

    assert result["total_rows"] == 2  # only the 2 OPEN rows are evaluated
    assert result["failed_rows"] == 1
    assert result["passed_rows"] == 1
    assert result["status"] == "FAILED"

    rows = violations.collect()
    assert len(rows) == 1
    assert rows[0].primary_key_value == "1"
    assert rows[0].violated_column == "owner"


def test_group_aggregate_matches(spark):
    df = spark.createDataFrame(
        [
            ("inv-1", 60, 100),
            ("inv-1", 40, 100),
            ("inv-2", 60, 100),
            ("inv-2", 30, 100),
        ],
        ["invoice_id", "line_amount", "invoice_total"],
    )
    rule = _rule({
        "group_column": "invoice_id",
        "aggregate_column": "line_amount",
        "reference_column": "invoice_total",
        "aggregate": "sum",
        "tolerance": 0.01,
    })
    result, violations = ValidateGroupAggregateMatchExpectation().validate(df, rule, spark)

    assert result["total_rows"] == 2  # 2 groups
    assert result["failed_rows"] == 1
    assert result["passed_rows"] == 1
    assert result["status"] == "FAILED"

    rows = violations.collect()
    assert len(rows) == 1
    assert rows[0].primary_key_value == "inv-2"
    assert rows[0].violated_column == "line_amount"


def test_combination_unique(spark):
    df = spark.createDataFrame(
        [(1, "a", "x"), (2, "a", "x"), (3, "b", "y")],
        ["id", "a", "b"],
    )
    rule = _rule({"columns": ["a", "b"], "pk_column": "id"})
    result, violations = UniqueColumnCombinationExpectation().validate(df, rule, spark)

    assert result["failed_rows"] == 2
    assert result["status"] == "FAILED"

    rows = violations.collect()
    assert len(rows) == 2
    # A3 fix: violated_column is the first key column, not "a, b".
    assert {r.violated_column for r in rows} == {"a"}
    assert rows[0].expected_condition == "UNIQUE(a, b)"


def test_sql_violations_column_is_null(spark):
    df = spark.createDataFrame([(1, "bad")], ["id", "flag"])
    df.createOrReplaceTempView("sql_violations_fixture")
    rule = _rule({"sql": "SELECT id FROM sql_violations_fixture WHERE flag = 'bad'"})
    result, violations = SqlValidationExpectation().validate(df, rule, spark)

    assert result["status"] == "FAILED"
    row = violations.collect()[0]
    # A3 fix: violated_column is NULL (filterable/honest), not the literal
    # "SQL_VALIDATION" string.
    assert row.violated_column is None
