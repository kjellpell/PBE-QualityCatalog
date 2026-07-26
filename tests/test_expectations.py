"""
Rule-type tests.

Covers the predicate semantics that the whole format rests on (NULL handling,
subject-column derivation), the scope-unit counting that fixes the mixed-units
bug, and each structured rule type.
"""

import pytest

from engine.expectations import (
    GROUP_SCOPED,
    RULE_TYPES,
    RuleConfigError,
    detect_rule_type,
    predicate_columns,
    run_rule,
)


# --------------------------------------------------------------------------
# check:
# --------------------------------------------------------------------------

def test_check_counts_only_evaluable_rows(spark):
    """
    A NULL predicate means "not evaluable": those rows are outside both the
    violations and the denominator. This mirrors a SQL CHECK constraint, which
    is satisfied unless the predicate is FALSE.
    """
    df = spark.createDataFrame(
        [(1, 10, 5), (2, 3, 5), (3, None, 5), (4, 7, None)], "id int, a int, b int"
    )
    result, violations = run_rule({"check": "a >= b"}, df, spark, pk_column="id")

    assert result["total_rows"] == 2      # rows 3 and 4 are unevaluable
    assert result["failed_rows"] == 1
    assert result["passed_rows"] == 1
    assert result["status"] == "FAILED"

    row = violations.collect()[0]
    assert row.primary_key_value == "2"
    assert row.violated_column == "a"     # subject = first referenced column
    assert row.actual_value == "3"
    assert row.expected_condition == "a >= b"


def test_check_is_not_null_evaluates_every_row(spark):
    df = spark.createDataFrame([(1, "x"), (2, None)], "id int, a string")
    result, violations = run_rule({"check": "a IS NOT NULL"}, df, spark, pk_column="id")

    assert (result["total_rows"], result["failed_rows"]) == (2, 1)
    assert violations.collect()[0].violated_column == "a"


def test_check_passes(spark):
    df = spark.createDataFrame([(1, 10), (2, 20)], "id int, a int")
    result, violations = run_rule({"check": "a >= 0"}, df, spark, pk_column="id")

    assert result["status"] == "PASSED"
    assert (result["total_rows"], result["failed_rows"]) == (2, 0)
    assert violations.count() == 0
    assert result["success_pct"] == 100.0


def test_when_narrows_the_denominator(spark):
    """`when:` must narrow what is evaluated, not merely mask violations."""
    df = spark.createDataFrame(
        [(1, "OPEN", None), (2, "OPEN", "alice"), (3, "CLOSED", None)],
        "id int, status string, owner string",
    )
    rule = {"when": "status = 'OPEN'", "check": "owner IS NOT NULL"}
    result, violations = run_rule(rule, df, spark, pk_column="id")

    assert result["total_rows"] == 2      # the CLOSED row is out of scope
    assert result["failed_rows"] == 1
    assert violations.collect()[0].primary_key_value == "1"


def test_when_excludes_rows_where_it_is_null(spark):
    df = spark.createDataFrame([(1, None, None)], "id int, status string, owner string")
    rule = {"when": "status = 'OPEN'", "check": "owner IS NOT NULL"}
    result, _ = run_rule(rule, df, spark, pk_column="id")

    assert result["total_rows"] == 0
    assert result["status"] == "PASSED"


# --------------------------------------------------------------------------
# predicate column derivation
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "expression,expected",
    [
        ("a >= b", ["a", "b"]),
        ("b >= a", ["b", "a"]),          # source order, not alphabetical
        ("stopdato IS NULL", ["stopdato"]),
        ("indikator IN ('x','y')", ["indikator"]),
        ("datediff(current_date(), startdato) <= 90", ["startdato"]),
        ("1 = 1", []),
    ],
)
def test_predicate_columns_preserves_source_order(spark, expression, expected):
    assert predicate_columns(expression) == expected


def test_check_without_a_column_subject_reports_null(spark):
    df = spark.createDataFrame([(1,)], "id int")
    result, violations = run_rule({"check": "1 = 0"}, df, spark, pk_column="id")

    assert result["failed_rows"] == 1
    assert violations.collect()[0].violated_column is None


# --------------------------------------------------------------------------
# structured rule types
# --------------------------------------------------------------------------

def test_unique(spark):
    df = spark.createDataFrame(
        [(1, "a", "x"), (2, "a", "x"), (3, "b", "y")], "id int, a string, b string"
    )
    result, violations = run_rule({"unique": ["a", "b"]}, df, spark, pk_column="id")

    assert (result["total_rows"], result["failed_rows"]) == (3, 2)
    assert {r.violated_column for r in violations.collect()} == {"a"}
    assert violations.collect()[0].expected_condition == "UNIQUE(a, b)"


def test_pairs_present_counts_groups_not_rows(spark):
    """
    The mixed-units fix: a group failing several pairs counts once, and the
    denominator is groups. Previously total_rows was a row count while
    failed_rows was a group count, which could make passed_rows negative.
    """
    rows = [
        ("g1", "start"), ("g1", "stop"), ("g1", "begin"), ("g1", "end"),  # satisfies both
        ("g2", "start"), ("g2", "begin"),                                 # fails both
    ]
    df = spark.createDataFrame(rows, "grp string, ev string")
    rule = {
        "pairs_present": {
            "event_column": "ev",
            "group_column": "grp",
            "required_pairs": [["start", "stop"], ["begin", "end"]],
        }
    }
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 2       # groups, not the 6 rows
    assert result["failed_rows"] == 1      # g2 fails two pairs but counts once
    assert result["passed_rows"] == 1      # never negative
    assert violations.count() == 2         # detail still lists both failing pairs
    assert {r.primary_key_value for r in violations.collect()} == {"g2"}


def test_gate_complete_counts_groups(spark):
    df = spark.createDataFrame(
        [("g1", "approved"), ("g1", "other"), ("g2", "other")], "grp string, ev string"
    )
    rule = {"gate_complete": {"event_column": "ev", "group_column": "grp", "value": "approved"}}
    result, violations = run_rule(rule, df, spark)

    assert (result["total_rows"], result["failed_rows"]) == (2, 1)
    assert violations.collect()[0].primary_key_value == "g2"


def test_group_aggregate_matches(spark):
    df = spark.createDataFrame(
        [("i1", 60.0, 100.0), ("i1", 40.0, 100.0), ("i2", 60.0, 100.0), ("i2", 30.0, 100.0)],
        "inv string, amount double, total double",
    )
    rule = {
        "group_aggregate_matches": {
            "group_column": "inv",
            "aggregate_column": "amount",
            "reference_column": "total",
        }
    }
    result, violations = run_rule(rule, df, spark)

    assert (result["total_rows"], result["failed_rows"]) == (2, 1)
    assert violations.collect()[0].primary_key_value == "i2"


def test_sequence_ordered_counts_groups(spark):
    rows = [
        ("g1", "a", 1), ("g1", "b", 2),      # in order
        ("g2", "b", 1), ("g2", "a", 2),      # reversed
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, seq int")
    rule = {
        "sequence_ordered": {
            "event_column": "ev", "group_column": "grp",
            "order_column": "seq", "sequence": ["a", "b"],
        }
    }
    result, violations = run_rule(rule, df, spark)

    assert (result["total_rows"], result["failed_rows"]) == (2, 1)
    assert violations.collect()[0].primary_key_value == "g2"


def test_exists_in(spark):
    spark.createDataFrame([("K1", "Y"), ("K2", "N")], "code string, active string") \
        .createOrReplaceTempView("exists_in_ref")
    df = spark.createDataFrame([(1, "K1"), (2, "K9")], "id int, code string")

    rule = {"exists_in": {"column": "code", "table": "exists_in_ref", "reference_column": "code"}}
    result, violations = run_rule(rule, df, spark, pk_column="id")

    assert (result["total_rows"], result["failed_rows"]) == (2, 1)
    assert violations.collect()[0].actual_value == "K9"


def test_exists_in_with_active_filter(spark):
    spark.createDataFrame([("K1", "Y"), ("K2", "N")], "code string, active string") \
        .createOrReplaceTempView("exists_in_ref2")
    df = spark.createDataFrame([(1, "K1"), (2, "K2")], "id int, code string")

    rule = {
        "exists_in": {
            "column": "code", "table": "exists_in_ref2", "reference_column": "code",
            "active_column": "active", "active_value": "Y",
        }
    }
    result, violations = run_rule(rule, df, spark, pk_column="id")

    assert result["failed_rows"] == 1      # K2 exists but is inactive
    assert violations.collect()[0].actual_value == "K2"


def test_row_count(spark):
    df = spark.createDataFrame([(1,), (2,)], "id int")

    passing, _ = run_rule({"row_count": {"threshold": 2}}, df, spark)
    assert passing["status"] == "PASSED"

    failing, violations = run_rule({"row_count": {"threshold": 5}}, df, spark)
    assert failing["status"] == "FAILED"
    assert failing["failed_rows"] == 2     # whole table fails
    assert violations.count() == 0         # nothing row-level to point at


def test_sql(spark):
    spark.createDataFrame([(1, "bad"), (2, "ok")], "id int, flag string") \
        .createOrReplaceTempView("sql_fixture")
    df = spark.createDataFrame([(1,)], "id int")

    rule = {"sql": {"query": "SELECT id FROM sql_fixture WHERE flag = 'bad'", "pk_column": "id"}}
    result, violations = run_rule(rule, df, spark)

    assert result["status"] == "FAILED"
    row = violations.collect()[0]
    assert row.primary_key_value == "1"
    assert row.violated_column is None     # no single offending column


# --------------------------------------------------------------------------
# configuration errors surface as ERROR, not as a crash
# --------------------------------------------------------------------------

def test_unknown_rule_type_is_an_error(spark):
    df = spark.createDataFrame([(1,)], "id int")
    result, violations = run_rule({"not_null": ["a"]}, df, spark, pk_column="id")

    assert result["status"] == "ERROR"
    assert "No rule type found" in result["details"]
    assert violations.count() == 0


def test_two_rule_types_is_an_error(spark):
    df = spark.createDataFrame([(1,)], "id int")
    result, _ = run_rule({"check": "id > 0", "unique": ["id"]}, df, spark, pk_column="id")

    assert result["status"] == "ERROR"
    assert "more than one rule type" in result["details"]


def test_missing_primary_key_is_an_error(spark):
    df = spark.createDataFrame([(1,)], "id int")
    result, _ = run_rule({"check": "id > 0"}, df, spark, pk_column=None)

    assert result["status"] == "ERROR"
    assert "primary key" in result["details"]


def test_unknown_column_is_an_error(spark):
    df = spark.createDataFrame([(1,)], "id int")
    result, _ = run_rule({"check": "id > 0"}, df, spark, pk_column="nope")

    assert result["status"] == "ERROR"
    assert "not found" in result["details"]


def test_rule_level_pk_overrides_catalog(spark):
    df = spark.createDataFrame([(1, 9, None)], "id int, other int, a string")
    _, violations = run_rule(
        {"check": "a IS NOT NULL", "pk_column": "other"}, df, spark, pk_column="id"
    )
    assert violations.collect()[0].primary_key_value == "9"


# --------------------------------------------------------------------------
# NULL group keys
# --------------------------------------------------------------------------

def _null_group_case(name):
    """(rule, rows, schema) per group-scoped type: two real groups, one failing,
    plus rows carrying a NULL group key."""
    if name == "gate_complete":
        return (
            {"gate_complete": {"event_column": "ev", "group_column": "grp", "value": "ok"}},
            [("g1", "ok"), ("g2", "x"), (None, "x"), (None, "ok")],
            "grp string, ev string",
        )
    if name == "pairs_present":
        return (
            {"pairs_present": {
                "event_column": "ev", "group_column": "grp",
                "required_pairs": [["start", "stop"]],
            }},
            [("g1", "start"), ("g1", "stop"), ("g2", "start"), (None, "start")],
            "grp string, ev string",
        )
    if name == "sequence_ordered":
        return (
            {"sequence_ordered": {
                "event_column": "ev", "group_column": "grp",
                "order_column": "seq", "sequence": ["a", "b"],
            }},
            [("g1", "a", 1), ("g1", "b", 2), ("g2", "b", 1), ("g2", "a", 2),
             (None, "b", 1), (None, "a", 2)],
            "grp string, ev string, seq int",
        )
    return (
        {"group_aggregate_matches": {
            "group_column": "grp", "aggregate_column": "amount", "reference_column": "total",
        }},
        [("g1", 60.0, 100.0), ("g1", 40.0, 100.0),
         ("g2", 60.0, 100.0), ("g2", 30.0, 100.0),
         (None, 5.0, 999.0)],
        "grp string, amount double, total double",
    )


@pytest.mark.parametrize("type_name", sorted(GROUP_SCOPED))
def test_null_group_key_is_neither_counted_nor_reported(spark, type_name):
    """
    A NULL group key is not a group. Counting it inflates the denominator, and
    reporting it emits a violation whose primary_key_value cannot be joined back
    to anything. gate_complete used to do the latter and sequence_ordered the
    former, while the other two already excluded NULLs.
    """
    rule, rows, schema = _null_group_case(type_name)
    df = spark.createDataFrame(rows, schema)
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 2, "NULL group key counted as a group"
    assert result["failed_rows"] == 1
    assert result["passed_rows"] == 1
    keys = [r.primary_key_value for r in violations.collect()]
    assert None not in keys, "violation emitted for a NULL group key"
    assert keys == ["g2"]


# --------------------------------------------------------------------------
# registry invariants
# --------------------------------------------------------------------------

def test_group_scoped_is_derived_from_the_registry(spark):
    assert GROUP_SCOPED == {
        "sequence_ordered", "pairs_present", "gate_complete", "group_aggregate_matches"
    }
    assert all(RULE_TYPES[name].scope == "group" for name in GROUP_SCOPED)


def test_detect_rule_type(spark):
    assert detect_rule_type({"rule_id": "X", "name": "n", "check": "a > 0"}) == "check"
    with pytest.raises(RuleConfigError):
        detect_rule_type({"rule_id": "X", "name": "n"})
