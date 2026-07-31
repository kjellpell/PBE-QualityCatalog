"""
event_flow: declared events must occur in order, as whole passes.

The cases in test_worked_examples are the specification, taken verbatim from the
requirement. The rest cover anchors, multi-valued closing events, gating, and
the same-date determinism that ordering by date alone would not give.
"""

from datetime import date

import pytest

from engine.rule_engine import run_rule

FLOW = {
    "event_column": "ev",
    "group_column": "grp",
    "order_column": "d",
    "starts_with": "start",
    "cycle": ["A", "B"],
    "ends_with": "end",
}


def _df(spark, events, group="g1", start_day=1):
    """One row per event, one day apart, in the order given."""
    rows = [(group, ev, date(2024, 1, start_day + i)) for i, ev in enumerate(events)]
    return spark.createDataFrame(rows, "grp string, ev string, d date")


def _run(spark, events, **overrides):
    cfg = {**FLOW, **overrides}
    return run_rule({"event_flow": cfg}, _df(spark, events), spark)


@pytest.mark.parametrize(
    "events,valid,why",
    [
        (["start", "A", "B", "end"], True, "one complete pass"),
        (["start", "A", "B", "A", "B", "end"], True, "two complete passes"),
        (["start", "B", "A", "end"], False, "cycle out of order"),
        (["B", "start", "A", "end"], False, "cycle event before the start anchor"),
        (["start", "A", "B", "A", "end"], False, "trailing A never closes"),
        (["start", "A", "A", "B", "B", "end"], False, "passes must alternate, not batch"),
    ],
)
def test_worked_examples(spark, events, valid, why):
    result, violations = _run(spark, events)
    assert result["status"] == ("PASSED" if valid else "FAILED"), why
    assert result["total_rows"] == 1
    assert result["failed_rows"] == (0 if valid else 1)
    if not valid:
        assert violations.count() == 1


# --------------------------------------------------------------------------
# anchors
# --------------------------------------------------------------------------

def test_anchors_are_optional(spark):
    """A bare cycle with no anchors is how a plain pair check is expressed."""
    result, _ = _run(spark, ["A", "B"], starts_with=None, ends_with=None)
    assert result["status"] == "PASSED"


def test_cycle_without_anchors_still_requires_whole_passes(spark):
    result, _ = _run(spark, ["A"], starts_with=None, ends_with=None)
    assert result["status"] == "FAILED"


def test_repeated_start_is_a_violation(spark):
    result, _ = _run(spark, ["start", "start", "A", "B", "end"])
    assert result["status"] == "FAILED"


def test_repeated_end_is_a_violation(spark):
    result, _ = _run(spark, ["start", "A", "B", "end", "end"])
    assert result["status"] == "FAILED"


def test_either_closing_event_may_end_the_flow(spark):
    for closing in ("done", "withdrawn"):
        result, _ = _run(
            spark, ["start", "A", "B", closing], ends_with=["done", "withdrawn"]
        )
        assert result["status"] == "PASSED", closing


def test_two_different_closing_events_is_a_violation(spark):
    result, _ = _run(
        spark, ["start", "A", "B", "done", "withdrawn"], ends_with=["done", "withdrawn"]
    )
    assert result["status"] == "FAILED"


# --------------------------------------------------------------------------
# what it deliberately ignores
# --------------------------------------------------------------------------

def test_unlisted_events_are_ignored(spark):
    result, _ = _run(spark, ["start", "noise", "A", "other", "B", "end"])
    assert result["status"] == "PASSED"


def test_group_with_no_listed_events_passes(spark):
    """Zero passes is valid — this is what 'both absent is fine' reduces to."""
    result, _ = _run(spark, ["noise", "other"])
    assert result["status"] == "PASSED"
    assert result["failed_rows"] == 0


def test_single_event_cycle_allows_any_number_of_repeats(spark):
    result, _ = _run(spark, ["start", "A", "A", "A", "end"], cycle=["A"])
    assert result["status"] == "PASSED"


def test_null_group_key_is_neither_counted_nor_reported(spark):
    rows = [
        ("g1", "start", date(2024, 1, 1)),
        ("g1", "A", date(2024, 1, 2)),
        ("g1", "B", date(2024, 1, 3)),
        ("g2", "A", date(2024, 1, 2)),          # unclosed pass -> fails
        (None, "A", date(2024, 1, 2)),          # not a group
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    result, violations = run_rule({"event_flow": {**FLOW, "ends_with": None}}, df, spark)

    assert result["total_rows"] == 2
    keys = [r.primary_key_value for r in violations.collect()]
    assert None not in keys
    assert keys == ["g2"]


# --------------------------------------------------------------------------
# determinism
# --------------------------------------------------------------------------

@pytest.mark.parametrize("reverse", [False, True])
def test_same_date_events_are_read_in_declared_order(spark, reverse):
    """
    Ordering by date alone leaves same-day events arbitrary. The rank tiebreaker
    makes the result independent of row arrival order — assert both orders.
    """
    same_day = date(2024, 1, 1)
    rows = [
        ("g1", "start", same_day),
        ("g1", "A", same_day),
        ("g1", "B", same_day),
        ("g1", "end", same_day),
    ]
    if reverse:
        rows = list(reversed(rows))
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    result, _ = run_rule({"event_flow": FLOW}, df, spark)
    assert result["status"] == "PASSED"


# --------------------------------------------------------------------------
# gating
# --------------------------------------------------------------------------

def test_gate_scopes_which_groups_are_evaluated(spark):
    rows = [
        ("gated", "opened", date(2024, 1, 1)),
        ("gated", "A", date(2024, 1, 2)),        # unclosed pass -> should fail
        ("ungated", "A", date(2024, 1, 2)),      # same problem, but out of scope
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    rule = {"event_flow": {
        **FLOW, "starts_with": None, "ends_with": None,
        "completion_gate": {"event_column": "ev", "value": "opened", "order_column": "d"},
    }}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 1
    assert [r.primary_key_value for r in violations.collect()] == ["gated"]


def test_gate_accepts_several_values(spark):
    rows = [
        ("g1", "opened_a", date(2024, 1, 1)), ("g1", "A", date(2024, 1, 2)),
        ("g2", "opened_b", date(2024, 1, 1)), ("g2", "A", date(2024, 1, 2)),
        ("g3", "irrelevant", date(2024, 1, 1)), ("g3", "A", date(2024, 1, 2)),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    rule = {"event_flow": {
        **FLOW, "starts_with": None, "ends_with": None,
        "completion_gate": {
            "event_column": "ev", "value": ["opened_a", "opened_b"], "order_column": "d",
        },
    }}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 2      # g3 is out of scope
    assert sorted(r.primary_key_value for r in violations.collect()) == ["g1", "g2"]


# --------------------------------------------------------------------------
# configuration errors
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "cfg,fragment",
    [
        ({"cycle": []}, "at least one event"),
        ({"cycle": ["A", "A"]}, "repeats an event"),
        ({"starts_with": ["a", "b"]}, "single event"),
        ({"cycle": ["A", "start"]}, "both an anchor and part of the cycle"),
    ],
)
def test_configuration_errors(spark, cfg, fragment):
    result, _ = _run(spark, ["start", "A", "B", "end"], **cfg)
    assert result["status"] == "ERROR"
    assert fragment in result["details"]
