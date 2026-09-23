"""
QC_EventFlow.check_event_flow: the same event_flow specification as
tests/test_event_flow.py, checked against the hand-written re-implementation
instead of QC_Engine's window/rank version. Cases are mirrored from that file
so the two implementations are held to the same behaviour.
"""

from datetime import date

import pytest

from tests.notebook_source import load_notebook

check_event_flow = load_notebook("QC_EventFlow").check_event_flow

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
    return check_event_flow(_df(spark, events), **cfg)


@pytest.mark.parametrize(
    "events,valid,why",
    [
        (["start", "A", "B", "end"], True, "one complete pass"),
        (["start", "A", "A", "B", "end"], True, "two A's in a row, closed by the one B"),
        (["start", "A", "B", "A", "B", "end"], True, "two complete passes"),
        (["start", "B", "A", "end"], False, "cycle out of order"),
        (["B", "start", "A", "end"], False, "cycle event before the start anchor"),
        (["start", "A", "B", "A", "end"], False, "trailing A never closes"),
        (["start", "A", "A", "B", "B", "end"], False, "the second B has nothing left to close"),
    ],
)
def test_worked_examples(spark, events, valid, why):
    violations = _run(spark, events)
    assert (violations.count() == 0) == valid, why


def test_repeated_closer_is_a_violation(spark):
    violations = _run(spark, ["start", "A", "B", "B", "end"])
    assert violations.count() == 1


def test_anchors_are_optional(spark):
    violations = _run(spark, ["A", "B"], starts_with=None, ends_with=None)
    assert violations.count() == 0


def test_cycle_without_anchors_still_requires_whole_passes(spark):
    violations = _run(spark, ["A"], starts_with=None, ends_with=None)
    assert violations.count() == 1


def test_repeated_start_is_a_violation(spark):
    violations = _run(spark, ["start", "start", "A", "B", "end"])
    assert violations.count() == 1


def test_repeated_end_is_a_violation(spark):
    violations = _run(spark, ["start", "A", "B", "end", "end"])
    assert violations.count() == 1


def test_either_closing_event_may_end_the_flow(spark):
    for closing in ("done", "withdrawn"):
        violations = _run(spark, ["start", "A", "B", closing], ends_with=["done", "withdrawn"])
        assert violations.count() == 0, closing


def test_two_different_closing_events_is_a_violation(spark):
    violations = _run(
        spark, ["start", "A", "B", "done", "withdrawn"], ends_with=["done", "withdrawn"]
    )
    assert violations.count() == 1


def test_unlisted_events_are_ignored(spark):
    violations = _run(spark, ["start", "noise", "A", "other", "B", "end"])
    assert violations.count() == 0


def test_group_with_no_listed_events_passes(spark):
    violations = _run(spark, ["noise", "other"])
    assert violations.count() == 0


def test_single_event_cycle_allows_any_number_of_repeats(spark):
    violations = _run(spark, ["start", "A", "A", "A", "end"], cycle=["A"])
    assert violations.count() == 0


def test_null_group_key_is_neither_counted_nor_reported(spark):
    rows = [
        ("g1", "start", date(2024, 1, 1)),
        ("g1", "A", date(2024, 1, 2)),
        ("g1", "B", date(2024, 1, 3)),
        ("g2", "A", date(2024, 1, 2)),          # unclosed pass -> fails
        (None, "A", date(2024, 1, 2)),          # not a group
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    violations = check_event_flow(df, **{**FLOW, "ends_with": None})

    keys = [r.primaernoekkel_verdi for r in violations.collect()]
    assert None not in keys
    assert keys == ["g2"]


@pytest.mark.parametrize("reverse", [False, True])
def test_same_date_events_are_read_in_declared_order(spark, reverse):
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
    violations = check_event_flow(df, **FLOW)
    assert violations.count() == 0


def test_gate_scopes_which_groups_are_evaluated(spark):
    rows = [
        ("gated", "opened", date(2024, 1, 1)),
        ("gated", "A", date(2024, 1, 2)),        # unclosed pass -> should fail
        ("ungated", "A", date(2024, 1, 2)),      # same problem, but out of scope
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    violations = check_event_flow(df, **{
        **FLOW, "starts_with": None, "ends_with": None,
        "completion_gate": {"event_column": "ev", "value": "opened", "order_column": "d"},
    })
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["gated"]


def test_gate_falls_back_to_ends_with_when_never_reached(spark):
    rows = [
        ("no_gate_but_closed", "A", date(2024, 1, 1)),   # unclosed pass
        ("no_gate_but_closed", "end", date(2024, 1, 2)),
        ("still_open", "A", date(2024, 1, 1)),           # never closes, no gate either
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    violations = check_event_flow(df, **{
        **FLOW, "starts_with": None,
        "completion_gate": {"event_column": "ev", "value": "opened", "order_column": "d"},
    })
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["no_gate_but_closed"]


def test_gate_accepts_several_values(spark):
    rows = [
        ("g1", "opened_a", date(2024, 1, 1)), ("g1", "A", date(2024, 1, 2)),
        ("g2", "opened_b", date(2024, 1, 1)), ("g2", "A", date(2024, 1, 2)),
        ("g3", "irrelevant", date(2024, 1, 1)), ("g3", "A", date(2024, 1, 2)),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    violations = check_event_flow(df, **{
        **FLOW, "starts_with": None, "ends_with": None,
        "completion_gate": {
            "event_column": "ev", "value": ["opened_a", "opened_b"], "order_column": "d",
        },
    })
    assert sorted(r.primaernoekkel_verdi for r in violations.collect()) == ["g1", "g2"]


def test_ends_with_alone_does_not_act_as_a_gate(spark):
    rows = [
        ("g1", "A", date(2024, 1, 1)),           # unclosed pass, never reaches end
        ("g2", "A", date(2024, 1, 1)), ("g2", "B", date(2024, 1, 2)),
        ("g2", "end", date(2024, 1, 3)),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    violations = check_event_flow(df, **{**FLOW, "starts_with": None})
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["g1"]


def test_identifier_column_is_looked_up_per_group(spark):
    rows = [
        ("g1", "A", date(2024, 1, 1), "SAK-1"),   # unclosed pass
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date, saksnummer string")
    violations = check_event_flow(
        df, **{**FLOW, "starts_with": None, "ends_with": None}, identifier_column="saksnummer"
    )
    row = violations.collect()[0]
    assert row.identifikator_verdi == "SAK-1"


def test_violation_columns_match_the_avvik_shape(spark):
    violations = _run(spark, ["start", "B", "A", "end"])
    assert violations.columns == [
        "primaernoekkel_verdi", "identifikator_verdi", "avvikende_kolonne",
        "faktisk_verdi", "forventet_betingelse", "avviksdetaljer",
    ]


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
    with pytest.raises(ValueError, match=fragment):
        _run(spark, ["start", "A", "B", "end"], **cfg)
