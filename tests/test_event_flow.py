"""
event_flow: declared events must occur in order, as whole passes.

The cases in test_worked_examples are the specification, taken verbatim from the
requirement. The rest cover anchors, multi-valued closing events, gating, and
the same-date determinism that ordering by date alone would not give.
"""

from datetime import date

import pytest

from tests.notebook_source import engine_namespace

run_rule = engine_namespace().run_rule

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
        (["start", "A", "A", "B", "end"], True, "two A's in a row, closed by the one B"),
        (["start", "A", "B", "A", "B", "end"], True, "two complete passes"),
        (["start", "B", "A", "end"], False, "cycle out of order"),
        (["B", "start", "A", "end"], False, "cycle event before the start anchor"),
        (["start", "A", "B", "A", "end"], False, "trailing A never closes"),
        (["start", "A", "A", "B", "B", "end"], False, "the second B has nothing left to close"),
    ],
)
def test_worked_examples(spark, events, valid, why):
    result, violations = _run(spark, events)
    assert result["status"] == ("Bestått" if valid else "Ikke bestått"), why
    assert result["total_rows"] == 1
    assert result["failed_rows"] == (0 if valid else 1)
    if not valid:
        assert violations.count() == 1


def test_repeated_closer_is_a_violation(spark):
    """The closer may occur only once per turn: a second B right after the
    first has nothing new to close, even though the batch it would have
    closed (the preceding A) was itself valid."""
    result, violations = _run(spark, ["start", "A", "B", "B", "end"])
    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert row.faktisk_verdi == "B"
    assert "forventet at neste hendelse skulle være 'A'" in row.avviksdetaljer


# --------------------------------------------------------------------------
# anchors
# --------------------------------------------------------------------------

def test_anchors_are_optional(spark):
    """A bare cycle with no anchors is how a plain pair check is expressed."""
    result, _ = _run(spark, ["A", "B"], starts_with=None, ends_with=None)
    assert result["status"] == "Bestått"


def test_cycle_without_anchors_still_requires_whole_passes(spark):
    result, _ = _run(spark, ["A"], starts_with=None, ends_with=None)
    assert result["status"] == "Ikke bestått"


def test_repeated_start_is_a_violation(spark):
    result, _ = _run(spark, ["start", "start", "A", "B", "end"])
    assert result["status"] == "Ikke bestått"


def test_repeated_end_is_a_violation(spark):
    result, _ = _run(spark, ["start", "A", "B", "end", "end"])
    assert result["status"] == "Ikke bestått"


def test_either_closing_event_may_end_the_flow(spark):
    for closing in ("done", "withdrawn"):
        result, _ = _run(
            spark, ["start", "A", "B", closing], ends_with=["done", "withdrawn"]
        )
        assert result["status"] == "Bestått", closing


def test_two_different_closing_events_is_a_violation(spark):
    result, _ = _run(
        spark, ["start", "A", "B", "done", "withdrawn"], ends_with=["done", "withdrawn"]
    )
    assert result["status"] == "Ikke bestått"


# --------------------------------------------------------------------------
# what it deliberately ignores
# --------------------------------------------------------------------------

def test_unlisted_events_are_ignored(spark):
    result, _ = _run(spark, ["start", "noise", "A", "other", "B", "end"])
    assert result["status"] == "Bestått"


def test_group_with_no_listed_events_passes(spark):
    """Zero passes is valid — this is what 'both absent is fine' reduces to."""
    result, _ = _run(spark, ["noise", "other"])
    assert result["status"] == "Bestått"
    assert result["failed_rows"] == 0


def test_violation_detail_explains_out_of_order_events(spark):
    rows = [("g1", "B", date(2024, 1, 1))]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    result, violations = run_rule({"event_flow": FLOW}, df, spark)

    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert "Uventet hendelse" in row.avviksdetaljer
    assert "forventet at neste hendelse" in row.avviksdetaljer
    assert "within" not in row.avviksdetaljer.lower()


def test_violation_detail_explains_incomplete_passes(spark):
    rows = [("g1", "A", date(2024, 1, 1))]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    result, violations = run_rule({"event_flow": {**FLOW, "starts_with": None, "ends_with": None}}, df, spark)

    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert "ble ikke fullført" in row.avviksdetaljer.lower()
    assert "fortsette som" in row.avviksdetaljer.lower()
    assert "within" not in row.avviksdetaljer.lower()


def test_violation_detail_reports_expected_next_event_after_repeated_passes(spark):
    rows = [
        ("g1", "A", date(2024, 1, 1)),
        ("g1", "B", date(2024, 1, 2)),
        ("g1", "A", date(2024, 1, 3)),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    result, violations = run_rule({"event_flow": {**FLOW, "starts_with": None, "ends_with": None}}, df, spark)

    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert "forventet at neste hendelse skulle være 'B'" in row.avviksdetaljer


def test_single_event_cycle_allows_any_number_of_repeats(spark):
    result, _ = _run(spark, ["start", "A", "A", "A", "end"], cycle=["A"])
    assert result["status"] == "Bestått"


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
    keys = [r.primaernoekkel_verdi for r in violations.collect()]
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
    assert result["status"] == "Bestått"


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
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["gated"]


def test_gate_falls_back_to_ends_with_when_never_reached(spark):
    """
    The case this exists for: a handler never sets the gate milestone, but the
    case still reaches its actual closing event. That group must not be
    skipped forever — it is evaluated once, at the point it closed, and an
    unclosed pass surfaces exactly as it would for a gated-in group.
    """
    rows = [
        ("no_gate_but_closed", "A", date(2024, 1, 1)),   # unclosed pass
        ("no_gate_but_closed", "end", date(2024, 1, 2)),
        ("still_open", "A", date(2024, 1, 1)),           # never closes, no gate either
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    rule = {"event_flow": {
        **FLOW, "starts_with": None,
        "completion_gate": {"event_column": "ev", "value": "opened", "order_column": "d"},
    }}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 1     # only the closed case is evaluated
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["no_gate_but_closed"]


def test_gate_reached_is_evaluated_even_without_ends_with_reached(spark):
    """Gate-in-progress behaviour from before the fallback existed is unchanged."""
    rows = [
        ("gated", "opened", date(2024, 1, 1)),
        ("gated", "A", date(2024, 1, 2)),        # unclosed pass -> should fail
        ("ungated", "A", date(2024, 1, 2)),      # same problem, but out of scope
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    rule = {"event_flow": {
        **FLOW, "starts_with": None,
        "completion_gate": {"event_column": "ev", "value": "opened", "order_column": "d"},
    }}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 1
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["gated"]


def test_gate_without_ends_with_configured_stays_strict(spark):
    """No ends_with means no fallback: a group that never reaches the gate is
    excluded even if the flow itself has no configured closing event at all."""
    rows = [
        ("gated", "opened", date(2024, 1, 1)), ("gated", "A", date(2024, 1, 2)),
        ("never_gated", "A", date(2024, 1, 2)),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    rule = {"event_flow": {
        "event_column": "ev", "group_column": "grp", "order_column": "d",
        "cycle": ["A", "B"],
        "completion_gate": {"event_column": "ev", "value": "opened", "order_column": "d"},
    }}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 1
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["gated"]


def test_ends_with_alone_does_not_act_as_a_gate(spark):
    """Declaring ends_with without completion_gate stays fully ungated."""
    rows = [
        ("g1", "A", date(2024, 1, 1)),           # unclosed pass, never reaches end
        ("g2", "A", date(2024, 1, 1)), ("g2", "B", date(2024, 1, 2)),
        ("g2", "end", date(2024, 1, 3)),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date")
    rule = {"event_flow": {**FLOW, "starts_with": None}}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 2     # both groups evaluated, no gate at all
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["g1"]


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
    assert sorted(r.primaernoekkel_verdi for r in violations.collect()) == ["g1", "g2"]


# --------------------------------------------------------------------------
# forgiving: completion_gate also closes an open pass, not just scopes groups
# --------------------------------------------------------------------------

def test_gate_forgives_an_open_pass(spark):
    """The lone A before the gate event is an incomplete pass; the gate closes
    it without a violation, and the flow still reaches its real ends_with."""
    result, _ = _run(
        spark, ["start", "A", "gate", "end"],
        completion_gate={"event_column": "ev", "value": "gate", "order_column": "d"},
    )
    assert result["status"] == "Bestått"
    assert result["failed_rows"] == 0


def test_gate_forgiveness_allows_a_fresh_complete_pass_afterward(spark):
    result, _ = _run(
        spark, ["start", "A", "gate", "A", "B", "end"],
        completion_gate={"event_column": "ev", "value": "gate", "order_column": "d"},
    )
    assert result["status"] == "Bestått"


def test_gate_forgiveness_does_not_excuse_misorder_inside_the_forgiven_pass(spark):
    """Forgiving waives completeness, not ordering: the B here is out of place
    (A must come first) even though the pass containing it later gets
    forgiven by the second gate occurrence."""
    result, violations = _run(
        spark, ["start", "A", "gate", "B", "gate", "A", "B", "end"],
        completion_gate={"event_column": "ev", "value": "gate", "order_column": "d"},
    )
    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert row.faktisk_verdi == "B"
    assert "forventet at neste hendelse skulle være 'A'" in row.avviksdetaljer
    assert row.forventet_betingelse == "start → (A, B)* → gate → end"


def test_gate_forgiveness_only_applies_to_earlier_passes_not_the_last_one(spark):
    """The final pass is still open when the group's data ends, and gets no
    gate occurrence after it to forgive it — still a violation, reported
    against that last pass's own expected next event."""
    result, violations = _run(
        spark, ["start", "A", "gate", "A"],
        completion_gate={"event_column": "ev", "value": "gate", "order_column": "d"},
        ends_with=None,
    )
    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert "forventet at neste hendelse skulle være 'B'" in row.avviksdetaljer


def test_gate_on_a_different_column_does_not_forgive(spark):
    """completion_gate still scopes groups via any column, but forgiving only
    applies when its event_column is this flow's own — a different column's
    value never resets an open pass, even one that coincides with a cycle
    event name."""
    rows = [
        ("g1", "A", date(2024, 1, 1), None),
        ("g1", "note", date(2024, 1, 2), "A"),
    ]
    df = spark.createDataFrame(rows, "grp string, ev string, d date, status string")
    rule = {"event_flow": {
        "event_column": "ev", "group_column": "grp", "order_column": "d",
        "cycle": ["A", "B"],
        "completion_gate": {"event_column": "status", "value": "A", "order_column": "d"},
    }}
    result, violations = run_rule(rule, df, spark)

    assert result["total_rows"] == 1            # scoping via `status` still applies
    assert result["status"] == "Ikke bestått"    # but forgiving does not — A is still unclosed
    assert [r.primaernoekkel_verdi for r in violations.collect()] == ["g1"]


def test_forventet_betingelse_is_bare_flow_notation(spark):
    """forventet_betingelse holds the technical notation, matching every other
    rule builder — no leftover English prose."""
    result, violations = _run(spark, ["B"])
    row = violations.collect()[0]
    assert row.forventet_betingelse == "start → (A, B)* → end"
    assert "Events must follow" not in row.forventet_betingelse


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
        (
            {"completion_gate": {"event_column": "ev", "value": "A", "order_column": "d"}},
            "also appear in starts_with/cycle/ends_with",
        ),
    ],
)
def test_configuration_errors(spark, cfg, fragment):
    result, _ = _run(spark, ["start", "A", "B", "end"], **cfg)
    assert result["status"] == "Feil"
    assert fragment in result["details"]
