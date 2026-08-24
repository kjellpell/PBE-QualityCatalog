# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# =============================================================================
# QC_EventFlow
#
# A standalone, hand-written re-implementation of QC_Engine's `event_flow`
# rule type (see `_build_event_flow` in QC_Engine.py and "event_flow" in
# RULES_GUIDE.md for the full specification and worked examples).
#
# QC_Engine's version has to serve every rule type through one generic
# Context/Evaluation contract, so it expresses "events must occur in order,
# as whole passes" as a stack of Window/rank arithmetic operating on the
# whole DataFrame at once. That is efficient but hard to read: nobody traces
# a rank column by hand.
#
# This notebook has one job, so it can afford to check each group the way a
# human would trace it — read the group's events in order and walk them left
# to right with a plain Python loop (`_first_problem` below). The events that
# matter are a small, named slice of the source table (milestones, not every
# row), so that walk runs on the driver after a `.collect()`, rather than as
# a UDF shipped to executors — which also sidesteps a real trap: a function
# closing over a notebook composed by `%run` cannot be pickled to a worker,
# since the worker has no module by that name to import it back from.
#
# It has no dependency on QC_Engine, QC_Rules, or the YAML rule catalog; it
# is meant to be `%run` (or copy-pasted) into a simpler validation notebook
# that calls `check_event_flow(...)` directly with plain arguments, one call
# per flow to check.
#
# Semantics preserved from the original:
#   - shape: starts_with (once) -> cycle (repeated as whole passes) -> ends_with
#     (once, any of several values); events not named anywhere are ignored.
#   - a pass that never closes is a violation, not just a mismatched event.
#   - completion_gate scopes which groups get evaluated, falling back to the
#     flow's own ends_with when a group reaches its closing event without the
#     gate ever having fired.
#   - same-date events are read in declared order (start, then cycle in
#     order, then end), so the result does not depend on row arrival order.
#   - a NULL group key is excluded, not reported.
#
# Output is the same six-column violation shape as `avvik`
# (primaernoekkel_verdi, identifikator_verdi, avvikende_kolonne,
# faktisk_verdi, forventet_betingelse, avviksdetaljer), so it can be written
# to the same table.
# =============================================================================

from collections import namedtuple

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

_VIOLATION_SCHEMA = StructType([
    StructField("primaernoekkel_verdi", StringType(), True),
    StructField("identifikator_verdi", StringType(), True),
    StructField("avvikende_kolonne", StringType(), True),
    StructField("faktisk_verdi", StringType(), True),
    StructField("forventet_betingelse", StringType(), True),
    StructField("avviksdetaljer", StringType(), True),
])

Problem = namedtuple("Problem", "reason bad_event expected_event")

# Each template references only the field(s) that reason actually fills in.
_MESSAGES = {
    "start_out_of_place": "Startet med '{bad}' i stedet for startpunktet '{expected}'.",
    "end_out_of_place": "'{bad}' avsluttet flyten for tidlig; flere hendelser kom etterpå.",
    "closed_mid_pass": "Flyten ble avsluttet med '{bad}' midt i en runde; forventet '{expected}' først.",
    "misplaced": "Uventet hendelse '{bad}'; forventet '{expected}'.",
    "unclosed": "Flyten ble startet, men aldri fullført; forventet at neste hendelse skulle være '{expected}'.",
}


def _first_problem(events: list[str], starts_with: str | None, cycle: list[str], ends_with: set[str]) -> Problem | None:
    """
    Walk one group's events — already sorted in declared order — and return
    the first thing wrong with the flow `starts_with? -> (cycle)* -> ends_with?`.

    None means the sequence is valid. Otherwise the returned Problem names
    what broke: an anchor in the wrong place, an event out of cycle order, or
    a pass that opened and never closed.
    """
    width = len(cycle)
    cycle_position = 0  # how many cycle events consumed so far, across all passes

    for position, event in enumerate(events):
        is_last = position == len(events) - 1

        if event == starts_with:
            if position != 0:
                return Problem("start_out_of_place", event, starts_with)
            continue

        if event in ends_with:
            if not is_last:
                return Problem("end_out_of_place", event, None)
            if cycle_position % width != 0:
                return Problem("closed_mid_pass", event, cycle[cycle_position % width])
            continue

        expected = cycle[cycle_position % width]
        if event != expected:
            return Problem("misplaced", event, expected)
        cycle_position += 1

    if cycle_position % width != 0:
        return Problem("unclosed", None, cycle[cycle_position % width])

    return None


def _validate_config(
    event_column: str, group_column: str, order_column: str,
    cycle: list[str], starts_with: str | None, ends_with: list[str],
) -> None:
    for name, value in (("event_column", event_column), ("group_column", group_column), ("order_column", order_column)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string.")
    if not cycle:
        raise ValueError("cycle must list at least one event.")
    if len(set(cycle)) != len(cycle):
        raise ValueError(f"cycle repeats an event: {cycle}.")
    if isinstance(starts_with, (list, tuple)):
        raise ValueError("starts_with takes a single event, not a list.")
    overlap = (set(ends_with) | ({starts_with} if starts_with else set())) & set(cycle)
    if overlap:
        raise ValueError(f"Event(s) {sorted(overlap)} are both an anchor and part of the cycle.")


def _flow_description(starts_with: str | None, cycle: list[str], ends_with: list[str]) -> str:
    parts = ([starts_with] if starts_with else []) + [f"({', '.join(cycle)})*"]
    if ends_with:
        parts.append(" or ".join(ends_with))
    return "Events must follow: " + " → ".join(parts)


def _groups_in_scope(df, group_column: str, event_column: str, ends_with: list[str], completion_gate: dict | None):
    """
    Distinct group values eligible for evaluation, or None for "every group".

    Ungated: every group. Gated: groups that reached the gate event, plus any
    group that reached its own closing event even though the gate never
    fired — a handler does not always remember to set the gate milestone, and
    a group should not be excluded forever just because of that.
    """
    gate = completion_gate or {}
    gate_column = gate.get("event_column")
    gate_values = gate.get("value")
    if not gate_column or not gate_values:
        return None

    gate_values = list(gate_values) if isinstance(gate_values, (list, tuple)) else [gate_values]
    gate_order_column = gate.get("order_column")
    missing = [c for c in (gate_column, gate_order_column) if c and c not in df.columns]
    if missing:
        raise ValueError(f"completion_gate column(s) not found in source: {sorted(missing)}.")

    reached_gate = df.filter(F.col(gate_column).isin(gate_values))
    if gate_order_column:
        reached_gate = reached_gate.filter(F.col(gate_order_column).isNotNull())
    ready = reached_gate.select(group_column).distinct()

    if ends_with:
        closed = df.filter(F.col(event_column).isin(ends_with)).select(group_column).distinct()
        ready = ready.union(closed).distinct()

    return ready


def check_event_flow(
    df,
    *,
    event_column: str,
    group_column: str,
    order_column: str,
    cycle: list[str],
    starts_with: str | None = None,
    ends_with: str | list[str] | None = None,
    completion_gate: dict | None = None,
    identifier_column: str | None = None,
):
    """
    Check that, within each group, declared events occur in order as whole
    passes: `starts_with` (once) -> `cycle` (repeated as complete passes) ->
    `ends_with` (once, any of several values). Events not named anywhere are
    ignored. Returns one row per failing group, in the standard `avvik` shape.

    Example — the MIL-004 pairing rule from QC_Rules.py, run directly instead
    of through a YAML catalog:

        violations = check_event_flow(
            df,
            event_column="milestone_title",
            group_column="fk_faser",
            order_column="milestonedate",
            cycle=["Anmodning om oppdatert plandokumentasjon",
                   "Mottatt oppdatert plandokumentasjon"],
        )
    """
    cycle = list(cycle)
    ends_with = list(ends_with) if isinstance(ends_with, (list, tuple)) else ([ends_with] if ends_with else [])
    _validate_config(event_column, group_column, order_column, cycle, starts_with, ends_with)

    missing = [c for c in (event_column, group_column, order_column) if c not in df.columns]
    if missing:
        raise ValueError(f"Column(s) not found in source: {sorted(missing)}.")

    scoped = df.filter(F.col(group_column).isNotNull())
    ready_groups = _groups_in_scope(scoped, group_column, event_column, ends_with, completion_gate)
    if ready_groups is not None:
        scoped = scoped.join(ready_groups, on=group_column, how="inner")

    ordered_values = ([starts_with] if starts_with else []) + cycle
    rank = {value: position for position, value in enumerate(ordered_values)}
    end_rank = len(ordered_values)
    for value in ends_with:
        rank[value] = end_rank

    listed = scoped.filter(
        F.col(event_column).isin(list(rank)) & F.col(order_column).isNotNull()
    ).select(group_column, event_column, order_column)

    if identifier_column:
        identifiers = scoped.groupBy(group_column).agg(
            F.first(F.col(identifier_column).cast("string"), ignorenulls=True).alias("_identifier")
        )
        listed = listed.join(identifiers, on=group_column, how="left")
    else:
        listed = listed.withColumn("_identifier", F.lit(None).cast("string"))

    flow = _flow_description(starts_with, cycle, ends_with)
    ends_with_set = set(ends_with)

    groups: dict = {}
    for row in listed.collect():
        groups.setdefault(row[group_column], []).append(row)

    violation_rows = []
    for group_value, rows in groups.items():
        ordered = sorted(rows, key=lambda row: (row[order_column], rank[row[event_column]]))
        events = [row[event_column] for row in ordered]

        problem = _first_problem(events, starts_with, cycle, ends_with_set)
        if problem is None:
            continue

        detail = _MESSAGES[problem.reason].format(bad=problem.bad_event, expected=problem.expected_event)
        violation_rows.append((
            str(group_value),
            rows[0]["_identifier"],  # constant per group: resolved once, above
            event_column,
            problem.bad_event,
            flow,
            detail,
        ))

    return df.sparkSession.createDataFrame(violation_rows, _VIOLATION_SCHEMA)
