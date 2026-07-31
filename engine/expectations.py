# =============================================================================
# engine/expectations.py
# Rule types for the PBE Quality Catalog.
#
# A rule is a predicate plus optional row scoping:
#
#     - rule_id: FAS-006
#       name:    Tidsbruk kan ikke være negativt tall
#       check:   tidsbruk >= 0
#
#     - rule_id: FAS-004
#       name:    Åpne faser må ha saksbehandler
#       when:    seneste_stoppmilepael_dato IS NULL
#       check:   saksansvarlig_kode IS NOT NULL
#
# `check:` mirrors a SQL CHECK constraint: a row violates it only when the
# predicate is FALSE.  A predicate that evaluates to NULL (because an operand
# is NULL) leaves the row unevaluated, exactly as SQL treats UNKNOWN.  Use an
# explicit `IS NOT NULL` to require presence.
#
# The remaining rule types cover checks that are not a single row predicate.
# Each declares a `scope`, which fixes the unit everything is counted in:
#
#   row    - one row is one unit
#   group  - one group is one unit
#   table  - the whole table is one unit
#
# This file has three sections:
#   1. Shared helpers
#   2. Rule-type builders   (one per YAML key)
#   3. Registry and the driver that runs a rule
#
# The driver — not the builders — owns `when:` filtering, primary-key
# resolution, counting, and assembling the result dict, so those cannot drift
# between rule types.
# =============================================================================

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructField, StructType


# =============================================================================
# 1. Shared helpers
# =============================================================================

_VIOLATION_COLUMNS = (
    "primary_key_value",
    "violated_column",
    "actual_value",
    "expected_condition",
    "violation_detail",
)

_VIOLATION_SCHEMA = StructType([
    StructField(name, StringType(), True) for name in _VIOLATION_COLUMNS
])


class RuleConfigError(Exception):
    """A rule is misconfigured. Reported as a rule-level ERROR, not a crash."""


def empty_violations(spark) -> DataFrame:
    return spark.createDataFrame([], _VIOLATION_SCHEMA)


def _safe_pct(passed: int, total: int) -> float:
    """passed/total as a percentage, guarding against division by zero."""
    return round(passed / total * 100, 2) if total else 100.0


def _require(cfg: dict, *keys: str) -> list:
    """Return the named config values, raising if any is absent or empty."""
    missing = [k for k in keys if cfg.get(k) in (None, "", [])]
    if missing:
        raise RuleConfigError(
            f"Missing required parameter(s): {', '.join(sorted(missing))}."
        )
    return [cfg[k] for k in keys]


def _require_columns(df: DataFrame, *columns: str) -> None:
    missing = [c for c in columns if c and c not in df.columns]
    if missing:
        raise RuleConfigError(f"Column(s) not found in source: {sorted(missing)}.")


def _as_str(col_name: str):
    return F.col(col_name).cast("string")


def _str_or_null(col_name: str):
    """Column value as a string, rendering NULL as the literal text 'NULL'."""
    return F.coalesce(_as_str(col_name), F.lit("NULL"))


def predicate_columns(expression: str) -> list[str]:
    """
    Columns referenced by a predicate, in the order they are written.

    Walks the *unresolved* expression tree, so it needs no schema and — unlike
    the analyzer's reference set, which is unordered — preserves source order.
    That ordering is what makes the first reference the natural subject of the
    predicate (`a` in `a >= b`).

    Best-effort: returns [] if the tree cannot be walked, in which case the
    caller falls back to a NULL violated_column rather than failing the rule.
    """
    try:
        node = F.expr(expression)._jc.expr()
    except Exception:
        return []

    found: list[str] = []

    def walk(current) -> None:
        try:
            if current.getClass().getSimpleName() == "UnresolvedAttribute":
                name = current.name()
                if name not in found:
                    found.append(name)
            children = current.children()
            for i in range(children.size()):
                walk(children.apply(i))
        except Exception:
            return

    walk(node)
    return found


def _predicate_subject(expression: str) -> str | None:
    """The column a predicate is 'about' — its first referenced column."""
    columns = predicate_columns(expression)
    return columns[0] if columns else None


def _as_list(raw) -> list:
    """
    Accept a scalar or a list wherever several values are permitted.

      "A"       -> ["A"]
      ["A","B"] -> ["A", "B"]     any one of them satisfies

    Used by `ends_with:` and `completion_gate.value:`, so a rule that closes on
    one of several events does not need list syntax when it closes on one.
    """
    if raw is None:
        return []
    return list(raw) if isinstance(raw, (list, tuple)) else [raw]


def _gate_predicate(gate: dict):
    """Predicate identifying the rows that mark a group as complete."""
    event_column = gate.get("event_column")
    values = _as_list(gate.get("value"))
    order_column = gate.get("order_column")

    predicate = F.col(event_column).isin(values)
    if order_column:
        predicate = predicate & F.col(order_column).isNotNull()
    return predicate


def _resolve_gate_groups(df: DataFrame, gate: dict, group_column: str) -> DataFrame:
    """
    Narrow to rows belonging to groups that have reached the gate event.

    An absent or incomplete gate leaves the frame untouched, so an
    ungated rule evaluates every group.
    """
    if not gate or not gate.get("event_column") or not _as_list(gate.get("value")):
        return df

    _require_columns(df, gate.get("event_column"), gate.get("order_column"))

    complete = df.filter(_gate_predicate(gate)).select(group_column).distinct()
    return df.join(complete, on=group_column, how="inner")


_AGGREGATE_FUNCTIONS = {
    "sum": F.sum,
    "count": F.count,
    "avg": F.avg,
    "min": F.min,
    "max": F.max,
}


@dataclass
class Context:
    """Everything a builder needs. Assembled by the driver."""
    df: DataFrame           # already narrowed by catalog `where:` and rule `when:`
    cfg: object             # the value of the rule-type key
    spark: object
    pk_column: str | None


@dataclass
class Evaluation:
    """
    What a builder produces.

    `evaluated` holds the units that were actually examined and `violations`
    the ones that failed — both counted in the rule type's declared scope, so
    passed = total - failed can never go negative.
    """
    evaluated: DataFrame
    violations: DataFrame
    describes: str
    failed_override: int | None = None


# =============================================================================
# 2. Rule-type builders
# =============================================================================

def _build_check(ctx: Context) -> Evaluation:
    """A single boolean predicate, evaluated per row with SQL CHECK semantics."""
    expression = ctx.cfg
    if not isinstance(expression, str) or not expression.strip():
        raise RuleConfigError("'check' must be a non-empty SQL predicate.")
    expression = expression.strip()

    predicate = F.expr(expression)
    # NULL predicate means "not evaluable" (an operand was NULL), so those rows
    # are outside the denominator as well as outside the violations.
    evaluated = ctx.df.filter(predicate.isNotNull())
    violating = ctx.df.filter(~predicate)

    subject = _predicate_subject(expression)
    if subject and subject in ctx.df.columns:
        actual = _as_str(subject)
        detail = F.concat(
            F.lit(f"{subject} = "), _str_or_null(subject),
            F.lit(f"; expected {expression}"),
        )
    else:
        actual = F.lit(None).cast("string")
        detail = F.lit(f"Row does not satisfy {expression}")

    violations = violating.select(
        _as_str(ctx.pk_column).alias("primary_key_value"),
        F.lit(subject).cast("string").alias("violated_column"),
        actual.alias("actual_value"),
        F.lit(expression).alias("expected_condition"),
        detail.alias("violation_detail"),
    )
    return Evaluation(evaluated, violations, expression)


def _build_unique(ctx: Context) -> Evaluation:
    """Every combination of the listed columns must occur at most once."""
    columns = ctx.cfg
    if isinstance(columns, str):
        columns = [columns]
    if not columns or not isinstance(columns, list):
        raise RuleConfigError("'unique' must be a column name or a list of them.")
    _require_columns(ctx.df, *columns)

    duplicates = (
        ctx.df.groupBy(*columns)
        .agg(F.count(F.lit(1)).alias("_n"))
        .filter(F.col("_n") > 1)
        .drop("_n")
    )
    violating = ctx.df.join(duplicates, on=columns, how="inner")

    combination = ", ".join(columns)
    condition = f"UNIQUE({combination})"
    violations = violating.select(
        _as_str(ctx.pk_column).alias("primary_key_value"),
        F.lit(columns[0]).alias("violated_column"),
        F.concat_ws("|", *[_as_str(c) for c in columns]).alias("actual_value"),
        F.lit(condition).alias("expected_condition"),
        F.lit(f"Duplicate combination of ({combination}).").alias("violation_detail"),
    )
    return Evaluation(ctx.df, violations, condition)



_ROW_COUNT_OPERATORS = {
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "=": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


def _build_row_count(ctx: Context) -> Evaluation:
    """The table as a whole must hold a given number of rows."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'row_count' must be a mapping.")
    (threshold,) = _require(cfg, "threshold")
    operator = str(cfg.get("operator", ">="))
    if operator not in _ROW_COUNT_OPERATORS:
        raise RuleConfigError(
            f"Unsupported operator '{operator}'. Allowed: {sorted(_ROW_COUNT_OPERATORS)}"
        )
    try:
        threshold = float(threshold)
    except (TypeError, ValueError):
        raise RuleConfigError(f"'threshold' must be numeric, got {threshold!r}.")

    total = ctx.df.count()
    holds = _ROW_COUNT_OPERATORS[operator](float(total), threshold)
    return Evaluation(
        evaluated=ctx.df,
        violations=empty_violations(ctx.spark),
        describes=f"COUNT(*) {operator} {threshold}",
        failed_override=0 if holds else total,
    )


def _build_event_flow(ctx: Context) -> Evaluation:
    """
    Within each group, declared events must occur in order, as whole passes.

    The shape is `starts_with` (once) -> `cycle` repeated as complete passes ->
    `ends_with` (once, any of several values). Events not named anywhere are
    ignored entirely, so unrelated activity between the declared ones is fine.

    A pass that never closes is the error this exists to catch: with
    `cycle: [A, B]`, `start A B A end` is wrong because the trailing A has no B,
    while `start A B A B end` is two complete passes and correct. Requiring the
    count to divide by the cycle length is what expresses that, and it also
    reproduces a plain "both or neither" pair check when the cycle is a pair and
    there are no anchors.
    """
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'event_flow' must be a mapping.")
    event_column, group_column, order_column = _require(
        cfg, "event_column", "group_column", "order_column"
    )
    cycle = [str(v) for v in (cfg.get("cycle") or [])]
    if not cycle:
        raise RuleConfigError("'cycle' must list at least one event.")
    if len(set(cycle)) != len(cycle):
        raise RuleConfigError(f"'cycle' repeats an event: {cycle}.")

    starts_with = cfg.get("starts_with")
    if isinstance(starts_with, (list, tuple)):
        raise RuleConfigError("'starts_with' takes a single event, not a list.")
    ends_with = _as_list(cfg.get("ends_with"))
    gate = cfg.get("completion_gate") or {}
    _require_columns(ctx.df, event_column, group_column, order_column)

    overlap = (set(ends_with) | ({starts_with} if starts_with else set())) & set(cycle)
    if overlap:
        raise RuleConfigError(
            f"Event(s) {sorted(overlap)} are both an anchor and part of the cycle."
        )

    width = len(cycle)
    START_RANK, END_RANK = 0, width + 1
    ranked_events = ([starts_with] if starts_with else []) + cycle + ends_with

    rank = F.lit(None).cast("int")
    for value, position in (
        [(starts_with, START_RANK)] if starts_with else []
    ) + [(v, i + 1) for i, v in enumerate(cycle)] + [(v, END_RANK) for v in ends_with]:
        rank = F.when(F.col(event_column) == value, F.lit(position)).otherwise(rank)

    df = _resolve_gate_groups(ctx.df, gate, group_column)
    listed = (
        df.filter(
            F.col(group_column).isNotNull()
            & F.col(event_column).isin(ranked_events)
            & F.col(order_column).isNotNull()
        )
        .withColumn("_rank", rank)
    )
    evaluated = (
        df.filter(F.col(group_column).isNotNull()).select(group_column).distinct()
    )

    # Ordering ties are broken by rank so two events recorded on the same date are
    # read in declared order. Without it the outcome depends on row arrival order.
    ordering = [F.col(order_column).asc(), F.col("_rank").asc()]
    by_group = Window.partitionBy(group_column).orderBy(*ordering)
    positioned = listed.withColumn("_pos", F.row_number().over(by_group)).withColumn(
        "_last", F.max("_pos").over(Window.partitionBy(group_column))
    )

    cycle_only = positioned.filter(
        (F.col("_rank") > START_RANK) & (F.col("_rank") < END_RANK)
    ).withColumn(
        "_cpos",
        F.row_number().over(Window.partitionBy(group_column).orderBy(*ordering)),
    )
    # The i-th cycle event must be cycle[i % width]; anything else is out of order.
    misplaced = F.col("_rank") != (((F.col("_cpos") - 1) % F.lit(width)) + 1)

    anchor_problem = (
        # a start that is not the first listed event, or an end that is not the last
        ((F.col("_rank") == START_RANK) & (F.col("_pos") != 1))
        | ((F.col("_rank") == END_RANK) & (F.col("_pos") != F.col("_last")))
    )

    offenders = (
        positioned.filter(anchor_problem)
        .select(group_column, order_column, event_column)
        .unionByName(
            cycle_only.filter(misplaced).select(group_column, order_column, event_column)
        )
    )

    counts = positioned.groupBy(group_column).agg(
        F.count(F.when(F.col("_rank") == START_RANK, F.lit(1))).alias("_starts"),
        F.count(F.when(F.col("_rank") == END_RANK, F.lit(1))).alias("_ends"),
        F.count(
            F.when(
                (F.col("_rank") > START_RANK) & (F.col("_rank") < END_RANK), F.lit(1)
            )
        ).alias("_cycle_events"),
    )
    structural = counts.filter(
        (F.col("_starts") > 1)
        | (F.col("_ends") > 1)
        | (F.col("_cycle_events") % F.lit(width) != 0)   # an unclosed pass
    ).select(group_column)

    # One row per failing group, naming the earliest event that broke the flow.
    first_offender = (
        offenders.groupBy(group_column)
        .agg(F.min(F.struct(F.col(order_column), F.col(event_column))).alias("_first"))
        .select(group_column, F.col(f"_first.{event_column}").alias("_bad_event"))
    )
    violating = (
        offenders.select(group_column)
        .unionByName(structural)
        .distinct()
        .join(first_offender, on=group_column, how="left")
    )

    flow = " → ".join(
        ([starts_with] if starts_with else [])
        + [f"({', '.join(cycle)})*"]
        + ([" or ".join(ends_with)] if ends_with else [])
    )
    condition = f"Events must follow: {flow}"
    violations = violating.select(
        _as_str(group_column).alias("primary_key_value"),
        F.lit(event_column).alias("violated_column"),
        _as_str("_bad_event").alias("actual_value"),
        F.lit(condition).alias("expected_condition"),
        F.concat(
            F.coalesce(
                F.concat(F.lit("'"), _as_str("_bad_event"), F.lit("' breaks the flow")),
                F.lit("Incomplete pass"),
            ),
            F.lit(f" within {group_column}="),
            _str_or_null(group_column),
            F.lit("."),
        ).alias("violation_detail"),
    )
    return Evaluation(evaluated, violations, condition)


def _build_required_event(ctx: Context) -> Evaluation:
    """Every group must contain at least one row carrying the required event."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'required_event' must be a mapping.")
    event_column, group_column, value = _require(
        cfg, "event_column", "group_column", "value"
    )
    order_column = cfg.get("order_column")
    _require_columns(ctx.df, event_column, group_column, order_column)

    grouped = ctx.df.filter(F.col(group_column).isNotNull())
    evaluated = grouped.select(group_column).distinct()
    reached = grouped.filter(
        _gate_predicate({
            "event_column": event_column, "value": value, "order_column": order_column,
        })
    ).select(group_column).distinct()
    violating = evaluated.join(reached, on=group_column, how="left_anti")

    condition = (
        f"Group must contain at least one row where {event_column} = '{value}'"
        + (f" and {order_column} IS NOT NULL" if order_column else "")
    )
    violations = violating.select(
        _as_str(group_column).alias("primary_key_value"),
        F.lit(event_column).alias("violated_column"),
        F.lit(None).cast("string").alias("actual_value"),
        F.lit(condition).alias("expected_condition"),
        F.lit(f"Required event '{value}' missing.").alias("violation_detail"),
    )
    return Evaluation(evaluated, violations, condition)


def _build_aggregate_matches(ctx: Context) -> Evaluation:
    """An aggregate over each group must match a reference value on the group."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'aggregate_matches' must be a mapping.")
    group_column, aggregate_column, reference_column = _require(
        cfg, "group_column", "aggregate_column", "reference_column"
    )
    aggregate = str(cfg.get("aggregate", "sum")).lower()
    if aggregate not in _AGGREGATE_FUNCTIONS:
        raise RuleConfigError(
            f"Unsupported aggregate '{aggregate}'. Allowed: {sorted(_AGGREGATE_FUNCTIONS)}"
        )
    try:
        tolerance = float(cfg.get("tolerance", 0.01))
    except (TypeError, ValueError):
        raise RuleConfigError(f"'tolerance' must be numeric, got {cfg.get('tolerance')!r}.")
    _require_columns(ctx.df, group_column, aggregate_column, reference_column)

    evaluated = (
        ctx.df.filter(
            F.col(group_column).isNotNull()
            & F.col(aggregate_column).isNotNull()
            & F.col(reference_column).isNotNull()
        )
        .groupBy(group_column, reference_column)
        .agg(_AGGREGATE_FUNCTIONS[aggregate](aggregate_column).alias("_aggregate"))
        .withColumn("_difference", F.abs(F.col("_aggregate") - F.col(reference_column)))
    )
    violating = evaluated.filter(F.col("_difference") > tolerance)

    condition = (
        f"ABS({aggregate.upper()}({aggregate_column}) - {reference_column}) <= {tolerance}"
    )
    violations = violating.select(
        _as_str(group_column).alias("primary_key_value"),
        F.lit(aggregate_column).alias("violated_column"),
        _as_str("_aggregate").alias("actual_value"),
        F.lit(condition).alias("expected_condition"),
        F.concat(
            F.lit(f"{aggregate.upper()}({aggregate_column}) = "), _str_or_null("_aggregate"),
            F.lit(f", expected {reference_column} = "), _str_or_null(reference_column),
            F.lit(", difference "), _str_or_null("_difference"),
        ).alias("violation_detail"),
    )
    return Evaluation(evaluated, violations, condition)


# =============================================================================
# 3. Registry and driver
# =============================================================================

@dataclass(frozen=True)
class RuleType:
    name: str                 # the YAML key; the rule-type name IS the key
    scope: str                # "row" | "group" | "table"
    build: Callable[[Context], Evaluation]
    unit: str                 # what one unit is called, for the details text
    # Config keys that must be present. Preflight reads these, so the contract
    # is declared once here rather than restated in a parallel table.
    required: tuple[str, ...] = ()
    # Config keys naming a column in the *source*. Reference-side keys are
    # deliberately absent: they point at another table and cannot be checked
    # against the source schema.
    column_keys: tuple[str, ...] = ()
    # Whether this type accepts a completion_gate block.
    gated: bool = False


RULE_TYPES: dict[str, RuleType] = {
    rule_type.name: rule_type
    for rule_type in (
        RuleType("check", "row", _build_check, "rows"),
        RuleType("unique", "row", _build_unique, "rows"),
        RuleType("row_count", "table", _build_row_count, "rows", required=("threshold",)),
        RuleType(
            "event_flow", "group", _build_event_flow, "groups",
            required=("event_column", "group_column", "order_column", "cycle"),
            column_keys=("event_column", "group_column", "order_column"),
            gated=True,
        ),
        RuleType(
            "required_event", "group", _build_required_event, "groups",
            required=("event_column", "group_column", "value"),
            column_keys=("event_column", "group_column", "order_column"),
        ),
        RuleType(
            "aggregate_matches", "group", _build_aggregate_matches, "groups",
            required=("group_column", "aggregate_column", "reference_column"),
            column_keys=("group_column", "aggregate_column", "reference_column"),
        ),
    )
}

# Rule-level keys holding a SQL predicate, validated against the real schema
# by preflight. `check` is only a predicate for the `check` rule type.
PREDICATE_KEYS = ("when", "check")

# Rule types whose primary_key_value is a group key rather than a row key.
GROUP_SCOPED = frozenset(t.name for t in RULE_TYPES.values() if t.scope == "group")

# Reserved rule-level keys that never name a rule type. Preflight imports this
# rather than restating it, so the two cannot disagree about what is legal.
RESERVED_RULE_KEYS = frozenset({"rule_id", "name", "description", "when", "pk_column"})


def detect_rule_type(rule: dict) -> str:
    """The rule-type key present on a rule. Raises if not exactly one."""
    present = [key for key in rule if key in RULE_TYPES]
    if len(present) == 1:
        return present[0]
    if not present:
        unknown = sorted(set(rule) - RESERVED_RULE_KEYS)
        raise RuleConfigError(
            "No rule type found. Expected exactly one of "
            f"{sorted(RULE_TYPES)}; got keys {unknown}."
        )
    raise RuleConfigError(
        f"Rule declares more than one rule type: {sorted(present)}. "
        "Split it into separate rules."
    )


def _error(message: str) -> dict:
    return {
        "total_rows": 0, "passed_rows": 0, "failed_rows": 0,
        "success_pct": 0.0, "status": "ERROR", "details": message,
    }


def run_rule(rule: dict, df: DataFrame, spark, pk_column=None) -> tuple:
    """
    Evaluate one rule and return ``(result_dict, violations_df)``.

    Owns everything common to all rule types: `when:` filtering, primary-key
    resolution, counting in the scope's unit, and building the result. Builders
    only describe what a violation is.
    """
    try:
        type_name = detect_rule_type(rule)
        rule_type = RULE_TYPES[type_name]

        scoped = df
        condition = rule.get("when")
        if condition is not None:
            if not isinstance(condition, str) or not condition.strip():
                raise RuleConfigError("'when' must be a non-empty SQL predicate.")
            # A row is in scope only where the condition is explicitly true;
            # an unevaluable (NULL) condition excludes the row, as SQL does.
            scoped = df.filter(F.expr(condition.strip()))

        resolved_pk = rule.get("pk_column") or pk_column
        if rule_type.scope == "row":
            if not resolved_pk:
                raise RuleConfigError(
                    "No primary key: set 'pk_column' on the rule or the catalog."
                )
            _require_columns(scoped, resolved_pk)

        evaluation = rule_type.build(
            Context(
                df=scoped, cfg=rule[type_name], spark=spark,
                pk_column=resolved_pk,
            )
        )

        total = evaluation.evaluated.count()
        if evaluation.failed_override is not None:
            failed = evaluation.failed_override
        elif rule_type.scope == "group":
            # One group counts once however many of its pairs or events failed,
            # so failures stay in the same unit as the denominator.
            failed = evaluation.violations.select("primary_key_value").distinct().count()
        else:
            failed = evaluation.violations.count()

        passed = total - failed
        unit = rule_type.unit
        details = (
            f"All {total} {unit} satisfy {evaluation.describes}."
            if failed == 0
            else f"{failed} of {total} {unit} violate {evaluation.describes}."
        )
        result = {
            "total_rows": total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": _safe_pct(passed, total),
            "status": "PASSED" if failed == 0 else "FAILED",
            "details": details,
        }
        violations = (
            evaluation.violations if failed else empty_violations(spark)
        )
        return result, violations

    except RuleConfigError as exc:
        return _error(str(exc)), empty_violations(spark)
