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

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructField, StructType


# =============================================================================
# 1. Shared helpers
# =============================================================================

VIOLATION_COLUMNS = (
    "primary_key_value",
    "violated_column",
    "actual_value",
    "expected_condition",
    "violation_detail",
)

_VIOLATION_SCHEMA = StructType([
    StructField(name, StringType(), True) for name in VIOLATION_COLUMNS
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


def _normalise_stops(raw_stop) -> list:
    """
    Normalise the stop slot of a required pair to a list.

      [A, B]      -> [B]        single stop
      [A, [B, C]] -> [B, C]     satisfied by either stop
    """
    return list(raw_stop) if isinstance(raw_stop, list) else [raw_stop]


def _gate_predicate(gate: dict):
    """Predicate identifying the rows that mark a group as complete."""
    event_column = gate.get("event_column")
    value = gate.get("value")
    order_column = gate.get("order_column")

    predicate = F.col(event_column) == value
    if order_column:
        predicate = predicate & F.col(order_column).isNotNull()
    return predicate


def _resolve_gate_groups(df: DataFrame, gate: dict, group_column: str) -> DataFrame:
    """
    Narrow to rows belonging to groups that have reached the gate event.

    An absent or incomplete gate leaves the frame untouched, so an
    ungated rule evaluates every group.
    """
    if not gate or not gate.get("event_column") or gate.get("value") is None:
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
    rule: dict
    spark: object
    pk_column: str | None
    ref_cache: dict = field(default_factory=dict)


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


def _build_exists_in(ctx: Context) -> Evaluation:
    """
    Every non-NULL value must exist in a reference table.

    With `active_column`/`active_value` the reference set is narrowed to rows
    that are currently active, which is what the old `reference_active` type
    did as a separate expectation.
    """
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'exists_in' must be a mapping.")
    column, table, reference_column = _require(cfg, "column", "table", "reference_column")
    active_column = cfg.get("active_column")
    active_value = cfg.get("active_value")
    if bool(active_column) != (active_value is not None):
        raise RuleConfigError(
            "'active_column' and 'active_value' must be given together."
        )
    _require_columns(ctx.df, column)

    cache_key = (table, reference_column, active_column, str(active_value))
    reference = ctx.ref_cache.get(cache_key) if ctx.ref_cache is not None else None
    if reference is None:
        try:
            source = ctx.spark.table(table)
            if active_column:
                if isinstance(active_value, str):
                    active = F.lower(_as_str(active_column)) == F.lower(F.lit(active_value))
                else:
                    active = F.col(active_column) == active_value
                source = source.filter(active)
            reference = (
                source.select(_as_str(reference_column).alias("_ref_key")).distinct()
            )
        except RuleConfigError:
            raise
        except Exception as exc:
            raise RuleConfigError(f"Could not load reference table '{table}': {exc}")
        if ctx.ref_cache is not None:
            ctx.ref_cache[cache_key] = reference

    evaluated = ctx.df.filter(F.col(column).isNotNull())
    violating = evaluated.alias("src").join(
        reference, F.col(f"src.{column}").cast("string") == F.col("_ref_key"), "left_anti"
    )

    qualifier = f" where {active_column} = {active_value}" if active_column else ""
    condition = f"{column} must exist in {table}.{reference_column}{qualifier}"
    violations = violating.select(
        _as_str(ctx.pk_column).alias("primary_key_value"),
        F.lit(column).alias("violated_column"),
        _as_str(column).alias("actual_value"),
        F.lit(condition).alias("expected_condition"),
        F.concat(
            F.lit("Value '"), _str_or_null(column),
            F.lit(f"' not found in {table}.{reference_column}{qualifier}."),
        ).alias("violation_detail"),
    )
    return Evaluation(evaluated, violations, condition)


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


def _build_sql(ctx: Context) -> Evaluation:
    """
    Every row returned by the query is a violation.

    Kept for checks that cannot be expressed as a row predicate. Unlike
    `check:`/`when:`, this runs an arbitrary statement.
    """
    cfg = ctx.cfg
    query = cfg.get("query") if isinstance(cfg, dict) else cfg
    if not isinstance(query, str) or not query.strip():
        raise RuleConfigError("'sql' must be a non-empty query.")
    pk_column = cfg.get("pk_column") if isinstance(cfg, dict) else None

    try:
        returned = ctx.spark.sql(query.strip())
    except Exception as exc:
        raise RuleConfigError(f"SQL execution error: {exc}")

    if pk_column and pk_column in returned.columns:
        pk = _as_str(pk_column)
    else:
        returned = returned.withColumn(
            "_row_num", F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))
        )
        pk = _as_str("_row_num")

    detail_columns = [c for c in returned.columns if c != "_row_num"]
    violations = returned.select(
        pk.alias("primary_key_value"),
        F.lit(None).cast("string").alias("violated_column"),
        F.lit(None).cast("string").alias("actual_value"),
        F.lit("Query must return 0 rows").alias("expected_condition"),
        F.to_json(F.struct(*[F.col(c) for c in detail_columns])).alias("violation_detail"),
    )
    # Only the returned rows were examined, so they are both the denominator
    # and the failures — matching how this check has always been reported.
    return Evaluation(returned, violations, "query returns no rows")


def _build_sequence_ordered(ctx: Context) -> Evaluation:
    """Within each group, values must appear in the declared order."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'sequence_ordered' must be a mapping.")
    event_column, group_column, order_column = _require(
        cfg, "event_column", "group_column", "order_column"
    )
    sequence = cfg.get("sequence") or []
    gate = cfg.get("completion_gate") or {}
    _require_columns(ctx.df, event_column, group_column, order_column)

    steps = [s.get("value", "") if isinstance(s, dict) else str(s) for s in sequence]
    if len(steps) < 2:
        raise RuleConfigError("'sequence' must contain at least 2 values.")

    df = _resolve_gate_groups(ctx.df, gate, group_column)

    rank = F.lit(None).cast("int")
    for position, value in reversed(list(enumerate(steps))):
        rank = F.when(F.col(event_column) == value, F.lit(position)).otherwise(rank)

    relevant = (
        df.filter(F.col(event_column).isin(steps) & F.col(order_column).isNotNull())
        .withColumn("_rank", rank)
    )
    # A group needs at least two ranked events before ordering means anything.
    evaluated = (
        relevant.groupBy(group_column)
        .agg(F.count(F.lit(1)).alias("_n"))
        .filter(F.col("_n") >= 2)
        .select(group_column)
    )
    ranked = relevant.join(evaluated, on=group_column, how="inner").withColumn(
        "_previous", F.lag("_rank").over(
            Window.partitionBy(group_column).orderBy(F.col(order_column).asc())
        )
    )
    out_of_order = F.col("_previous").isNotNull() & (F.col("_rank") <= F.col("_previous"))
    violating = (
        ranked.withColumn("_bad", out_of_order.cast("int"))
        .groupBy(group_column)
        .agg(
            F.max("_bad").alias("_has_bad"),
            F.min(F.when(F.col("_bad") == 1, F.col(event_column))).alias("_first_bad"),
        )
        .filter(F.col("_has_bad") == 1)
    )

    condition = "Values must appear in sequence: " + " → ".join(steps)
    violations = violating.select(
        _as_str(group_column).alias("primary_key_value"),
        F.lit(event_column).alias("violated_column"),
        _as_str("_first_bad").alias("actual_value"),
        F.lit(condition).alias("expected_condition"),
        F.concat(
            F.lit("'"), _str_or_null("_first_bad"),
            F.lit(f"' appears out of sequence within {group_column}="),
            _str_or_null(group_column), F.lit("."),
        ).alias("violation_detail"),
    )
    return Evaluation(evaluated, violations, condition)


def _build_pairs_present(ctx: Context) -> Evaluation:
    """Both members of each required pair must occur within the same group."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'pairs_present' must be a mapping.")
    event_column, group_column = _require(cfg, "event_column", "group_column")
    pairs = cfg.get("required_pairs") or []
    mode = cfg.get("mode", "both")
    gate = cfg.get("completion_gate") or {}
    if mode not in ("both", "stop_requires_start"):
        raise RuleConfigError(
            f"Unsupported mode '{mode}'. Allowed: 'both', 'stop_requires_start'."
        )
    _require_columns(ctx.df, event_column, group_column)

    normalised = []
    for index, pair in enumerate(pairs):
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise RuleConfigError(
                f"required_pairs[{index}] must be [start, stop] or [start, [stop, ...]]."
            )
        stops = _normalise_stops(pair[1])
        if not stops:
            raise RuleConfigError(f"required_pairs[{index}]: stop slot must not be empty.")
        normalised.append((pair[0], stops))

    df = _resolve_gate_groups(ctx.df, gate, group_column)
    evaluated = df.filter(F.col(group_column).isNotNull()).select(group_column).distinct()
    if not normalised:
        return Evaluation(evaluated, empty_violations(ctx.spark), "required pairs present")

    events = sorted({e for start, stops in normalised for e in [start] + stops})
    presence = (
        df.filter(F.col(group_column).isNotNull() & F.col(event_column).isin(events))
        .groupBy(group_column)
        .agg(*[
            F.max(F.when(F.col(event_column) == event, F.lit(True)).otherwise(F.lit(False)))
            .alias(f"_has_{i}")
            for i, event in enumerate(events)
        ])
    )
    column_for = {event: f"_has_{i}" for i, event in enumerate(events)}

    per_pair = []
    for start, stops in normalised:
        has_stop = F.col(column_for[stops[0]])
        for stop in stops[1:]:
            has_stop = has_stop | F.col(column_for[stop])
        frame = presence.withColumn("_has_stop", has_stop)
        has_start = F.col(column_for[start])
        stop_label = " or ".join(f"'{s}'" for s in stops)

        if mode == "stop_requires_start":
            failing = F.col("_has_stop") & ~has_start
            condition = f"'{start}' must exist whenever {stop_label} does, per {group_column}"
            actual = F.lit(stop_label)
        else:
            failing = has_start != F.col("_has_stop")
            condition = f"Both '{start}' and {stop_label} must exist per {group_column}"
            actual = F.when(has_start, F.lit(start)).otherwise(F.lit(stop_label))

        per_pair.append(
            frame.filter(failing).select(
                _as_str(group_column).alias("primary_key_value"),
                F.lit(event_column).alias("violated_column"),
                actual.alias("actual_value"),
                F.lit(condition).alias("expected_condition"),
                F.concat(
                    F.lit(f"Incomplete pair ('{start}' / {stop_label}) for {group_column}="),
                    _str_or_null(group_column), F.lit("."),
                ).alias("violation_detail"),
            )
        )

    violations = per_pair[0]
    for extra in per_pair[1:]:
        violations = violations.unionByName(extra)
    return Evaluation(evaluated, violations, "required pairs present")


def _build_gate_complete(ctx: Context) -> Evaluation:
    """Every group must contain at least one row carrying the required event."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'gate_complete' must be a mapping.")
    event_column, group_column, value = _require(
        cfg, "event_column", "group_column", "value"
    )
    order_column = cfg.get("order_column")
    _require_columns(ctx.df, event_column, group_column, order_column)

    evaluated = ctx.df.select(group_column).distinct()
    reached = ctx.df.filter(
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


def _build_group_aggregate_matches(ctx: Context) -> Evaluation:
    """An aggregate over each group must match a reference value on the group."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'group_aggregate_matches' must be a mapping.")
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


RULE_TYPES: dict[str, RuleType] = {
    rule_type.name: rule_type
    for rule_type in (
        RuleType("check", "row", _build_check, "rows"),
        RuleType("unique", "row", _build_unique, "rows"),
        RuleType("exists_in", "row", _build_exists_in, "rows"),
        RuleType("sql", "row", _build_sql, "rows"),
        RuleType("row_count", "table", _build_row_count, "rows"),
        RuleType("sequence_ordered", "group", _build_sequence_ordered, "groups"),
        RuleType("pairs_present", "group", _build_pairs_present, "groups"),
        RuleType("gate_complete", "group", _build_gate_complete, "groups"),
        RuleType(
            "group_aggregate_matches", "group", _build_group_aggregate_matches, "groups"
        ),
    )
}

# Rule types whose primary_key_value is a group key rather than a row key.
GROUP_SCOPED = frozenset(t.name for t in RULE_TYPES.values() if t.scope == "group")

# Reserved rule-level keys that never name a rule type.
_RESERVED_KEYS = frozenset({"rule_id", "name", "description", "when", "pk_column"})


def detect_rule_type(rule: dict) -> str:
    """The rule-type key present on a rule. Raises if not exactly one."""
    present = [key for key in rule if key in RULE_TYPES]
    if len(present) == 1:
        return present[0]
    if not present:
        unknown = sorted(set(rule) - _RESERVED_KEYS)
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


def run_rule(rule: dict, df: DataFrame, spark, pk_column=None, ref_cache=None) -> tuple:
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
        if rule_type.scope == "row" and type_name != "sql":
            if not resolved_pk:
                raise RuleConfigError(
                    "No primary key: set 'pk_column' on the rule or the catalog."
                )
            _require_columns(scoped, resolved_pk)

        evaluation = rule_type.build(
            Context(
                df=scoped, cfg=rule[type_name], rule=rule, spark=spark,
                pk_column=resolved_pk, ref_cache=ref_cache if ref_cache is not None else {},
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
