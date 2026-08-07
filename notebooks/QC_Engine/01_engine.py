# =============================================================================
# QC_Engine
#
# The Quality Catalog validation engine. This notebook only defines functions
# and schemas — run it via `%run QC_Engine`, then call
# `configure(QUALITY_CATALOG_CONFIG, QUALITY_CATALOG_RUNTIME)` before anything
# else.
#
# Cells, in order: runtime helpers, output schemas and violation lifecycle,
# the rule types, and the orchestration.
# =============================================================================

# =============================================================================
# Runtime helpers — settings, target resolution, execution metrics
# =============================================================================


"""Runtime helpers for Quality Catalog operations."""

import re
from types import SimpleNamespace

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _safe_table_name(name: str) -> str:
    """Validate and return a bare table name (no schema prefix).

    Accepts only alphanumeric characters and underscores — the valid set for
    Delta/Hive table names.  Raises ``ValueError`` if the name contains any
    other character, blocking SQL injection via crafted table name values in
    the config.
    """
    bare = name.strip()
    if not re.match(r"^[a-zA-Z0-9_]+$", bare):
        raise ValueError(
            f"Invalid table name '{bare}': only letters, digits, and underscores "
            f"are allowed.  Check the table names in QUALITY_CATALOG_CONFIG."
        )
    return bare


def build_settings(mapping, required_keys: list[str], label: str) -> SimpleNamespace:
    """Validate a settings mapping and return it with attribute access.

    Config reaches the engine as a plain dict defined in the QC_Config
    notebook, which ``%run`` puts in the caller's namespace.  Wrapping it in a
    ``SimpleNamespace`` keeps every read in the engine written as
    ``CONFIG.DEFAULT_SCHEMA`` rather than a subscript, and keeps ``getattr``
    with a default working for optional keys.
    """
    if not isinstance(mapping, dict):
        raise TypeError(f"{label} must be a dict, got {type(mapping).__name__}.")

    missing = sorted(key for key in required_keys if key not in mapping)
    if missing:
        raise RuntimeError(f"Missing required {label} keys: {', '.join(missing)}")

    return SimpleNamespace(**mapping)


def _validate_table_reference(value: str, setting: str) -> str:
    """Check every part of a possibly schema-qualified name.

    Output table names reach Spark SQL through f-strings, so this is the one
    place they can be checked.  `_safe_table_name` was written for exactly that
    and then only ever used on the metrics fallback path, which left a typo in
    QUALITY_CATALOG_CONFIG to surface as a parse error deep in a query rather
    than as a named setting at configure() time.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{setting} must be a non-empty table name.")
    for part in value.split("."):
        try:
            _safe_table_name(part)
        except ValueError as exc:
            raise ValueError(f"{setting}: {exc}") from None
    return value.strip()


def _qualify(table_name: str, default_schema: str) -> str:
    """Prepend default_schema only when table_name is not already schema-qualified."""
    return table_name if "." in table_name else f"{default_schema}.{table_name}"


def resolve_targets(config) -> dict[str, str]:
    schema = _validate_table_reference(config.DEFAULT_SCHEMA, "DEFAULT_SCHEMA")
    results    = _qualify(_validate_table_reference(config.DQ_RESULTS_TABLE,           "DQ_RESULTS_TABLE"),           schema)
    violations = _qualify(_validate_table_reference(config.DQ_VIOLATIONS_TABLE,        "DQ_VIOLATIONS_TABLE"),        schema)
    metrics    = _qualify(_validate_table_reference(config.DQ_EXECUTION_METRICS_TABLE, "DQ_EXECUTION_METRICS_TABLE"), schema)
    return {
        "results_table":           results,
        "violations_table":        violations,
        "execution_metrics_table": metrics,
    }


# The complete set of keys a rule catalog's header may carry.
# `load_rule_catalogs` reads exactly these; preflight
# rejects anything else, so a misspelled `where:` is reported rather than
# silently ignored — which would drop the filter and evaluate every rule
# against rows it never scoped.
# `description` is for whoever reads the YAML and is deliberately unused.
CATALOG_KEYS = frozenset({
    "rule_group",
    "table",
    "database",
    "description",
    "pk_column",
    "where",
    "joins",
    "rules",
})


def classify_retryable_error(message: str | None, runtime_settings) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in runtime_settings.RETRYABLE_ERROR_MARKERS)


_EXECUTION_METRIC_SCHEMA = StructType([
    StructField("script_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("output_target", StringType(), True),
    StructField("artifact_target", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("started_at_utc", TimestampType(), True),
    StructField("finished_at_utc", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("is_retryable", BooleanType(), True),
    StructField("error_message", StringType(), True),
])


def write_execution_metric(spark, table_name: str, payload: dict) -> None:
    row = {
        "script_name": payload.get("script_name"),
        "status": payload.get("status"),
        "output_target": payload.get("output_target"),
        "artifact_target": payload.get("artifact_target"),
        "row_count": int(payload.get("row_count", 0)) if payload.get("row_count") is not None else None,
        "started_at_utc": payload.get("started_at_utc"),
        "finished_at_utc": payload.get("finished_at_utc"),
        "duration_seconds": float(payload.get("duration_seconds")) if payload.get("duration_seconds") is not None else None,
        "is_retryable": payload.get("is_retryable"),
        "error_message": payload.get("error_message"),
    }
    df = spark.createDataFrame([row], schema=_EXECUTION_METRIC_SCHEMA)

    try:
        df.write.mode("append").saveAsTable(table_name)
    except Exception as exc:
        message = str(exc)
        if "." in table_name and any(marker in message.upper() for marker in (
            "SCHEMA_NOT_FOUND",
            "DATABASE_NOT_FOUND",
            "REQUIRES_SINGLE_PART_NAMESPACE",
            "TABLE_OR_VIEW_NOT_FOUND",
        )):
            fallback_table = _safe_table_name(table_name.split(".")[-1])
            try:
                df.write.mode("append").saveAsTable(fallback_table)
                print(
                    "Warning: metrics table namespace was not available for "
                    f"'{table_name}'; wrote metric to fallback table "
                    f"'{fallback_table}'."
                )
                return
            except Exception as fallback_exc:
                print(
                    f"Warning: failed to write execution metric to '{table_name}' "
                    f"and fallback '{fallback_table}': {fallback_exc}"
                )
                return

        print(
            f"Warning: failed to write execution metric to '{table_name}': {exc}"
        )


# =============================================================================
# Output schemas and the violation lifecycle
# =============================================================================


# =============================================================================
# Canonical output schemas, and resolution tracking for the violation log.
#
# The table schemas live here so they are defined once: QC_Setup_Tables
# generates its Delta DDL from them rather than restating the column lists.
#
# Public API
# ----------
# RESULT_SCHEMA                – canonical Spark schema for dq_run_results rows
# VIOLATION_SCHEMA             – canonical Spark schema for dq_violations rows
# _apply_resolution_tracking() – DataFrame-based persistence
# =============================================================================

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Canonical schema for dq_run_results
# ---------------------------------------------------------------------------

RESULT_SCHEMA = StructType([
    StructField("run_id",               StringType(),    False),
    StructField("run_timestamp",        TimestampType(), False),
    StructField("batch_date",           DateType(),      False),
    StructField("rule_group",           StringType(),    False),
    StructField("rule_id",              StringType(),    False),
    StructField("rule_name",            StringType(),    False),
    StructField("table_name",           StringType(),    False),
    StructField("expectation",          StringType(),    False),
    StructField("total_rows",           LongType(),      True),
    StructField("passed_rows",          LongType(),      True),
    StructField("failed_rows",          LongType(),      True),
    StructField("success_pct",          DoubleType(),    True),
    StructField("status",               StringType(),    False),
    StructField("details",              StringType(),    True),
    StructField("rule_duration_seconds", DoubleType(),   True),
    # Populated only when status = 'ERROR'; NULL for PASSED/FAILED rules.
    # Values: 'infrastructure' | 'configuration' | 'source_data'
    StructField("error_category",       StringType(),    True),
])


# ---------------------------------------------------------------------------
# Canonical schema for dq_violations
# ---------------------------------------------------------------------------

VIOLATION_SCHEMA = StructType([
    StructField("run_id",              StringType(),    False),
    StructField("run_timestamp",       TimestampType(), False),
    StructField("batch_date",          DateType(),      False),
    StructField("rule_group",          StringType(),    False),
    StructField("rule_id",             StringType(),    False),
    StructField("rule_name",           StringType(),    False),
    StructField("table_name",          StringType(),    False),
    StructField("primary_key_value",   StringType(),    True),
    StructField("violated_column",     StringType(),    True),
    StructField("actual_value",        StringType(),    True),
    StructField("expected_condition",  StringType(),    True),
    StructField("violation_detail",    StringType(),    True),
    StructField("issue_status",        StringType(),    False),
    # Stored as an ISO-8601 string ("2026-04-03T10:00:00") so that the value
    # can be read in environments without full Delta/Spark type coercion.
    StructField("resolution_timestamp", StringType(),   True),
    # Set once when the violation is first detected; preserved on every subsequent
    # run so violation age can be calculated as (now - first_seen_at).
    StructField("first_seen_at",       TimestampType(), True),
    # "row" — primary_key_value is a PK in table_name; "group" — it's a group key
    # (event_flow, required_event, aggregate_matches).
    StructField("violation_scope",     StringType(),    True),
])


# ---------------------------------------------------------------------------
# Delta DataFrame persistence
# ---------------------------------------------------------------------------

def _apply_resolution_tracking(
    current_violations_df: DataFrame,
    spark_session,
    violations_table: str = "dq_violations",
    run_timestamp: datetime | None = None,
) -> None:
    """
    Persist violations with resolution tracking using pure DataFrame operations.

    Fabric's SQL engine cannot resolve schema-qualified Hive metastore table
    names inside MERGE statements, so all persistence uses the DataFrame API
    (spark.table / DataFrame.write.saveAsTable) which bypasses that limitation.

    Logic applied:
      1. Violations still present → row replaced with current run metadata
         (run_id, run_timestamp, batch_date, violation_detail, actual_value);
         issue_status stays 'Active'.
      2. Brand-new violations → inserted with issue_status = 'Active' and
         resolution_timestamp = NULL.
      3. Previously Active violations absent from this run → issue_status set
         to 'Resolved', resolution_timestamp set to the run timestamp.
      4. Already-Resolved historical rows → kept unchanged.

    Parameters
    ----------
    current_violations_df : Spark DataFrame matching VIOLATION_SCHEMA
    spark_session         : active SparkSession
    violations_table      : fully-qualified table name (e.g. "datakvalitet.dq_violations")
    run_timestamp         : timestamp to record for resolutions
                            (defaults to datetime.now(timezone.utc))
    """
    _REQUIRED_COLUMNS = {
        "rule_id", "primary_key_value", "violated_column",
        "expected_condition", "issue_status",
    }
    missing = _REQUIRED_COLUMNS - set(current_violations_df.columns)
    if missing:
        raise ValueError(
            f"_apply_resolution_tracking: input DataFrame is missing required "
            f"columns: {sorted(missing)}"
        )

    ts = (run_timestamp or datetime.now(timezone.utc)).isoformat()

    # violated_column and expected_condition are nullable; replace NULL with a
    # sentinel for joining so two NULL values are treated as the same key.
    #
    # expected_condition is part of the key because group-style expectations
    # (e.g. event_flow) emit several distinct violations for the same
    # (rule_id, primary_key_value, violated_column) — one per required pair —
    # differing only in expected_condition.  Without it those rows would collapse
    # to one under dropDuplicates/left-anti and the extra violations would be
    # lost.  expected_condition is a deterministic rule/pair-level string (it
    # never contains per-row data), so keying on it keeps resolution stable.
    _SENTINEL = "__NULL__"
    _jk = ["rule_id", "primary_key_value", "_vk", "_ek"]

    def _with_join_key(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("_vk", F.coalesce(F.col("violated_column"), F.lit(_SENTINEL)))
            .withColumn("_ek", F.coalesce(F.col("expected_condition"), F.lit(_SENTINEL)))
        )

    try:
        merge_key = ["rule_id", "primary_key_value", "violated_column", "expected_condition"]
        current_violations_df = current_violations_df.dropDuplicates(merge_key)

        # Break the read's lineage to violations_table before the final write
        # targets the same table, otherwise Spark's analyzer rejects the write
        # with UNSUPPORTED_OVERWRITE.TABLE ("can't overwrite the target that is
        # also being read from") even though the write only happens afterwards.
        existing_df     = spark_session.table(violations_table).localCheckpoint(eager=True)
        existing_active = existing_df.filter(F.col("issue_status") == "Active")
        # Everything that is not Active is carried through unchanged.  Use a
        # NULL-safe negation so legacy rows with a NULL issue_status (e.g. rows
        # predating the column) are preserved rather than dropped on rewrite.
        existing_other  = existing_df.filter(
            ~(F.col("issue_status") == "Active") | F.col("issue_status").isNull()
        )

        curr_jk = _with_join_key(current_violations_df)
        act_jk  = _with_join_key(existing_active)

        # Violations not previously active → insert as Active
        brand_new = (
            curr_jk
            .join(act_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk", "_ek")
        )

        # Still-active violations → refresh run metadata but preserve first_seen_at
        # from the existing row so violation age is measured from initial detection.
        _orig_first_seen = act_jk.select(_jk + ["first_seen_at"]).withColumnRenamed(
            "first_seen_at", "_orig_first_seen_at"
        )
        still_active = (
            curr_jk
            .join(_orig_first_seen, on=_jk, how="inner")
            .withColumn("first_seen_at", F.col("_orig_first_seen_at"))
            .drop("_orig_first_seen_at", "_vk", "_ek")
        )

        # Previously active, absent from current run → mark Resolved
        stale_active = (
            act_jk
            .join(curr_jk.select(_jk), on=_jk, how="left_anti")
            .drop("_vk", "_ek")
            .withColumn("issue_status", F.lit("Resolved"))
            .withColumn("resolution_timestamp", F.lit(ts))
        )

        final_df = (
            existing_other
            .unionByName(stale_active)
            .unionByName(still_active)
            .unionByName(brand_new)
        )

        final_df.write.mode("overwrite").saveAsTable(violations_table)
        print(f"  Resolution tracking applied on '{violations_table}'.")

    except Exception as exc:
        raise RuntimeError(
            f"Violations not written. Original error: {exc}"
        ) from exc


# =============================================================================
# Rule types — what counts as a violation
# =============================================================================


# =============================================================================
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

import re
from dataclasses import dataclass
from typing import Any, Callable

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
        expr = F.expr(expression)
        jc = getattr(expr, "_jc", None)
        node = jc.expr() if jc is not None else None
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


def _comparison_parts(expression: str) -> tuple[str, str] | None:
    """Return the operator and RHS for a simple comparison predicate."""
    match = re.match(r"^\s*([A-Za-z_][\w\.]*)\s*(>=|<=|>|<|=|!=)\s*(.+?)\s*$", expression)
    if not match:
        return None
    return match.group(2), match.group(3).strip()


def _comparison_phrase(operator: str) -> str:
    return {
        ">=": "is less than the required value",
        ">": "is less than or equal to the required value",
        "<=": "is greater than the required value",
        "<": "is greater than or equal to the required value",
        "=": "does not match the required value",
        "!=": "matches the required value",
    }.get(operator, "does not satisfy the required value")


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

    if not isinstance(event_column, str) or not event_column:
        raise RuleConfigError("completion_gate.event_column must be a non-empty string.")

    predicate = F.col(event_column).isin(values)
    if order_column:
        predicate = predicate & F.col(order_column).isNotNull()
    return predicate


def _resolve_gate_groups(
    df: DataFrame,
    gate: dict,
    group_column: str,
    fallback_column: str | None = None,
    fallback_values: list | None = None,
) -> DataFrame:
    """
    Narrow to rows belonging to groups that have reached the gate event — or,
    for a group the gate never fired on, one that reached `fallback_values`
    instead (the flow's own `ends_with`).

    A handler does not always remember to set the gate milestone. Without the
    fallback, a group that never got gated in is excluded forever, even after
    it has genuinely closed — silently dropping coverage rather than merely
    delaying it. `ends_with` already means "the event(s) that close this
    flow", so reusing it here is the same concept read twice, not a second
    one: once a case reaches its own closing event it is evaluated regardless
    of whether the gate fired first.

    An absent or incomplete gate leaves the frame untouched, so an
    ungated rule evaluates every group.
    """
    if not gate or not gate.get("event_column") or not _as_list(gate.get("value")):
        return df

    event_column = gate.get("event_column")
    order_column = gate.get("order_column")
    if not isinstance(event_column, str) or not event_column:
        raise RuleConfigError("completion_gate.event_column must be a non-empty string.")
    if order_column is not None and not isinstance(order_column, str):
        raise RuleConfigError("completion_gate.order_column must be a string if provided.")

    _require_columns(df, *(c for c in (event_column, order_column) if c))

    ready = df.filter(_gate_predicate(gate)).select(group_column).distinct()
    if fallback_column and fallback_values:
        closed = (
            df.filter(F.col(fallback_column).isin(fallback_values))
            .select(group_column).distinct()
        )
        ready = ready.unionByName(closed).distinct()
    return df.join(ready, on=group_column, how="inner")


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
    spark: Any
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


# =============================================================================
# 2. Rule-type builders
# =============================================================================

def _build_check(ctx: Context) -> Evaluation:
    """A single boolean predicate, evaluated per row with SQL CHECK semantics."""
    if ctx.pk_column is None:
        raise RuleConfigError("Missing pk_column configuration.")

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
            F.lit("; expected "),
            F.lit(expression),
        )
        comparison = _comparison_parts(expression)
        if comparison is not None:
            operator, rhs = comparison
            detail = F.concat(
                detail,
                F.lit("; "),
                F.lit(subject),
                F.lit(" "),
                _str_or_null(subject),
                F.lit(f" {_comparison_phrase(operator)} "),
                F.lit(rhs),
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
    if ctx.pk_column is None:
        raise RuleConfigError("Missing pk_column configuration.")

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


def _build_row_count(ctx: Context) -> Evaluation:
    """The scoped table must have a row count within configured bounds."""
    cfg = ctx.cfg
    if not isinstance(cfg, dict):
        raise RuleConfigError("'row_count' must be a mapping.")

    minimum_raw, maximum_raw = _require(cfg, "minimum", "maximum")
    try:
        minimum = int(minimum_raw)
        maximum = int(maximum_raw)
    except (TypeError, ValueError):
        raise RuleConfigError(
            f"'minimum' and 'maximum' must be integers, got {minimum_raw!r} and {maximum_raw!r}."
        )
    if minimum > maximum:
        raise RuleConfigError(
            f"'minimum' ({minimum}) cannot be greater than 'maximum' ({maximum})."
        )

    count = ctx.df.count()
    evaluated = ctx.spark.createDataFrame([(count,)], "row_count long")

    if minimum <= count <= maximum:
        violations = empty_violations(ctx.spark)
    else:
        expected = f"{minimum} <= row_count <= {maximum}"
        detail = f"Table has {count} rows; expected between {minimum} and {maximum}."
        violations = ctx.spark.createDataFrame(
            [(None, "row_count", str(count), expected, detail)],
            _VIOLATION_SCHEMA,
        )

    return Evaluation(evaluated, violations, f"row_count in [{minimum}, {maximum}]")


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
    if not isinstance(event_column, str) or not event_column:
        raise RuleConfigError("'event_column' must be a non-empty string.")
    if not isinstance(group_column, str) or not group_column:
        raise RuleConfigError("'group_column' must be a non-empty string.")
    if not isinstance(order_column, str) or not order_column:
        raise RuleConfigError("'order_column' must be a non-empty string.")
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

    df = _resolve_gate_groups(
        ctx.df, gate, group_column,
        fallback_column=event_column, fallback_values=ends_with,
    )
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

    expected_event_for_cycle = F.lit(None).cast("string")
    for idx, value in enumerate(cycle):
        expected_event_for_cycle = F.when(
            (((F.col("_cpos") - 1) % F.lit(width)) + 1) == idx + 1,
            F.lit(value),
        ).otherwise(expected_event_for_cycle)

    next_expected_event = F.lit(None).cast("string")
    for idx, value in enumerate(cycle):
        next_value = cycle[(idx + 1) % width]
        next_expected_event = F.when(
            (((F.col("_cpos") - 1) % F.lit(width)) + 1) == idx + 1,
            F.lit(next_value),
        ).otherwise(next_expected_event)

    offenders = (
        positioned.filter(anchor_problem)
        .withColumn("_expected_event", F.lit(None).cast("string"))
        .select(group_column, order_column, event_column, "_expected_event")
        .unionByName(
            cycle_only.filter(misplaced)
            .withColumn("_expected_event", expected_event_for_cycle)
            .select(group_column, order_column, event_column, "_expected_event")
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

    structural_expectations = (
        cycle_only.withColumn("_expected_event", next_expected_event)
        .withColumn("_row_number", F.row_number().over(Window.partitionBy(group_column).orderBy(*ordering)))
        .withColumn("_last_row_number", F.max("_row_number").over(Window.partitionBy(group_column)))
        .filter(F.col("_row_number") == F.col("_last_row_number"))
        .select(group_column, "_expected_event")
        .join(structural, on=group_column, how="inner")
        .select(group_column, F.col("_expected_event").alias("_structural_expected_event"))
    )

    # One row per failing group, naming the earliest event that broke the flow.
    first_offender = (
        offenders.groupBy(group_column)
        .agg(
            F.min(F.struct(F.col(order_column), F.col(event_column), F.col("_expected_event"))).alias("_first")
        )
        .select(
            group_column,
            F.col(f"_first.{event_column}").alias("_bad_event"),
            F.col("_first._expected_event").alias("_expected_event"),
        )
    )
    violating = (
        offenders.select(group_column)
        .unionByName(structural)
        .distinct()
        .join(first_offender, on=group_column, how="left")
        .join(structural_expectations, on=group_column, how="left")
        .withColumn(
            "_expected_event",
            F.coalesce(F.col("_expected_event"), F.col("_structural_expected_event")),
        )
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
        F.when(
            F.col("_bad_event").isNotNull() & F.col("_expected_event").isNotNull(),
            F.concat(
                F.lit("Unexpected event '"),
                _as_str("_bad_event"),
                F.lit("' in the sequence; expected the next event to be '"),
                _as_str("_expected_event"),
                F.lit("'."),
            ),
        ).when(
            F.col("_expected_event").isNotNull() & F.col("_bad_event").isNull(),
            F.concat(
                F.lit("A flow started but did not complete; expected to continue as "),
                F.lit(flow),
                F.lit("; expected the next event to be '"),
                _as_str("_expected_event"),
                F.lit("'."),
            ),
        ).when(
            F.col("_bad_event").isNotNull(),
            F.concat(
                F.lit("Unexpected event '"),
                _as_str("_bad_event"),
                F.lit("' in the sequence; expected "),
                F.lit(flow),
            ),
        ).otherwise(
            F.concat(
                F.lit("A flow started but did not complete; expected to continue as "),
                F.lit(flow),
                F.lit("."),
            )
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
    if not isinstance(event_column, str) or not event_column:
        raise RuleConfigError("'event_column' must be a non-empty string.")
    if not isinstance(group_column, str) or not group_column:
        raise RuleConfigError("'group_column' must be a non-empty string.")
    if order_column is not None and not isinstance(order_column, str):
        raise RuleConfigError("'order_column' must be a string if provided.")
    _require_columns(ctx.df, event_column, group_column, *(c for c in (order_column,) if c))

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
        RuleType(
            "row_count", "table", _build_row_count, "tables",
            required=("minimum", "maximum"),
        ),
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
        if rule_type.scope == "group":
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


# =============================================================================
# Orchestration — run every catalog and write the outputs
# =============================================================================


# =============================================================================
# Data quality validation engine for the PBE Quality Catalog.
#
# Flow:
#   1. Load active rules from the YAML catalogs in RULE_CATALOG_SOURCES.
#   2. For each rule group, load the source table from the Spark metastore
#      and apply any configured pre-joins.
#   3. For each rule:
#        a. Dispatch to the matching rule type in the rule-type registry.
#        b. Collect per-rule results (counts, success %, status).
#        c. Collect per-row violation details.
#   4. Write summary rows to dq_run_results (Delta table).
#   5. Write violation rows to dq_violations via DataFrame-based resolution tracking.
#   6. Write execution metrics to dq_execution_metrics.
#
# Schedule: nightly (after source tables are refreshed).
# Prerequisites: QC_Setup_Tables must have been run at least once.
# =============================================================================

import concurrent.futures
import time
import uuid

from datetime import datetime, date, timezone
from functools import reduce

from pyspark.sql import SparkSession, functions as F


CONFIG_REQUIRED_KEYS = [
    "DEFAULT_SCHEMA",
    "DQ_RESULTS_TABLE",
    "DQ_VIOLATIONS_TABLE",
    "DQ_EXECUTION_METRICS_TABLE",
]
RUNTIME_REQUIRED_KEYS = [
    "FAIL_ON_EMPTY_SOURCE",
    "RETRYABLE_ERROR_MARKERS",
    "MAX_RULE_RETRIES",
    "RULE_TIMEOUT_SECONDS",
]

# Set by configure(); every entry point calls it before running anything.
# Nothing here runs at load time: this notebook is `%run` as a library, and a
# library that opens a Spark session or reads config on load is a library that
# cannot be loaded before its config exists.
CONFIG = None
RUNTIME = None
TARGETS = None
spark = None

# Set by run_quality_catalog() at the start of each run, not at load time —
# this module is a library that may be loaded long before it is used.
RUN_ID = None
RUN_TIMESTAMP = None
BATCH_DATE = None


def configure(config_mapping: dict, runtime_mapping: dict) -> None:
    """Bind the engine to a config/runtime pair and resolve output targets.

    Both arguments are the plain dicts defined in the QC_Config notebook.
    """
    global CONFIG, RUNTIME, TARGETS, spark

    spark = SparkSession.builder.getOrCreate()
    spark.sql("SET spark.sql.ansi.enabled = false")

    CONFIG = build_settings(config_mapping, CONFIG_REQUIRED_KEYS, "QUALITY_CATALOG_CONFIG")
    RUNTIME = build_settings(runtime_mapping, RUNTIME_REQUIRED_KEYS, "QUALITY_CATALOG_RUNTIME")
    TARGETS = resolve_targets(CONFIG)

    print("Rule types loaded:", sorted(RULE_TYPES))
    print(f"Results   : {TARGETS['results_table']}")
    print(f"Violations: {TARGETS['violations_table']}")
    print(f"Metrics   : {TARGETS['execution_metrics_table']}")


def _require_configured() -> None:
    if TARGETS is None or spark is None:
        raise RuntimeError(
            "The engine is not configured. Call "
            "configure(QUALITY_CATALOG_CONFIG, QUALITY_CATALOG_RUNTIME) first."
        )


# Helper functions
# -----------------------------------------------------------------------------
def _normalize_join_cfg(join_cfg: dict) -> dict:
    """PyYAML 1.1 parses bare 'on' as boolean True. Restore it to the string key."""
    if isinstance(join_cfg, dict) and True in join_cfg and "on" not in join_cfg:
        result = dict(join_cfg)
        result["on"] = result.pop(True)
        return result
    return join_cfg


def load_rule_catalogs(rule_sources: dict) -> list[dict]:
    """
    Load rule catalogs from YAML text, keyed by catalog name.

    ``rule_sources`` is RULE_CATALOG_SOURCES from the QC_Rules notebook: a
    mapping of catalog name to the YAML document as a string.  Each document
    must have a top-level structure:

        rule_group: <str>
        table:      <str>
        database:   <str>          # optional
        pk_column:  <str>          # row key, shared by every rule in the file
        where:      <sql>          # optional row filter for the whole catalog
        joins:      [...]          # optional
        rules:      [...]

    Catalogs are loaded in alphabetical order of their key.
    """
    import yaml as _yaml

    if not isinstance(rule_sources, dict):
        raise TypeError(
            "RULE_CATALOG_SOURCES must be a dict of catalog name to YAML text, "
            f"got {type(rule_sources).__name__}."
        )
    if not rule_sources:
        raise RuntimeError(
            "RULE_CATALOG_SOURCES is empty. Ensure the QC_Rules notebook ran "
            "(%run QC_Rules) and defines at least one catalog."
        )
    print(f"  Rule catalogs found: {len(rule_sources)}")

    catalogs = []
    unusable: list[str] = []
    total_rules = 0
    for name in sorted(rule_sources):
        try:
            doc = _yaml.safe_load(rule_sources[name])
        except Exception as exc:
            unusable.append(f"{name}: could not be parsed ({exc})")
            continue

        if not isinstance(doc, dict):
            unusable.append(f"{name}: did not parse to a mapping")
            continue

        rules = doc.get("rules") or []
        if not rules:
            unusable.append(f"{name}: contains no rules")
            continue

        catalog = {
            "rule_group": doc.get("rule_group") or name,
            "table":      doc.get("table", ""),
            "database":   doc.get("database", ""),
            "pk_column":  doc.get("pk_column"),
            "joins":      [_normalize_join_cfg(j) for j in (doc.get("joins") or [])],
            "where":      doc.get("where"),
            "rules":      rules,
        }
        catalogs.append(catalog)
        total_rules += len(rules)
        print(f"  Rule group: {catalog['rule_group']} ({len(rules)} rules)  [{name}]")

    # A catalog that cannot be loaded is a silent loss of coverage, not a warning.
    # Skipping it would leave the run reporting Succeeded over fewer rules, and the
    # quality score can *rise*, because the rules that disappeared included the
    # failing ones. Nobody reads stdout at 03:00, so fail the run instead.
    if unusable:
        raise RuntimeError(
            "Rule catalogs in RULE_CATALOG_SOURCES could not be loaded:\n"
            + "\n".join(f"  - {item}" for item in unusable)
            + "\nFix or remove the catalog cell. Running without it would report a "
            "quality score over fewer rules than intended."
        )

    if not catalogs:
        raise RuntimeError("No valid rule catalogs loaded from RULE_CATALOG_SOURCES.")

    print(f"  Total: {total_rules} rules across {len(catalogs)} catalog(s).")
    return catalogs


_INFRA_ERROR_MARKERS = ["timeout", "connection", "unavailable", "throttle"]
_SOURCE_ERROR_MARKERS = [
    "source table is empty", "table not found", "table or view not found",
    "no such table", "path does not exist",
]


def _classify_error_category(status: str, details: str | None) -> str | None:
    if status != "ERROR":
        return None
    lowered = (details or "").lower()
    if any(m in lowered for m in _INFRA_ERROR_MARKERS):
        return "infrastructure"
    if any(m in lowered for m in _SOURCE_ERROR_MARKERS):
        return "source_data"
    return "configuration"


def _empty_results():
    return spark.createDataFrame([], RESULT_SCHEMA)


def _empty_violations():
    return spark.createDataFrame([], VIOLATION_SCHEMA)


def _align_df_to_table_schema(df, table_name: str):
    """
    Align a DataFrame to an existing Delta table schema.

    - Drops columns that are not present in the target table.
    - Adds missing target columns as NULL cast to the target data type.
    - Reorders columns to the target table order.
    """
    target_schema = spark.table(table_name).schema
    target_fields = {field.name: field for field in target_schema.fields}
    target_names = [field.name for field in target_schema.fields]
    source_names = set(df.columns)

    extra_cols = sorted(col for col in df.columns if col not in target_fields)
    if extra_cols:
        print(f"  Note: dropping columns not in {table_name}: {extra_cols}")

    aligned = df
    for field in target_schema.fields:
        if field.name not in source_names:
            aligned = aligned.withColumn(field.name, F.lit(None).cast(field.dataType))

    return aligned.select(*target_names)


def _resolve_catalog_where(catalog: dict, runtime_settings) -> str | None:
    """Catalog-level `where:` predicate, honouring a per-rule_group override.

    An override whose value is None disables the catalog's own filter.
    """
    overrides = getattr(runtime_settings, "CATALOG_FILTER_OVERRIDES", {}) or {}
    if not isinstance(overrides, dict):
        raise ValueError("CATALOG_FILTER_OVERRIDES must be a dict keyed by rule_group.")

    rule_group = catalog.get("rule_group")
    if rule_group in overrides:
        return overrides[rule_group]
    return catalog.get("where")


def _apply_catalog_where(source_df, where_clause: str | None, rule_group: str):
    """Apply the catalog-level row filter and return (filtered_df, description)."""
    if not where_clause:
        return source_df, None
    if not isinstance(where_clause, str):
        raise ValueError(f"[{rule_group}] 'where' must be a SQL predicate string.")
    return source_df.filter(F.expr(where_clause)), where_clause


def run_validation(
    rule_catalog: dict,
    source_df,
    pk_col: str,
) -> tuple:
    """
    Validate source_df against all rules in rule_catalog.

    Parameters
    ----------
    rule_catalog : dict loaded from YAML rule files (one rule group)
    source_df    : full Spark DataFrame to validate
    pk_col       : primary key column of source_df (stored as primary_key_value
                   in each violation row)

    Returns
    -------
    results_df    Spark DataFrame matching RESULT_SCHEMA
    violations_df Spark DataFrame matching VIOLATION_SCHEMA
    """
    rule_group = rule_catalog["rule_group"]
    table_name = rule_catalog["table"]
    rules      = rule_catalog.get("rules", [])

    all_results    = []
    violation_dfs  = []

    for rule in rules:
        rule_id   = rule.get("rule_id", "?")
        rule_name = rule.get("name", "")
        try:
            exp_name = detect_rule_type(rule)
        except RuleConfigError as exc:
            exp_name = "?"
            print(f"  → [{rule_id}] {rule_name} ... ERROR")
            all_results.append((
                RUN_ID, RUN_TIMESTAMP, BATCH_DATE, rule_group, rule_id, rule_name,
                table_name, exp_name, 0, 0, 0, 0.0, "ERROR",
                f"[{table_name}/{rule_id}] {exc}", 0.0, "configuration",
            ))
            continue

        print(f"  → [{rule_id}] {rule_name} ({exp_name}) ... ", end="")
        _rule_start = time.perf_counter()

        _timeout_s   = RUNTIME.RULE_TIMEOUT_SECONDS
        _max_retries = RUNTIME.MAX_RULE_RETRIES
        for _attempt in range(_max_retries + 1):
            try:
                result, viols_spark = _run_validator(
                    source_df, rule, spark, pk_col, _timeout_s
                )
                break
            except concurrent.futures.TimeoutError:
                result = {
                    "total_rows":  0,
                    "passed_rows": 0,
                    "failed_rows": 0,
                    "success_pct": 0.0,
                    "status":      "ERROR",
                    "details":     f"[{table_name}/{rule_id}] Timed out after {_timeout_s}s.",
                }
                viols_spark = None
                break  # do not retry on timeout
            except Exception as exc:
                if _attempt < _max_retries and classify_retryable_error(str(exc), RUNTIME):
                    print(
                        f"\n    Retrying [{rule_id}] "
                        f"(attempt {_attempt + 1}/{_max_retries}): {exc}"
                    )
                    time.sleep(2 ** (_attempt + 1))
                    continue
                result = {
                    "total_rows":  0,
                    "passed_rows": 0,
                    "failed_rows": 0,
                    "success_pct": 0.0,
                    "status":      "ERROR",
                    "details":     f"[{table_name}/{rule_id}/{exp_name}] Error: {exc}",
                }
                viols_spark = None
                break

        _rule_elapsed = time.perf_counter() - _rule_start
        print(f"{result['status']} ({_rule_elapsed:.2f}s)")

        all_results.append((
            RUN_ID,
            RUN_TIMESTAMP,
            BATCH_DATE,
            rule_group,
            rule_id,
            rule_name,
            table_name,
            exp_name,
            result["total_rows"],
            result["passed_rows"],
            result["failed_rows"],
            result["success_pct"],
            result["status"],
            result["details"],
            round(_rule_elapsed, 3),
            _classify_error_category(result["status"], result["details"]),
        ))

        if viols_spark is not None:
            viols_spark = viols_spark.select(
                F.lit(RUN_ID).alias("run_id"),
                F.lit(RUN_TIMESTAMP).alias("run_timestamp"),
                F.lit(str(BATCH_DATE)).cast("date").alias("batch_date"),
                F.lit(rule_group).alias("rule_group"),
                F.lit(rule_id).alias("rule_id"),
                F.lit(rule_name).alias("rule_name"),
                F.lit(table_name).alias("table_name"),
                F.col("primary_key_value"),
                F.col("violated_column"),
                F.col("actual_value"),
                F.col("expected_condition"),
                F.col("violation_detail"),
                F.lit("Active").alias("issue_status"),
                F.lit(None).cast("string").alias("resolution_timestamp"),
                # resolution.py preserves this value for still-active violations;
                # brand-new violations use the current run timestamp as their origin.
                F.lit(RUN_TIMESTAMP).alias("first_seen_at"),
                F.lit("group" if exp_name in GROUP_SCOPED else "row").alias("violation_scope"),
            )
            violation_dfs.append(viols_spark)

    results_df = spark.createDataFrame(all_results, schema=RESULT_SCHEMA)
    all_violations = (
        reduce(lambda a, b: a.unionByName(b), violation_dfs)
        if violation_dfs
        else _empty_violations()
    )
    return results_df, all_violations


def _run_validator(
    source_df,
    rule: dict,
    spark_session,
    pk_col,
    timeout_s: float,
) -> tuple:
    """Execute one rule in a bounded-time background thread.

    On timeout, raises ``concurrent.futures.TimeoutError`` and releases the
    executor without waiting.  The underlying PySpark/JVM job continues to
    completion in the background — it cannot be cancelled at the Python level,
    but the main run proceeds without blocking.

    When a timeout does *not* occur the thread is already finished by the time
    we call ``shutdown(wait=True)``, so there is no extra blocking cost.
    """
    call = lambda: run_rule(rule, source_df, spark_session, pk_col)

    _executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    _future = _executor.submit(call)
    _timed_out = False
    try:
        return _future.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError:
        _timed_out = True
        raise
    finally:
        _executor.shutdown(wait=not _timed_out)


def run_quality_catalog(rule_sources: dict) -> tuple[int, int]:
    global RUN_ID, RUN_TIMESTAMP, BATCH_DATE

    _require_configured()

    RUN_ID = str(uuid.uuid4())
    RUN_TIMESTAMP = datetime.now(timezone.utc)
    BATCH_DATE = date.today()

    print(f"Run ID    : {RUN_ID}")
    print(f"Timestamp : {RUN_TIMESTAMP.isoformat()}")
    print(f"Batch date: {BATCH_DATE}")

    print("\nLoading rule catalogs…")
    all_catalogs = load_rule_catalogs(rule_sources)
    result_dfs    = []
    violation_dfs = []

    for catalog in all_catalogs:
        rule_group = catalog["rule_group"]
        table_name = catalog["table"]
        database = catalog.get("database", "")
        pk_col = catalog.get("pk_column")
        joins_cfg = catalog.get("joins", [])

        full_table = f"{database}.{table_name}" if database else table_name
        print(f"\n=== {rule_group.upper()} VALIDATIONS ({full_table}) ===")

        source_df = spark.read.table(full_table)
        source_count = source_df.count()
        if source_count == 0 and RUNTIME.FAIL_ON_EMPTY_SOURCE:
            raise RuntimeError(f"Source table is empty: {full_table}")
        print(f"  Source rows: {source_count:,}")

        for join_cfg in joins_cfg:
            if not isinstance(join_cfg, dict):
                print(f"  Warning: skipping invalid join config (not a dict): {join_cfg}")
                continue

            join_table = join_cfg.get("table")
            if not join_table:
                print(f"  Warning: skipping join config without table: {join_cfg}")
                continue

            join_how = join_cfg.get("how", "left")
            join_select = join_cfg.get("select")
            join_on = join_cfg.get("on")
            left_on = join_cfg.get("left_on")
            right_on = join_cfg.get("right_on")

            join_df = spark.read.table(join_table)
            if join_select:
                select_cols = list(join_select)
                # Ensure join key is present when using left_on/right_on format.
                if right_on and right_on not in select_cols:
                    select_cols.append(right_on)
                join_df = join_df.select(*select_cols)

            if join_on not in (None, ""):
                source_df = source_df.join(join_df, on=join_on, how=join_how)
                print(f"  Joined with {join_table} on {join_on} ({join_how})")
            elif left_on and right_on:
                source_df = source_df.join(join_df, source_df[left_on] == join_df[right_on], how=join_how)
                print(f"  Joined with {join_table} on {left_on}={right_on} ({join_how})")
            else:
                print(
                    "  Warning: skipping join config without key(s). "
                    f"Expected 'on' or both 'left_on'/'right_on': {join_cfg}"
                )

        effective_where = _resolve_catalog_where(catalog, RUNTIME)
        source_df, filter_desc = _apply_catalog_where(source_df, effective_where, rule_group)
        if filter_desc:
            print(f"  Applied catalog filter: {filter_desc}")

        # Cache source DataFrame so that each rule in this catalog reads it
        # from memory rather than re-scanning the table (and re-executing any
        # joins) on every rule.
        source_df = source_df.cache()
        scoped_count = source_df.count()  # materialise the cache before rules run
        print(f"  Rows after joins/filters: {scoped_count:,}")

        try:
            results_df, violations_df = run_validation(
                rule_catalog=catalog,
                source_df=source_df,
                pk_col=pk_col,
            )
        finally:
            source_df.unpersist()

        result_dfs.append(results_df)
        violation_dfs.append(violations_df)

    all_results_combined = (
        reduce(lambda a, b: a.unionByName(b), result_dfs)
        if result_dfs
        else _empty_results()
    )
    all_violations_combined = (
        reduce(lambda a, b: a.unionByName(b), violation_dfs)
        if violation_dfs
        else _empty_violations()
    )

    all_results_combined.cache()
    all_violations_combined.cache()
    results_count    = all_results_combined.count()
    violations_count = all_violations_combined.count()

    # Print the top-5 slowest rules to help identify bottlenecks.
    if results_count > 0:
        print("\n--- Top-5 slowest rules (by rule_duration_seconds) ---")
        all_results_combined.orderBy(
            "rule_duration_seconds", ascending=False
        ).select("rule_id", "rule_name", "status", "rule_duration_seconds").show(
            5, truncate=False
        )

    print("\nWriting results to Delta tables…")

    results_to_write = _align_df_to_table_schema(all_results_combined, TARGETS["results_table"])
    results_to_write.write.mode("append").saveAsTable(TARGETS["results_table"])
    print(f"  {TARGETS['results_table']} : {results_count} rows written.")

    _apply_resolution_tracking(
        all_violations_combined,
        spark_session=spark,
        violations_table=TARGETS["violations_table"],
        run_timestamp=RUN_TIMESTAMP,
    )
    print(f"  {TARGETS['violations_table']} : {violations_count} violations processed.")

    print("\n=== DATA QUALITY RUN SUMMARY ===")
    spark.createDataFrame([(RUN_ID,)], ["_run_id"]).createOrReplaceTempView("_dq_run_id")
    spark.sql(
        f"""
        SELECT
            rule_group,
            COUNT(*)                                                    AS total_rules,
            SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END)         AS passed,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)         AS failed,
            SUM(CASE WHEN status = 'ERROR'  THEN 1 ELSE 0 END)         AS errors,
            ROUND(
                SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END)
                * 100.0 / COUNT(*), 1
            )                                                           AS quality_score_pct
        FROM {TARGETS['results_table']}
        WHERE run_id = (SELECT _run_id FROM _dq_run_id)
        GROUP BY rule_group
        ORDER BY rule_group
        """
    ).show(truncate=False)
    try:
        spark.catalog.dropTempView("_dq_run_id")
    except Exception:
        pass  # best-effort cleanup; non-fatal

    all_results_combined.unpersist()
    all_violations_combined.unpersist()
    return results_count, violations_count


def run_with_metrics(rule_sources: dict, script_name: str) -> tuple[int, int]:
    """Run the catalog and record the outcome in dq_execution_metrics.

    A crashed run must still leave evidence of why, so the failure metric is
    written before the original exception is re-raised.
    """
    _require_configured()

    started = datetime.now(timezone.utc)
    try:
        results_count, violations_count = run_quality_catalog(rule_sources)
    except Exception as exc:
        finished = datetime.now(timezone.utc)
        write_execution_metric(
            spark,
            TARGETS["execution_metrics_table"],
            {
                "script_name": script_name,
                "status": "Failed",
                "output_target": TARGETS["results_table"],
                "artifact_target": TARGETS["violations_table"],
                "row_count": 0,
                "started_at_utc": started,
                "finished_at_utc": finished,
                "duration_seconds": float((finished - started).total_seconds()),
                "is_retryable": classify_retryable_error(str(exc), RUNTIME),
                "error_message": str(exc)[:4000],
            },
        )
        raise

    finished = datetime.now(timezone.utc)
    write_execution_metric(
        spark,
        TARGETS["execution_metrics_table"],
        {
            "script_name": script_name,
            "status": "Succeeded",
            "output_target": TARGETS["results_table"],
            "artifact_target": TARGETS["violations_table"],
            "row_count": int(results_count),
            "started_at_utc": started,
            "finished_at_utc": finished,
            "duration_seconds": float((finished - started).total_seconds()),
            "is_retryable": False,
            "error_message": None,
        },
    )
    print(f"Validation finished at {finished.isoformat()}")
    print(f"Duration seconds: {(finished - started).total_seconds():.1f}")
    return results_count, violations_count
