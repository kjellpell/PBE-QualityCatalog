"""
Preflight checks for the Quality Catalog validation runner.

Catches at deploy time what would otherwise fail at 03:00 in a scheduled job:
missing source tables, misconfigured rules, and — most usefully — predicates
that do not resolve against the real table schema. Predicates are validated by
Spark's own analyzer rather than a hand-maintained list of column-bearing
parameter names, so a typo in a `check:` or `where:` fails here.

The rule contract itself is read from engine.expectations.RULE_TYPES, so it
cannot drift from what the engine will actually run.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _resolve_repo_root() -> Path:
    """Locate the repo root so `engine` is importable. Must run before that import."""
    if "__file__" in globals():
        return Path(__file__).resolve().parent

    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents, Path("/lakehouse/default/Files")]:
        if (candidate / "engine").exists():
            return candidate
    return cwd


REPO_ROOT = _resolve_repo_root()
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from engine.expectations import PREDICATE_KEYS, RULE_TYPES, detect_rule_type
    from engine.runtime import (
        load_config_module,
        require_config_keys,
        resolve_rules_dir,
        resolve_targets,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - deployment problem
    raise ModuleNotFoundError(
        f"Could not import the engine package from {REPO_ROOT}. "
        "Preflight and the runner must share one implementation, so it does not "
        "carry a fallback copy. Ensure the engine/ directory is deployed next to "
        f"this notebook. Original error: {exc}"
    ) from exc


CONFIG_KEYS = [
    "DEFAULT_SCHEMA",
    "RULES_DIR",
    "DQ_RESULTS_TABLE",
    "DQ_VIOLATIONS_TABLE",
    "DQ_EXECUTION_METRICS_TABLE",
]
RUNTIME_KEYS = [
    "DRY_RUN",
    "FAIL_ON_EMPTY_RULES",
    "FAIL_ON_EMPTY_SOURCE",
    "RETRYABLE_ERROR_MARKERS",
    "MAX_RULE_RETRIES",
    "RULE_TIMEOUT_SECONDS",
]

_RESERVED_RULE_KEYS = {"rule_id", "name", "description", "when", "pk_column"}


def _brief(exc: Exception) -> str:
    """First line of a Spark error, which carries the useful part."""
    return str(exc).strip().splitlines()[0]


def check_predicate(probe, expression: str, label: str) -> list[str]:
    """Resolve a predicate against the real schema via Spark's analyzer."""
    if not isinstance(expression, str) or not expression.strip():
        return [f"{label} must be a non-empty SQL predicate."]
    try:
        probe.selectExpr(expression)
    except Exception as exc:
        return [f"{label} does not resolve: {_brief(exc)}"]
    return []


def check_rule(rule: dict, probe, source_columns: set[str], where: str) -> list[str]:
    """Validate one rule against the engine's own contract and the real schema."""
    rule_id = rule.get("rule_id", "?")
    prefix = f"[{where} / {rule_id}]"
    errors: list[str] = []

    if not rule.get("rule_id"):
        errors.append(f"[{where}] A rule is missing 'rule_id'.")
    if not rule.get("name"):
        errors.append(f"{prefix} Missing 'name'.")

    try:
        type_name = detect_rule_type(rule)
    except Exception as exc:
        return errors + [f"{prefix} {exc}"]

    rule_type = RULE_TYPES[type_name]
    config = rule[type_name]

    for key in PREDICATE_KEYS:
        if key == "check" and type_name != "check":
            continue
        value = rule.get(key) if key != "check" else config
        if value is not None:
            errors.extend(check_predicate(probe, value, f"{prefix} '{key}'"))

    pk_column = rule.get("pk_column")
    if pk_column and pk_column not in source_columns:
        errors.append(f"{prefix} pk_column '{pk_column}' not found in source table.")

    if rule_type.required and not isinstance(config, dict):
        return errors + [f"{prefix} '{type_name}' must be a mapping."]

    if isinstance(config, dict):
        missing = [k for k in rule_type.required if config.get(k) in (None, "", [])]
        if missing:
            errors.append(
                f"{prefix} '{type_name}' is missing required key(s): {sorted(missing)}."
            )
        for key in rule_type.column_keys:
            column = config.get(key)
            if isinstance(column, str) and column and column not in source_columns:
                errors.append(
                    f"{prefix} {key} '{column}' not found in source table."
                )
        gate = config.get("completion_gate")
        if gate is not None:
            if not rule_type.gated:
                errors.append(f"{prefix} '{type_name}' does not accept completion_gate.")
            elif not isinstance(gate, dict):
                errors.append(f"{prefix} completion_gate must be a mapping.")
            else:
                for key in ("event_column", "order_column"):
                    column = gate.get(key)
                    if isinstance(column, str) and column and column not in source_columns:
                        errors.append(
                            f"{prefix} completion_gate.{key} '{column}' not found in source table."
                        )

    if type_name == "unique":
        columns = [config] if isinstance(config, str) else config
        if not isinstance(columns, list) or not columns:
            errors.append(f"{prefix} 'unique' must be a column name or a list of them.")
        else:
            for column in columns:
                if column not in source_columns:
                    errors.append(f"{prefix} unique column '{column}' not found in source table.")

    unknown = sorted(set(rule) - _RESERVED_RULE_KEYS - set(RULE_TYPES))
    if unknown:
        errors.append(f"{prefix} Unrecognised key(s): {unknown}.")

    return errors


def check_catalog(catalog: dict, probe, source_columns: set[str], where: str) -> list[str]:
    """Validate a catalog header and every rule in it."""
    errors: list[str] = []

    if not catalog.get("pk_column"):
        errors.append(
            f"[{where}] Missing 'pk_column'. Row-scoped rules need a key to "
            "identify offending rows."
        )
    elif catalog["pk_column"] not in source_columns:
        errors.append(
            f"[{where}] pk_column '{catalog['pk_column']}' not found in source table."
        )

    if catalog.get("where") is not None:
        errors.extend(check_predicate(probe, catalog["where"], f"[{where}] 'where'"))

    rules = catalog.get("rules") or []
    if not rules:
        errors.append(f"[{where}] Catalog has no rules.")

    seen: set[str] = set()
    for rule in rules:
        if not isinstance(rule, dict):
            errors.append(f"[{where}] Every entry under 'rules' must be a mapping.")
            continue
        rule_id = rule.get("rule_id")
        if rule_id in seen:
            errors.append(f"[{where}] Duplicate rule_id '{rule_id}'.")
        seen.add(rule_id)
        errors.extend(check_rule(rule, probe, source_columns, where))

    return errors


def _source_columns(spark, catalog: dict, full_table: str) -> set[str]:
    """Source columns, including any brought in by catalog-level joins."""
    columns = set(spark.read.table(full_table).columns)
    for join in catalog.get("joins") or []:
        if not isinstance(join, dict):
            continue
        for column in join.get("select") or []:
            if isinstance(column, str):
                columns.add(column)
    return columns


def main() -> None:
    config_module, config_path = load_config_module("QualityCatalogConfig")
    runtime_module, runtime_path = load_config_module("QualityCatalogRuntime")
    require_config_keys(config_module, CONFIG_KEYS, "QualityCatalogConfig")
    require_config_keys(runtime_module, RUNTIME_KEYS, "QualityCatalogRuntime")

    targets = resolve_targets(config_module, runtime_module)
    rules_dir = resolve_rules_dir(config_module, REPO_ROOT)
    if not rules_dir.exists():
        raise RuntimeError(f"Rules directory not found: {rules_dir}")

    # Match the runner's glob exactly, or a .yml catalog would run unchecked.
    yaml_files = sorted(rules_dir.glob("*.yaml")) + sorted(rules_dir.glob("*.yml"))
    if not yaml_files:
        raise RuntimeError(f"No rule catalogs found in {rules_dir}")

    spark = SparkSession.builder.getOrCreate()
    missing_sources: list[str] = []
    errors: list[str] = []
    rule_count = 0

    for yaml_path in yaml_files:
        with open(yaml_path, "r", encoding="utf-8") as handle:
            catalog = yaml.safe_load(handle)
        if not isinstance(catalog, dict):
            errors.append(f"[{yaml_path.name}] Catalog did not parse to a mapping.")
            continue

        database = catalog.get("database", "")
        table = catalog.get("table", "")
        full_table = f"{database}.{table}" if database else table
        if not table:
            errors.append(f"[{yaml_path.name}] Missing 'table'.")
            continue
        if not spark.catalog.tableExists(full_table):
            missing_sources.append(full_table)
            continue

        try:
            source_df = spark.read.table(full_table)
            columns = _source_columns(spark, catalog, full_table)
            # Probe carries the joined-in columns too, so predicates that
            # reference them resolve exactly as they will at run time.
            probe = source_df
            for column in sorted(columns - set(source_df.columns)):
                probe = probe.withColumn(column, F.lit(None).cast("string"))
        except Exception as exc:
            errors.append(f"[{yaml_path.name}] Could not read {full_table}: {_brief(exc)}")
            continue

        rule_count += len(catalog.get("rules") or [])
        errors.extend(check_catalog(catalog, probe, columns, yaml_path.name))

    if missing_sources:
        raise RuntimeError(f"Missing source tables: {sorted(set(missing_sources))}")

    if errors:
        raise RuntimeError(
            "Preflight failed:\n" + "\n".join(f"  ✗ {e}" for e in errors)
        )

    print("Quality Catalog preflight passed.")
    print(f"  Config path:      {config_path}")
    print(f"  Runtime path:     {runtime_path}")
    print(f"  Dry run:          {runtime_module.DRY_RUN}")
    print(f"  Rules dir:        {rules_dir}")
    print(f"  Results table:    {targets['results_table']}")
    print(f"  Violations table: {targets['violations_table']}")
    print(f"  Metrics table:    {targets['execution_metrics_table']}")
    print(f"  Rule catalogs:    {len(yaml_files)}")
    print(f"  Rules:            {rule_count}")


if __name__ == "__main__":
    main()
