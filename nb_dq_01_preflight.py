"""Preflight checks for the Quality Catalog validation runner."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession


REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.runtime import load_config_module, require_config_keys, resolve_rules_dir, resolve_targets


# Column-name keys that can be referenced in a rule's top-level fields or
# parameters.  Checked against the actual source table columns in preflight.
_RULE_COLUMN_KEYS = {
    "column",
    "column_A",
    "column_B",
    "pk_column",
    "group_column",
    "value_column",
    "sort_column",
    "condition_column",
    "required_column",
    "aggregate_column",
    "reference_column",
    "check_columns",        # list
    "condition_column",
}


def _extract_rule_columns(rule: dict) -> list[str]:
    """Return every column name referenced by a rule (top-level + parameters)."""
    cols: list[str] = []
    params = rule.get("parameters", {})

    # Top-level column key
    if "column" in rule:
        cols.append(rule["column"])

    for key in _RULE_COLUMN_KEYS:
        val = params.get(key)
        if isinstance(val, str) and val:
            cols.append(val)
        elif isinstance(val, list):
            cols.extend(v for v in val if isinstance(v, str) and v)

    return cols


def _check_columns_for_catalog(
    catalog: dict,
    source_columns: set[str],
    yaml_name: str,
) -> list[str]:
    """
    Compare columns referenced in the catalog's rules against source_columns.
    Returns a list of human-readable warning strings (empty = no issues).
    Note: reference tables (for validate_foreign_key) are not checked here
    because they may live in separate schemas not visible at preflight time.
    """
    warnings: list[str] = []
    for rule in catalog.get("rules", []):
        rule_id = rule.get("rule_id", "?")
        exp_name = rule.get("expectation", "")
        for col in _extract_rule_columns(rule):
            if col and col not in source_columns:
                warnings.append(
                    f"[{yaml_name} / {rule_id} / {exp_name}] "
                    f"Column '{col}' not found in source table."
                )
    return warnings


def main() -> None:
    config_module, config_path = load_config_module("QualityCatalogConfig")
    runtime_module, runtime_path = load_config_module("QualityCatalogRuntime")
    require_config_keys(
        config_module,
        [
            "DEFAULT_SCHEMA",
            "RULES_DIR",
            "DQ_RESULTS_TABLE",
            "DQ_VIOLATIONS_TABLE",
            "DQ_EXECUTION_METRICS_TABLE",
        ],
        "QualityCatalogConfig",
    )
    require_config_keys(
        runtime_module,
        [
            "DRY_RUN",
            "FAIL_ON_EMPTY_RULES",
            "FAIL_ON_EMPTY_SOURCE",
            "MAX_RETRIES",
            "RETRYABLE_ERROR_MARKERS",
        ],
        "QualityCatalogRuntime",
    )
    targets = resolve_targets(config_module, runtime_module)
    rules_dir = resolve_rules_dir(config_module, REPO_ROOT)

    if not rules_dir.exists():
        raise RuntimeError(f"Rules directory not found: {rules_dir}")

    yaml_files = sorted(rules_dir.glob("*.yaml"))
    if not yaml_files:
        raise RuntimeError(f"No rule catalogs found in {rules_dir}")

    spark = SparkSession.builder.getOrCreate()
    missing_sources: list[str] = []
    column_warnings: list[str] = []

    for yaml_path in yaml_files:
        with open(yaml_path, "r", encoding="utf-8") as handle:
            catalog = yaml.safe_load(handle)
        database = catalog.get("database", "")
        table = catalog["table"]
        full_table = f"{database}.{table}" if database else table

        if not spark.catalog.tableExists(full_table):
            missing_sources.append(full_table)
            continue

        # Column existence pre-check: compare rule column references against
        # the actual source table schema so typos are caught before execution.
        try:
            source_cols = set(spark.read.table(full_table).columns)
            column_warnings.extend(
                _check_columns_for_catalog(catalog, source_cols, yaml_path.name)
            )
        except Exception as exc:
            print(f"  Warning: could not read schema for {full_table}: {exc}")

    if missing_sources:
        raise RuntimeError(f"Missing source tables: {sorted(set(missing_sources))}")

    if column_warnings:
        print("\n[PREFLIGHT] Column reference warnings (rules will error at runtime):")
        for w in column_warnings:
            print(f"  ⚠  {w}")
        print()

    print("Quality Catalog preflight passed.")
    print(f"  Config path:      {config_path}")
    print(f"  Runtime path:     {runtime_path}")
    print(f"  Dry run:          {runtime_module.DRY_RUN}")
    print(f"  Rules dir:        {rules_dir}")
    print(f"  Results table:    {targets['results_table']}")
    print(f"  Violations table: {targets['violations_table']}")
    print(f"  Metrics table:    {targets['execution_metrics_table']}")
    print(f"  Rule catalogs:    {len(yaml_files)}")
    if column_warnings:
        print(f"  Column warnings:  {len(column_warnings)}")


if __name__ == "__main__":
    main()
