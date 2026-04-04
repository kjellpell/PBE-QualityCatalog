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


def main() -> None:
    config_module, config_path = load_config_module("QualityCatalogConfig")
    runtime_module, runtime_path = load_config_module("QualityCatalogRuntime")
    require_config_keys(
        config_module,
        [
            "DEFAULT_SCHEMA",
            "CONFIG_VERSION",
            "PIPELINE_VERSION",
            "RULES_DIR",
            "GX_SAMPLE_SIZE",
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
    for yaml_path in yaml_files:
        with open(yaml_path, "r", encoding="utf-8") as handle:
            catalog = yaml.safe_load(handle)
        database = catalog.get("database", "")
        table = catalog["table"]
        full_table = f"{database}.{table}" if database else table
        if not spark.catalog.tableExists(full_table):
            missing_sources.append(full_table)

    if missing_sources:
        raise RuntimeError(f"Missing source tables: {sorted(set(missing_sources))}")

    print("Quality Catalog preflight passed.")
    print(f"  Config path:      {config_path}")
    print(f"  Runtime path:     {runtime_path}")
    print(f"  Dry run:          {runtime_module.DRY_RUN}")
    print(f"  Rules dir:        {rules_dir}")
    print(f"  Results table:    {targets['results_table']}")
    print(f"  Violations table: {targets['violations_table']}")
    print(f"  Metrics table:    {targets['execution_metrics_table']}")
    print(f"  Rule catalogs:    {len(yaml_files)}")


if __name__ == "__main__":
    main()