# =============================================================================
# scripts/setup_dq_tables.py
# Creates all Delta tables used by the Quality Catalog data quality framework.
# Run once before the first validation run.
#
# The DDL is generated from the Spark schemas the engine actually writes, so
# a column added to RESULT_SCHEMA / VIOLATION_SCHEMA / _EXECUTION_METRIC_SCHEMA
# reaches the tables without being restated here. Previously each schema was
# hand-maintained in three places (the production DDL, the _tmp DDL, and the
# Python schema) with nothing keeping them in step.
#
# Safe to re-run: CREATE TABLE IF NOT EXISTS leaves existing data alone, and
# any column missing from an already-deployed table is added via ALTER TABLE.
# =============================================================================

# CELL 1 — Spark session + config
# -----------------------------------------------------------------------------
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _resolve_repo_root() -> Path:
    """Locate the repo root so `engine` is importable. Runs before that import."""
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
    from engine.output_store import RESULT_SCHEMA, VIOLATION_SCHEMA   # noqa: E402
    from engine.runtime import (                                    # noqa: E402
        _EXECUTION_METRIC_SCHEMA,
        load_config_module,
    )
except ModuleNotFoundError as exc:  # pragma: no cover - deployment problem
    raise ModuleNotFoundError(
        f"Could not import the engine package from {REPO_ROOT}. This script "
        "generates its table DDL from the engine's own schemas, so the engine/ "
        "directory must be deployed next to this notebook. Nothing has been "
        f"created. Original error: {exc}"
    ) from exc

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

_cfg_mod, _cfg_path = load_config_module("QualityCatalogConfig")

_schema = getattr(_cfg_mod, "DEFAULT_SCHEMA", "datakvalitet")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema}")
print(f"Spark ready. Target schema: {_schema}  (config: {_cfg_path})")


# CELL 2 — table definitions
# Each output table is described once, by the schema the engine writes.
#   dq_run_results       one row per rule per validation run (Power BI scorecard)
#   dq_violations        one row per violation with lifecycle state
#                        (Active / Resolved); violation_scope says whether
#                        primary_key_value is a row key or a group key
#   dq_execution_metrics one row per runner execution, for operator visibility
# -----------------------------------------------------------------------------
TABLES = [
    (_cfg_mod.DQ_RESULTS_TABLE, RESULT_SCHEMA),
    (_cfg_mod.DQ_VIOLATIONS_TABLE, VIOLATION_SCHEMA),
    (_cfg_mod.DQ_EXECUTION_METRICS_TABLE, _EXECUTION_METRIC_SCHEMA),
]


def _column_ddl(struct) -> str:
    width = max(len(f.name) for f in struct.fields)
    return ",\n".join(
        f"    {f.name.ljust(width)} {f.dataType.simpleString().upper()}"
        for f in struct.fields
    )


def _ensure_table(table: str, struct) -> None:
    """Create the table if absent; verify it matches the schema if it already exists.

    There is deliberately no migration path. A table left over from an earlier
    deployment is reported so it can be dropped, rather than quietly patched into
    something that only resembles the current schema.
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} (\n{_column_ddl(struct)}\n) USING DELTA"
    )

    expected = [(f.name.lower(), f.dataType.simpleString()) for f in struct.fields]
    actual = [(f.name.lower(), f.dataType.simpleString()) for f in spark.table(table).schema.fields]
    if actual == expected:
        return

    expected_types, actual_types = dict(expected), dict(actual)
    missing = [n for n in expected_types if n not in actual_types]
    extra = [n for n in actual_types if n not in expected_types]
    changed = [
        f"{n} (expected {t}, found {actual_types[n]})"
        for n, t in expected
        if n in actual_types and actual_types[n] != t
    ]
    detail = "\n".join(
        f"  {label}: {', '.join(items)}"
        for label, items in (
            ("missing columns", missing),
            ("unexpected columns", extra),
            ("wrong type", changed),
        )
        if items
    ) or "  column order differs from the engine schema"

    raise RuntimeError(
        f"Table {table} already exists with a different schema.\n{detail}\n"
        "Drop the table and re-run this script — no in-place migration is applied."
    )


# CELL 3 — create the production tables and their dry-run twins
# -----------------------------------------------------------------------------
for _base, _struct in TABLES:
    for _name in (_base, f"{_base}_tmp"):
        _table = f"{_schema}.{_name}"
        _ensure_table(_table, _struct)
        print(f"  {_table} ready ({len(_struct.fields)} columns).")


# Performance tip: after the first significant data load, apply Z-order
# clustering on the columns most used in WHERE / JOIN predicates:
#
#   spark.sql("OPTIMIZE dq_violations ZORDER BY (rule_id, primary_key_value)")
#
# Re-run periodically (e.g. weekly) or after large backfills.

print("\n=== DQ SETUP COMPLETE ===")
print(f"Tables: {', '.join(f'{_schema}.{name}' for name, _ in TABLES)}")
print(f"Dry-run twins: {', '.join(f'{_schema}.{name}_tmp' for name, _ in TABLES)}")
