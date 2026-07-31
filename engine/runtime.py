"""Runtime helpers for Quality Catalog operations."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


LAKEHOUSE_CONFIG_DIR_CANDIDATES = (
    Path("/lakehouse/default/Files/Configs"),
    Path("/lakehouse/default/Files/configs"),
)


def _resolve_config_module_path(module_name: str) -> Path:
    """Resolve config module path across known Lakehouse config directories."""
    searched: list[str] = []
    for config_dir in LAKEHOUSE_CONFIG_DIR_CANDIDATES:
        module_path = config_dir / f"{module_name}.py"
        searched.append(str(module_path))
        if module_path.exists():
            return module_path

    raise FileNotFoundError(
        "Config file not found for module "
        f"'{module_name}'. Searched:\n - "
        + "\n - ".join(searched)
        + "\nEnsure the config file is uploaded to one of these Lakehouse paths."
    )


def _safe_table_name(name: str) -> str:
    """Validate and return a bare table name (no schema prefix).

    Accepts only alphanumeric characters and underscores — the valid set for
    Delta/Hive table names.  Raises ``ValueError`` if the name contains any
    other character, blocking SQL injection via crafted table name values in
    the config files.
    """
    bare = name.strip()
    if not re.match(r"^[a-zA-Z0-9_]+$", bare):
        raise ValueError(
            f"Invalid table name '{bare}': only letters, digits, and underscores "
            f"are allowed.  Check table name settings in QualityCatalogConfig.py."
        )
    return bare


def resolve_repo_root() -> Path:
    """Return the repository root directory.

    When running from a file on disk, uses this file's location (engine/) to
    derive the parent.  In a Fabric notebook (no ``__file__``), walks up from
    ``cwd`` looking for the ``engine/`` directory marker.
    """
    if "__file__" in globals():
        return Path(__file__).resolve().parent.parent

    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents, Path("/lakehouse/default/Files")]:
        if (candidate / "engine").exists():
            return candidate
    return cwd


def load_config_module(module_name: str):
    module_path = _resolve_config_module_path(module_name)

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load configuration module from: {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, module_path


def require_config_keys(config_module, required_keys: list[str], module_name: str) -> None:
    missing = sorted(key for key in required_keys if not hasattr(config_module, key))
    if missing:
        raise RuntimeError(f"Missing required {module_name} keys: {', '.join(missing)}")


def _qualify(table_name: str, default_schema: str) -> str:
    """Prepend default_schema only when table_name is not already schema-qualified."""
    return table_name if "." in table_name else f"{default_schema}.{table_name}"


def resolve_targets(config_module, runtime_module) -> dict[str, str]:
    schema = config_module.DEFAULT_SCHEMA
    results    = _qualify(config_module.DQ_RESULTS_TABLE,            schema)
    violations = _qualify(config_module.DQ_VIOLATIONS_TABLE,         schema)
    metrics    = _qualify(config_module.DQ_EXECUTION_METRICS_TABLE,  schema)

    if runtime_module.DRY_RUN:
        return {
            "results_table":           f"{results}_tmp",
            "violations_table":        f"{violations}_tmp",
            "execution_metrics_table": f"{metrics}_tmp",
        }
    return {
        "results_table":           results,
        "violations_table":        violations,
        "execution_metrics_table": metrics,
    }


# The complete set of keys a rule catalog's header may carry. `_load_all_rules`
# in engine/validation_runner.py reads exactly these; preflight rejects anything
# else, so a misspelled `where:` is reported rather than silently ignored — which
# would drop the filter and evaluate every rule against rows it never scoped.
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


def resolve_rules_dir(config_module, repo_root: Path, must_exist: bool = False) -> Path:
    rules_dir = Path(config_module.RULES_DIR)
    if not rules_dir.is_absolute():
        rules_dir = repo_root / rules_dir
    if must_exist and not rules_dir.exists():
        raise FileNotFoundError(
            f"RULES_DIR not found: {rules_dir}\n"
            f"Ensure RULES_DIR in QualityCatalogConfig.py points to your rules folder."
        )
    return rules_dir


def classify_retryable_error(message: str | None, runtime_module) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in runtime_module.RETRYABLE_ERROR_MARKERS)


_EXECUTION_METRIC_SCHEMA = StructType([
    StructField("script_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("dry_run", BooleanType(), True),
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
        "dry_run": payload.get("dry_run"),
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