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


LAKEHOUSE_CONFIG_DIR = Path("/lakehouse/default/Files/Configs")


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
    module_path = LAKEHOUSE_CONFIG_DIR / f"{module_name}.py"
    if not module_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {module_path}\n"
            f"Ensure {module_name}.py is uploaded to the Lakehouse at "
            f"/lakehouse/default/Files/Configs/"
        )

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

    # IC tables — use getattr so the function works even if the caller's config
    # module pre-dates the IC additions (e.g. in older test fixtures).
    ic_results    = _qualify(getattr(config_module, "IC_RUN_RESULTS_TABLE",  "ic_run_results"),  schema)
    ic_exceptions = _qualify(getattr(config_module, "IC_EXCEPTIONS_TABLE",   "ic_exceptions"),   schema)

    if runtime_module.DRY_RUN:
        return {
            "results_table":           f"{results}_tmp",
            "violations_table":        f"{violations}_tmp",
            "execution_metrics_table": f"{metrics}_tmp",
            "ic_results_table":        f"{ic_results}_tmp",
            "ic_exceptions_table":     f"{ic_exceptions}_tmp",
        }
    return {
        "results_table":           results,
        "violations_table":        violations,
        "execution_metrics_table": metrics,
        "ic_results_table":        ic_results,
        "ic_exceptions_table":     ic_exceptions,
    }


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
        print(
            f"Warning: failed to write execution metric to '{table_name}': {exc}"
        )