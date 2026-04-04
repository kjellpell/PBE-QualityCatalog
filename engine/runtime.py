"""Runtime helpers for Quality Catalog operations."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path


LAKEHOUSE_CONFIG_DIR = Path("/lakehouse/default/Files/Configs")
LOCAL_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def load_config_module(module_name: str):
    lakehouse_path = LAKEHOUSE_CONFIG_DIR / f"{module_name}.py"
    local_path = LOCAL_CONFIG_DIR / f"{module_name}.py"
    require_lakehouse = os.getenv("REQUIRE_LAKEHOUSE_CONFIG", "0").lower() in {"1", "true", "yes"}
    module_path = lakehouse_path if lakehouse_path.exists() else local_path

    if require_lakehouse and not lakehouse_path.exists():
        raise FileNotFoundError(f"Missing required Lakehouse config: {lakehouse_path}")
    if not module_path.exists():
        raise FileNotFoundError(f"Configuration module not found: {module_path}")

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


def resolve_targets(config_module, runtime_module) -> dict[str, str]:
    metrics_table = f"{config_module.DEFAULT_SCHEMA}.{config_module.DQ_EXECUTION_METRICS_TABLE}"
    if runtime_module.DRY_RUN:
        return {
            "results_table": f"{config_module.DQ_RESULTS_TABLE}_tmp",
            "violations_table": f"{config_module.DQ_VIOLATIONS_TABLE}_tmp",
            "execution_metrics_table": f"{metrics_table}_tmp",
        }
    return {
        "results_table": config_module.DQ_RESULTS_TABLE,
        "violations_table": config_module.DQ_VIOLATIONS_TABLE,
        "execution_metrics_table": metrics_table,
    }


def resolve_rules_dir(config_module, repo_root: Path) -> Path:
    rules_dir = Path(config_module.RULES_DIR)
    if not rules_dir.is_absolute():
        rules_dir = repo_root / rules_dir
    return rules_dir


def classify_retryable_error(message: str | None, runtime_module) -> bool:
    lowered = (message or "").lower()
    return any(marker in lowered for marker in runtime_module.RETRYABLE_ERROR_MARKERS)


def write_execution_metric(spark, table_name: str, payload: dict) -> None:
    try:
        spark.createDataFrame([payload]).write.mode("append").saveAsTable(table_name)
    except Exception as exc:
        print(f"Warning: failed to write execution metric to {table_name}: {exc}")