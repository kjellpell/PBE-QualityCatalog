# =============================================================================
# nb_dq_03_run_validation.py
# Notebook runner for executing the Quality Catalog validation engine in Fabric.
#
# Expected Lakehouse layout:
#   /lakehouse/default/Files/engine/
#   /lakehouse/default/Files/Configs/
#   /lakehouse/default/Files/rules/
# =============================================================================

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

ENGINE_DIR = Path("/lakehouse/default/Files/engine")
CONFIG_DIR = Path("/lakehouse/default/Files/Configs")
RULES_DIR = Path("/lakehouse/default/Files/rules")
RUNNER_PATH = ENGINE_DIR / "validation_runner.py"

spark = SparkSession.builder.getOrCreate()

REQUIRED_FILES = [
    ENGINE_DIR / "__init__.py",
    ENGINE_DIR / "runtime.py",
    ENGINE_DIR / "expectations.py",
    ENGINE_DIR / "resolution.py",
    ENGINE_DIR / "yaml_rules.py",
    RUNNER_PATH,
    CONFIG_DIR / "QualityCatalogConfig.py",
    CONFIG_DIR / "QualityCatalogRuntime.py",
]


def _table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


# -----------------------------------------------------------------------------
# STEP 1: Pre-check required files and rule catalogs
# -----------------------------------------------------------------------------
missing = [str(path) for path in REQUIRED_FILES if not path.exists()]
yaml_files = sorted(RULES_DIR.glob("*.yaml"))
if not yaml_files:
    missing.append(f"{RULES_DIR}/*.yaml (no YAML files found)")

if missing:
    raise FileNotFoundError("Missing required deployment files:\n- " + "\n- ".join(missing))

print("Pre-check passed.")
print(f"Rule YAML files found: {len(yaml_files)}")


# -----------------------------------------------------------------------------
# STEP 2: Execute engine/validation_runner.py
# Loaded via importlib rather than runpy so that:
#   - Exceptions propagate with their original types (no runpy wrapping).
#   - Both modules share the same SparkSession object from getOrCreate().
#   - The if __name__ == "__main__" guard in validation_runner.py is respected,
#     so module-level setup runs at exec_module() time and main() is called
#     explicitly below.
# -----------------------------------------------------------------------------
_engine_parent = str(ENGINE_DIR.parent)
if _engine_parent not in sys.path:
    sys.path.insert(0, _engine_parent)

started = datetime.now(timezone.utc)
print(f"Starting validation runner at {started.isoformat()}")

_vr_spec = importlib.util.spec_from_file_location("validation_runner", str(RUNNER_PATH))
_vr_mod = importlib.util.module_from_spec(_vr_spec)
_vr_spec.loader.exec_module(_vr_mod)  # runs config loading, SparkSession setup, globals

results_count, violations_count = _vr_mod.main()

finished = datetime.now(timezone.utc)
print(f"Validation runner finished at {finished.isoformat()}")
print(f"Duration seconds: {(finished - started).total_seconds():.1f}")


# -----------------------------------------------------------------------------
# STEP 3: Show latest run evidence
# -----------------------------------------------------------------------------
_cfg_spec = importlib.util.spec_from_file_location("QualityCatalogConfig", str(CONFIG_DIR / "QualityCatalogConfig.py"))
_cfg_mod = importlib.util.module_from_spec(_cfg_spec); _cfg_spec.loader.exec_module(_cfg_mod)
_rt_spec = importlib.util.spec_from_file_location("QualityCatalogRuntime", str(CONFIG_DIR / "QualityCatalogRuntime.py"))
_rt_mod = importlib.util.module_from_spec(_rt_spec); _rt_spec.loader.exec_module(_rt_mod)

_schema = getattr(_cfg_mod, "DEFAULT_SCHEMA", "default")
_suffix = "_tmp" if _rt_mod.DRY_RUN else ""
_q = lambda t: f"{_schema}.{t}{_suffix}"

results_candidates = [_q(_cfg_mod.DQ_RESULTS_TABLE)]
violations_candidates = [_q(_cfg_mod.DQ_VIOLATIONS_TABLE)]
metrics_candidates = [_q(_cfg_mod.DQ_EXECUTION_METRICS_TABLE)]

results_table = next((t for t in results_candidates if _table_exists(t)), None)
violations_table = next((t for t in violations_candidates if _table_exists(t)), None)
metrics_table = next((t for t in metrics_candidates if _table_exists(t)), None)

print("Detected tables:")
print("  results_table:", results_table)
print("  violations_table:", violations_table)
print("  metrics_table:", metrics_table)

if metrics_table:
    print("\nLatest execution metrics rows:")
    spark.sql(
        f"""
        SELECT
            script_name,
            status,
            dry_run,
            row_count,
            started_at_utc,
            finished_at_utc,
            duration_seconds,
            is_retryable,
            error_message
        FROM {metrics_table}
        ORDER BY finished_at_utc DESC
        LIMIT 10
        """
    ).show(truncate=False)

if results_table:
    latest = spark.sql(
        f"SELECT run_id FROM {results_table} ORDER BY run_timestamp DESC LIMIT 1"
    ).collect()
    if latest:
        run_id = latest[0]["run_id"]
        print(f"\nRule-group summary for latest run_id: {run_id}")
        # Pass run_id via a temp view rather than string interpolation to
        # prevent second-order SQL injection from crafted values in the table.
        spark.createDataFrame([(run_id,)], ["_run_id"]).createOrReplaceTempView("_ev_run_id")
        spark.sql(
            f"""
            SELECT
                rule_group,
                COUNT(*) AS total_rules,
                SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors
            FROM {results_table}
            WHERE run_id = (SELECT _run_id FROM _ev_run_id)
            GROUP BY rule_group
            ORDER BY rule_group
            """
        ).show(truncate=False)
        try:
            spark.catalog.dropTempView("_ev_run_id")
        except Exception:
            pass  # best-effort cleanup; non-fatal

if violations_table:
    print("\nCurrent violations by issue_status:")
    spark.sql(
        f"""
        SELECT issue_status, COUNT(*) AS cnt
        FROM {violations_table}
        GROUP BY issue_status
        ORDER BY cnt DESC
        """
    ).show(truncate=False)
