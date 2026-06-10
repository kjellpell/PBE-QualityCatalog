# =============================================================================
# engine/validation_runner.py
# Data quality validation engine for the PBE Quality Catalog.
#
# Flow:
#   1. Load active rules from YAML files in RULES_DIR.
#   2. For each rule group, load the source table from the Spark metastore
#      and apply any configured pre-joins.
#   3. For each rule:
#        a. Dispatch to the matching validator in CUSTOM_EXPECTATION_REGISTRY.
#        b. Collect per-rule results (counts, success %, status).
#        c. Collect per-row violation details.
#   4. Write summary rows to dq_run_results (Delta table).
#   5. Write violation rows to dq_violations via MERGE-based resolution tracking.
#   6. Write execution metrics to dq_execution_metrics.
#
# Schedule: nightly (after source tables are refreshed).
# Prerequisites: nb_dq_00_setup.py must have been run at least once.
# =============================================================================


# CELL 2 — Imports and run metadata
# -----------------------------------------------------------------------------
import concurrent.futures
import time
import uuid

from datetime import datetime, date, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, DateType,
)

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")


# CELL 3 — Import custom expectation registry and resolution helpers
# All expectation classes are consolidated in engine/expectations.py.
# Resolution-tracking helpers live in engine/resolution.py.
# -----------------------------------------------------------------------------
import sys
from functools import reduce
import importlib.util

def _resolve_repo_root() -> Path:
    """Resolve repository root. Canonical implementation in engine/runtime.py."""
    if "__file__" in globals():
        return Path(__file__).resolve().parent.parent

    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents, Path("/lakehouse/default/Files")]:
        if (candidate / "engine").exists():
            return candidate
    return cwd


_repo_root = str(_resolve_repo_root())
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

def _load_module_from_path(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module {module_name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


try:
    from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
    from engine.resolution import (
        VIOLATION_SCHEMA,
        _apply_resolution_tracking,
    )
    from engine.runtime import (
        classify_retryable_error,
        load_config_module,
        require_config_keys,
        resolve_targets,
        write_execution_metric,
    )
except ModuleNotFoundError:
    print(
        "  Note: engine package not importable as a module; "
        "using inline file-based imports (Fabric notebook mode)."
    )
    engine_dir = Path(_repo_root) / "engine"
    expectations_py = engine_dir / "expectations.py"
    resolution_py = engine_dir / "resolution.py"
    runtime_py = engine_dir / "runtime.py"
    missing = [
        str(p)
        for p in [expectations_py, resolution_py, runtime_py]
        if not p.exists()
    ]
    if missing:
        raise ModuleNotFoundError(
            "Could not import engine modules in notebook mode. "
            "Ensure engine files exist in Lakehouse Files. Missing: "
            f"{missing}"
        )

    expectations_module = _load_module_from_path("qc_expectations", expectations_py)
    resolution_module = _load_module_from_path("qc_resolution", resolution_py)
    runtime_module = _load_module_from_path("qc_runtime", runtime_py)

    CUSTOM_EXPECTATION_REGISTRY = expectations_module.CUSTOM_EXPECTATION_REGISTRY
    VIOLATION_SCHEMA = resolution_module.VIOLATION_SCHEMA
    _apply_resolution_tracking = resolution_module._apply_resolution_tracking

    classify_retryable_error = runtime_module.classify_retryable_error
    load_config_module = runtime_module.load_config_module
    require_config_keys = runtime_module.require_config_keys
    resolve_targets = runtime_module.resolve_targets
    write_execution_metric = runtime_module.write_execution_metric

print("Custom expectation registry loaded:", list(CUSTOM_EXPECTATION_REGISTRY))

CONFIG, CONFIG_PATH = load_config_module("QualityCatalogConfig")
RUNTIME, RUNTIME_PATH = load_config_module("QualityCatalogRuntime")
require_config_keys(
    CONFIG,
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
    RUNTIME,
    [
        "DRY_RUN",
        "FAIL_ON_EMPTY_RULES",
        "FAIL_ON_EMPTY_SOURCE",
        "RETRYABLE_ERROR_MARKERS",
        "MAX_RULE_RETRIES",
        "RULE_TIMEOUT_SECONDS",
    ],
    "QualityCatalogRuntime",
)
TARGETS = resolve_targets(CONFIG, RUNTIME)

RUN_ID = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.now(timezone.utc)
BATCH_DATE = date.today()
STARTED_AT = datetime.now(timezone.utc)

print(f"Run ID    : {RUN_ID}")
print(f"Timestamp : {RUN_TIMESTAMP.isoformat()}")
print(f"Batch date: {BATCH_DATE}")
print(f"Config    : {CONFIG_PATH}")
print(f"Runtime   : {RUNTIME_PATH}")
print(f"Dry run   : {RUNTIME.DRY_RUN}")
print(f"Results   : {TARGETS['results_table']}")
print(f"Violations: {TARGETS['violations_table']}")
print(f"Metrics   : {TARGETS['execution_metrics_table']}")


# CELL 4 — Helper functions
# -----------------------------------------------------------------------------
def _normalize_join_cfg(join_cfg: dict) -> dict:
    """PyYAML 1.1 parses bare 'on' as boolean True. Restore it to the string key."""
    if isinstance(join_cfg, dict) and True in join_cfg and "on" not in join_cfg:
        result = dict(join_cfg)
        result["on"] = result.pop(True)
        return result
    return join_cfg


def _load_all_rules() -> list[dict]:
    """
    Load rule catalogs directly from YAML files in RULES_DIR.

    Each YAML file must have a top-level structure:
        rule_group: <str>
        table:      <str>
        database:   <str>          # optional
        pk_column:  <str>          # optional, default "id"
        joins:      [...]          # optional
        catalog_filter: {...}      # optional (date_range/custom)
        rules:      [...]

    Files are loaded alphabetically.
    """
    import yaml as _yaml

    rules_dir = _resolve_rules_dir()
    yaml_files = sorted(rules_dir.glob("*.yaml")) + sorted(rules_dir.glob("*.yml"))
    if not yaml_files:
        raise RuntimeError(
            f"No YAML rule files found in {rules_dir}. "
            f"Ensure *.yaml files are present at that path."
        )
    print(f"  Rule YAML files found: {len(yaml_files)}")

    catalogs = []
    total_rules = 0
    for yaml_path in yaml_files:
        try:
            with open(yaml_path, "r", encoding="utf-8") as fh:
                doc = _yaml.safe_load(fh)
        except Exception as exc:
            print(f"  Warning: could not parse {yaml_path.name}: {exc} — skipping.")
            continue

        if not isinstance(doc, dict):
            print(f"  Warning: {yaml_path.name} did not parse to a dict — skipping.")
            continue

        rules = doc.get("rules") or []
        if not rules:
            print(f"  Warning: {yaml_path.name} has no rules — skipping.")
            continue

        catalog = {
            "rule_group": doc.get("rule_group") or yaml_path.stem,
            "table":      doc.get("table", ""),
            "database":   doc.get("database", ""),
            "pk_column":  doc.get("pk_column", "id"),
            "joins":      [_normalize_join_cfg(j) for j in (doc.get("joins") or [])],
            "catalog_filter": doc.get("catalog_filter"),
            "rules":      rules,
        }
        catalogs.append(catalog)
        total_rules += len(rules)
        print(f"  Rule group: {catalog['rule_group']} ({len(rules)} rules)  [{yaml_path.name}]")

    if not catalogs:
        raise RuntimeError(f"No valid rule catalogs loaded from {rules_dir}.")

    print(f"  Total: {total_rules} rules across {len(catalogs)} catalog(s).")
    return catalogs


def _resolve_rules_dir() -> Path:
    """Resolve RULES_DIR from config, relative to repo root if not absolute."""
    from engine.runtime import resolve_rules_dir
    return resolve_rules_dir(CONFIG, Path(_repo_root), must_exist=True)


# CELL 5 — Schema for result accumulation
# -----------------------------------------------------------------------------
RESULT_SCHEMA = StructType([
    StructField("run_id",               StringType(),    False),
    StructField("run_timestamp",        TimestampType(), False),
    StructField("batch_date",           DateType(),      False),
    StructField("rule_group",           StringType(),    False),
    StructField("rule_id",              StringType(),    False),
    StructField("rule_name",            StringType(),    False),
    StructField("table_name",           StringType(),    False),
    StructField("expectation",          StringType(),    False),
    StructField("routing_team",          StringType(),    False),
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


_INFRA_ERROR_MARKERS = ["timeout", "connection", "unavailable", "throttle"]
_SOURCE_ERROR_MARKERS = [
    "source table is empty", "table not found", "table or view not found",
    "no such table", "path does not exist",
]
_CONFIG_ERROR_MARKERS = [
    "unknown expectation", "column(s) not found", "column not found",
    "missing required", "unsupported operator", "must be numeric",
    "must be a mapping", "invalid parameter",
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


def _apply_default_pk_column(rule: dict, catalog_pk_col: str | None) -> dict:
    """Return a rule copy with parameters.pk_column defaulted from catalog PK."""
    if not catalog_pk_col:
        return rule

    params = rule.get("parameters")
    if isinstance(params, dict):
        current_pk = params.get("pk_column")
        if current_pk not in (None, ""):
            return rule
        params_out = dict(params)
    else:
        params_out = {}

    params_out["pk_column"] = catalog_pk_col
    rule_out = dict(rule)
    rule_out["parameters"] = params_out
    return rule_out


def _resolve_catalog_filter(catalog: dict, runtime_module) -> dict | None:
    """Resolve effective catalog filter with optional runtime override."""
    base_filter = catalog.get("catalog_filter")
    overrides = getattr(runtime_module, "CATALOG_FILTER_OVERRIDES", {}) or {}
    if not isinstance(overrides, dict):
        raise ValueError("CATALOG_FILTER_OVERRIDES must be a dict keyed by rule_group.")

    rule_group = catalog.get("rule_group")
    if rule_group in overrides:
        return overrides[rule_group]
    return base_filter


def _apply_catalog_filter(source_df, catalog_filter: dict | None, rule_group: str):
    """Apply catalog-level source filtering and return (filtered_df, description)."""
    if not catalog_filter:
        return source_df, None
    if not isinstance(catalog_filter, dict):
        raise ValueError(f"[{rule_group}] catalog_filter must be a mapping.")

    filter_type = catalog_filter.get("type")
    if filter_type == "date_range":
        date_column = catalog_filter.get("date_column")
        lookback_days = catalog_filter.get("lookback_days")
        include_nulls = bool(catalog_filter.get("include_nulls", False))

        if not date_column or not isinstance(date_column, str):
            raise ValueError(f"[{rule_group}] catalog_filter.date_column is required for date_range.")
        if date_column not in source_df.columns:
            raise ValueError(f"[{rule_group}] catalog_filter.date_column '{date_column}' not found in source columns.")
        if lookback_days is None:
            raise ValueError(f"[{rule_group}] catalog_filter.lookback_days is required for date_range.")
        if int(lookback_days) < 0:
            raise ValueError(f"[{rule_group}] catalog_filter.lookback_days must be >= 0.")

        cutoff_expr = F.date_sub(F.current_date(), int(lookback_days))
        predicate = F.col(date_column).cast("date") >= cutoff_expr
        if include_nulls:
            predicate = predicate | F.col(date_column).isNull()
        filtered_df = source_df.filter(predicate)
        desc = (
            f"date_range: {date_column} >= current_date()-{int(lookback_days)}"
            + (" (including NULLs)" if include_nulls else "")
        )
        return filtered_df, desc

    if filter_type == "custom":
        where_clause = catalog_filter.get("where_clause")
        if not where_clause or not isinstance(where_clause, str):
            raise ValueError(f"[{rule_group}] catalog_filter.where_clause is required for custom filter.")
        return source_df.filter(F.expr(where_clause)), f"custom: {where_clause}"

    raise ValueError(
        f"[{rule_group}] Unsupported catalog_filter.type '{filter_type}'. "
        "Allowed: 'date_range', 'custom'."
    )


# CELL 6 — Main validation engine
# -----------------------------------------------------------------------------
def run_validation(
    rule_catalog: dict,
    source_df,
    pk_col: str,
    ref_cache: dict = None,
) -> tuple:
    """
    Validate source_df against all rules in rule_catalog.

    Parameters
    ----------
    rule_catalog : dict loaded from YAML rule files (one rule group)
    source_df    : full Spark DataFrame to validate
    pk_col       : primary key column of source_df (stored as primary_key_value
                   in each violation row)
    ref_cache    : optional dict for caching reference table DataFrames across
                   rules, keyed by (table, column); avoids redundant reads

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
        rule = _apply_default_pk_column(rule, pk_col)

        rule_id   = rule["rule_id"]
        rule_name = rule["name"]
        exp_name  = rule["expectation"]
        routing_team = rule.get("routing", "none")

        print(f"  → [{rule_id}] {rule_name} ({exp_name}) ... ", end="")
        _rule_start = time.perf_counter()

        if exp_name not in CUSTOM_EXPECTATION_REGISTRY:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"[{table_name}/{rule_id}] Unknown expectation: '{exp_name}'",
            }
            viols_spark = None
        else:
            validator    = CUSTOM_EXPECTATION_REGISTRY[exp_name]()
            _timeout_s   = RUNTIME.RULE_TIMEOUT_SECONDS
            _max_retries = RUNTIME.MAX_RULE_RETRIES
            for _attempt in range(_max_retries + 1):
                try:
                    result, viols_spark = _run_validator(
                        validator, source_df, rule, spark, exp_name, ref_cache, _timeout_s
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
            routing_team,
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
                F.lit(routing_team).alias("routing_team"),
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
    validator,
    source_df,
    rule: dict,
    spark_session,
    exp_name: str,
    ref_cache,
    timeout_s: float,
) -> tuple:
    """Execute validator.validate() in a bounded-time background thread.

    On timeout, raises ``concurrent.futures.TimeoutError`` and releases the
    executor without waiting.  The underlying PySpark/JVM job continues to
    completion in the background — it cannot be cancelled at the Python level,
    but the main run proceeds without blocking.

    When a timeout does *not* occur the thread is already finished by the time
    we call ``shutdown(wait=True)``, so there is no extra blocking cost.
    """
    # Registry IDs whose validators accept a shared reference-table cache.
    if ref_cache is not None and exp_name in ("reference_exists", "reference_active"):
        call = lambda: validator.validate(source_df, rule, spark_session, ref_cache=ref_cache)
    else:
        call = lambda: validator.validate(source_df, rule, spark_session)

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


def main() -> tuple[int, int]:
    print("\nLoading rule catalogs from YAML files…")
    all_catalogs = _load_all_rules()
    if not all_catalogs and RUNTIME.FAIL_ON_EMPTY_RULES:
        raise RuntimeError("No rule catalogs loaded from YAML files.")
    result_dfs    = []
    violation_dfs = []

    # Shared cache for reference tables used by the reference_exists and
    # reference_active expectations.  Keyed per reference table/column so
    # repeated rules against the same reference read it only once across
    # all catalogs.
    ref_cache: dict = {}

    for catalog in all_catalogs:
        rule_group = catalog["rule_group"]
        table_name = catalog["table"]
        database = catalog.get("database", "")
        pk_col = catalog.get("pk_column", "id")
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

        effective_filter = _resolve_catalog_filter(catalog, RUNTIME)
        source_df, filter_desc = _apply_catalog_filter(source_df, effective_filter, rule_group)
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
                ref_cache=ref_cache,
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

    # In dry-run mode overwrite the tmp tables so each test run starts clean.
    # Fabric's SQL engine cannot resolve schema-qualified _tmp table names in
    # MERGE (Hive metastore vs Lakehouse catalog mismatch), so we skip MERGE
    # entirely for dry-runs and use the DataFrame API (which works) instead.
    _write_mode = "overwrite" if RUNTIME.DRY_RUN else "append"

    results_to_write = _align_df_to_table_schema(all_results_combined, TARGETS["results_table"])
    results_to_write.write.mode(_write_mode).saveAsTable(TARGETS["results_table"])
    print(f"  {TARGETS['results_table']} : {results_count} rows written.")

    if RUNTIME.DRY_RUN:
        _viol_to_write = _align_df_to_table_schema(
            all_violations_combined, TARGETS["violations_table"]
        )
        _viol_to_write.write.mode("overwrite").saveAsTable(TARGETS["violations_table"])
    else:
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


if __name__ == "__main__":
    try:
        results_count, violations_count = main()
        FINISHED_AT = datetime.now(timezone.utc)
        write_execution_metric(
            spark,
            TARGETS["execution_metrics_table"],
            {
                "script_name": "validation_runner",
                "status": "Succeeded",
                "dry_run": RUNTIME.DRY_RUN,
                "output_target": TARGETS["results_table"],
                "artifact_target": TARGETS["violations_table"],
                "row_count": int(results_count),
                "started_at_utc": STARTED_AT,
                "finished_at_utc": FINISHED_AT,
                "duration_seconds": float((FINISHED_AT - STARTED_AT).total_seconds()),
                "is_retryable": False,
                "error_message": None,
            },
        )
    except Exception as exc:
        FINISHED_AT = datetime.now(timezone.utc)
        write_execution_metric(
            spark,
            TARGETS["execution_metrics_table"],
            {
                "script_name": "validation_runner",
                "status": "Failed",
                "dry_run": RUNTIME.DRY_RUN,
                "output_target": TARGETS["results_table"],
                "artifact_target": TARGETS["violations_table"],
                "row_count": 0,
                "started_at_utc": STARTED_AT,
                "finished_at_utc": FINISHED_AT,
                "duration_seconds": float((FINISHED_AT - STARTED_AT).total_seconds()),
                "is_retryable": classify_retryable_error(str(exc), RUNTIME),
                "error_message": str(exc)[:4000],
            },
        )
        raise
