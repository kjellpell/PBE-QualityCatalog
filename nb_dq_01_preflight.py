"""Preflight checks for the Quality Catalog validation runner."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from pyspark.sql import SparkSession


LAKEHOUSE_CONFIG_DIR = Path("/lakehouse/default/Files/configs")


def _resolve_repo_root() -> Path:
    """Resolve repository root. Canonical implementation in engine/runtime.py."""
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
    from engine.runtime import load_config_module, require_config_keys, resolve_rules_dir, resolve_targets
except ModuleNotFoundError:
    print(
        "  Note: engine package not importable as a module; "
        "using inline file-based imports (Fabric notebook mode)."
    )
    import importlib.util

    def load_config_module(module_name: str):
        module_path = LAKEHOUSE_CONFIG_DIR / f"{module_name}.py"
        if not module_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {module_path}\n"
                f"Ensure {module_name}.py is uploaded to the Lakehouse at "
                f"/lakehouse/default/Files/configs/"
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
        return table_name if "." in table_name else f"{default_schema}.{table_name}"

    def resolve_targets(config_module, runtime_module) -> dict[str, str]:
        schema = config_module.DEFAULT_SCHEMA
        results = _qualify(config_module.DQ_RESULTS_TABLE, schema)
        violations = _qualify(config_module.DQ_VIOLATIONS_TABLE, schema)
        metrics = _qualify(config_module.DQ_EXECUTION_METRICS_TABLE, schema)
        ic_results = _qualify(getattr(config_module, "IC_RUN_RESULTS_TABLE", "ic_run_results"), schema)
        ic_exceptions = _qualify(getattr(config_module, "IC_EXCEPTIONS_TABLE", "ic_exceptions"), schema)

        if runtime_module.DRY_RUN:
            return {
                "results_table": f"{results}_tmp",
                "violations_table": f"{violations}_tmp",
                "execution_metrics_table": f"{metrics}_tmp",
                "ic_results_table": f"{ic_results}_tmp",
                "ic_exceptions_table": f"{ic_exceptions}_tmp",
            }
        return {
            "results_table": results,
            "violations_table": violations,
            "execution_metrics_table": metrics,
            "ic_results_table": ic_results,
            "ic_exceptions_table": ic_exceptions,
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


# Column-name keys that can be referenced in a rule's top-level fields or
# parameters.  Checked against the actual source table columns in preflight.
# All keys use canonical names per ARCHITECTURE.md "Expectation Naming Contract".
_RULE_COLUMN_KEYS = {
    "column",
    "columns",
    "left_column",
    "right_column",
    "filter_column",
    "pk_column",
    "group_column",
    "event_column",
    "order_column",
    "when_column",
    "required_column",
    "aggregate_column",
    "reference_column",
    "checked_columns",
    "source_column",
    "open_state_column",
    "reference_table",
    "reference_active_column",
}

# Per-expectation required canonical parameter keys.
# Preflight rejects any rule missing a required key for its expectation.
#
# IMPORTANT: the keys of this dict must stay in lock-step with the engine's
# CUSTOM_EXPECTATION_REGISTRY (engine/expectations.py).  _assert_registry_parity()
# enforces this at preflight time so drift fails here rather than at runtime.
_REQUIRED_PARAMETER_KEYS: dict[str, list[str]] = {
    "not_null":                   ["columns"],
    "not_null_when":              ["when_column", "columns"],
    "comparison":                 ["left_column", "operator"],
    "value_when":                 ["when_column", "required_column", "required_value"],
    "reference_exists":           ["column", "reference_table", "reference_column"],
    "reference_active":           ["column", "reference_table", "reference_column",
                                   "reference_active_column", "reference_active_value"],
    "row_count":                  ["threshold"],
    "combination_unique":         ["columns"],
    "state_duration_within_limit": ["start_column", "open_state_column", "pk_column", "max_days"],
    "sequence_ordered":           ["event_column", "group_column", "order_column", "expected_sequence"],
    "pairs_present":              ["event_column", "group_column", "required_pairs"],
    "gate_complete":              ["event_column", "group_column", "value_to_check"],
    "group_aggregate_matches":    ["group_column", "aggregate_column", "reference_column"],
    "sql_violations":             [],  # sql may be top-level or in parameters
    "value_in_list":              ["column", "allowed_values"],
}


def _engine_registry_keys() -> set[str] | None:
    """Return the engine's registered expectation names, or None if the engine
    module cannot be imported in this environment (e.g. no Spark on the path).

    Mirrors the Fabric file-based import fallback used in validation_runner.py:
    first try the package import, then load engine/expectations.py directly.
    """
    try:
        from engine.expectations import CUSTOM_EXPECTATION_REGISTRY
        return set(CUSTOM_EXPECTATION_REGISTRY)
    except Exception:
        pass
    try:
        import importlib.util

        exp_path = REPO_ROOT / "engine" / "expectations.py"
        if not exp_path.exists():
            return None
        spec = importlib.util.spec_from_file_location("qc_expectations_preflight", str(exp_path))
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return set(module.CUSTOM_EXPECTATION_REGISTRY)
    except Exception:
        return None


def _assert_registry_parity() -> None:
    """Fail fast if the preflight expectation list has drifted from the engine
    registry, so rules the engine cannot run are caught before execution rather
    than producing ERROR results at runtime (and valid rules are not rejected)."""
    engine_keys = _engine_registry_keys()
    if engine_keys is None:
        print("  Note: engine registry not importable here — skipping parity check.")
        return
    preflight_keys = set(_REQUIRED_PARAMETER_KEYS)
    only_preflight = preflight_keys - engine_keys
    only_engine = engine_keys - preflight_keys
    if only_preflight or only_engine:
        raise RuntimeError(
            "Preflight expectation list has drifted from the engine registry.\n"
            f"  In preflight but not engine (would ERROR at runtime): {sorted(only_preflight)}\n"
            f"  In engine but not preflight (valid rules wrongly rejected): {sorted(only_engine)}\n"
            "Update _REQUIRED_PARAMETER_KEYS in nb_dq_01_preflight.py to match "
            "CUSTOM_EXPECTATION_REGISTRY in engine/expectations.py."
        )


def _extract_rule_columns(rule: dict) -> list[str]:
    """Return every column name referenced by a rule (top-level + parameters)."""
    cols: list[str] = []
    params = rule.get("parameters", {})

    # Top-level column key
    if "column" in rule:
        val = rule["column"]
        if isinstance(val, list):
            cols.extend(v for v in val if isinstance(v, str) and v)
        elif isinstance(val, str) and val:
            cols.append(val)

    for key in _RULE_COLUMN_KEYS:
        val = params.get(key)
        if isinstance(val, str) and val:
            cols.append(val)
        elif isinstance(val, list):
            cols.extend(v for v in val if isinstance(v, str) and v)

    # Nested completion_gate block (sequence_ordered / pairs_present /
    # gate_complete) references its own event/order columns.
    gate = params.get("completion_gate")
    if isinstance(gate, dict):
        for key in ("event_column", "order_column"):
            val = gate.get(key)
            if isinstance(val, str) and val:
                cols.append(val)

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


def _check_nested_reference(rule: dict, yaml_name: str, rule_id: str) -> list[str]:
    """Detect legacy nested 'reference:' block that the engine no longer reads.
    Authors must flatten it to reference_table / reference_column etc."""
    errors: list[str] = []
    params = rule.get("parameters") or {}
    if not isinstance(params, dict):
        return errors
    if "reference" in params and isinstance(params["reference"], dict):
        exp = rule.get("expectation", "?")
        errors.append(
            f"[{yaml_name} / {rule_id}] Expectation '{exp}' has a nested 'parameters.reference:' "
            f"block which the engine does not read. Flatten it to top-level parameter keys: "
            f"reference_table, reference_column"
            + (", reference_active_column, reference_active_value" if exp in ("reference_active",) else "")
            + ". See ARCHITECTURE.md 'Expectation Naming Contract'."
        )
    return errors


def _check_required_parameter_keys(rule: dict, yaml_name: str, rule_id: str) -> list[str]:
    """Validate that all required canonical parameter keys are present for the expectation."""
    errors: list[str] = []
    exp = rule.get("expectation", "")
    required = _REQUIRED_PARAMETER_KEYS.get(exp)
    if required is None:
        return errors  # unknown expectation — handled elsewhere

    params = rule.get("parameters") or {}
    # Some rules (e.g. not_null, sql_violations) declare their key at top level
    top_level = {k: rule.get(k) for k in ("column", "sql")}

    for key in required:
        val = params.get(key) if isinstance(params, dict) else None
        # Allow top-level fallback for 'column'
        if val is None and key == "column":
            val = rule.get("column")
        if val is None or val == "" or val == []:
            errors.append(
                f"[{yaml_name} / {rule_id}] Expectation '{exp}' is missing required "
                f"parameter '{key}'. See ARCHITECTURE.md 'Expectation Naming Contract'."
            )
    return errors


def _check_parameter_contract_for_catalog(catalog: dict, yaml_name: str) -> list[str]:
    """Validate canonical parameter contract for all rules in a catalog.
    Checks for legacy nested reference: blocks and missing required canonical parameter keys."""
    errors: list[str] = []
    for rule in catalog.get("rules", []):
        rule_id = rule.get("rule_id", "?")
        exp = rule.get("expectation", "")

        # Reject expectations not registered in the engine so they fail here
        # rather than silently producing an ERROR result at runtime.
        if exp and exp not in _REQUIRED_PARAMETER_KEYS:
            errors.append(
                f"[{yaml_name} / {rule_id}] Unknown expectation '{exp}'. "
                f"Valid expectations: {sorted(_REQUIRED_PARAMETER_KEYS)}."
            )
            continue  # no point checking params for an unknown expectation

        # Check for legacy nested reference: block (engine ignores it silently)
        errors.extend(_check_nested_reference(rule, yaml_name, rule_id))

        # Check that all required canonical keys are present
        errors.extend(_check_required_parameter_keys(rule, yaml_name, rule_id))

    return errors


def _check_catalog_filter(catalog: dict, source_columns: set[str], yaml_name: str) -> list[str]:
    """Validate optional catalog_filter block."""
    errors: list[str] = []
    rule_group = catalog.get("rule_group", "?")
    filt = catalog.get("catalog_filter")
    if filt is None:
        return errors
    if not isinstance(filt, dict):
        return [f"[{yaml_name} / {rule_group}] catalog_filter must be a mapping."]

    ftype = filt.get("type")
    if ftype not in {"date_range", "custom"}:
        return [
            f"[{yaml_name} / {rule_group}] Unsupported catalog_filter.type '{ftype}'. "
            "Allowed: date_range, custom."
        ]

    if ftype == "date_range":
        date_col = filt.get("date_column")
        lookback = filt.get("lookback_days")
        if not date_col or not isinstance(date_col, str):
            errors.append(f"[{yaml_name} / {rule_group}] catalog_filter.date_column is required for date_range.")
        elif date_col not in source_columns:
            errors.append(
                f"[{yaml_name} / {rule_group}] catalog_filter.date_column '{date_col}' not found in source table."
            )
        if lookback is None:
            errors.append(f"[{yaml_name} / {rule_group}] catalog_filter.lookback_days is required for date_range.")
        else:
            try:
                if int(lookback) < 0:
                    errors.append(f"[{yaml_name} / {rule_group}] catalog_filter.lookback_days must be >= 0.")
            except Exception:
                errors.append(f"[{yaml_name} / {rule_group}] catalog_filter.lookback_days must be an integer.")

    if ftype == "custom":
        where_clause = filt.get("where_clause")
        if not where_clause or not isinstance(where_clause, str):
            errors.append(f"[{yaml_name} / {rule_group}] catalog_filter.where_clause is required for custom.")

    return errors


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
            "RETRYABLE_ERROR_MARKERS",
            "MAX_RULE_RETRIES",
            "RULE_TIMEOUT_SECONDS",
        ],
        "QualityCatalogRuntime",
    )
    # Catch preflight/engine expectation drift before doing any rule work.
    _assert_registry_parity()

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
    contract_errors: list[str] = []

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
            raw_source_cols = set(spark.read.table(full_table).columns)
            # Extend with columns introduced by catalog-level joins so rules
            # that reference joined columns are not falsely flagged as missing.
            source_cols = set(raw_source_cols)
            for _join in catalog.get("joins") or []:
                for _col in _join.get("select") or []:
                    if isinstance(_col, str):
                        source_cols.add(_col)
            column_warnings.extend(
                _check_columns_for_catalog(catalog, source_cols, yaml_path.name)
            )
            contract_errors.extend(
                _check_parameter_contract_for_catalog(catalog, yaml_path.name)
            )
            contract_errors.extend(
                _check_catalog_filter(catalog, source_cols, yaml_path.name)
            )
        except Exception as exc:
            print(f"  Warning: could not read schema for {full_table}: {exc}")

    if missing_sources:
        raise RuntimeError(f"Missing source tables: {sorted(set(missing_sources))}")

    if contract_errors:
        msg = "\n".join(contract_errors)
        raise RuntimeError(f"Preflight failed due to rule contract errors:\n{msg}")

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
