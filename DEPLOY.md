# Deployment Runbook

This repository runs the YAML-driven Quality Catalog validation workflow in Microsoft Fabric.

## Deploy files to Lakehouse

### Required for notebook-first operation

Copy these files to Lakehouse Files:

| Repo file | Lakehouse target |
|---|---|
| `config/QualityCatalogConfig.py` | `/lakehouse/default/Files/configs/QualityCatalogConfig.py` |
| `config/QualityCatalogRuntime.py` | `/lakehouse/default/Files/configs/QualityCatalogRuntime.py` |
| `rules/*.yaml` | `/lakehouse/default/Files/rules/*.yaml` |
| `engine/` (all files, incl. `__init__.py`) | `/lakehouse/default/Files/engine/` |

Notes:

- Both config files must exist at `/lakehouse/default/Files/configs/`.
- `engine/` is **required**: `scripts/run_validation.py` loads
  `/lakehouse/default/Files/engine/runner.py` and fails its
  pre-check if any engine file is missing.
- Rule catalogs are required for `scripts/preflight_checks.py` and
   `scripts/run_validation.py`.
- Keep `RULES_DIR = "rules"` unless you intentionally move rule files elsewhere.

## Run order

1. Run `scripts/setup_dq_tables.py` once per environment to create the Delta tables. It is
   rerunnable, but applies no migration: a table whose shape has drifted is
   reported so it can be dropped, not patched in place.
2. Run `scripts/preflight_checks.py` before promoting runtime changes.
3. Run `scripts/run_validation.py` after source tables refresh.
   This notebook runner executes `/lakehouse/default/Files/engine/runner.py`
   and prints a post-run summary.
4. Validate `dq_run_results`, `dq_violations`, and `default.dq_execution_metrics`.

## Runtime controls

`config/QualityCatalogRuntime.py` controls runtime behavior.

Current fields:

- `FAIL_ON_EMPTY_SOURCE`
- `MAX_RULE_RETRIES`
- `RULE_TIMEOUT_SECONDS`
- `RETRYABLE_ERROR_MARKERS`
- `CATALOG_FILTER_OVERRIDES`

## Validation checklist

1. Confirm both config files exist in `/lakehouse/default/Files/configs/`.
2. Run `scripts/setup_dq_tables.py` if the environment is new.
3. Run `scripts/preflight_checks.py`.
4. Execute one validation run in the target environment.
5. Verify output counts and error-free summary.