# Deployment Runbook

This repository runs the YAML-driven Quality Catalog validation workflow in Microsoft Fabric.

## Deploy files to Lakehouse

### Required for notebook-first operation

Copy these files to Lakehouse Files:

| Repo file | Lakehouse target |
|---|---|
| `config/QualityCatalogConfig.py` | `/lakehouse/default/Files/Configs/QualityCatalogConfig.py` |
| `config/QualityCatalogRuntime.py` | `/lakehouse/default/Files/Configs/QualityCatalogRuntime.py` |
| `rules/*.yaml` | `/lakehouse/default/Files/rules/*.yaml` |
| `engine/` (all files, incl. `__init__.py`) | `/lakehouse/default/Files/engine/` |

Notes:

- Both config files must exist at `/lakehouse/default/Files/Configs/`.
- `engine/` is **required**: `nb_dq_03_run_validation.py` loads
  `/lakehouse/default/Files/engine/validation_runner.py` and fails its
  pre-check if any engine file is missing.
- Rule catalogs are required for `nb_dq_01_preflight.py` and
   `nb_dq_03_run_validation.py`.
- Keep `RULES_DIR = "rules"` unless you intentionally move rule files elsewhere.

## Run order

1. Run `nb_dq_00_setup.py` once per environment to create or upgrade Delta tables.
2. Run `nb_dq_01_preflight.py` before promoting runtime changes.
3. Run `nb_dq_03_run_validation.py` after source tables refresh.
   This notebook runner executes `/lakehouse/default/Files/engine/validation_runner.py`
   and prints a post-run summary.
4. Validate `dq_run_results`, `dq_violations`, and `default.dq_execution_metrics`.

## Runtime controls

`config/QualityCatalogRuntime.py` controls runtime behavior.

Current fields:

- `DRY_RUN`
- `FAIL_ON_EMPTY_SOURCE`
- `MAX_RULE_RETRIES`
- `RULE_TIMEOUT_SECONDS`
- `RETRYABLE_ERROR_MARKERS`
- `CATALOG_FILTER_OVERRIDES`

When `DRY_RUN = True`:

- results are written to `dq_run_results_tmp`
- violations are written to `dq_violations_tmp`
- execution metrics are written to `default.dq_execution_metrics_tmp`

## Validation checklist

1. Confirm both config files exist in `/lakehouse/default/Files/Configs/`.
2. Run `nb_dq_00_setup.py` if the environment is new.
3. Run `nb_dq_01_preflight.py`.
4. Execute one dry run.
5. Verify output counts and error-free summary.
6. Switch `DRY_RUN = False` before production scheduling.