# Deployment Runbook

This repository runs the YAML-driven Quality Catalog validation workflow in Microsoft Fabric.

## Deploy configuration files

Copy these files to Lakehouse Files:

| Repo file | Lakehouse target |
|---|---|
| `config/QualityCatalogConfig.py` | `/lakehouse/default/Files/Configs/QualityCatalogConfig.py` |
| `config/QualityCatalogRuntime.py` | `/lakehouse/default/Files/Configs/QualityCatalogRuntime.py` |

In production, set `REQUIRE_LAKEHOUSE_CONFIG=1` so local fallback is disabled.

## Run order

1. Run `nb_dq_00_setup.py` once per environment to create or upgrade Delta tables.
2. Run `nb_dq_01_preflight.py` before promoting runtime changes.
3. Run `engine/validation_runner.py` after source tables refresh.
4. Validate `dq_run_results`, `dq_violations`, and `default.dq_execution_metrics`.

## Runtime controls

`config/QualityCatalogRuntime.py` controls runtime behavior.

Current fields:

- `DRY_RUN`
- `FAIL_ON_EMPTY_RULES`
- `FAIL_ON_EMPTY_SOURCE`
- `MAX_RETRIES`
- `RETRYABLE_ERROR_MARKERS`

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