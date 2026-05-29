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

Notes:

- Both config files must exist at `/lakehouse/default/Files/Configs/`.
- Rule catalogs are required for `nb_dq_01_preflight.py` and `nb_dq_02_migrate_rules.py`.
- Keep `RULES_DIR = "rules"` unless you intentionally move rule files elsewhere.

### Optional, only if running the engine module directly

If you execute `engine/validation_runner.py` as a module/script in Fabric, also copy:

| Repo folder | Lakehouse target |
|---|---|
| `engine/` | `/lakehouse/default/Files/engine/` |

If you run notebook-native entrypoints only, `engine/` does not need to be present in Lakehouse Files.

## Run order

1. Run `nb_dq_00_setup.py` once per environment to create or upgrade Delta tables.
2. Run `nb_dq_01_preflight.py` before promoting runtime changes.
3. Run `nb_dq_03_run_validation.py` after source tables refresh.
   This notebook runner executes `/lakehouse/default/Files/engine/validation_runner.py`
   and prints a post-run summary.
4. Validate `dq_run_results`, `dq_violations`, and `default.dq_execution_metrics`.
5. Run `nb_dq_04_routing.py` to enrich violations with owner/context columns
   into `dq_violations_enriched` (consumed by Power BI).
6. Optionally run `nb_dq_06_notify.py` to send Teams DM notifications via Power
   Automate (handler new-violation DMs and manager escalation DMs); attempts are
   logged to `dq_notification_log`.

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