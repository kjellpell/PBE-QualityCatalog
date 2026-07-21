# PBE Quality Catalog

Data quality and internal control (IC) validation engine for the PBE case management platform,
built on Apache Spark and Delta Lake, designed to run as Fabric Lakehouse notebooks.

This README is for IT operations and maintainers.
For business rule authoring, see RULES_GUIDE.md.
For architecture and design decisions, see ARCHITECTURE.md.

---

## What It Does

The Quality Catalog runs data quality checks across Process, Milestone, and Invoice data and stores both summary and row-level outputs in Delta tables.

Core capabilities:

- Single validation pipeline across domains
- Rules loaded directly from YAML catalogs in `rules/` (the `rule_catalog` Delta
  table is a legacy migration artifact — see ARCHITECTURE.md)
- Run metrics for observability and support
- Current-state issue tracking (Active and Resolved)
- Clear IT/business ownership split
- Active reference validation (checks existence AND active status in a reference table)
- Time-in-state validation (flags records open beyond a configurable day threshold)

---

## Why IT Uses It

- Reliable operations:
  preflight catches missing sources and config before schedule time.
- Maintainable design:
  stable engine code, rules managed as YAML catalogs in `rules/`.
- Observable runs:
  each execution logs status, timing, retryability, and targets.
- Safer rollout:
  dry-run writes to temporary tables.
- Backward-safe schema setup:
  setup is rerunnable and additive.

---

## Repository Structure

```
PBE-QualityCatalog/
├── config/
│   ├── QualityCatalogConfig.py     (table names, paths)
│   └── QualityCatalogRuntime.py    (behavior flags, retry/timeout)
├── engine/
│   ├── expectations.py             (expectation classes + registry)
│   ├── resolution.py               (Active/Resolved violation tracking)
│   ├── runtime.py                  (config loading, target resolution, metrics)
│   └── validation_runner.py        (main orchestration)
├── rules/                          (YAML rule catalogs — loaded directly by the engine)
│   ├── faktura.yaml
│   ├── faser.yaml
│   └── milepeler.yaml
├── nb_dq_00_setup.py               (Delta table DDL)
├── nb_dq_01_preflight.py           (pre-run checks)
├── nb_dq_03_run_validation.py      (Fabric wrapper for the engine)
├── ARCHITECTURE.md
├── DAX_POWERBI.md
├── DEPLOY.md
├── OPERATIONS_QUICK_REF.md
├── README.md
└── RULES_GUIDE.md
```

---

## Runtime Model

### Config layers

- QualityCatalogConfig.py:
  table names, paths, and execution metadata markers.
- QualityCatalogRuntime.py:
  behavior flags and retry markers.

### Config location

Config files must be uploaded to the Lakehouse at:

    /lakehouse/default/Files/Configs/QualityCatalogConfig.py
    /lakehouse/default/Files/Configs/QualityCatalogRuntime.py

The engine will raise a clear error if either file is missing.

### Key runtime toggles

- DRY_RUN:
  write to temporary targets with _tmp suffix.
- FAIL_ON_EMPTY_RULES:
  fail if no rules are found in the YAML catalogs.
- FAIL_ON_EMPTY_SOURCE:
  fail if a configured source table is empty.
- MAX_RETRIES and RETRYABLE_ERROR_MARKERS:
  classify retryability in execution metrics.

---

## Execution Flow

1. Load config/runtime modules and validate required keys.
2. Resolve output targets (production or dry-run).
3. Load rules directly from the YAML catalogs in `rules/`.
4. For each rule group:
  - Read source table from Spark metastore.
  - Apply optional pre-joins.
  - Dispatch each rule to its validator in CUSTOM_EXPECTATION_REGISTRY.
5. Append summary rows to dq_run_results.
6. Apply MERGE-based issue lifecycle to dq_violations.
7. Write execution evidence to dq_execution_metrics.

---

## Output Tables

### dq_run_results

One row per rule per run.

Primary uses:

- Quality score and trend reporting
- Rule-level pass/fail/error analysis
- Severity and category slicing

### dq_violations

One row per unique rule/key issue, maintained as current state.

Primary uses:

- Record-level remediation queues
- Active issue monitoring
- Resolution trend tracking

### default.dq_execution_metrics

One row per runner execution.

Primary uses:

- Run reliability monitoring
- Failure triage and retry decisions
- Duration and throughput trends

---

## Resolution Tracking

Resolution tracking is MERGE-based:

1. Previously Active issues missing from the current run are marked Resolved.
2. Still-active issues are refreshed with latest run metadata.
3. New issues are inserted as Active.

If resolution tracking fails, the run fails with "Violations not written" — no
partial data is committed. Rerun nb_dq_00_setup.py to ensure required tables
and columns exist, then re-run validation.

---

## IT Runbook

### First-time setup

1. Run nb_dq_00_setup.py to create Delta tables.
2. Deploy the YAML catalogs in `rules/` to the Lakehouse (no Delta migration
   needed — rules are loaded directly from YAML).

### Preflight before promotion or scheduling

1. Run nb_dq_01_preflight.py.
2. Confirm the YAML catalogs load and contain rules.
3. Confirm all referenced source tables exist.

### Scheduled execution

1. Run nb_dq_03_run_validation.py (the Fabric wrapper for
   engine/validation_runner.py) after source refresh.
2. Verify summary output and row counts.
3. Confirm evidence in dq_execution_metrics.

For a one-page checklist, see OPERATIONS_QUICK_REF.md.

---

## Troubleshooting

- No rules found:
  verify the YAML catalogs in `rules/` are deployed and contain rules.
- Missing source tables:
  run preflight and confirm metastore names.
- Config loading failures:
  verify Lakehouse config path — ensure both config files exist at /lakehouse/default/Files/Configs/.
- Violation MERGE failures:
  rerun nb_dq_00_setup.py and verify Delta support.
- Unexpected expectation errors:
  validate expectation names, parameters, and source columns in the YAML rule files.
- `validate_active_reference` errors:
  confirm reference table name, column names, and that `active_value` matches the exact value used in the reference table (including capitalisation).
- `validate_time_in_state` errors:
  confirm `start_column` is a date or timestamp column, and that `open_when_column` exists in the source table.

---

## Testing

The repository currently has no automated test suite. Recommended validation
before promoting changes:

1. `python -m py_compile engine/*.py nb_dq_*.py` — catch syntax errors.
2. Run `nb_dq_01_preflight.py` — catches missing tables/columns, parameter
  contract errors.
3. Run `nb_dq_03_run_validation.py` with `DRY_RUN = True` — full run against
   `_tmp` output tables without touching production data.

---

## Related Documents

- ARCHITECTURE.md: architecture decisions and file map
- DEPLOY.md: deployment and environment guidance
- DAX_POWERBI.md: Power BI measures reference
- RULES_GUIDE.md: business rule authoring guide
- OPERATIONS_QUICK_REF.md: one-page operations checklist