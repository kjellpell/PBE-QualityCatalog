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
- Rules loaded from the `rule_catalog` Delta table
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
  stable engine code, rules managed in `rule_catalog` Delta table.
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
│   ├── QualityCatalogConfig.py
│   └── QualityCatalogRuntime.py
├── engine/
│   ├── expectations.py
│   ├── resolution.py
│   ├── runtime.py
│   └── validation_runner.py
├── rules/                          (YAML rule files — loaded directly by validation engine)
│   ├── invoice_rules.yaml
│   ├── milestone_rules.yaml
│   └── process_rules.yaml
├── tests/
│   ├── __init__.py
│   ├── test_expectations.py
│   └── test_yaml_rules.py
├── nb_dq_00_setup.py
├── nb_dq_01_preflight.py
├── nb_dq_02_migrate_rules.py
├── ARCHITECTURE.md
├── DEPLOY.md
├── IC_RULES_GUIDE.md
├── KG.md
├── OPERATIONS_QUICK_REF.md
├── README.md
├── RULES_GUIDE.md
└── dq_powerbi_measures.md
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
  fail if no active rules are found in rule_catalog.
- FAIL_ON_EMPTY_SOURCE:
  fail if a configured source table is empty.
- MAX_RETRIES and RETRYABLE_ERROR_MARKERS:
  classify retryability in execution metrics.

---

## Execution Flow

1. Load config/runtime modules and validate required keys.
2. Resolve output targets (production or dry-run).
3. Load active rules from rule_catalog Delta table.
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

If MERGE is unavailable, the system falls back to append and logs a warning.
Rerun nb_dq_00_setup.py to ensure required tables and columns exist.

---

## IT Runbook

### First-time setup

1. Run nb_dq_00_setup.py to create Delta tables.
2. Run nb_dq_02_migrate_rules.py to populate rule_catalog from YAML files.

### Preflight before promotion or scheduling

1. Run nb_dq_01_preflight.py.
2. Confirm rule_catalog has Active rows.
3. Confirm all referenced source tables exist.

### Scheduled execution

1. Run engine/validation_runner.py after source refresh.
2. Verify summary output and row counts.
3. Confirm evidence in dq_execution_metrics.

For a one-page checklist, see OPERATIONS_QUICK_REF.md.

---

## Troubleshooting

- No active rules found:
  verify rule_catalog has Active rows.
- Missing source tables:
  run preflight and confirm metastore names.
- Config loading failures:
  verify Lakehouse config path — ensure both config files exist at /lakehouse/default/Files/Configs/.
- Violation MERGE failures:
  rerun nb_dq_00_setup.py and verify Delta support.
- Unexpected expectation errors:
  validate expectation names, parameters, and source columns in rule_catalog.
- `validate_active_reference` errors:
  confirm reference table name, column names, and that `active_value` matches the exact value used in the reference table (including capitalisation).
- `validate_time_in_state` errors:
  confirm `start_column` is a date or timestamp column, and that `open_when_column` exists in the source table.

---

## Testing

Recommended local validation:

```bash
pip install pytest pyspark
pytest tests/ -v
```

Tests cover core custom expectations, YAML rule parsing, and resolution helper behavior in local Spark mode.

---

## Related Documents

- ARCHITECTURE.md: architecture decisions and file map
- DEPLOY.md: deployment and environment guidance
- dq_powerbi_measures.md: reporting measures reference
- RULES_GUIDE.md: business rule authoring guide
- OPERATIONS_QUICK_REF.md: one-page operations checklist