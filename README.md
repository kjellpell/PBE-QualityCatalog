# PBE Quality Catalog

YAML-driven data quality framework for the PBE case management platform, built on Apache Spark, Delta Lake, and Great Expectations (GX Core).

This README is for IT operations and maintainers.
For business rule authoring, see RULES_GUIDE.md.

---

## What It Does

The Quality Catalog runs data quality checks across Process, Milestone, and Invoice data and stores both summary and row-level outputs in Delta tables.

Core capabilities:

- Single validation pipeline across domains
- Automatic discovery of YAML rule catalogs
- Run metrics for observability and support
- Current-state issue tracking (Active and Resolved)
- Clear IT/business ownership split

---

## Why IT Uses It

- Reliable operations:
  preflight catches missing sources and config before schedule time.
- Maintainable design:
  stable engine code, changeable YAML rules.
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
├── rules/
│   ├── process_rules.yaml
│   ├── milestone_rules.yaml
│   └── invoice_rules.yaml
├── tests/
│   └── test_expectations.py
├── nb_dq_00_setup.py
├── nb_dq_01_preflight.py
├── DEPLOY.md
├── RULES_GUIDE.md
├── OPERATIONS_QUICK_REF.md
├── dq_powerbi_measures.md
└── README.md
```

---

## Runtime Model

### Config layers

- QualityCatalogConfig.py:
  table names, rules folder, sample size, and execution metadata markers.
- QualityCatalogRuntime.py:
  behavior flags and retry markers.

### Config loading order

1. /lakehouse/default/Files/Configs
2. repo-local config fallback

Set REQUIRE_LAKEHOUSE_CONFIG=1 to require Lakehouse config.

### Key runtime toggles

- DRY_RUN:
  write to temporary targets with _tmp suffix.
- FAIL_ON_EMPTY_RULES:
  fail if no YAML rule catalogs are found.
- FAIL_ON_EMPTY_SOURCE:
  fail if a configured source table is empty.
- MAX_RETRIES and RETRYABLE_ERROR_MARKERS:
  classify retryability in execution metrics.

---

## Execution Flow

1. Load config/runtime modules and validate required keys.
2. Resolve output targets (production or dry-run).
3. Discover all rules/*.yaml catalogs automatically.
4. For each catalog:
   - Read source table from Spark metastore.
   - Apply optional pre-joins.
   - Run each rule via GX native or custom expectation registry.
5. Append summary rows to dq_run_results.
6. Apply MERGE-based issue lifecycle updates in dq_violations.
7. Write execution evidence to default.dq_execution_metrics.

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

1. Install prerequisites in the Spark environment:
   - great-expectations==1.3.10
2. Run nb_dq_00_setup.py once.

### Preflight before promotion or scheduling

1. Run nb_dq_01_preflight.py.
2. Confirm catalogs are discoverable.
3. Confirm all referenced source tables exist.

### Scheduled execution

1. Run engine/validation_runner.py after source refresh.
2. Verify summary output and row counts.
3. Confirm evidence in dq_execution_metrics.

For a one-page checklist, see OPERATIONS_QUICK_REF.md.

---

## Ownership

### IT team

- Runtime, deployment, scheduling
- Config control and environment hardening
- Output table lifecycle and schema compatibility
- Monitoring, alerting, incident response
- Custom expectation engineering when needed

### Business team

- Rule definition and maintenance in YAML
- Severity/category/owner governance
- Interpretation and follow-up of violations

Business authoring guidance is in RULES_GUIDE.md.

---

## Troubleshooting

- No catalogs found:
  verify RULES_DIR and FAIL_ON_EMPTY_RULES.
- Missing source tables:
  run preflight and confirm metastore names.
- Config loading failures:
  verify Lakehouse config path and REQUIRE_LAKEHOUSE_CONFIG.
- Violation MERGE failures:
  rerun nb_dq_00_setup.py and verify Delta support.
- Unexpected expectation errors:
  validate expectation names, parameters, and source columns in YAML.

---

## Testing

Recommended local validation:

```bash
pip install pytest pyspark
pytest tests/ -v
```

Tests cover core custom expectations and resolution helper behavior in local Spark mode.

---

## Related Documents

- DEPLOY.md: deployment and environment guidance
- dq_powerbi_measures.md: reporting measures reference
- RULES_GUIDE.md: business rule authoring guide
- OPERATIONS_QUICK_REF.md: one-page operations checklist