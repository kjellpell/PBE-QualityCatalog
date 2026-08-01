# PBE Quality Catalog

Data quality validation engine for the PBE case management platform,
built on Apache Spark and Delta Lake, designed to run as Fabric Lakehouse notebooks.

This README is for IT operations and maintainers.
For rule authoring, see RULES_GUIDE.md.
For architecture and design decisions, see ARCHITECTURE.md.

---

## What It Does

The Quality Catalog runs data quality checks across Process, Milestone, and Invoice data and stores both summary and row-level outputs in Delta tables.

Core capabilities:

- Single validation pipeline across domains
- Rules loaded directly from YAML catalogs in `rules/` (the `rule_catalog` Delta
  table is a legacy migration artifact — see ARCHITECTURE.md)
- Run metrics for observability and support
- Current-state issue tracking (Active and Resolved), preserving when an issue
  was first seen so violation age is answerable
- Rules authored as Spark SQL predicates, validated against the real schema
  before a run
- Group-level checks (required event pairs, ordering, completion gates) that a
  single-row predicate cannot express

---

## Why IT Uses It

- Reliable operations:
  preflight catches missing sources and config before schedule time.
- Maintainable design:
  stable engine code, rules managed as YAML catalogs in `rules/`.
- Observable runs:
  each execution logs status, timing, retryability, and targets.
- Predictable schema setup:
  setup is rerunnable, generates its DDL from the engine schemas, and reports a
  pre-existing table whose shape has drifted instead of migrating it in place.

---

## Repository Structure

```
PBE-QualityCatalog/
├── config/
│   ├── QualityCatalogConfig.py     (table names, paths)
│   └── QualityCatalogRuntime.py    (behavior flags, retry/timeout)
├── engine/
│   ├── rule_engine.py              (rule types + the driver that runs a rule)
│   ├── output_store.py             (output schemas, Active/Resolved tracking)
│   ├── runtime.py                  (config loading, target resolution, metrics)
│   └── runner.py                   (main orchestration)
├── rules/                          (YAML rule catalogs — loaded directly by the engine)
│   ├── faktura.yaml
│   ├── faser.yaml
│   └── milepeler.yaml
├── tests/                          (pytest suite; see requirements-dev.txt)
├── scripts/
│   ├── setup_dq_tables.py          (Delta table DDL, generated from the schemas)
│   ├── preflight_checks.py         (pre-run checks)
│   └── run_validation.py           (Fabric wrapper for the engine)
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

- FAIL_ON_EMPTY_SOURCE:
  fail if a configured source table is empty.
- MAX_RETRIES and RETRYABLE_ERROR_MARKERS:
  classify retryability in execution metrics.

---

## Execution Flow

1. Load config/runtime modules and validate required keys.
2. Resolve output targets.
3. Load rules directly from the YAML catalogs in `rules/`.
4. For each rule group:
  - Read source table from Spark metastore.
  - Apply optional pre-joins and the catalog `where:` filter.
  - Run each rule through the driver in engine/rule_engine.py.
5. Append summary rows to dq_run_results.
6. Apply the issue lifecycle to dq_violations.
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

Each run diffs the current violations against the stored ones:

1. Previously Active issues missing from the current run are marked Resolved.
2. Still-active issues are refreshed with latest run metadata, keeping their
   original `first_seen_at`.
3. New issues are inserted as Active.

Persistence uses the DataFrame API rather than SQL `MERGE` — Fabric cannot
resolve schema-qualified names inside a `MERGE` statement.

If resolution tracking fails, the run fails with "Violations not written" — no
partial data is committed. Rerun scripts/setup_dq_tables.py to confirm the tables exist
with the expected schema, then re-run validation.

---

## IT Runbook

### First-time setup

1. Run scripts/setup_dq_tables.py to create Delta tables.
2. Deploy the YAML catalogs in `rules/` to the Lakehouse (no Delta migration
   needed — rules are loaded directly from YAML).

### Preflight before promotion or scheduling

1. Run scripts/preflight_checks.py.
2. Confirm the YAML catalogs load and contain rules.
3. Confirm all referenced source tables exist.

### Scheduled execution

1. Run scripts/run_validation.py (the Fabric wrapper for
   engine/runner.py) after source refresh.
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
- Violation write failures:
  rerun nb_dq_00_setup.py and verify Delta support.
- Rule configuration errors:
  run preflight — it resolves every `where:`, `when:` and `check:` predicate
  against the real schema and reports the offending rule.

---

## Testing

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

The suite runs locally against PySpark and Delta — no Fabric needed. Delta is
required rather than plain parquet, because the resolution path does a
read-then-overwrite that parquet rejects.

| Test module | Covers |
|---|---|
| `test_expectations.py` | Each rule type, predicate NULL semantics, scope counting |
| `test_preflight.py` | Rule-contract and predicate validation, incl. typo detection |
| `test_resolution.py` | Violation lifecycle: new → Active → Resolved, `first_seen_at` |
| `test_equivalence.py` | Every catalog end to end, diffed against a committed baseline |
| `test_docs.py` | The rule-type reference in RULES_GUIDE.md matches the engine |

`test_equivalence.py` is the regression gate: it fails on any unintended change
to `dq_run_results` or `dq_violations`. When output changes are intended,
regenerate with `DQ_UPDATE_BASELINE=1 python -m pytest tests/test_equivalence.py`
and review the diff.

Before promoting changes:

1. `python -m pytest tests/ -v`
2. `python -m py_compile engine/*.py nb_dq_*.py`
3. Run `scripts/preflight_checks.py` — catches missing tables and unresolvable predicates.
4. Run `scripts/run_validation.py` in the target environment and verify outputs.

---

## Related Documents

- ARCHITECTURE.md: architecture decisions and file map
- DEPLOY.md: deployment and environment guidance
- DAX_POWERBI.md: Power BI measures reference
- RULES_GUIDE.md: rule authoring reference
- OPERATIONS_QUICK_REF.md: one-page operations checklist