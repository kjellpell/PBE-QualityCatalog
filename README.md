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

## How the Engine Works

`engine/` is the four-file core that everything else (the `scripts/` wrappers,
the notebooks, the tests) calls into. Nothing in here talks to a rule author —
that's `rules/*.yaml`. Nothing in here is Fabric-specific — that's
`scripts/run_validation.py`. The split is deliberate: `engine/` is the part
that stays the same across environments.

```
                  ┌───────────────────┐
  rules/*.yaml ──▶│ engine/runner.py  │──▶ dq_run_results
 source tables ──▶│  (orchestration)  │──▶ dq_violations
                  └─────────┬─────────┘──▶ dq_execution_metrics
                            │ calls
        ┌───────────────────┼────────────────────┐
        ▼                   ▼                    ▼
 rule_engine.py        output_store.py        runtime.py
 (what a violation      (schemas +          (config loading,
  IS, and how to         Active/Resolved     target resolution,
  count it)              resolution)         metrics)
```

### `engine/runner.py` — orchestration

The entry point. For each YAML catalog in `rules/`, it:

1. Reads the catalog's source table from the Spark metastore.
2. Applies any `joins:` and the catalog-level `where:` filter, then caches
   the resulting DataFrame so every rule in the catalog reads it once instead
   of re-scanning the source table per rule.
3. Runs each rule through `engine/rule_engine.py`, with a bounded timeout and
   retry-on-transient-error (`RULE_TIMEOUT_SECONDS` / `MAX_RULE_RETRIES` /
   `RETRYABLE_ERROR_MARKERS`, all from `QualityCatalogRuntime.py`).
4. Collects one summary row and zero-or-more violation rows per rule.

Once every catalog has run, it writes the combined summary rows to
`dq_run_results`, hands the combined violations to `output_store.py` for
resolution tracking, and records a success/failure row in
`dq_execution_metrics` — including on the exception path, so a crashed run
still leaves evidence of why.

### `engine/rule_engine.py` — what a violation is

Defines the six rule types a YAML file can declare, and the single driver,
`run_rule()`, that executes any of them. The split matters: **rule-type
builders only describe what counts as a violation; the driver owns everything
that must behave identically across types** — `when:` filtering, primary-key
resolution, counting in the correct unit, and status/details assembly — so
that behavior can't drift between rule types.

| YAML key | Scope | Checks |
|---|---|---|
| `check` | row | A SQL predicate (`tidsbruk >= 0`). Fails only where the predicate evaluates to FALSE — a NULL operand leaves the row unevaluated, exactly like a SQL `CHECK` constraint. |
| `unique` | row | No duplicate values in a column (or column set). |
| `row_count` | table | The table's row count falls within `minimum`/`maximum`. |
| `event_flow` | group | A group's events occur in the required order/cycle; supports a `completion_gate` to scope the check to groups that have reached a given state. |
| `required_event` | group | A group contains a required event at least once. |
| `aggregate_matches` | group | An aggregate over a group's rows (sum, count, …) matches a reference column. |

`scope` (`row` / `group` / `table`) fixes what one "unit" is when counting
pass/fail: a `group`-scoped rule counts a group once no matter how many of its
rows or pairs fail, so `passed_rows` can never go negative. Every rule type
resolves to exactly one YAML key per rule — `detect_rule_type()` rejects a
rule that declares zero or more than one.

### `engine/output_store.py` — schemas and violation lifecycle

Two things live here, and only here, so they're defined once instead of
restated across the engine and `scripts/setup_dq_tables.py`:

- `RESULT_SCHEMA` / `VIOLATION_SCHEMA` — the canonical Spark schemas for
  `dq_run_results` and `dq_violations`. `scripts/setup_dq_tables.py` generates
  its Delta DDL from these, so the table shape can't drift from what the
  runner actually writes.
- `_apply_resolution_tracking()` — turns each run's raw violations into
  current state: a violation missing from this run that was previously
  `Active` is marked `Resolved`; a still-failing violation keeps its original
  `first_seen_at` so violation age is always answerable; a new violation is
  inserted as `Active`. Implemented with plain DataFrame reads/writes rather
  than a Delta `MERGE`, because Fabric's SQL engine can't resolve
  schema-qualified metastore table names inside a `MERGE` statement.

### `engine/runtime.py` — config and metrics plumbing

Shared helpers that don't belong to rule evaluation itself:

- Locates and loads `QualityCatalogConfig.py` / `QualityCatalogRuntime.py` from
  the Lakehouse `Files/configs/` path, and validates required keys are present.
- Resolves fully-qualified output table names (`resolve_targets`) and the
  rules directory (`resolve_rules_dir`).
- Classifies whether an error message matches a configured retryable pattern
  (`classify_retryable_error`).
- Writes rows to `dq_execution_metrics` (`write_execution_metric`), with a
  fallback to an unqualified table name if the configured schema/namespace
  isn't resolvable — so a metrics-write problem never masks the run's actual
  pass/fail outcome.

---

## Runtime Model

### Config layers

- QualityCatalogConfig.py:
  table names, paths, and execution metadata markers.
- QualityCatalogRuntime.py:
  behavior flags and retry markers.

### Config location

Config files must be uploaded to the Lakehouse at:

    /lakehouse/default/Files/configs/QualityCatalogConfig.py
    /lakehouse/default/Files/configs/QualityCatalogRuntime.py

The engine will raise a clear error if either file is missing.

### Key runtime toggles

- FAIL_ON_EMPTY_SOURCE:
  fail if a configured source table is empty.
- MAX_RETRIES and RETRYABLE_ERROR_MARKERS:
  classify retryability in execution metrics.

---

## Execution Flow

Quick-reference version of "How the Engine Works" above:

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
  verify Lakehouse config path — ensure both config files exist at /lakehouse/default/Files/configs/.
- Violation write failures:
  rerun scripts/setup_dq_tables.py and verify Delta support.
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
2. `python -m py_compile engine/*.py scripts/*.py`
3. Run `scripts/preflight_checks.py` — catches missing tables and unresolvable predicates.
4. Run `scripts/run_validation.py` in the target environment and verify outputs.

---

## Related Documents

- ARCHITECTURE.md: architecture decisions and file map
- DEPLOY.md: deployment and environment guidance
- DAX_POWERBI.md: Power BI measures reference
- RULES_GUIDE.md: rule authoring reference
- OPERATIONS_QUICK_REF.md: one-page operations checklist