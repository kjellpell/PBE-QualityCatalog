# PBE Quality Catalog

Data quality validation engine for the PBE case management platform,
built on Apache Spark and Delta Lake, shipped as Fabric PySpark notebooks.

This README is for IT operations and maintainers.
For rule authoring, see RULES_GUIDE.md.
For architecture and design decisions, see ARCHITECTURE.md.

---

## What It Does

The Quality Catalog runs data quality checks across Process, Milestone, and Invoice data and stores both summary and row-level outputs in Delta tables.

Core capabilities:

- Single validation pipeline across domains
- Rules authored as YAML in the `QC_Rules` notebook and loaded directly from it
- Everything deployable: engine, config and rules are notebook items, so a
  Fabric deployment pipeline promotes the whole thing dev → test → production
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
- Deployable by pipeline:
  no Lakehouse Files to copy by hand — every part is a notebook item.
- Maintainable design:
  stable engine code, rules managed as YAML catalogs in `QC_Rules`.
- Observable runs:
  each execution logs status, timing, retryability, and targets.
- Predictable schema setup:
  setup is rerunnable, generates its DDL from the engine schemas, and reports a
  pre-existing table whose shape has drifted instead of migrating it in place.

---

## Repository Structure

```
PBE-QualityCatalog/
├── notebooks/                      (the deployable artifact — import into Fabric)
│   ├── QC_Config.ipynb             library: config + runtime settings
│   ├── QC_Rules.ipynb              library: the YAML rule catalogs, one per cell
│   ├── QC_Engine.ipynb             library: the whole validation engine
│   ├── QC_Setup_Tables.ipynb       entry point: create the Delta output tables
│   ├── QC_Preflight.ipynb          entry point: pre-run checks
│   └── QC_Run_Validation.ipynb     entry point: run the catalog (schedule this)
├── tests/                          (pytest suite, run against the notebooks)
├── ARCHITECTURE.md
├── DAX_POWERBI.md
├── DEPLOY.md
├── OPERATIONS_QUICK_REF.md
├── README.md
└── RULES_GUIDE.md
```

There is no separate `engine/` or `rules/` directory: Fabric deployment
pipelines do not promote Lakehouse Files, so the code and the rules live inside
notebook items, which they do promote. The notebooks are the source of truth —
the tests read them directly.

---

## How the Engine Works

`QC_Engine` is the core that the three entry-point notebooks and the tests call
into. Nothing in it talks to a rule author — that's `QC_Rules`. Nothing in it
knows about a particular workspace — that's `QC_Config`. The split is
deliberate: `QC_Engine` is the part that stays the same across environments.

An entry-point notebook composes them, one `%run` per cell, then calls in:

```
%run QC_Config      →  QUALITY_CATALOG_CONFIG, QUALITY_CATALOG_RUNTIME
%run QC_Rules       →  RULE_CATALOG_SOURCES
%run QC_Engine      →  configure(), run_with_metrics(), …
```

`%run` executes the referenced notebook in the same Spark session, so all three
end up in one namespace — which is why no two notebooks may define the same
top-level name (`tests/test_notebooks.py` enforces it).

```
    RULE_CATALOG_SOURCES ──▶┌───────────────────┐──▶ dq_run_results
           source tables ──▶│   orchestration   │──▶ dq_violations
                            └─────────┬─────────┘──▶ dq_execution_metrics
                                      │ calls
        ┌─────────────────────────────┼──────────────────────┐
        ▼                             ▼                      ▼
   rule types                  output schemas          runtime helpers
 (what a violation          (+ Active/Resolved        (settings, target
  IS, and how to               resolution)             resolution, metrics)
  count it)
```

### Orchestration

The entry point. For each YAML catalog in `RULE_CATALOG_SOURCES`, it:

1. Reads the catalog's source table from the Spark metastore.
2. Applies any `joins:` and the catalog-level `where:` filter, then caches
   the resulting DataFrame so every rule in the catalog reads it once instead
   of re-scanning the source table per rule.
3. Runs each rule through the rule-type registry, with a bounded timeout and
   retry-on-transient-error (`RULE_TIMEOUT_SECONDS` / `MAX_RULE_RETRIES` /
   `RETRYABLE_ERROR_MARKERS`, all from `QUALITY_CATALOG_RUNTIME`).
4. Collects one summary row and zero-or-more violation rows per rule.

Once every catalog has run, it writes the combined summary rows to
`dq_run_results`, hands the combined violations to resolution tracking, and
records a success/failure row in `dq_execution_metrics` — including on the
exception path, so a crashed run still leaves evidence of why.

Nothing runs when `QC_Engine` is `%run`: it defines names only.
`configure(QUALITY_CATALOG_CONFIG, QUALITY_CATALOG_RUNTIME)` is what opens the
Spark session and resolves the output targets, and every entry point calls it
before anything else.

### Rule types — what a violation is

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

### Output schemas and violation lifecycle

Two things live in one cell, and only there, so they're defined once instead of
restated across the engine and `QC_Setup_Tables`:

- `RESULT_SCHEMA` / `VIOLATION_SCHEMA` — the canonical Spark schemas for
  `dq_run_results` and `dq_violations`. `QC_Setup_Tables` generates its Delta
  DDL from these, so the table shape can't drift from what the runner actually
  writes.
- `_apply_resolution_tracking()` — turns each run's raw violations into
  current state: a violation missing from this run that was previously
  `Active` is marked `Resolved`; a still-failing violation keeps its original
  `first_seen_at` so violation age is always answerable; a new violation is
  inserted as `Active`. Implemented with plain DataFrame reads/writes rather
  than a Delta `MERGE`, because Fabric's SQL engine can't resolve
  schema-qualified metastore table names inside a `MERGE` statement.

### Runtime helpers — config and metrics plumbing

Shared helpers that don't belong to rule evaluation itself:

- Validates that the config dicts carry every required key and gives them
  attribute access (`build_settings`).
- Resolves fully-qualified output table names (`resolve_targets`).
- Classifies whether an error message matches a configured retryable pattern
  (`classify_retryable_error`).
- Writes rows to `dq_execution_metrics` (`write_execution_metric`), with a
  fallback to an unqualified table name if the configured schema/namespace
  isn't resolvable — so a metrics-write problem never masks the run's actual
  pass/fail outcome.

---

## Runtime Model

### Config layers

Both live in `QC_Config`, as plain dicts:

- `QUALITY_CATALOG_CONFIG`:
  target schema and the three output table names.
- `QUALITY_CATALOG_RUNTIME`:
  behavior flags, retry markers, and catalog filter overrides.

The values are the same in every stage, so `QC_Config` is promoted unchanged by
the deployment pipeline. `configure()` raises a clear error naming any missing
key.

### Key runtime toggles

- FAIL_ON_EMPTY_SOURCE:
  fail if a configured source table is empty.
- MAX_RETRIES and RETRYABLE_ERROR_MARKERS:
  classify retryability in execution metrics.

---

## Execution Flow

Quick-reference version of "How the Engine Works" above:

1. `configure()` validates the config dicts and opens the Spark session.
2. Resolve output targets.
3. Load rules from the YAML catalogs in `RULE_CATALOG_SOURCES`.
4. For each rule group:
  - Read source table from Spark metastore.
  - Apply optional pre-joins and the catalog `where:` filter.
  - Run each rule through `run_rule()`.
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

### dq_execution_metrics

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
partial data is committed. Rerun `QC_Setup_Tables` to confirm the tables exist
with the expected schema, then re-run validation.

---

## IT Runbook

### First-time setup

1. Import the six notebooks from `notebooks/` into the workspace and attach a
   default lakehouse to the three entry-point notebooks.
2. Run `QC_Setup_Tables` to create the Delta tables.

See DEPLOY.md for the deployment-pipeline steps.

### Preflight before promotion or scheduling

1. Run `QC_Preflight`.
2. Confirm the YAML catalogs load and contain rules.
3. Confirm all referenced source tables exist.

### Scheduled execution

1. Run `QC_Run_Validation` after source refresh.
2. Verify summary output and row counts.
3. Confirm evidence in dq_execution_metrics.

For a one-page checklist, see OPERATIONS_QUICK_REF.md.

---

## Troubleshooting

- No rules found:
  confirm `QC_Rules` was `%run` and that its cells populate
  `RULE_CATALOG_SOURCES`.
- Missing source tables:
  run preflight and confirm metastore names.
- `NameError` on `configure`, `RULE_CATALOG_SOURCES`, … :
  a `%run` cell did not execute — run the notebook from the top.
- Config errors:
  `configure()` names the missing key; fix it in `QC_Config`.
- Violation write failures:
  rerun `QC_Setup_Tables` and verify Delta support.
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

The tests read the notebooks directly: `tests/notebook_source.py` executes a
notebook's code cells into a module namespace, skipping `%run` cells and the
`entrypoint`-tagged final cell, which is exactly what Fabric does minus the
pressing of Run. So there is no second copy of the engine to keep in step —
what the tests exercise is what gets deployed.

| Test module | Covers |
|---|---|
| `test_expectations.py` | Each rule type, predicate NULL semantics, scope counting |
| `test_preflight.py` | Rule-contract and predicate validation, incl. typo detection |
| `test_resolution.py` | Violation lifecycle: new → Active → Resolved, `first_seen_at` |
| `test_rule_loading.py` | Catalog loading, and that an unusable catalog fails loudly |
| `test_equivalence.py` | Every catalog end to end, diffed against a committed baseline |
| `test_notebooks.py` | The notebooks compile, compose, and define no clashing names |
| `test_docs.py` | The rule-type reference in RULES_GUIDE.md matches the engine |

`test_equivalence.py` is the regression gate: it fails on any unintended change
to `dq_run_results` or `dq_violations`. When output changes are intended,
regenerate with `DQ_UPDATE_BASELINE=1 python -m pytest tests/test_equivalence.py`
and review the diff.

Before promoting changes:

1. `python -m pytest tests/ -v`
2. Run `QC_Preflight` — catches missing tables and unresolvable predicates.
3. Run `QC_Run_Validation` in the target environment and verify outputs.

---

## Related Documents

- ARCHITECTURE.md: architecture decisions and file map
- DEPLOY.md: deployment and environment guidance
- DAX_POWERBI.md: Power BI measures reference
- RULES_GUIDE.md: rule authoring reference
- OPERATIONS_QUICK_REF.md: one-page operations checklist