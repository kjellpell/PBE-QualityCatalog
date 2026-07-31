# Architecture — PBE Quality Catalog

## Overview

The PBE Quality Catalog is a data quality validation engine built on Apache
Spark and Delta Lake, designed to run as Fabric Lakehouse notebooks.

Rule authors are SQL-fluent engineers, and the alternative being weighed
against is hand-written SQL. That shapes the rule format: a rule is a Spark SQL
predicate, and the engine supplies what a query cannot — shared boilerplate,
violation lifecycle state, and schema validation before the run.

## Key Design Decisions

### No Great Expectations (GX)

GX Core is **not used**. All validation logic lives in `engine/expectations.py`.
Do not add GX imports, GX dependencies, or GX-based code paths.

### Rules live in YAML

At runtime the engine loads rules directly from YAML files in `rules/`. Each
file is a rule catalog for one rule group.

A Delta-table-based rule store (`rule_catalog`, loaded via the retired
`nb_dq_02_migrate_rules.py`) was built and tested alongside YAML early on — both
worked. YAML was chosen because it keeps rules diffable and reviewable next to
the engine they run against. This is a standing decision, not a technical
limitation — re-evaluate it if the tradeoffs change, rather than treating
Delta-based rule loading as ruled out.

### A rule is a predicate

Most rules are a single boolean predicate under `check:`, optionally scoped by
`when:`. The remaining rule types cover checks a row predicate cannot express:
uniqueness and checks over groups of rows.

`check:` follows SQL `CHECK` constraint semantics — a row fails only when the
predicate is FALSE, so a NULL predicate leaves the row unevaluated. This is what
`~F.expr(...)` already does in Spark; it is inherited, not implemented.

### Rules cannot modify data

Every rule type is a predicate or a declarative block; none executes a
caller-supplied statement. `check:`, `when:` and `where:` go through `F.expr()`,
which builds a column expression and cannot carry DDL or DML. A `sql:` rule type
existed and was removed: nothing used it, `spark.sql()` ran whatever string it
was given, and `DRY_RUN` did not protect against it — dry-run redirects the
*output* tables, so a mutating query would still have run against production.
Removing it makes read-only a property of the engine rather than a convention.

A check needing a second table uses `joins:` in the catalog header, so the
joined columns are available to an ordinary predicate.

### One driver owns the common work

`run_rule()` in `engine/expectations.py` owns `when:` filtering, primary-key
resolution, counting, status derivation, and building the violations frame.
Rule-type builders only describe what a violation *is*. Anything shared lives in
the driver so it cannot drift between rule types.

### Scope fixes the counting unit

Each rule type declares `scope` — `row`, `group`, or `table` — and the driver counts in
that unit. A group-scoped rule counts groups in both the numerator and the
denominator, so a group failing several pairs counts once and `passed_rows` can
never go negative.

Table-scoped checks are limited to bounded row-volume validation (`row_count`)
for silver-layer blocker workflows. `FAIL_ON_EMPTY_SOURCE` remains a guard on
the *run* rather than the data: it aborts before an empty source can be
reported as 100% passing.

## Runtime Flow

```
nb_dq_00_setup.py           → Create Delta tables (run once; DDL generated from
                              the engine's own schemas)
nb_dq_01_preflight.py       → Pre-run checks (source tables exist, predicates
                              resolve against the real schema, rule contract)
nb_dq_03_run_validation.py  → Fabric wrapper that executes engine/validation_runner.py:
  1. Load rules from rules/*.yaml and *.yml
  2. Per catalog: load source table, apply joins, apply the catalog `where:`
  3. Dispatch each rule through run_rule()
  4. Write results    → dq_run_results
  5. Write violations → dq_violations    (DataFrame-based Active/Resolved tracking)
  6. Write metrics    → dq_execution_metrics
```

## File Map

| File | Purpose |
|------|---------|
| `engine/expectations.py` | Rule types, the registry, and the driver that runs a rule |
| `engine/resolution.py` | Output schemas + violation persistence with Active/Resolved tracking |
| `engine/runtime.py` | Config loading (Lakehouse only), target resolution, metrics writing |
| `engine/validation_runner.py` | Main orchestration engine |
| `config/QualityCatalogConfig.py` | Table names and paths |
| `config/QualityCatalogRuntime.py` | Behavior flags (dry-run, retry, fail-on-empty) |
| `nb_dq_00_setup.py` | Delta table DDL, generated from the engine schemas |
| `nb_dq_01_preflight.py` | Pre-run checks |
| `nb_dq_03_run_validation.py` | Fabric wrapper that executes `engine/validation_runner.py` |
| `rules/*.yaml` | Rule catalogs — one file per rule group, loaded at runtime |
| `tests/` | Rule-type, preflight, resolution and end-to-end equivalence tests |

## Rule Contract

The contract is declared once, in `RULE_TYPES` (`engine/expectations.py`). Each
entry carries the YAML key, its scope, its required config keys, which of those
name source columns, and whether it accepts a `completion_gate`. Preflight reads
that structure rather than restating it, so the two cannot drift.

See [RULES_GUIDE.md](RULES_GUIDE.md) for the authoring reference; the rule-type
table there is checked against `RULE_TYPES` by `tests/test_docs.py`.

### Primary keys

One convention: a rule uses its own `pk_column` if set, otherwise the catalog's.
Group-scoped rules key violations by their `group_column` instead. A catalog
without a `pk_column` fails preflight rather than silently defaulting.

### Predicate columns

For a `check:` rule, `violated_column` is the first column referenced by the
predicate. It is derived by walking the *unresolved* expression tree, which
preserves source order — Spark's analyzer reference set does not, and would
report `a` for `b >= a`. The walk is best-effort: if it fails, `violated_column`
is NULL rather than the rule erroring.

## Status Constants

Use these exact strings — typos silently break resolution tracking.

- **Run result status** (`dq_run_results`): `"PASSED"`, `"FAILED"`, `"ERROR"`
- **Violation status** (`dq_violations`): `"Active"`, `"Resolved"`
- **Violation scope** (`dq_violations`): `"row"`, `"group"`, `"table"`
- **Execution metric status** (`dq_execution_metrics`): `"Succeeded"`, `"Failed"`

## Delta Tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `dq_run_results` | `validation_runner.py` | One row per rule per run |
| `dq_violations` | `validation_runner.py` (via `resolution.py`) | Current-state violation log (`Active`/`Resolved`) |
| `dq_execution_metrics` | `validation_runner.py` | Run-level observability |

Schemas are defined in `engine/resolution.py` (`RESULT_SCHEMA`,
`VIOLATION_SCHEMA`) and `engine/runtime.py` (`_EXECUTION_METRIC_SCHEMA`).
`nb_dq_00_setup.py` generates its DDL from them, so adding a column there is
enough.

### Violation persistence

`_apply_resolution_tracking()` reads the existing violations table, diffs it
against the current run, and rewrites it:

- new violation → inserted `Active`;
- still present → run metadata refreshed, `first_seen_at` preserved;
- previously active, now absent → `Resolved` with a resolution timestamp;
- already resolved → carried through unchanged.

Violations are keyed on `(rule_id, primary_key_value, violated_column,
expected_condition)`. `expected_condition` is part of the key because a
group-scoped rule can emit several distinct violations for one group that differ
only in which pair failed.

Persistence uses the DataFrame API rather than SQL `MERGE`: Fabric's SQL engine
cannot resolve schema-qualified Hive metastore names inside a `MERGE`. The read
is checkpointed before the overwrite, otherwise Spark rejects writing to a table
the plan still reads from.

## Testing

`python -m pytest tests/ -v` (see `requirements-dev.txt`). Tests need Delta, not
plain parquet — the resolution path does a read-then-overwrite that parquet
rejects.

`tests/test_equivalence.py` runs every catalog end to end against synthetic
fixtures and diffs the output against a committed baseline. Regenerate it
deliberately with `DQ_UPDATE_BASELINE=1` when output changes are intended.
