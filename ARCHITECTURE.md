# Architecture — PBE Quality Catalog

## Overview

The PBE Quality Catalog is a data quality validation engine
built on Apache Spark and Delta Lake, designed to run as Fabric Lakehouse notebooks.

## Key Design Decisions

### No Great Expectations (GX)
GX Core is **not used**. All validation logic runs through custom expectation classes
registered in `CUSTOM_EXPECTATION_REGISTRY` (see `engine/expectations.py`).
Do not add GX imports, GX dependencies, or GX-based code paths.

### Rules live in YAML
At runtime, the validation engine loads rules directly from YAML files in `rules/`.
Each YAML file is a rule catalog for one rule group (Process, Milestone, Invoice).
A Delta-table-based rule store (`rule_catalog`, loaded via the retired
`nb_dq_02_migrate_rules.py`) was built and tested alongside YAML early on — both
approaches worked. YAML was chosen because it gave the cleanest IT-owns-engine /
business-owns-rules split at the time, not because Delta-based loading failed.
This is a standing decision, not a technical limitation — re-evaluate it if the
tradeoffs change, rather than treating Delta-based rule loading as ruled out.

### Custom expectations only
Every expectation is a Python class with a `validate(df, rule, spark)` method.
New expectations are added to `engine/expectations.py` and registered in
`CUSTOM_EXPECTATION_REGISTRY`. No other file needs to change.

## Runtime Flow

```
nb_dq_00_setup.py           → Create Delta tables (run once)
nb_dq_01_preflight.py       → Pre-run checks (source tables exist, column refs valid,
                              preflight/engine expectation parity)
nb_dq_03_run_validation.py  → Fabric wrapper that executes engine/validation_runner.py:
  1. Load rules from rules/*.yaml
  2. For each rule: load source table, apply joins declared in YAML
  3. Dispatch each rule to CUSTOM_EXPECTATION_REGISTRY
  4. Write results    → dq_run_results
  5. Write violations → dq_violations    (DataFrame-based Active/Resolved tracking)
  6. Write metrics    → dq_execution_metrics
```

## File Map

| File | Purpose |
|------|---------|
| `engine/expectations.py` | All custom expectation classes + registry |
| `engine/resolution.py` | DataFrame-based violation persistence and Active/Resolved tracking |
| `engine/runtime.py` | Config loading (Lakehouse only), target resolution, metrics writing |
| `engine/validation_runner.py` | Main orchestration engine |
| `config/QualityCatalogConfig.py` | Table names and paths |
| `config/QualityCatalogRuntime.py` | Behavior flags (dry-run, retry, fail-on-empty) |
| `nb_dq_00_setup.py` | Delta table DDL (CREATE TABLE IF NOT EXISTS) |
| `nb_dq_01_preflight.py` | Pre-run checks (source tables, column refs, registry parity) |
| `nb_dq_03_run_validation.py` | Fabric wrapper that executes `engine/validation_runner.py` |
| `rules/*.yaml` | Rule catalogs — one file per rule group, loaded at runtime |

## Status Constants

The following string values are used as status/state values across the codebase.
Use these exact strings — typos will silently break resolution-tracking logic.

- **Rule status** (rule_catalog): `"Active"`
- **Run result status** (dq_run_results): `"PASSED"`, `"FAILED"`, `"ERROR"`
- **Violation status** (dq_violations): `"Active"`, `"Resolved"`
- **Execution metric status** (dq_execution_metrics): `"Succeeded"`, `"Failed"`


### Expectation IDs

| Expectation | Meaning |
|---|---|
| `not_null` | Column must be populated |
| `not_null_when` | Column must be populated when condition is true |
| `comparison` | Column compared to another column or scalar value by operator |
| `value_in_list` | Value must be in allowed list |
| `value_when` | Column must equal a required value when condition is true |
| `reference_exists` | Value must exist in a reference table |
| `reference_active` | Value must exist in a reference table and be marked active |
| `row_count` | Table row count satisfies a threshold (guards against failed or runaway loads) |
| `combination_unique` | Column combination is unique |
| `state_duration_within_limit` | Time in open state must not exceed max days |
| `sequence_ordered` | Values appear in expected order within each group |
| `pairs_present` | Required event pairs both exist within each group (`mode: both` or `stop_requires_start`) |
| `gate_complete` | Every group must contain the required completion marker value |
| `group_aggregate_matches` | Group aggregate must match a reference column value |
| `sql_violations` | Custom SQL returns only violating rows |

### YAML Parameter Keys

All parameter names use the `_column` suffix to map directly to DataFrame column names. Plural suffix for list-typed parameters. Nested `reference:` blocks are not supported — use the flat keys.

| Parameter key | Used by expectation(s) |
|---|---|
| `columns` | not_null, not_null_when, combination_unique |
| `column` | not_null (single), value_in_list, reference_exists, reference_active |
| `when_column` | not_null_when, value_when |
| `operator` | comparison, row_count, value_when; not_null_when (`IS NULL`, `IS NOT NULL`, `==`) |
| `value` | not_null_when / value_when trigger value (with `operator: ==`) |
| `required_column`, `required_value` | value_when |
| `left_column`, `right_column`, `right_value` | comparison (`right_column` and `right_value` are mutually exclusive) |
| `filter_column`, `filter_values` | comparison (optional row filter applied before evaluation) |
| `allowed_values` | value_in_list |
| `event_column` | sequence_ordered, pairs_present, gate_complete |
| `order_column` | sequence_ordered; gate_complete (optional) |
| `expected_sequence` | sequence_ordered |
| `required_pairs` | pairs_present |
| `mode` | pairs_present (`both` or `stop_requires_start`) |
| `completion_gate` | sequence_ordered, pairs_present, gate_complete (optional nested block: `event_column`, `value`, `order_column`) |
| `value_to_check` | gate_complete |
| `group_column` | sequence_ordered, pairs_present, gate_complete, group_aggregate_matches |
| `aggregate_column`, `aggregate`, `tolerance` | group_aggregate_matches |
| `reference_table`, `reference_column` | reference_exists, reference_active; `reference_column` also names the comparison column in group_aggregate_matches |
| `reference_active_column`, `reference_active_value` | reference_active |
| `start_column`, `open_state_column`, `open_state_value`, `max_days` | state_duration_within_limit |
| `threshold` | row_count |
| `sql` | sql_violations (also accepted at rule top level) |
| `pk_column` | optional per-rule override of the catalog `pk_column` (row-level expectations) |

### Violation keys

`dq_violations.primary_key_value` is keyed per rule type:

- **Row-level expectations** (not_null, comparison, value_in_list, …) write the
  value of the catalog `pk_column` (or the rule's `pk_column` override).
- **Group-keyed expectations** (`sequence_ordered`, `pairs_present`,
  `gate_complete`, `group_aggregate_matches`) write the rule's `group_column`
  value — one violation per group, not per row.

### NULL semantics

Comparison-style expectations evaluate only rows where the compared columns are
non-NULL (Spark three-valued logic would otherwise make `5 > NULL` neither pass
nor fail). Rows skipped this way count as neither passed nor failed; use
`not_null` / `not_null_when` to enforce presence.

### Vocabulary

- Use **column** (not "field") everywhere when referring to a DataFrame/table column.
- List-typed parameters use **plural** (`columns`, `allowed_values`, `filter_values`, `required_pairs`). Scalar-typed parameters use **singular** (`when_column`, `left_column`, `event_column`, etc.).
- The `when_column` parameter is the column whose value is tested to decide whether the rule applies. The `_when` suffix in an expectation ID signals conditional application (`not_null_when`, `value_when`).

### Notes

- **Engine** (`engine/expectations.py`): Canonical IDs and parameter keys enforced in `CUSTOM_EXPECTATION_REGISTRY` and all expectation classes.
- **Preflight** (`nb_dq_01_preflight.py`): Detects legacy nested `reference:` blocks; validates required canonical keys per expectation; checks column references (including `filter_column` and `completion_gate` columns) against the source schema.
- **YAML catalogs**: All active rule files (`rules/faser.yaml`, `rules/milepeler.yaml`, `rules/faktura.yaml`) use canonical IDs and keys.

## Delta Tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `dq_run_results` | `validation_runner.py` | One row per rule per run |
| `dq_violations` | `validation_runner.py` (via resolution.py) | Current-state violation log (`Active`/`Resolved`) |
| `dq_execution_metrics` | `validation_runner.py` | Run-level observability |