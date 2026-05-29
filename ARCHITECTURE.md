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
Rules are maintained as YAML and do not go through Delta. (Earlier revisions
migrated rules through a Delta `rule_catalog` table; that path is retired.)
Do not add Delta-based rule loading to the validation engine.

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
  4. Write results    → dq_run_results   (routing_team from YAML routing: field)
  5. Write violations → dq_violations    (MERGE-based Active/Resolved tracking)
  6. Write metrics    → dq_execution_metrics
nb_dq_04_routing.py         → Post-validation enrichment:
  1. Read violations for the latest run
  2. Join routing_team from rules index
  3. For each catalog: load source table, apply YAML joins, look up owner via ansatte
  4. Write dq_violations_enriched (union across all catalogs, context columns, owner)
nb_dq_06_notify.py          → Teams DM notifications via Power Automate webhooks:
  1. Read Active violations from dq_violations_enriched
  2. Handler DMs for violations first seen today; manager DMs for escalations
     (open longer than the catalog's escalation_days)
  3. Log attempts to dq_notification_log
```

## File Map

| File | Purpose |
|------|---------|
| `engine/expectations.py` | All custom expectation classes + registry |
| `engine/resolution.py` | MERGE-based violation and IC exception persistence |
| `engine/runtime.py` | Config loading (Lakehouse only), target resolution, metrics writing |
| `engine/validation_runner.py` | Main orchestration engine |
| `config/QualityCatalogConfig.py` | Table names, paths, ansatte lookup settings |
| `config/QualityCatalogRuntime.py` | Behavior flags (dry-run, retry, fail-on-empty) |
| `nb_dq_00_setup.py` | Delta table DDL (CREATE TABLE IF NOT EXISTS) |
| `nb_dq_01_preflight.py` | Pre-run checks (source tables, column refs, registry parity) |
| `nb_dq_03_run_validation.py` | Fabric wrapper that executes `engine/validation_runner.py` |
| `nb_dq_04_routing.py` | Post-validation enrichment → dq_violations_enriched |
| `nb_dq_06_notify.py` | Teams DM notifications via Power Automate → dq_notification_log |
| `rules/*.yaml` | Rule catalogs — one file per rule group, loaded at runtime |

## Status Constants

The following string values are used as status/state values across the codebase.
Use these exact strings — typos will silently break MERGE logic.

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
| `column` | not_null (single), reference_exists, reference_active, value_in_list |
| `columns` | not_null, not_null_when, combination_unique |
| `when_column` | not_null_when, value_when |
| `left_column` | comparison |
| `right_column` | comparison |
| `right_value` | comparison (scalar mode) |
| `operator` | comparison, value_when, row_count |
| `allowed_values` | value_in_list |
| `event_column` | sequence_ordered, pairs_present, gate_complete |
| `order_column` | sequence_ordered, gate_complete |
| `expected_sequence` | sequence_ordered |
| `required_pairs` | pairs_present |
| `mode` | pairs_present (`both` or `stop_requires_start`) |
| `value_to_check` | gate_complete |
| `required_column` / `required_value` | value_when |
| `open_state_column` | state_duration_within_limit |
| `open_state_value` | state_duration_within_limit |
| `start_column` | state_duration_within_limit |
| `max_days` | state_duration_within_limit |
| `reference_table` | reference_exists, reference_active |
| `reference_column` | reference_exists, reference_active, group_aggregate_matches |
| `reference_active_column` | reference_active |
| `reference_active_value` | reference_active |
| `group_column` | sequence_ordered, pairs_present, gate_complete, group_aggregate_matches |
| `aggregate_column` / `aggregate` / `tolerance` | group_aggregate_matches |
| `threshold` | row_count |
| `sql` | sql_violations |
| `pk_column` | most expectations (defaults to the catalog `pk_column`) |

### Vocabulary

- Use **column** (not "field") everywhere when referring to a DataFrame/table column.
- List-typed parameters use **plural** (`columns`, `required_pairs`, `allowed_values`). Scalar-typed parameters use **singular** (`when_column`, `left_column`, `event_column`, etc.).
- The `when_column` parameter is the column whose value is tested to decide whether the rule applies. The `_when` suffix in an expectation ID signals conditional application (`not_null_when`, `value_when`).

### Notes

- **Engine** (`engine/expectations.py`): Canonical IDs and parameter keys enforced in `CUSTOM_EXPECTATION_REGISTRY` and all expectation classes.
- **Preflight** (`nb_dq_01_preflight.py`): Detects legacy nested `reference:` blocks; validates required canonical keys per expectation.
- **YAML catalogs**: All three active rule files use canonical IDs and keys.

## Delta Tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `dq_run_results` | `validation_runner.py` | One row per rule per run; includes `routing_team` |
| `dq_violations` | `validation_runner.py` (via resolution.py) | Current-state violation log (`Active`/`Resolved`); includes `routing_team` |
| `dq_violations_enriched` | `nb_dq_04_routing.py` | Unified violation table with owner, routing_team, and catalog-specific context columns; used by Power BI handler/manager report |
| `dq_execution_metrics` | `validation_runner.py` | Run-level observability |
| `dq_notification_log` | `nb_dq_06_notify.py` | One row per Teams DM attempt (handler/manager), with status |