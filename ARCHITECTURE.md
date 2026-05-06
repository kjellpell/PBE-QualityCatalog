# Architecture — PBE Quality Catalog

## Overview

The PBE Quality Catalog is a data quality and internal control (IC) validation engine
built on Apache Spark and Delta Lake, designed to run as Fabric Lakehouse notebooks.

## Key Design Decisions

### No Great Expectations (GX)
GX Core is **not used**. All validation logic runs through custom expectation classes
registered in `CUSTOM_EXPECTATION_REGISTRY` (see `engine/expectations.py`).
Do not add GX imports, GX dependencies, or GX-based code paths.

### Rules live in Delta, not YAML
At runtime, rules are loaded from the `rule_catalog` Delta table — **not** from YAML files.
The YAML files in `rules/` are migration source artifacts used by `nb_dq_02_migrate_rules.py`
for one-time insertion into `rule_catalog`. They will be deleted after migration is complete.
Do not add YAML-loading logic to the validation engine.

### Custom expectations only
Every expectation is a Python class with a `validate(df, rule, spark)` method.
New expectations are added to `engine/expectations.py` and registered in
`CUSTOM_EXPECTATION_REGISTRY`. No other file needs to change.

## Runtime Flow

```
nb_dq_00_setup.py          → Create Delta tables (run once)
nb_dq_02_migrate_rules.py  → One-time YAML → rule_catalog migration
nb_dq_01_preflight.py      → Pre-run validation (sources exist, columns match)
engine/validation_runner.py → Main engine:
  1. Load rules from rule_catalog Delta table
  2. For each rule group: load source table, apply joins
  3. Dispatch each rule to CUSTOM_EXPECTATION_REGISTRY
  4. Write results → dq_run_results
  5. Write violations → dq_violations (MERGE-based resolution)
  6. Write IC exceptions → ic_exceptions (4-state lifecycle)
  7. Write metrics → dq_execution_metrics
```

## File Map

| File | Purpose |
|------|---------|
| `engine/expectations.py` | All custom expectation classes + registry |
| `engine/resolution.py` | MERGE-based violation and IC exception persistence |
| `engine/runtime.py` | Config loading (Lakehouse only), target resolution, metrics writing |
| `engine/validation_runner.py` | Main orchestration engine |
| `config/QualityCatalogConfig.py` | Table names, paths, IC settings |
| `config/QualityCatalogRuntime.py` | Behavior flags (dry-run, retry, fail-on-empty) |
| `nb_dq_00_setup.py` | Delta table DDL (CREATE TABLE IF NOT EXISTS) |
| `nb_dq_01_preflight.py` | Pre-run checks (source tables, column refs) |
| `nb_dq_02_migrate_rules.py` | One-time YAML → rule_catalog migration |
| `nb_ic_01_manage_exceptions.py` | Manual IC exception lifecycle management |
| `nb_ic_02_attest_manual_control.py` | Manual control attestation |
| `rules/*.yaml` | Migration source files (temporary, will be deleted) |
| `tests/test_expectations.py` | Unit tests for expectation classes |
| `tests/test_yaml_rules.py` | Unit tests for YAML rule parsing (migration helper) |

## Status Constants

The following string values are used as status/state values across the codebase.
Use these exact strings — typos will silently break MERGE logic.

- **Rule status** (rule_catalog): `"Active"`
- **Run result status** (dq_run_results): `"PASSED"`, `"FAILED"`, `"ERROR"`
- **Violation status** (dq_violations): `"Active"`, `"Resolved"`
- **IC exception status** (ic_exceptions): `"Open"`, `"Remediated"`, `"Verified"`, `"Waived"`
- **Execution metric status** (dq_execution_metrics): `"Succeeded"`, `"Failed"`


### Expectation IDs

| Expectation | Meaning |
|---|---|
| `not_null` | Column must be populated |
| `not_null_when` | Column must be populated when condition is true |
| `comparison` | Two columns compared by operator |
| `value_in_list` | Value must be in allowed list |
| `greater_than` | Column is greater than threshold |
| `value_when` | Column must have value X when condition is true |
| `reference_exists` | Reference must exist in target table |
| `reference_active` | Reference must exist and be marked active |
| `aggregate_threshold` | Aggregate satisfies threshold condition |
| `row_count_in_range` | Row count within min/max bounds |
| `combination_unique` | Column combination is unique |
| `state_duration_within_limit` | Time in open state must not exceed max days |
| `sequence_ordered` | Values appear in expected order |
| `pairs_present` | Required pairs both exist or both absent |
| `stops_paired_with_starts` | Stop value cannot exist without start value |
| `sql_violations` | Custom SQL returns only violating rows |
| `gate_complete` | Group must contain the required completion marker value |
| `columns_excluded` | No row must satisfy the forbidden condition |
| `group_aggregate_matches` | Group aggregate must match a reference column value |

### YAML Parameter Keys

All parameter names use the `_column` suffix to map directly to DataFrame column names. Plural suffix for list-typed parameters. Nested `reference:` blocks are not supported — use the flat keys.

| Parameter key | Used by expectation(s) |
|---|---|
| `when_column` | not_null_when, value_when |
| `checked_columns` | not_null_when |
| `left_column` | comparison |
| `right_column` | comparison |
| `allowed_values` | value_in_list |
| `event_column` | sequence_ordered, pairs_present, stops_paired_with_starts |
| `order_column` | sequence_ordered |
| `start_markers` | pairs_present, stops_paired_with_starts |
| `stop_markers` | pairs_present, stops_paired_with_starts |
| `open_state_column` | state_duration_within_limit |
| `open_state_value` | state_duration_within_limit |
| `reference_table` | reference_exists, reference_active |
| `reference_column` | reference_exists, reference_active |
| `reference_active_column` | reference_active |
| `reference_active_value` | reference_active |
| `group_column` | sequence_ordered, pairs_present, stops_paired_with_starts |
| `source_column` | reference_active, reference_exists |
| `max_days` | state_duration_within_limit |

### Vocabulary

- Use **column** (not "field") everywhere when referring to a DataFrame/table column.
- List-typed parameters use **plural** (`checked_columns`, `columns`, `start_markers`, `stop_markers`, `allowed_values`). Scalar-typed parameters use **singular** (`when_column`, `left_column`, `event_column`, etc.).
- The `when_column` parameter is the column whose value is tested to decide whether the rule applies. The `_when` suffix in an expectation ID signals conditional application (`not_null_when`, `value_when`).

### Notes

- **Engine** (`engine/expectations.py`): Canonical IDs and parameter keys enforced in `CUSTOM_EXPECTATION_REGISTRY` and all expectation classes.
- **Preflight** (`nb_dq_01_preflight.py`): Detects legacy nested `reference:` blocks; validates required canonical keys per expectation.
- **YAML catalogs**: All five active rule files use canonical IDs and keys.
- **IC notebook** (`nb_ic_01_manage_exceptions.py`): Task Flow parameter is `primary_key_value` (matches the column it filters on in `ic_exceptions`).

## Delta Tables

| Table | Written by | Purpose |
|-------|-----------|---------|
| `rule_catalog` | `nb_dq_02_migrate_rules.py` | Active rule definitions |
| `dq_run_results` | `validation_runner.py` | One row per rule per run |
| `dq_violations` | `validation_runner.py` (via resolution.py) | Current-state violation log |
| `dq_execution_metrics` | `validation_runner.py` | Run-level observability |
| `ic_run_results` | `validation_runner.py` | IC-specific run results |
| `ic_exceptions` | `validation_runner.py` (via resolution.py) | IC exception lifecycle |
| `ic_manual_attestations` | `nb_ic_02_attest_manual_control.py` | Manual control attestations |
| `ic_control_register` | `nb_dq_00_setup.py` | Control register metadata |
