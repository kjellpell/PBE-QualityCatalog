# PBE Quality Catalog

YAML-driven data quality framework for the PBE case management platform, built on Apache Spark and [Great Expectations](https://greatexpectations.io/) (GX Core).

---

## Repository Structure

```
PBE-QualityCatalog/
├── engine/
│   ├── expectations.py          # All custom expectation classes (consolidated)
│   └── validation_runner.py     # Core engine — discovers and runs all rule files
├── rules/
│   ├── process_rules.yaml       # Rules for Saksbehandling.Prosesser
│   ├── milestone_rules.yaml     # Rules for Saksbehandling.Milepel
│   └── invoice_rules.yaml       # Rules for Saksbehandling.Fakturalinjer
├── outputs/
│   └── validation_results/      # Delta table result logs (dq_run_results, dq_violations)
├── tests/
│   └── test_expectations.py     # Unit tests for expectation classes
├── nb_dq_00_setup.py            # One-time Delta table setup (run before first validation)
├── nb_01_setup.py               # General environment setup
├── dq_powerbi_measures.md       # Power BI DAX measure reference
└── README.md
```

---

## Quick Start

### 1. Prerequisites

- Microsoft Fabric workspace (or any Spark 3.x + Delta Lake environment)
- Great Expectations Core: `pip install great-expectations==1.3.10`

### 2. First-time setup

Run `nb_dq_00_setup.py` once to create the Delta output tables:

```
dq_run_results   – one row per rule per run (summary / scorecard)
dq_violations    – one row per offending record per rule per run (drill-through)
```

### 3. Running validations

Execute `engine/validation_runner.py`.  The engine will:

1. Scan the `rules/` folder and load **all** `*.yaml` files automatically.
2. For each rule file, load the source table from the Spark metastore using
   the `database` and `table` fields in the YAML header.
3. Apply any configured pre-joins (e.g. enriching Prosesser with Status).
4. Run every rule in the file, dispatching to either:
   - **GX native** expectations (via the GX Core validator on a Pandas sample), or
   - **Custom PySpark** expectations (via `CUSTOM_EXPECTATION_REGISTRY`).
5. Write results to `dq_run_results` and `dq_violations`.

---

## Adding New Rules

No Python changes are needed to add a new rule.

1. Open the appropriate YAML file in `rules/` (or create a new one for a new domain).
2. Copy an existing rule block as a template and set the new `rule_id`, `name`, `expectation`, and `parameters`.
3. Save the file.  The next validation run picks it up automatically.

### Adding a new rule file (new domain)

1. Create `rules/my_domain_rules.yaml` with the required header fields:

```yaml
rule_group: MyDomain
table: MyTable
database: MyDatabase
description: Rules for MyDomain

pk_column: MyPrimaryKey
prosess_id_column: prosess_id   # or null if not applicable

rules:
  - rule_id: MY-001
    ...
```

2. The runner discovers and processes it on the next run — no code changes required.

---

## Adding New Expectation Classes

All expectation logic lives in `engine/expectations.py`.

1. Create a class with a `validate(df, rule, spark) → (result_dict, violations_df)` method.
2. Add it to `CUSTOM_EXPECTATION_REGISTRY` at the bottom of `engine/expectations.py`.
3. Reference it by name in any YAML rule file.

### Contract

```python
# result_dict
{
    "total_rows":  int,    # rows evaluated
    "passed_rows": int,
    "failed_rows": int,
    "success_pct": float,  # 0.0–100.0
    "status":      str,    # "PASSED" | "FAILED" | "ERROR"
    "details":     str,    # human-readable summary
}

# violations_df columns
primary_key_value   STRING
violated_column     STRING
actual_value        STRING
expected_condition  STRING
violation_detail    STRING
```

---

## Available Expectation Types

| Expectation | Type | Description |
|---|---|---|
| `expect_column_values_to_not_be_null` | GX native | Null check on a column |
| `expect_column_values_to_be_in_set` | GX native | Allowed value set |
| `validate_column_comparison` | Custom | Column A `<op>` Column B per row |
| `sql_validation` / `sql` | Custom | SQL query — any returned row = violation |
| `validate_aggregate_rule` | Custom | Aggregate (SUM/COUNT/AVG/MIN/MAX) satisfies `<op>` threshold |
| `expect_column_sum_to_equal` | Custom | SUM(column) == expected ± tolerance |
| `expect_row_count_to_be_between` | Custom | Row count within [min, max] |
| `expect_unique_combination_of_columns` | Custom | Composite uniqueness check |
| `validate_foreign_key` | Custom | Referential integrity across tables |
| `validate_not_null_when` | Custom | check_columns must be NOT NULL when condition_column matches |
| `validate_column_exclusions` | Custom | Forbidden-state check — no row may satisfy the given condition |
| `validate_sequence_order` | Custom | Values appear in the specified chronological order per group |
| `validate_paired_presence` | Custom | Both sides of each required pair must be present per group |
| `validate_no_orphan` | Custom | Stop event without a corresponding start event |
| `validate_conditional_column_value` | Custom | Column value must satisfy a condition when another column matches |
| `validate_group_aggregate_match` | Custom | Aggregate per group matches a reference column within tolerance |

### `validate_column_exclusions` — Negative / Forbidden-State Validation

Use this expectation to assert that a particular combination of column values is **never** allowed.  Any row matching the `condition` is treated as a violation.

```yaml
- rule: "Columns A and B cannot both be NULL"
  expectation: "validate_column_exclusions"
  parameters:
    condition: "ColumnA IS NULL AND ColumnB IS NULL"
    pk_column: "Saksnummer"
    severity:  "Critical"
```

**Parameters**

| Parameter | Required | Description |
|---|---|---|
| `condition` | ✅ | Spark SQL expression that identifies forbidden rows. Any row matching this filter is a violation. |
| `pk_column` | ❌ | Primary key column used to identify violating rows (default: `"id"`). |

---

## Output Tables

### `dq_run_results` — one row per rule per run

| Column | Description |
|---|---|
| `run_id` | UUID for this run |
| `rule_group` | Process / Milestone / Invoice |
| `rule_id` | e.g. PROC-004 |
| `rule_name` | Human-readable rule name |
| `table_name` | Source table validated |
| `expectation` | Expectation type applied |
| `severity` | critical / high / medium / low |
| `status` | PASSED / FAILED / ERROR |
| `success_pct` | % of rows that passed |
| `rule_category` | Completeness / Business Logic / etc. |

### `dq_violations` — current-state violation tracking (one row per unique issue)

Each row tracks the **current state** of a single data quality issue identified by `(rule_id, primary_key_value)`.  On every run the engine automatically updates the status using a Delta MERGE so there is never more than one active record per unique issue.

| Column | Description |
|---|---|
| `run_id` | UUID for the validation run that last touched this violation |
| `rule_id` | Links back to `dq_run_results` |
| `failure_type` | Failure category from the rule definition (e.g. `Completeness`, `Business Logic`, `Referential Integrity`) — use for Power BI slice-and-dice |
| `prosess_id` | Process ID for drill-through |
| `primary_key_value` | PK of the offending row — use this to locate the responsible owner outside the framework |
| `violated_column` | Column that caused the violation |
| `violation_detail` | Human-readable description of the issue |
| `severity` | critical / high / medium / low |
| `issue_status` | `Active` while the violation persists; `Resolved` once the underlying data is fixed |
| `resolution_timestamp` | ISO-8601 timestamp of when the issue was automatically resolved (NULL while still Active) |
| `run_timestamp` | Timestamp of the most recent validation run that confirmed this violation |

---

## Automated Resolution Tracking

Every validation run performs a three-step MERGE against `dq_violations`:

1. **Detect resolved issues** — Any violation that was `Active` in the previous run but whose `(rule_id, primary_key_value)` is **absent** from the current run's violations is automatically marked `Resolved` with a `resolution_timestamp`.
2. **Refresh still-active violations** — Violations that persist have their `run_timestamp` and `violation_detail` updated so the record always reflects the latest run.
3. **Insert new violations** — Brand-new violations are inserted with `issue_status = 'Active'` and `resolution_timestamp = NULL`.

This means Power BI / Excel dashboards can filter on `issue_status = 'Active'` to see **only open issues**, and on `issue_status = 'Resolved'` to track how quickly issues are fixed.

### Backward compatibility

If the MERGE fails (e.g. the table does not yet exist in the metastore), the engine falls back to a plain append and prints a warning.  Re-running `nb_dq_00_setup.py` creates or upgrades the table (using `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE … ADD COLUMNS IF NOT EXISTS`) so the full MERGE-based tracking is enabled without data loss.

---

## Running Tests

```bash
pip install pytest pyspark
pytest tests/ -v
```

Tests in `tests/test_expectations.py` cover the core expectation classes and the resolution-tracking helpers without requiring a real Spark cluster (runs in local mode).

---

## Phase Roadmap

| Phase | Status | Focus |
|---|---|---|
| **Phase 1** | ✅ Complete | Consolidated expectations, modular YAML rules, dynamic engine, FK + aggregate validations |
| **Phase 2** | ✅ Complete | Removed backward-compatible aliases; added `validate_column_exclusions` for negative/forbidden-state validations |
| **Phase 3** | ✅ Complete | Automated resolution tracking (`issue_status`, `resolution_timestamp`) via Delta MERGE |
| **Phase 4** | ✅ Complete | Removed all remaining backward-compat aliases; enriched `dq_violations` with `failure_type` for Power BI categorisation; bug fixes |
