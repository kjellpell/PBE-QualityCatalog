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

### `dq_violations` — one row per offending record per rule per run

| Column | Description |
|---|---|
| `rule_id` | Links back to `dq_run_results` |
| `prosess_id` | Process ID for drill-through |
| `primary_key_value` | PK of the offending row |
| `violated_column` | Column that caused the violation |
| `violation_detail` | Human-readable description of the issue |

---

## Running Tests

```bash
pip install pytest pyspark
pytest tests/ -v
```

Tests in `tests/test_expectations.py` cover the core expectation classes
without requiring a real Spark cluster (runs in local mode).

---

## Phase Roadmap

| Phase | Status | Focus |
|---|---|---|
| **Phase 1** | ✅ Complete | Consolidated expectations, modular YAML rules, dynamic engine, FK + aggregate validations |
| **Phase 2** | ✅ Complete | Removed backward-compatible aliases; added `validate_column_exclusions` for negative/forbidden-state validations |
| **Phase 3** | Planned | Rule dependencies, enhanced Delta logging for Power BI |
