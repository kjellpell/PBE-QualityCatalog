# PBE Quality Catalog — Maintenance Guide

## System Overview

The PBE Quality Catalog is a data quality validation engine for Microsoft Fabric that runs rule-based tests against data tables in a Lakehouse. Rules are defined in YAML files and reference expectation types (validators) that check conditions such as null counts, row counts, column uniqueness, foreign key references, and SQL-based assertions. The engine executes these rules, records validation results in Delta tables (`dq_run_results` and `dq_violations`), and calculates data quality metrics. When a validation fails, the violation is persisted with full context (table name, column, actual value, expected condition) so that root-cause analysis and remediation can be tracked over time.

---

## Notebook Reference

| Notebook | Purpose | When to Run | Key Outputs |
|----------|---------|------------|-------------|
| `nb_dq_00_setup.py` | Creates Delta tables and schemas | Once at deployment, or to reset/repair tables | Delta tables: `dq_run_results`, `dq_violations`, `dq_execution_metrics` |
| `nb_dq_01_preflight.py` | Validates YAML rule syntax and checks for deployment issues | Before `nb_dq_03_run_validation.py` (optional; can skip in CI) | Prints warnings/errors if rules are malformed or tables are missing |
| `nb_dq_03_run_validation.py` | Executes all rules and persists violations | On-demand or on a schedule (e.g., nightly) | Populates `dq_run_results`, `dq_violations`, `dq_execution_metrics` with results from all rules |

---

## Expectation Types Reference

| Expectation Name | What It Checks | Example Use Case |
|------------------|----------------|------------------|
| `not_null` | Column has no NULL values | Invoice line items must have a po_number |
| `comparison` | Two columns satisfy an operator (=, <>, <, >, <=, >=) | revenue >= cost |
| `sql_violations` | Custom SQL query returns a count that must be <= threshold (e.g., 0) | `SELECT COUNT(*) FROM orders WHERE status IS NULL` should return 0 |
| `row_count_in_range` | Row count falls within `min_value` to `max_value` | Daily transactions should be 1000–10000 rows |
| `combination_unique` | Combination of columns is unique (no duplicates) | (customer_id, invoice_date, line_item) must be unique |
| `reference_exists` | Column values exist in a reference table column | All product_ids must exist in product_master.product_id |
| `not_null_when` | Column is NOT NULL if a condition is true | If status='invoiced', amount must NOT be NULL |
| `columns_excluded` | Column values must NOT be in a list (exclusion list) | status must NOT be in ('Cancelled', 'Deleted') |
| `sequence_ordered` | Values in a column follow a defined sequence within groups | In a sequence [Sent, Received, Verified, Closed], no out-of-order transitions |
| `gate_complete` | At least one non-NULL value in a column per group | Each customer_id group must have at least one payment_date |
| `pairs_present` | If one column has a value, another must also | If start_date is NOT NULL, end_date must be NOT NULL |
| `stops_paired_with_starts` | Stop events must be paired with preceding start events | Every 'order_stopped' row must have a matching earlier 'order_started' |
| `value_when` | Column value matches a condition when another column has a value | When order_type='return', status must='processing' |
| `group_aggregate_matches` | Aggregate of a column matches expected value per group | SUM(line_amount) per order_id must equal order_total |
| `reference_active` | Column values exist in reference table AND ref row is marked active | All supplier_ids must exist in supplier_master AND status='active' |
| `aggregate_threshold` | Aggregate of a column is within a threshold | AVG(process_time_sec) per day must be <300 seconds |
| `state_duration_within_limit` | Time spent in a state (before transition) is within limits | An order can be in 'pending' state for max 7 days before moving to 'processing' |

---

## When Something Breaks

### Most Likely Failure Points & Error Messages

#### 1. **Missing or Corrupted Delta Table** 
- **Error:** `RuntimeError: MERGE failed — violations not written. Check Delta table locks and schema drift.`  
- **Causes:** Table was deleted, schema changed, or Delta transaction log is corrupted  
- **Fix:** Run `nb_dq_00_setup.py` to recreate the table; if in CI, add `--force` flag to drop and recreate  

#### 2. **Join Key Mismatch or Missing Data**
- **Error:** `Rule {rule_id}: empty after join — check join keys and source data`  
- **Causes:** Reference table does not contain expected rows; join columns have no matching values  
- **Fix:** Verify reference table is populated; check join key column names and data types match between source and reference  

#### 3. **Invalid YAML Rule Definition**
- **Error:** `Unknown expectation: '{expectation_name}'` (e.g., `"validate_column_comparison"` is now `"comparison"`)  
- **Causes:** Old rule definition uses deprecated expectation name or typo in expectation field  
- **Fix:** Check `MAINTENANCE_GUIDE.md` "Expectation Types Reference" table; update YAML to use canonical name  

#### 4. **SQL Validation Query Fails**
- **Error:** `Exception in rule {rule_id}: SQL error: {details}`  
- **Causes:** Column name does not exist; SQL syntax error; type mismatch  
- **Fix:** Test the SQL query directly in Spark SQL; verify column names and quoting  

#### 5. **File Not Found at Startup**
- **Error:** `FileNotFoundError: Missing required deployment files: {list}`  
- **Causes:** Engine file not deployed to Lakehouse; incorrect Lakehouse path  
- **Fix:** Verify all engine files are in `/lakehouse/default/Files/engine/`; re-deploy if necessary  

#### 6. **Preflight Validation Fails**
- **Error:** `Warning: {issue}` from `nb_dq_01_preflight.py`  
- **Causes:** YAML syntax error; duplicate rule IDs; missing expectation type  
- **Fix:** Run `nb_dq_01_preflight.py` to identify and fix issues before running validation  

#### 7. **Low Data Quality Score (Unexpected Failures)**
- **Error:** Violations reported where none were expected  
- **Causes:** Data quality actually degraded; rule threshold is too strict; reference table out-of-date  
- **Fix:** Investigate a sample violation in Power BI; triage root cause (data pipeline issue or rule needs adjustment)  

#### 8. **Performance Degradation (Slow Validation Run)**
- **Error:** `nb_dq_03_run_validation.py` takes much longer than usual  
- **Causes:** New large join; Lakehouse table not optimized; too many rules  
- **Fix:** Add `OPTIMIZE table_name` on large tables; split rules into smaller batches if needed  

#### 9. **Duplicate Rows in Violations**
- **Error:** Same violation appears multiple times in `dq_violations`  
- **Causes:** Rule's primary key definition is incomplete; multiple rows match the same violation  
- **Fix:** Ensure `primary_key_value` includes all columns needed to uniquely identify a row  

#### 10. **mssparkutils Unavailable (Fabric Session Issue)**
- **Error:** `RuntimeError: mssparkutils unavailable — this notebook must run inside a Fabric session`  
- **Causes:** Notebook is running outside Fabric; Fabric session has terminated  
- **Fix:** Ensure notebooks run in a Fabric Synapse job or interactive session; check capacity is active  

---

## Common Actions

### Re-run a Single Rule
```python
# Filter nb_dq_03_run_validation to run only one rule catalog:
# Edit RUNTIME_OVERRIDE["rule_catalogs"] to include only the catalog containing the rule
RUNTIME_OVERRIDE = {"rule_catalogs": ["process_rules.yaml"]}
# Re-run nb_dq_03_run_validation.py
```

### Clear All Violations and Start Fresh
```sql
TRUNCATE TABLE dbo.dq_violations;
TRUNCATE TABLE dbo.dq_run_results;
TRUNCATE TABLE dbo.dq_execution_metrics;
```
Then run `nb_dq_03_run_validation.py` again.

### Add a New Expectation Type
1. Create a class in `engine/expectations.py` that implements `validate(df, rule, spark) -> tuple`  
2. Register it in `CUSTOM_EXPECTATION_REGISTRY` at the bottom of the file  
3. Add an entry to the "Expectation Types Reference" table in this guide  
4. Use the new expectation in YAML rules  

### Debug a Specific Rule
1. Find the rule ID in the YAML file  
2. Query `dbo.dq_violations` for that rule_id  
3. If violations are there, inspect the `actual_value`, `expected_condition`, and `violation_detail` columns  
4. If no violations but rule failed with ERROR status, check `dbo.dq_run_results` for the error message in the `details` column  

---

## Future Considerations

### Migrating from `mssparkutils` to `notebookutils`

Microsoft Fabric is gradually moving from `mssparkutils` to `notebookutils`. This engine currently uses Spark APIs only and does not depend on `mssparkutils`. However, if runtime parameters or Power Automate integrations are added in the future, be aware that `mssparkutils` may be deprecated. When that happens:

- Replace `mssparkutils.notebook.getParam()` → `notebookutils.notebook.getParam()`  
- Replace `mssparkutils.runtime.context()` → `notebookutils.runtime.context()`  
- Update error messages and retry logic accordingly  

For now, this is not urgent; this guide will be updated when the transition is required.

---

## Rollback & Recovery

### If Validations Fail Catastrophically
1. Stop the validation job  
2. Run `nb_dq_00_setup.py` to rebuild tables (this will drop and recreate them)  
3. Re-run `nb_dq_01_preflight.py` to verify rules are valid  
4. Re-run `nb_dq_03_run_validation.py` with one rule catalog at a time to isolate the issue  

### If a YAML Rule is Malformed
1. Fix the YAML (check for typos, missing fields, invalid YAML syntax)  
2. Run `nb_dq_01_preflight.py` to validate  
3. If it passes, re-run `nb_dq_03_run_validation.py`  

### If the Engine Code is Broken
1. Roll back `engine/` directory to the last working commit  
2. Re-run `nb_dq_01_preflight.py` and `nb_dq_03_run_validation.py`  
3. If that succeeds, investigate the recent changes in Git  

---

## Support & Escalation

- **Rule Syntax Questions:** See RULES_GUIDE.md in the repo  
- **Expectation Type Customization:** See ARCHITECTURE.md for API details  
- **Data Quality Investigation:** Use Power BI dashboards to drill into violations; correlate with upstream data pipeline logs  
- **Performance Issues:** Profile the slow rule in Spark SQL; consider query optimization or data partitioning  

---

**Last Updated:** 2026-05-06  
**Version:** 1.0 (No Maintenance Mode)
