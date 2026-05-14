# Rules Guide For Business Teams

This guide is the business-first reference for defining data quality rules with the canonical naming model.

## Start Here

Use this when you want to express a rule in plain business language and map it to a valid rule contract.

Use [README.md](README.md) for runtime and deployment details.
Use [ARCHITECTURE.md](ARCHITECTURE.md) for engine internals.

## Fabric Notebook Workflow

All operations are expected to run in Microsoft Fabric notebooks.

1. Update rule definitions in your managed source (typically `rule_catalog`; YAML may be used only as migration seed where applicable).
2. Run [nb_dq_01_preflight.py](nb_dq_01_preflight.py).
3. Run one validation cycle with `DRY_RUN = True`.
4. Review outputs in temporary tables.
5. Switch to `DRY_RUN = False` after validation is confirmed.

Dry-run outputs:
- `dq_run_results_tmp`
- `dq_violations_tmp`
- `default.dq_execution_metrics_tmp`

## Rule Authoring Flow

Write the rule as an intent sentence first.

Examples:
- "If the case is open, assigned handler must be present."
- "End date must be on or after start date."
- "Invoice type must be one of approved values."

Then choose rule type:

| Intent pattern | Canonical expectation |
|---|---|
| Field is always required | `not_null` |
| If condition is true, field(s) required | `not_null_when` |
| Compare two fields in the same row | `comparison` |
| Field must be in allowed list | `value_in_list` |
| Threshold on one field | `greater_than` |
| If condition is true, another field must equal a value | `value_when` |
| Parent/reference must exist | `reference_exists` |
| Reference must exist and be active | `reference_active` |
| Row count must be within range | `row_count_in_range` |
| Column combination must be unique | `combination_unique` |
| Aggregate must meet threshold | `aggregate_threshold` |
| Time open/state duration bounded | `state_duration_within_limit` |
| Group events must be ordered | `sequence_ordered` |
| Group event pairs must be present | `pairs_present` |
| Stop event cannot exist without start event | `stops_paired_with_starts` |
| Advanced custom logic | `sql_violations` |

## Operator Taxonomy

Use the correct operator family for the rule type.

| Operator family | Allowed values | Typical expectations |
|---|---|---|
| Trigger operators | `IS NULL`, `IS NOT NULL`, `==` | `not_null_when` |
| Comparison operators | `>`, `<`, `>=`, `<=`, `==`, `!=` | `comparison`, `value_when`, `aggregate_threshold` |

`value` is required only when the selected operator needs a value.

## Canonical Parameter Contract

### Common keys

- `column` for single-column checks
- `pk_column` for violation identity
- `parameters` for multi-column or advanced rules

### Canonical parameter keys

| Parameter key | Meaning |
|---|---|
| `left_column` | Left side for field comparison |
| `right_column` | Right side for field comparison |
| `when_column` | Column evaluated by trigger condition |
| `checked_columns` | List of columns required under trigger |
| `allowed_values` | Approved value list |
| `event_column` | Event or milestone name column |
| `order_column` | Sequencing or ordering column |
| `open_state_column` | Column used to identify open state |
| `open_state_value` | Value that means open |
| `reference_table` | Reference table name |
| `reference_column` | Join/value column in reference table |
| `reference_active_column` | Active flag column in reference table |
| `reference_active_value` | Active flag value |
| `group_column` | Group identity for grouped checks |
| `source_column` | Source-side reference field |
| `max_days` | Max allowed days in state |

## Business Patterns With Canonical Examples

### Pattern 1: Required field

```yaml
- rule_id: PROC-011
  name: StartDate must be present
  description: Every process requires a start date for reporting and SLA tracking.
  expectation: not_null
  column: StartDate
  severity: high
  rule_category: Completeness
  owner: Saksteam
```

### Pattern 2: Conditional required fields

```yaml
- rule_id: PROC-012
  name: Open cases must have a handler
  description: Cases with no end date must be assigned.
  expectation: not_null_when
  parameters:
    when_column: ActualEndDate
    operator: IS NULL
    checked_columns:
      - Saksbehandler_kode
    pk_column: Saksnummer
  severity: high
  rule_category: Completeness
  owner: Saksteam
```

### Pattern 3: Compare two fields

```yaml
- rule_id: PROC-013
  name: End date cannot be before start date
  description: Timeline order must be valid.
  expectation: comparison
  parameters:
    left_column: ActualEndDate
    right_column: StartDate
    operator: ">="
    pk_column: Saksnummer
  severity: high
  rule_category: Business Logic
  owner: Saksteam
```

### Pattern 4: Allowed values

```yaml
- rule_id: INV-010
  name: Invoice type must be approved
  description: Only approved business values are allowed.
  expectation: value_in_list
  column: Faktura_type
  parameters:
    allowed_values:
      - Standard
      - Kreditnota
  severity: medium
  rule_category: Business Logic
  owner: Finansteam
```

### Pattern 5: Conditional value rule

```yaml
- rule_id: INV-011
  name: Negative amount requires credit note type
  description: Negative lines are only valid for credit notes.
  expectation: value_when
  parameters:
    when_column: linje_belop
    operator: "<"
    value: 0
    required_column: Faktura_type
    required_value: Kreditnota
    pk_column: Fakturanr
  severity: high
  rule_category: Business Logic
  owner: Finansteam
```

### Pattern 6: Active reference

```yaml
- rule_id: PROC-014
  name: Assigned handler must be active
  description: Open assignments must reference active personnel.
  expectation: reference_active
  parameters:
    source_column: Saksbehandler_kode
    reference_table: HR.Employees
    reference_column: EmployeeCode
    reference_active_column: IsActive
    reference_active_value: true
    pk_column: Saksnummer
  severity: critical
  rule_category: Referential Integrity
  owner: Saksteam
```

### Pattern 7: SQL violations

```yaml
- rule_id: INV-012
  name: Invoice totals cannot be zero
  description: Sum of line amounts per invoice cannot be zero.
  expectation: sql_violations
  parameters:
    sql: |
      SELECT Fakturanr
      FROM Saksbehandling.Fakturalinjer
      GROUP BY Fakturanr
      HAVING SUM(linje_belop) = 0
  severity: medium
  rule_category: Aggregate
  owner: Finansteam
```

## Header Contract

Recommended catalog-level header:

```yaml
rule_group: Process
database: Saksbehandling
table: Prosesser
description: Data quality rules for process records
pk_column: Saksnummer
rules:
  - rule_id: PROC-001
    name: Example
    expectation: not_null
    column: Saksnummer
    severity: critical
    category: Completeness
    owner: Saksteam
```

### `catalog_filter` — scope the source rows before rules run

Applies a filter to the source table once, before any rules execute. All rules in the catalog see only the filtered rows. Two types are supported.

**`date_range`** — keep rows where a date column falls within a rolling window:

```yaml
catalog_filter:
  type: date_range
  date_column: CreatedDate    # must exist in the source table
  lookback_days: 90           # integer >= 0; rows older than this are excluded
  include_nulls: true         # optional; also keeps rows where date_column IS NULL
```

**`custom`** — any valid Spark SQL predicate:

```yaml
catalog_filter:
  type: custom
  where_clause: "Status IN ('Open', 'Pending')"
```

Preflight checks that the type is one of the two allowed values, that required sub-keys are present, and (for `date_range`) that `date_column` exists in the source table.

You can override or disable a YAML `catalog_filter` without touching the YAML file by setting `CATALOG_FILTER_OVERRIDES` in `QualityCatalogConfig.py`:

```python
CATALOG_FILTER_OVERRIDES = {
    "Process": {                        # rule_group name
        "type": "date_range",
        "date_column": "ActualEndDate",
        "lookback_days": 90,
        "include_nulls": True,
    },
    "Invoice": None,                    # None disables the YAML filter entirely
}
```

### `joins` — enrich the source table before rules run

Joins one or more tables onto the source table before rules execute. Each entry in the list is applied in order. Rules then see the joined columns as if they were native to the source.

Use this when a rule needs a column that lives in a related table rather than the source table itself.

**Simple join on a shared column name:**

```yaml
joins:
  - table: Saksbehandling.saker   # fully-qualified table name
    on: Saksnummer                # column name shared by both tables
    how: left                     # join type: left (default), inner, right, full
    select:                       # optional — which columns to keep from the joined table
      - Saksnummer
      - Status
      - Saksbehandler_kode
```

**Join on columns with different names in each table:**

```yaml
joins:
  - table: HR.Employees
    left_on: Saksbehandler_kode   # column in the source table
    right_on: EmployeeCode        # column in the joined table
    how: left
    select:
      - EmployeeCode
      - IsActive
      - Department
```

Key behaviours:
- `how` defaults to `left` if omitted.
- `select` is optional. If provided, only those columns are brought in from the joined table (the join key is automatically included if missing from the list).
- Use `on` when both tables share the same column name. Use `left_on` + `right_on` when they differ.
- A join config with no key (`on`, `left_on`, or `right_on`) is skipped with a warning during the run.

## Quality Checklist Before Save

- Rule ID is unique in rule group
- Canonical expectation name is used
- Canonical parameter names are used
- Operator matches operator family
- `pk_column` is correct
- Description explains business risk, not just technical check
- Rule passes preflight and dry-run

## Common Mistakes

| Mistake | Impact | Fix |
|---|---|---|
| Unknown expectation/parameter names | Preflight failure | Check ARCHITECTURE.md for canonical names |
| Wrong operator family | False positives or runtime errors | Use trigger operators only for conditional-required patterns |
| Missing `pk_column` where needed | Hard to trace violations | Set at catalog level or per rule |
| SQL returns non-violating rows | False failures | Ensure SQL returns violations only |
| Running production mode first | Polluted production outputs | Run preflight and dry-run first |
