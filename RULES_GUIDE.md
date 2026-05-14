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

## Expectation Parameter Reference

Quick lookup for every expectation: what YAML fields it reads, which are required, and a minimal example for each type not already covered in Business Patterns above.

---

### `not_null`

Every row must have a non-null value in the target column.

| Parameter | Level | Required | Notes |
|---|---|---|---|
| `column` | top-level | yes | Column to check |
| `parameters.pk_column` | parameters | no | Column used to identify violating rows (default: `id`) |

*See Pattern 1.*

---

### `not_null_when`

All `checked_columns` must be non-null whenever `when_column` satisfies the trigger.

| Parameter | Required | Notes |
|---|---|---|
| `when_column` | yes | Column that triggers the check |
| `operator` | yes | `IS NULL`, `IS NOT NULL`, or `==` |
| `value` | if `operator` is `==` | The value `when_column` must equal |
| `checked_columns` | yes | List of columns that must be non-null when triggered |
| `pk_column` | no | Default: `id` |

*See Pattern 2.*

---

### `comparison`

Every row must satisfy `left_column <operator> right_column`. Rows where either column is NULL are skipped.

| Parameter | Required | Notes |
|---|---|---|
| `left_column` | yes | Left-hand column |
| `right_column` | yes | Right-hand column |
| `operator` | yes | `>`, `<`, `>=`, `<=`, `==`, `!=` |
| `pk_column` | no | Default: `id` |

*See Pattern 3.*

---

### `value_in_list`

All non-null values in the column must belong to an approved list.

| Parameter | Level | Required | Notes |
|---|---|---|---|
| `column` | top-level or parameters | yes | Column to check |
| `parameters.allowed_values` | parameters | yes | Non-empty list of permitted values |
| `parameters.pk_column` | parameters | no | Default: same as `column` |

*See Pattern 4.*

---

### `greater_than`

All non-null values in the column must be strictly greater than `threshold`.

| Parameter | Level | Required | Notes |
|---|---|---|---|
| `column` | top-level or parameters | yes | Numeric column to check |
| `parameters.threshold` | parameters | yes | Numeric lower bound (exclusive) |
| `parameters.pk_column` | parameters | no | Default: same as `column` |

```yaml
- rule_id: INV-020
  name: Invoice amount must be positive
  description: Line amounts must be greater than zero.
  expectation: greater_than
  column: linje_belop
  parameters:
    threshold: 0
    pk_column: Fakturanr
  severity: high
  rule_category: Business Logic
  owner: Finansteam
```

---

### `value_when`

When `when_column <operator> value`, `required_column` must equal `required_value`.

| Parameter | Required | Notes |
|---|---|---|
| `when_column` | yes | Column checked for the trigger condition |
| `operator` | yes | `<`, `>`, `<=`, `>=`, `==`, `!=` |
| `value` | yes | Threshold value for the trigger condition |
| `required_column` | yes | Column that must equal `required_value` when triggered |
| `required_value` | yes | The required value |
| `pk_column` | no | Default: `id` |

*See Pattern 5.*

---

### `reference_exists`

Every non-null value in `column` must exist in `reference_table.reference_column`.

| Parameter | Required | Notes |
|---|---|---|
| `column` | yes | Source column whose values are checked |
| `reference_table` | yes | Fully-qualified reference table (e.g. `HR.Employees`) |
| `reference_column` | yes | Column in the reference table holding valid values |
| `pk_column` | no | Default: same as `column` |

```yaml
- rule_id: PROC-020
  name: Case type must exist in reference
  description: CaseType must reference a known type in the classification table.
  expectation: reference_exists
  parameters:
    column: CaseType
    reference_table: Config.CaseTypes
    reference_column: TypeCode
    pk_column: Saksnummer
  severity: high
  rule_category: Referential Integrity
  owner: Saksteam
```

---

### `reference_active`

Every non-null value in `source_column` must exist in the reference table **and** match a row where `reference_active_column` equals `reference_active_value`.

| Parameter | Required | Notes |
|---|---|---|
| `source_column` | yes | Source column whose values are checked |
| `reference_table` | yes | Fully-qualified reference table |
| `reference_column` | yes | Join column in the reference table |
| `reference_active_column` | yes | Column holding the active flag |
| `reference_active_value` | yes | Value that means "active" (string or boolean) |
| `pk_column` | no | Default: same as `source_column` |

*See Pattern 6.*

---

### `row_count_in_range`

The table row count must be between `min_value` and `max_value` (inclusive).

| Parameter | Required | Notes |
|---|---|---|
| `min_value` | yes | Minimum acceptable row count |
| `max_value` | yes | Maximum acceptable row count |

```yaml
- rule_id: PROC-030
  name: Process table must not be empty or oversized
  description: Row count sanity check to catch truncation or runaway loads.
  expectation: row_count_in_range
  parameters:
    min_value: 1000
    max_value: 5000000
  severity: critical
  rule_category: Completeness
  owner: Saksteam
```

---

### `combination_unique`

The combination of `columns` must be unique across all rows.

| Parameter | Required | Notes |
|---|---|---|
| `columns` | yes | Non-empty list of column names that must form a unique key |
| `pk_column` | no | Default: first column in `columns` |

```yaml
- rule_id: INV-030
  name: Invoice line must be unique per invoice
  description: Each Fakturanr + LinjeNr combination must appear at most once.
  expectation: combination_unique
  parameters:
    columns:
      - Fakturanr
      - LinjeNr
    pk_column: Fakturanr
  severity: critical
  rule_category: Uniqueness
  owner: Finansteam
```

---

### `aggregate_threshold`

An aggregate (`sum`, `count`, `avg`, `min`, `max`) of a column must satisfy `<operator> threshold`.

| Parameter | Required | Notes |
|---|---|---|
| `column` | if `aggregate` is not `count` | Numeric column to aggregate |
| `aggregate` | no | `sum`, `count`, `avg`, `min`, `max` (default: `sum`) |
| `operator` | no | `>`, `<`, `>=`, `<=`, `==`, `!=` (default: `>=`) |
| `threshold` | yes | The value the aggregate must satisfy |

```yaml
- rule_id: INV-031
  name: Total invoice amount must be positive
  description: The sum of all line amounts must be above zero.
  expectation: aggregate_threshold
  parameters:
    column: linje_belop
    aggregate: sum
    operator: ">"
    threshold: 0
  severity: high
  rule_category: Aggregate
  owner: Finansteam
```

---

### `state_duration_within_limit`

Rows still in the open state must not have been open longer than `max_days`.

| Parameter | Required | Notes |
|---|---|---|
| `start_column` | yes | Date/timestamp column marking when the state began |
| `open_state_column` | yes | Column checked to decide if the row is still open |
| `open_state_value` | no | Value that means "open". Omit (or set to `null`) to treat IS NULL as open |
| `pk_column` | yes | Primary key column for violation reporting |
| `max_days` | yes | Maximum allowed days in the open state (integer >= 0) |

```yaml
- rule_id: PROC-040
  name: Open case must be resolved within 90 days
  description: Cases without an end date must not remain open longer than 90 days.
  expectation: state_duration_within_limit
  parameters:
    start_column: StartDate
    open_state_column: ActualEndDate   # IS NULL means still open
    pk_column: Saksnummer
    max_days: 90
  severity: high
  rule_category: SLA
  owner: Saksteam
```

---

### `sequence_ordered`

Within each group, values in `event_column` must appear in the order defined by `expected_sequence` (sorted by `order_column`).

| Parameter | Required | Notes |
|---|---|---|
| `event_column` | yes | Column holding the sequence step names |
| `group_column` | yes | Column identifying the group |
| `order_column` | yes | Column used to sort rows within the group |
| `expected_sequence` | yes | Ordered list of step names. Plain strings are strict (no repeats). Use `{value: X, flexible: true}` dict form to allow consecutive repeats of a step |
| `completion_gate` | no | Restrict evaluation to groups that have completed a gate step — sub-keys: `event_column`, `value`, `order_column` (optional) |

```yaml
- rule_id: PROC-050
  name: Case milestones must occur in order
  description: Received must precede Reviewed, which must precede Closed.
  expectation: sequence_ordered
  parameters:
    event_column: MilestoneType
    group_column: Saksnummer
    order_column: MilestoneDate
    expected_sequence:
      - Received
      - Reviewed
      - Closed
  severity: high
  rule_category: Business Logic
  owner: Saksteam
```

---

### `pairs_present`

Within each group, both the start and stop markers of each required pair must exist.

| Parameter | Required | Notes |
|---|---|---|
| `event_column` | yes | Column holding the event/milestone values |
| `group_column` | yes | Column identifying the group |
| `required_pairs` | yes | List of `[start_marker, stop_marker]` pairs. The stop slot can be a list `[stop1, stop2]` — the pair is satisfied when any stop value is present |
| `completion_gate` | no | Same structure as in `sequence_ordered` |

```yaml
- rule_id: PROC-051
  name: Every opened case must be closed
  description: A Received milestone must be paired with a Closed milestone.
  expectation: pairs_present
  parameters:
    event_column: MilestoneType
    group_column: Saksnummer
    required_pairs:
      - [Received, Closed]
  severity: high
  rule_category: Completeness
  owner: Saksteam
```

---

### `stops_paired_with_starts`

A stop-type event must not exist in a group without the corresponding start-type event.

| Parameter | Required | Notes |
|---|---|---|
| `event_column` | yes | Column holding the event/milestone values |
| `group_column` | yes | Column identifying the group |
| `pairs` | yes | List of `[start_marker, stop_marker]` pairs. Stop slot accepts a list for multi-stop form |

```yaml
- rule_id: PROC-052
  name: Closure cannot exist without an opening
  description: A Closed milestone is invalid if Received never occurred.
  expectation: stops_paired_with_starts
  parameters:
    event_column: MilestoneType
    group_column: Saksnummer
    pairs:
      - [Received, Closed]
  severity: critical
  rule_category: Business Logic
  owner: Saksteam
```

---

### `gate_complete`

Every group must contain at least one row where `event_column` equals `value_to_check`.

| Parameter | Required | Notes |
|---|---|---|
| `event_column` | yes | Column holding the event values |
| `group_column` | yes | Column identifying the group |
| `value_to_check` | yes | The required value that must be present in each group |
| `order_column` | no | When provided, the gate row must also have a non-null value in this column |
| `trigger` | no | Human-readable label used in violation details (default: `Approval completed`) |

```yaml
- rule_id: PROC-053
  name: Every case must have an approval event
  description: Each Saksnummer must have at least one Approved milestone.
  expectation: gate_complete
  parameters:
    event_column: MilestoneType
    group_column: Saksnummer
    value_to_check: Approved
    trigger: Case approval
  severity: critical
  rule_category: Business Logic
  owner: Saksteam
```

---

### `columns_excluded`

No row may satisfy the forbidden-state `condition`. Any row matching the expression is a violation.

| Parameter | Required | Notes |
|---|---|---|
| `condition` | yes | Spark SQL expression that identifies forbidden rows |
| `pk_column` | no | Default: `id` |

```yaml
- rule_id: PROC-060
  name: Active case cannot have both start and end null
  description: An active case must have at least one date populated.
  expectation: columns_excluded
  parameters:
    condition: "StartDate IS NULL AND ActualEndDate IS NULL"
    pk_column: Saksnummer
  severity: high
  rule_category: Business Logic
  owner: Saksteam
```

---

### `group_aggregate_matches`

The aggregate of `aggregate_column` within each group must equal the value in `reference_column` within a tolerance.

| Parameter | Required | Notes |
|---|---|---|
| `group_column` | yes | Column identifying each group |
| `aggregate_column` | yes | Numeric column to aggregate within each group |
| `reference_column` | yes | Column holding the expected group total (must be joined in if it lives in another table) |
| `aggregate` | no | `sum`, `count`, `avg`, `min`, `max` (default: `sum`) |
| `tolerance` | no | Maximum allowed absolute difference (default: `0.01`) |

```yaml
- rule_id: INV-040
  name: Invoice line totals must match header total
  description: SUM(linje_belop) per invoice must equal Faktura_totalbelop.
  expectation: group_aggregate_matches
  parameters:
    group_column: Fakturanr
    aggregate_column: linje_belop
    reference_column: Faktura_totalbelop
    aggregate: sum
    tolerance: 0.01
  severity: critical
  rule_category: Aggregate
  owner: Finansteam
```

---

### `sql_violations`

Runs a custom SQL query. Every row returned is treated as a violation — write the query to return only offending rows.

| Parameter | Required | Notes |
|---|---|---|
| `sql` | yes | SQL query; returned rows = violations |
| `pk_column` | no | Column in the SQL result to use as the primary key value in violations |

*See Pattern 7.*

---

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
