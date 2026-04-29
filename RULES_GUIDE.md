# Rules Guide For Business Teams

This guide shows how to create and update data quality rules in plain language.

Rules are authored in YAML files in the `rules/` folder. The validation runner loads
those YAML files at runtime.

---

## Who This Guide Is For

This guide is for business users who own rule content and quality policy.

If you need engine, runtime, or deployment details, use README.md (IT guide).

---

## What A Rule Is

A rule is a check that confirms expected data quality.

Examples:

- A required field is filled in
- A date order makes sense
- A value is in an approved list
- A milestone sequence is complete
- A child row points to a valid parent row

If data breaks a rule, it appears in violation output for follow-up.

---

## Where Rules Are Managed

Rules are managed in YAML catalogs under `rules/`.

Each YAML file represents one rule group (for example Process, Invoice, Milestone).
The validation engine reads those YAML files directly during execution.

To add or update rules, edit the relevant YAML file and run validation.
The runtime source of truth for rules is the YAML files.

---

## How To Add A Rule

1. Pick the next rule_id in your rule group (for example PROC-011).
2. Choose an expectation.
3. Use `column` for single-column checks, or `parameters` for multi-column/advanced checks.
4. Set severity, category, owner, and a clear business description.
5. Ensure `pk_column` is correct for the rule group.
6. Save the YAML and run validation.

Canonical conditional parameter design:

- Use `operator` for condition/comparison operators.
- Use `value` only when the selected operator needs a value.
- Do not use legacy keys such as `condition_operator` or `condition_value`.

PK guidance:

- Prefer setting `pk_column` once at catalog level (header).
- Rules inherit that PK automatically when `parameters.pk_column` is omitted.
- Set `parameters.pk_column` on a rule only when that specific rule needs a different identifier.

---

## Catalog-Level Settings (Header)

Each YAML catalog starts with a header block before `rules:`.
These settings apply to all rules in that catalog.

### Required header settings

- `rule_group`
  - Logical name shown in outputs (for example `Process`, `Invoice`, `Milestone`).
- `table`
  - Source table name (for example `Prosesser`).
- `rules`
  - List of rule entries.

### Strongly recommended header settings

- `database`
  - Schema/database name used with `table` (for example `Saksbehandling`).
  - Engine resolves source as `database.table`.
- `pk_column`
  - Main identifier for violations in this catalog.
  - If omitted, runtime defaults to `id`.
- `description`
  - Human-readable catalog summary.

### Optional header settings

- `catalog_filter`
  - Limits source rows before rules run.
  - Supported forms:
    - `type: date_range`
      - `date_column` (required)
      - `lookback_days` (required, integer >= 0)
      - `include_nulls` (optional, default `false`)
    - `type: custom`
      - `where_clause` (required SQL predicate)

- `joins`
  - Pre-joins additional tables before validation.
  - Each join entry supports:
    - `table` (required)
    - `how` (optional, default `left`)
    - Either `on` OR both `left_on` + `right_on`
    - `select` (optional list of columns from join table)

Notes:

- Catalog filters can be overridden at runtime via `CATALOG_FILTER_OVERRIDES` in runtime config.
- Column names used in `catalog_filter` and rule parameters are contract-checked in preflight.

### Complete header example

```yaml
rule_group: Process
table: Prosesser
database: Saksbehandling
description: Data quality rules for process records
pk_column: Saksnummer

catalog_filter:
  type: custom
  where_clause: "Status IN ('Open', 'Pending')"

joins:
  - table: Saksbehandling.saker
    on: Saksnummer
    how: left
    select:
      - Saksnummer
      - Status
      - Saksbehandler_kode

rules:
  - rule_id: PROC-001
    name: Saksnummer cannot be null
    expectation: expect_column_values_to_not_be_null
    column: Saksnummer
    severity: critical
    category: Completeness
    owner: Saksteam
```

---

## Rule Template

Use this as a starting point:

```yaml
- rule_id: PROC-010
  name: Short business-friendly title
  description: Why this rule matters for data quality and decisions.
  expectation: expect_column_values_to_not_be_null
  column: ExampleColumn
  severity: high
  category: Completeness
  owner: Saksteam
```

Some rule types use parameters instead of a single column field.

---

## Expectation Reference

This section lists the core expectations, their required settings, and practical examples.

Quick overview (one sentence each):

- `expect_column_values_to_not_be_null`: Fails rows where a required field is null.
- `expect_column_values_to_be_greater_than`: Fails rows where a numeric/date value is not greater than a threshold.
- `expect_column_values_to_be_in_set`: Fails rows where a value is outside an approved value list.
- `validate_column_comparison`: Compares two columns in the same row and fails rows where the operator condition is not met.
- `validate_not_null_when`: Requires one or more columns to be non-null when a condition on another column is true.
- `validate_aggregate_rule`: Evaluates an aggregate (such as sum or count) and fails when it violates the threshold condition.
- `validate_conditional_column_value`: Enforces that when one column meets a condition, another column must have a specific value.
- `expect_row_count_to_be_between`: Fails when the total row count is outside a configured min/max interval.
- `expect_unique_combination_of_columns`: Fails duplicate rows for a configured composite key.
- `validate_foreign_key`: Fails rows whose reference value does not exist in the target table.
- `validate_active_reference`: Fails rows whose reference exists but is not active (or does not exist).
- `validate_time_in_state`: Fails rows that stay open longer than allowed based on start/open conditions.
- `validate_sequence_order`: Fails groups where milestone/event order breaks the expected sequence.
- `validate_paired_presence`: Fails groups where required start/stop milestone pairs are incomplete.
- `validate_no_orphan`: Fails groups where a stop-type milestone exists without its required start-type milestone.
- `sql_validation` / `sql`: Runs custom SQL that returns only violating rows.

### Core Built-In Expectations

1. `expect_column_values_to_not_be_null`

Required:
- `column`

Example:

```yaml
- rule_id: PROC-011
  name: StartDate must be present
  expectation: expect_column_values_to_not_be_null
  column: StartDate
  severity: high
  category: Completeness
  owner: Saksteam
```

2. `expect_column_values_to_be_greater_than`

Required:
- `column`
- `parameters.value`

Example:

```yaml
- rule_id: INV-013
  name: linje_belop must be greater than zero
  expectation: expect_column_values_to_be_greater_than
  column: linje_belop
  parameters:
    value: 0
  severity: medium
  category: Business Logic
  owner: Finansteam
```

3. `expect_column_values_to_be_in_set`

Required:
- `column`
- `parameters.value_set` (list)

Example:

```yaml
- rule_id: INV-010
  name: Faktura_type must be approved
  expectation: expect_column_values_to_be_in_set
  column: Faktura_type
  parameters:
    value_set:
      - Standard
      - Kreditnota
  severity: medium
  category: Business Logic
  owner: Finansteam
```

### Generic Expectations

1. `validate_column_comparison`

Required:
- `parameters.column_A`
- `parameters.column_B`
- `parameters.operator` (`>`, `<`, `>=`, `<=`, `==`, `!=`)

Optional:
- `parameters.pk_column` (falls back to catalog-level `pk_column`)

Example:

```yaml
- rule_id: PROC-012
  name: ActualEndDate must be on or after StartDate
  expectation: validate_column_comparison
  parameters:
    column_A: ActualEndDate
    column_B: StartDate
    operator: ">="
    pk_column: Saksnummer
  severity: high
  category: Business Logic
  owner: Saksteam
```

2. `validate_not_null_when`

Required:
- `parameters.condition_column`
- `parameters.operator` (`==`, `IS NOT NULL`, `IS NULL`)
- `parameters.check_columns` (list)

Optional:
- `parameters.pk_column` (falls back to catalog-level `pk_column`)

Conditional:
- `parameters.value` is required when `operator == "=="`.

Example:

```yaml
- rule_id: PROC-003
  name: Saksbehandler_kode cannot be null for open cases
  expectation: validate_not_null_when
  parameters:
    condition_column: ActualEndDate
    operator: IS NULL
    check_columns:
      - Saksbehandler_kode
    pk_column: Saksnummer
  severity: high
  category: Completeness
  owner: Saksteam
```

3. `validate_aggregate_rule`

Required:
- `parameters.aggregate` (`sum`, `count`, `avg`, `min`, `max`)
- `parameters.operator` (`>`, `<`, `>=`, `<=`, `==`, `!=`)
- `parameters.threshold`

Conditional:
- `parameters.column` is required for all aggregates except `count`.

Example:

```yaml
- rule_id: INV-014
  name: Total invoice amount must be positive
  expectation: validate_aggregate_rule
  parameters:
    column: linje_belop
    aggregate: sum
    operator: ">"
    threshold: 0
  severity: medium
  category: Aggregate
  owner: Finansteam
```

4. `validate_conditional_column_value`

Required:
- `parameters.condition_column`
- `parameters.operator` (`>`, `<`, `>=`, `<=`, `==`, `!=`)
- `parameters.value`
- `parameters.required_column`
- `parameters.required_value`

Optional:
- `parameters.pk_column` (falls back to catalog-level `pk_column`)

Example:

```yaml
- rule_id: INV-004
  name: Negative amounts only allowed on credit notes
  expectation: validate_conditional_column_value
  parameters:
    condition_column: linje_belop
    operator: "<"
    value: 0
    required_column: Faktura_type
    required_value: Kreditnota
    pk_column: Fakturanr
  severity: high
  category: Business Logic
  owner: Finansteam
```

### SQL Fallback

1. `sql_validation`
2. `sql` (shorthand alias)

Required:
- `parameters.sql` for `sql_validation`
- top-level `sql` for shorthand form

Optional:
- `pk_column`

Example:

```yaml
- rule_id: INV-012
  name: Invoice totals cannot be zero
  expectation: sql
  sql: |
    SELECT Fakturanr, SUM(linje_belop) AS total_belop
    FROM Saksbehandling.Fakturalinjer
    GROUP BY Fakturanr
    HAVING SUM(linje_belop) = 0
  severity: medium
  category: Aggregate
  owner: Finansteam
```

SQL rule guidance:
- SQL must return only violating rows.
- If SQL returns 0 rows, the rule passes.

### Aggregate / Uniqueness Expectations

1. `expect_row_count_to_be_between`

Required:
- `parameters.min_value`
- `parameters.max_value`

2. `expect_unique_combination_of_columns`

Required:
- `parameters.columns` (list)

Recommended:
- `parameters.pk_column`

### Cross-Table Expectations

1. `validate_foreign_key`

Required:
- `parameters.column`
- `parameters.reference.table`
- `parameters.reference.column`

Optional:
- `parameters.pk_column`

Recommended:
- Set `parameters.pk_column` explicitly for FK rules when the FK column is not the best violation identifier.

2. `validate_active_reference`

Required:
- `parameters.source_column`
- `parameters.reference.table`
- `parameters.reference.column`
- `parameters.reference.active_column`
- `parameters.reference.active_value`

Optional:
- `parameters.pk_column` (falls back to `source_column` if omitted)

3. `validate_time_in_state`

Required:
- `parameters.start_column`
- `parameters.open_when_column`
- `parameters.open_when_value`
- `parameters.pk_column`
- `parameters.max_days`

### Sequence / Pair Expectations

1. `validate_sequence_order`
2. `validate_paired_presence`
3. `validate_no_orphan`

These are used mainly for milestone-process integrity patterns and support
group-based sequence/pair validation.

---

## Common Rule Types With Examples

### 1. Required value (field cannot be empty)

```yaml
- rule_id: PROC-011
  name: StartDate must be present
  description: Every process needs a start date for timeline reporting.
  expectation: expect_column_values_to_not_be_null
  column: StartDate
  severity: high
  category: Completeness
  owner: Saksteam
```

Use this when missing values create reporting or operational risk.

### 2. Allowed value list

```yaml
- rule_id: INV-010
  name: Faktura_type must be approved
  description: Invoice type must be one of the approved business values.
  expectation: expect_column_values_to_be_in_set
  column: Faktura_type
  parameters:
    value_set:
      - Standard
      - Kreditnota
  severity: medium
  category: Business Logic
  owner: Finansteam
```

Use this when a field should only contain known values.

### 3. Compare two fields (date or number logic)

```yaml
- rule_id: PROC-012
  name: ActualEndDate must be on or after StartDate
  description: A process cannot end before it starts.
  expectation: validate_column_comparison
  parameters:
    column_A: ActualEndDate
    column_B: StartDate
    operator: ">="
    pk_column: Saksnummer
  severity: high
  category: Business Logic
  owner: Saksteam
```

Use this when one field must be greater than, less than, or equal to another.

### 4. Foreign key check (reference must exist)

```yaml
- rule_id: INV-011
  name: prosess_id must exist in Prosesser
  description: Every invoice line must connect to a valid process.
  expectation: validate_foreign_key
  parameters:
    column: prosess_id
    pk_column: Fakturanr
    reference:
      table: Saksbehandling.Prosesser
      column: Saksnummer
  severity: high
  category: Referential Integrity
  owner: Finansteam
```

Use this when a child row must point to an existing parent row.

### 5. SQL rule (advanced checks)

```yaml
- rule_id: INV-012
  name: Invoice totals cannot be zero
  description: Zero-total invoices should be reviewed.
  expectation: sql
  sql: |
    SELECT Fakturanr, SUM(linje_belop) AS total_belop
    FROM Saksbehandling.Fakturalinjer
    GROUP BY Fakturanr
    HAVING SUM(linje_belop) = 0
  severity: medium
  category: Aggregate
  owner: Finansteam
```

Important: SQL must return only violating rows. If SQL returns rows, the rule fails.

---

### 6. Paired milestone checks (start and stop must both exist)

Use `validate_paired_presence` when two milestone values must always appear together in the same group. If one exists without the other, the rule fails.

**Single stop (strict pair):**

```yaml
- rule_id: MIL-004
  name: Start and Stop milestones must both be present
  expectation: validate_paired_presence
  parameters:
    value_column: Milepel
    group_column: prosess_id
    required_pairs:
      - [Startbehandling, Stoppbehandling]
  severity: critical
  category: Business Logic
  owner: IT
```

**Multiple acceptable stops (one-to-many):**

Sometimes a process can end in more than one way — for example, it can be either stopped normally or cancelled. Use a list in the stop position to allow any of those values:

```yaml
- rule_id: MIL-004
  name: Process must be closed with a stop or cancellation milestone
  expectation: validate_paired_presence
  parameters:
    value_column: Milepel
    group_column: prosess_id
    required_pairs:
      - [Startbehandling, [Stoppbehandling, Kansellert]]
  severity: critical
  category: Business Logic
  owner: IT
```

This passes when a group has `Startbehandling` + at least one of `Stoppbehandling` or `Kansellert`. It fails when `Startbehandling` exists with neither, or when `Stoppbehandling`/`Kansellert` exists without `Startbehandling`.

**Orphan check (stop without start is invalid):**

Use `validate_no_orphan` when a stop milestone must not exist on its own — the corresponding start must also be present. A start without a stop is allowed (the process may still be running).

```yaml
- rule_id: MIL-005
  name: Stop milestone requires a matching start milestone
  expectation: validate_no_orphan
  parameters:
    value_column: Milepel
    group_column: prosess_id
    pairs:
      - [Startbehandling, [Stoppbehandling, Kansellert]]
  severity: high
  category: Business Logic
  owner: IT
```

This fails when `Stoppbehandling` OR `Kansellert` exists for a group that has no `Startbehandling`.

---

### 7. Active reference check (referenced record must be active)

Use `validate_active_reference` when a column references another table and the referenced row must not only exist but also be marked as active. This is the correct expectation for checks like "handler must be an active employee" or "approver must still be employed".

```yaml
- rule_id: PROC-009
  name: Saksbehandler_kode must reference an active employee
  description: >
    Every handler assigned to a process must exist as an active employee.
    A handler who has left the organisation must not remain assigned to open cases.
  expectation: validate_active_reference
  parameters:
    source_column: Saksbehandler_kode
    pk_column: Saksnummer
    reference:
      table: Saksbehandling.Ansatte
      column: AnsattKode
      active_column: Status
      active_value: Aktiv
  severity: high
  category: Referential Integrity
  owner: Saksteam
```

Key parameters:
- `source_column` — the column in your table that holds the reference value
- `reference.table` — the table that holds the valid active records
- `reference.column` — the column in the reference table to match against
- `reference.active_column` — the column that holds the active/inactive flag
- `reference.active_value` — the value that means "active" (works for both text and true/false)

Use this instead of `validate_foreign_key` when you need to check both existence AND active status.

---

### 8. Time-in-state check (record has been open too long)

Use `validate_time_in_state` when a record that has not yet been closed (no end date, no payment date, etc.) must not remain in that state beyond a defined number of days.

```yaml
- rule_id: PROC-010
  name: Open cases must not exceed 30 days without closure
  description: >
    A process with no ActualEndDate must not have been running for more than
    30 days since StartDate. Cases exceeding this threshold require review.
  expectation: validate_time_in_state
  parameters:
    start_column: StartDate
    open_when_column: ActualEndDate
    open_when_value: null
    pk_column: Saksnummer
    max_days: 30
  severity: high
  category: Business Logic
  owner: Saksteam
```

Another example — unpaid invoices:

```yaml
- rule_id: INV-009
  name: Unpaid invoices must not exceed 60 days outstanding
  description: >
    An invoice with no PaymentDate must not have been outstanding for more
    than 60 days since InvoiceDate.
  expectation: validate_time_in_state
  parameters:
    start_column: InvoiceDate
    open_when_column: PaymentDate
    open_when_value: null
    pk_column: Fakturanr
    max_days: 60
  severity: high
  category: Business Logic
  owner: Finansteam
```

Key parameters:
- `start_column` — when the record entered the state (e.g. StartDate, InvoiceDate)
- `open_when_column` — the column that is empty when the record is still open
- `open_when_value` — use `null` to mean "column is empty", or provide a specific value
- `max_days` — the maximum number of days the record is allowed to remain in this state

---

### 9. Sequence with repeated steps (allow_loops)

Use `validate_sequence_order` with `allow_loops: true` when a step in the sequence is allowed to repeat before the sequence continues. Without `allow_loops`, any repeated step is flagged as a violation.

```yaml
- rule_id: MIL-009
  name: Milestones with repeated steps must still follow the defined sequence
  description: >
    Some processes allow a milestone step (e.g. Behandling) to repeat before
    the sequence continues. When allow_loops is true, repeated steps are
    accepted without being flagged as out-of-order.
  expectation: validate_sequence_order
  parameters:
    value_column: Milepel
    group_column: prosess_id
    sort_column: StartDate
    expected_sequence:
      - Startbehandling
      - Behandling
      - Stoppbehandling
    allow_loops: true
    completion_gate:
      value_column: Milepel
      value: Stoppbehandling
      sort_column: EndDate
  severity: high
  category: Business Logic
  owner: IT
```

Use `allow_loops: false` (or omit it) when each step must appear exactly once.

> **What is completion_gate?**
> The `completion_gate` is an optional filter that limits which groups are evaluated. Only groups that have reached the gate value (e.g. have a `Stoppbehandling` with a non-null `EndDate`) are included. Groups that are still in progress are skipped. This avoids false positives for processes that are not yet finished.

---

## Severity Guidance

- critical:
  major business, regulatory, or trust risk
- high:
  significant operational impact
- medium:
  meaningful issue with lower urgency
- low:
  minor quality or hygiene issue

Use severity consistently so prioritization and reporting remain reliable.

---

## Good Naming Guidelines

A good rule name is:

- short
- specific
- clear to non-technical readers
- explicit about expected behavior

Good example: ActualEndDate must be on or after StartDate

---

## Rule ID Guidelines

Use domain prefixes consistently, for example:

- PROC for process rules
- MIL for milestone rules
- INV for invoice rules

Keep IDs unique and continue numbering in each file.

Rule IDs must always be set explicitly in every rule block (`rule_id: PROC-001`). There is no automatic ID generation in the main rule files — every new rule needs a manually chosen ID.

---

## Primary Key Column Guidelines

`pk_column` identifies which source column is stored as `primary_key_value` in the
violations table (`dq_violations`). This value is what links a violation back to a
specific record in the source data.

**Every rule group must use exactly one `pk_column` across all its rules.**

The violations table is related to the source entity table in the Power BI semantic
model using `primary_key_value`. Because a single column cannot hold two different
types of keys, mixing `pk_column` values within the same rule group breaks that
relationship and makes drill-through impossible.

| Rule group | Source table | `pk_column` to use |
|---|---|---|
| Process | Saksbehandling.Prosesser | `Saksnummer` |
| Invoice | Saksbehandling.fakturalinjer | `Fakturanr` |

If you need to validate records from a different source table, create a **new rule group**
rather than mixing keys in an existing one. Ask IT if you are unsure which group a new
rule belongs to.

---

## Before You Save: Checklist

- Rule ID is unique
- Expectation name is valid
- Column names are correct
- Required parameters are included
- Severity, category, and owner are set
- Description explains business purpose
- `pk_column` matches the single primary key used by all other rules in this rule group

---

## Common Mistakes To Avoid

- Typos in column names
- Missing required parameters
- Reusing an existing rule_id
- Misusing `pk_column` (repeating it per rule or overriding one rule with a different key) instead of keeping one consistent catalog-level key per rule group
- SQL returning normal rows instead of violating rows
- SQL not returning a stable identifier column when follow-up/drill-through is needed
- Severity set too low for high-impact checks
- Using `validate_foreign_key` when the referenced record could be inactive — use `validate_active_reference` instead
- Setting `open_when_value` to a string when the column uses a date — use `null` for date columns with no end date
- Using `operator: "=="` in `validate_not_null_when` but forgetting `value`
- Misconfiguring `catalog_filter` (for example missing `lookback_days` for `date_range`)
- Defining a join without `on` (or `left_on` + `right_on`), which causes the join to be skipped

---

## Final Recommendation

Start simple:

1. Prefer standard expectations first.
2. Use parameterized custom expectations where available.
3. Use SQL for advanced exceptions.
4. Reuse patterns already present in your domain file.

Consistent rule style makes maintenance easier for both business and IT.
