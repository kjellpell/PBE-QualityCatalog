# Rules Guide For Business Teams

This guide shows how to create and update data quality rules in plain language.

You do not need to change Python code. You only edit YAML files in the rules folder.

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

## Where To Edit Rules

Rule files are in the rules folder, grouped by business area:

- process_rules.yaml
- milestone_rules.yaml
- invoice_rules.yaml

Pick the file for your domain and add or update rules under the rules section.

---

## Quick Rule Workflow

1. Open the correct domain YAML file.
2. Copy a similar existing rule.
3. Change rule_id, name, description, expectation, and parameters.
4. Set severity, category, and owner.
5. Save.
6. Ask IT to include the change in normal preflight and run cycles.

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

Use domain prefixes consistently:

- PROC for process rules
- MIL for milestone rules
- INV for invoice rules

Keep IDs unique and continue numbering in each file.

---

## Before You Save: Checklist

- Rule ID is unique
- Expectation name is valid
- Column names are correct
- Required parameters are included
- Severity, category, and owner are set
- Description explains business purpose

---

## Common Mistakes To Avoid

- Typos in column names
- Missing required parameters
- Reusing an existing rule_id
- SQL returning normal rows instead of violating rows
- Severity set too low for high-impact checks

---

## When To Ask IT For Help

Ask IT when:

- You need a new expectation type not already available
- You are unsure whether to use YAML parameters or SQL
- Preflight fails due to missing source tables/config
- You need help with technical run failures

---

## Final Recommendation

Start simple:

1. Prefer standard expectations first.
2. Use parameterized custom expectations where available.
3. Use SQL for advanced exceptions.
4. Reuse patterns already present in your domain file.

Consistent rule style makes maintenance easier for both business and IT.
