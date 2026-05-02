# IC Rules Guide For Control Owners

This guide explains how to define Internal Control (IC) rules with the canonical naming model.

IC rules use the same validation engine as DQ rules, but produce IC exception lifecycle output.

## Start Here

Use this guide when the intent is control effectiveness, not only data quality.

Examples:
- Segregation of duties
- Timeliness/SLA controls
- Mandatory completion before state transition
- Active assignment controls

Use [RULES_GUIDE.md](RULES_GUIDE.md) for shared expectation patterns.
Use [README.md](README.md) for runtime/deployment.

## Fabric Notebook Workflow

1. Update IC rule definitions in your managed source.
2. Run [nb_dq_01_preflight.py](nb_dq_01_preflight.py).
3. Run with `DRY_RUN = True`.
4. Review `ic_exceptions_tmp` and DQ temp outputs.
5. Switch to `DRY_RUN = False` only after sign-off.

## What Makes A Rule IC

A rule is treated as IC when at least one IC identifier is present:
- `control_ref`
- `control_type`
- `risk_domain`

Without these, the rule behaves as a standard DQ rule.

## IC vs DQ Behavior

| Aspect | DQ | IC |
|---|---|---|
| Purpose | Data quality | Control effectiveness |
| Extra output | None | `ic_run_results`, `ic_exceptions` |
| Auto-close when issue disappears | Yes (`dq_violations`) | No, human transition required |
| Lifecycle | Active/Resolved | Open/Remediated/Verified/Waived |

## IC Rule ID Convention

| Domain | Prefix | Example |
|---|---|---|
| Process | `IC-PROC-` | `IC-PROC-001` |
| Invoice | `IC-INV-` | `IC-INV-001` |
| IT | `IC-IT-` | `IC-IT-001` |
| HR | `IC-HR-` | `IC-HR-001` |

Do not reuse retired IDs.

## Canonical Contract For IC Rules

IC rules use the same canonical expectation and parameter naming as DQ rules.

### Operator taxonomy

| Operator family | Allowed values | Typical IC use |
|---|---|---|
| Trigger operators | `IS NULL`, `IS NOT NULL`, `==` | "If case is open then owner required" |
| Comparison operators | `>`, `<`, `>=`, `<=`, `==`, `!=` | SoD, date ordering, thresholds |

## IC-Only Fields

Add below standard rule metadata.

```yaml
control_ref: COSO-CC5.2
control_type: Detective
risk_domain: Operational
remediation_due_days: 5
```

### Field meanings

| Field | Purpose |
|---|---|
| `control_ref` | Framework/policy reference |
| `control_type` | `Preventive`, `Detective`, or `Corrective` |
| `risk_domain` | `Financial`, `Operational`, `Compliance`, or `IT` |
| `remediation_due_days` | SLA days from first detection |

## Starter IC Patterns

### Pattern 1: Segregation of duties

```yaml
- rule_id: IC-INV-001
  name: Approver cannot equal creator
  description: Requesting and approving the same transaction is not allowed.
  expectation: field_comparison
  parameters:
    left_column: ApprovedBy
    right_column: CreatedBy
    operator: "!="
    pk_column: Fakturanr
  severity: critical
  category: Segregation of Duties
  owner: Finansteam
  control_ref: COSO-CC5.2
  control_type: Detective
  risk_domain: Financial
  remediation_due_days: 3
```

### Pattern 2: Open case requires assignment

```yaml
- rule_id: IC-PROC-002
  name: Open cases must have active handler
  description: Unassigned open cases indicate control break in operational ownership.
  expectation: not_null_when
  parameters:
    when_column: ActualEndDate
    operator: IS NULL
    checked_columns:
      - Saksbehandler_kode
    pk_column: Saksnummer
  severity: high
  category: Ownership Control
  owner: Saksteam
  control_ref: COSO-CC1.1
  control_type: Preventive
  risk_domain: Operational
  remediation_due_days: 5
```

### Pattern 3: Active assignment control

```yaml
- rule_id: IC-PROC-003
  name: Assigned handler must be active employee
  description: Open workload cannot remain assigned to inactive employees.
  expectation: reference_active
  parameters:
    source_column: Saksbehandler_kode
    reference_table: HR.Employees
    reference_column: EmployeeCode
    reference_active_column: IsActive
    reference_active_value: true
    pk_column: Saksnummer
  severity: critical
  category: Access Control
  owner: Saksteam
  control_ref: ISO27001-A.9.2.5
  control_type: Detective
  risk_domain: IT
  remediation_due_days: 2
```

### Pattern 4: Timeliness/SLA

```yaml
- rule_id: IC-PROC-004
  name: Open cases older than SLA are violations
  description: Open cases must be handled within SLA window.
  expectation: state_duration_within_limit
  parameters:
    start_column: StartDate
    open_state_column: ActualEndDate
    open_state_value: null
    max_days: 30
    pk_column: Saksnummer
  severity: high
  category: SLA Control
  owner: Saksteam
  control_ref: OPS-SLA-001
  control_type: Detective
  risk_domain: Operational
  remediation_due_days: 7
```

### Pattern 5: Custom control query

```yaml
- rule_id: IC-INV-005
  name: Payments must not precede approval
  description: Any payment earlier than approval date is a control violation.
  expectation: sql_violations
  parameters:
    sql: |
      SELECT Fakturanr
      FROM Finance.InvoicePayments
      WHERE PaymentDate < ApprovalDate
  severity: critical
  category: Payment Control
  owner: Finansteam
  control_ref: SOX-302
  control_type: Detective
  risk_domain: Financial
  remediation_due_days: 1
```

## IC Exception Lifecycle

`ic_exceptions` transitions:
- `Open` to `Remediated`
- `Open` to `Verified`
- `Open` to `Waived`
- `Remediated` to `Verified`

The engine does not auto-close IC exceptions. Human workflow is required.

## Checklist Before Save

- Rule ID uses IC prefix and is unique
- Canonical expectation name is used
- Canonical parameter names are used
- At least one IC identifier field is set
- `control_type` and `risk_domain` values are valid
- `remediation_due_days` is integer if used
- Rule passes preflight and dry-run

## Common Mistakes

| Mistake | Impact | Fix |
|---|---|---|
| Missing IC identifier fields | Rule treated as DQ only | Add `control_ref` or `control_type` or `risk_domain` |
| Unknown names in parameters | Preflight failure | Check ARCHITECTURE.md for canonical keys |
| Wrong `pk_column` | Hard to trace incidents | Use stable business key per group |
| Running production first | Noisy exceptions | Dry-run and review first |
| `report_link` points to restricted URL | Evidence retrieval warning | Use accessible share link |
