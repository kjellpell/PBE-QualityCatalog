# IC Rules Guide For Control Owners

This guide explains how to write and maintain Internal Control (IC) rules.

IC rules use the same engine as data quality rules. Rules are managed in the `rule_catalog`
Delta table — the YAML files in `rules/` are one-time migration source artifacts and are
**not** read by the validation engine at runtime. No Python code changes are needed to add
or update rules.

---

## Who This Guide Is For

This guide is for control owners and risk managers who define internal control requirements.

For data quality rules (completeness, format, referential integrity), use `RULES_GUIDE.md` instead.

For engine, deployment, or runtime details, use `README.md`.

---

## What An IC Rule Is

An IC rule is an automated check that verifies an internal control is operating effectively.

Examples:

- The person who approves a payment is not the same person who requested it (segregation of duties)
- A case cannot be closed until all mandatory milestones are completed
- An invoice is not paid before it is approved

When data fails an IC rule, the violation is recorded in `ic_exceptions` and enters a lifecycle that requires human sign-off before it is closed.

---

## How IC Rules Differ From DQ Rules

| Aspect | DQ Rule | IC Rule |
|--------|---------|---------|
| Purpose | Data quality (completeness, format, consistency) | Control effectiveness |
| Output tables | `dq_run_results`, `dq_violations` | Both the above **plus** `ic_run_results`, `ic_exceptions` |
| Exception lifecycle | Active → Resolved (automatic when data is fixed) | Open → Remediated → Verified (or Waived) — humans must sign off |
| "Not seen in current run" | Auto-resolves | Stays Open — not auto-closed |
| Extra YAML fields | None | `control_ref`, `control_type`, `risk_domain`, `remediation_due_days` |

A rule is treated as IC if it carries at least one of: `control_ref`, `control_type`, `risk_domain`.

---

## Rule ID Naming Convention

IC rule IDs use the `IC-` prefix followed by a domain shortcode and a number:

| Domain | Prefix | Example |
|--------|--------|---------|
| Process / case management | `IC-PROC-` | `IC-PROC-001` |
| Invoice / finance | `IC-INV-` | `IC-INV-001` |
| IT / access | `IC-IT-` | `IC-IT-001` |
| HR / people | `IC-HR-` | `IC-HR-001` |

Do not reuse rule IDs. If a rule is retired, leave its ID unused.

---

## The Four IC-Only YAML Fields

Add these fields below the standard `owner:` field in any rule you want treated as an IC rule.

```yaml
control_ref: COSO-CC5.2         # optional — framework reference (free text)
control_type: Detective          # optional — Preventive | Detective | Corrective
risk_domain: Financial           # optional — Financial | Operational | Compliance | IT
remediation_due_days: 5          # optional — positive integer, SLA in calendar days
```

### `control_ref`
Free text reference to a control framework, standard, or policy. Examples:
- `COSO-CC5.2` (COSO Internal Control — Monitoring Activities)
- `ISO27001-A.9.4` (ISO 27001 — System and Application Access Control)
- `SOX-302` (Sarbanes-Oxley Section 302)

No validation is enforced — use whichever taxonomy your organisation follows.

### `control_type`
Classifies when the control acts:
- `Preventive` — stops a violation from occurring
- `Detective` — identifies a violation after it has occurred
- `Corrective` — restores normal operation after a violation

A warning is logged if an unexpected value is used, but the run will not fail.

### `risk_domain`
The risk category this control addresses:
- `Financial` — financial reporting accuracy, payment controls
- `Operational` — process completeness, SLA compliance
- `Compliance` — regulatory or legal requirements
- `IT` — access, change management, data integrity

### `remediation_due_days`
Integer. The number of calendar days from when the exception is first seen to when it must be resolved. Used to compute `remediation_due_date` in `ic_exceptions`. Leave blank if there is no SLA.

---

## Severity Guidance

| Severity | Use when |
|----------|----------|
| `critical` | Regulatory requirement, financial reporting risk, or fraud risk |
| `high` | Material operational risk, significant compliance exposure |
| `medium` | Control weakness with limited immediate impact |
| `low` | Process improvement opportunity |

---

## Full Rule Template

```yaml
rule_group: IC-Process        # your domain group name (IC- prefix recommended)
table: Prosesser              # source table name (without database prefix)
database: Saksbehandling      # source database / schema
pk_column: Saksnummer         # primary key column — stored as primary_key_value in ic_exceptions

rules:

  - rule_id: IC-PROC-001
    name: Short human-readable title
    description: >
      Why this control matters and what it is checking.
      One or two sentences is enough.
    expectation: validate_column_comparison   # or sql, or any supported expectation
    parameters:
      column_A: ApprovedBy
      column_B: CreatedBy
      operator: "!="
      pk_column: Saksnummer
    severity: critical
    category: Segregation of Duties
    owner: Saksteam

    # IC-only fields — remove any you do not need
    control_ref: COSO-CC5.2
    control_type: Detective
    risk_domain: Operational
    remediation_due_days: 3
```

---

## IC Exception Lifecycle

When a violation is first seen, it is inserted into `ic_exceptions` with `ic_status = Open`.

```
Open  ──────────────────────►  Verified
  │   (human via Power BI)       ▲
  │                              │ (also from Remediated)
  ├───────────────────────────►  Remediated
  │   (human via Power BI)
  │
  └───────────────────────────►  Waived
      (human via Power BI,
       requires waiver_reason ≥ 10 chars)
```

**The engine never closes an IC exception.** If a violation disappears from the source data, the Open row stays Open until a human transitions it. This is intentional — it ensures a human verifies the fix, not just that the data changed.

If a Verified or Waived exception re-appears in a later run, a brand-new Open exception is created. The closed row is not modified.

**Who transitions exceptions:**
- `Open → Remediated`: control owner (confirms fix has been applied)
- `Remediated → Verified`: second reviewer (confirms the fix is effective)
- `Open → Verified`: reviewer (skipping remediation, direct sign-off)
- `Open → Waived`: risk owner (with documented waiver reason)

Transitions are made from the IC Exceptions page in Power BI using the Translytical Task Flow panel.

---

## Automated Controls vs Manual Controls

**Automated controls** (this guide) are rules in `rule_catalog` that the engine checks against live data on every run.

**Manual controls** are controls that cannot be automated — e.g. a manager sign-off meeting, a quarterly review process. They are registered in `ic_control_register` (maintained manually) and attested periodically via the Manual Controls page in Power BI. The attestation notebook (`nb_ic_02_attest_manual_control`) records the attested_by, period_covered, report_link, and optionally downloads the evidence file.

To register a manual control, insert a row directly into `ic_control_register`:

```sql
INSERT INTO ic_control_register VALUES (
  'IC-MAN-001',                -- control_id
  'Quarterly access review',   -- name
  'Manager reviews all active user accounts quarterly and revokes excess access.',
  'ISO27001-A.9.2.5',          -- control_ref
  'Manual',                    -- execution_type
  'Detective',                 -- control_type
  'IT',                        -- risk_domain
  'Medium',                    -- inherent_risk
  'IT-Security',               -- control_owner
  'Quarterly',                 -- review_frequency
  'Quarterly',                 -- attestation_frequency
  NULL,                        -- last_design_review_at
  true,                        -- active
  current_timestamp(),
  current_timestamp()
)
```

---

## Starter Examples

See `rules/ic_process_rules.yaml` and `rules/ic_invoice_rules.yaml` for working examples covering:
- Segregation of duties (`validate_column_comparison` with `!=`)
- Sequence / timing controls (`validate_column_comparison` with `>=`)
- Completeness before state transition (`sql` expectation with correlated subquery)
- Budget ceiling check (`sql` expectation with JOIN)

---

## Checklist Before Saving

- [ ] Rule ID starts with `IC-` and does not exist in any other rule file
- [ ] `rule_group` starts with `IC-` (e.g. `IC-Process`, `IC-Invoice`)
- [ ] `table` and `database` match an actual table in the Fabric metastore
- [ ] `pk_column` is the primary key column of the source table
- [ ] At least one of `control_ref`, `control_type`, `risk_domain` is set (otherwise the rule is treated as a DQ rule)
- [ ] `control_type` is one of `Preventive`, `Detective`, `Corrective`
- [ ] `risk_domain` is one of `Financial`, `Operational`, `Compliance`, `IT`
- [ ] `remediation_due_days` is a positive integer or absent
- [ ] Column names in `parameters` match actual column names in the source table (run preflight to check)
- [ ] Severity is appropriate (`critical` for regulatory/financial risk, `high` for operational risk)

---

## Common Mistakes

| Mistake | Effect | Fix |
|---------|--------|-----|
| Using `PROC-` prefix instead of `IC-PROC-` | Rule ID collides with DQ rules in process_rules.yaml | Use `IC-PROC-` for IC rules |
| Not setting any IC identifier field | Rule treated as DQ, violations go to dq_violations only, no 4-state lifecycle | Add at least one of control_ref, control_type, risk_domain |
| Column name typo in parameters | Preflight warns; rule runs as ERROR | Run preflight and check warnings |
| `remediation_due_days` as string (`"5"`) | Type error at runtime | Use integer: `5`, not `"5"` |
| Providing an SSO-protected URL as report_link | Evidence file download fails (warning logged) | Use an "Anyone with the link" SharePoint share link |
