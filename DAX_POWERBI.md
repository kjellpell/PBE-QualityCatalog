# Power BI - DQ Catalog (Core Engine)

This guide contains DAX examples for the core engine outputs only.

## Source Tables

| Table | Description |
|---|---|
| `dq_run_results` | One row per rule per validation run |
| `dq_violations` | One row per violation key with lifecycle state (`Active` / `Resolved`) |

## Core Measures

### DQ Score %

```dax
DQ Score % =
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status] = "PASSED"
    ),
    COUNTROWS( dq_run_results )
) * 100
```

### Total Rules

```dax
Total Rules =
COUNTROWS( dq_run_results )
```

### Rules Passed

```dax
Rules Passed =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "PASSED"
)
```

### Rules Failed

```dax
Rules Failed =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "FAILED"
)
```

### Rules In Error

```dax
Rules In Error =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "ERROR"
)
```

### Active Violations

```dax
Active Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[issue_status] = "Active"
)
```

### Resolved Violations

```dax
Resolved Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[issue_status] = "Resolved"
)
```

### Latest Run Timestamp

```dax
Latest Run Timestamp =
MAX( dq_run_results[run_ts] )
```

## Suggested Report Pages

1. Run Overview:
   KPIs for DQ Score %, Total Rules, Rules Failed, Rules In Error.
2. Rule Group Health:
   Bar/column chart by `rule_group` and `status`.
3. Active Violation Backlog:
   Table filtered to `dq_violations[issue_status] = "Active"`.
   `violated_column` is always a real column name in `table_name` (or `NULL`
   for the `sql` rule type, which has no single offending column) — safe to
   group/count by. `violation_scope` tells you how to read
   `primary_key_value`: `"row"` means it's the PK of the offending row in
   `table_name`; `"group"` (used by `sequence_ordered`, `pairs_present`,
   `gate_complete`, `group_aggregate_matches`) means it's a group key, not a
   row PK — don't join it back to `table_name` as if it were one.
4. Resolution Trend:
   Time series of `Active` vs `Resolved` by `first_seen_at` / `resolved_at`.

## Notes

- This core baseline does not use enriched violation output tables.
- This core baseline does not use owner/routing/escalation fields.
