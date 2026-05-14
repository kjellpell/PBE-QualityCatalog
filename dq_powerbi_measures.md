# Power BI — DQ Catalog: DAX Measures and Report Guide

All measures operate on the two Delta tables written by the validation engine:

| Table | Description |
|---|---|
| `dq_run_results` | One row per rule per validation run (summary) |
| `dq_violations`  | One row per violating record per rule; `issue_status` = `Active` or `Resolved` |

**Note:** All violation measures filter to `issue_status = "Active"` to exclude previously resolved issues.

---

## 1. Core Quality Measures

### 1.1 Total DQ Score (%)

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

> Use as a KPI card with green/red conditional formatting.

---

### 1.2 DQ Score by Rule Group

```dax
DQ Score % by Group =
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status] = "PASSED"
    ),
    COUNTROWS( dq_run_results )
) * 100
```

> Use in a bar or donut chart sliced by `dq_run_results[rule_group]`
> to compare quality across Process, Milestone, and Invoice side by side.

---

### 1.3 Total Rules Evaluated

```dax
Total Rules =
COUNTROWS( dq_run_results )
```

---

### 1.4 Rules Passed

```dax
Rules Passed =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "PASSED"
)
```

---

### 1.5 Rules Failed

```dax
Rules Failed =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "FAILED"
)
```

---

### 1.6 Rules in Error

```dax
Rules in Error =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "ERROR"
)
```

---

## 2. Violation Measures

### 2.1 Active Violations

```dax
Active Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[issue_status] = "Active"
)
```

---

### 2.2 Violations by Severity

```dax
Critical Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity]     = "critical",
    dq_violations[issue_status] = "Active"
)

High Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity]     = "high",
    dq_violations[issue_status] = "Active"
)

Medium Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity]     = "medium",
    dq_violations[issue_status] = "Active"
)
```

---

### 2.3 Violation Rate (per 1 000 rows)

```dax
Violation Rate per 1000 =
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_violations ),
        dq_violations[issue_status] = "Active"
    ),
    SUMX( DISTINCT( dq_run_results[run_id] ), CALCULATE( SUM( dq_run_results[total_rows] ) ) )
) * 1000
```

---

## 3. Trend Measures

### 3.1 Quality Score — Latest Run

```dax
DQ Score % Latest Run =
VAR LatestRun =
    CALCULATE(
        MAX( dq_run_results[run_timestamp] ),
        ALL( dq_run_results )
    )
RETURN
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status]        = "PASSED",
        dq_run_results[run_timestamp] = LatestRun
    ),
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[run_timestamp] = LatestRun
    )
) * 100
```

---

### 3.2 Quality Score — Previous Run

```dax
DQ Score % Previous Run =
VAR RunDates =
    CALCULATETABLE(
        DISTINCT( dq_run_results[run_timestamp] ),
        ALL( dq_run_results )
    )
VAR LatestRun = MAXX( RunDates, [run_timestamp] )
VAR PrevRun   = MAXX( FILTER( RunDates, [run_timestamp] < LatestRun ), [run_timestamp] )
RETURN
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status]        = "PASSED",
        dq_run_results[run_timestamp] = PrevRun
    ),
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[run_timestamp] = PrevRun
    )
) * 100
```

---

### 3.3 Quality Score Change (vs. Previous Run)

```dax
DQ Score Change % =
[DQ Score % Latest Run] - [DQ Score % Previous Run]
```

> Display as a KPI card with green / red conditional formatting.

---

## 4. Recommended Report Pages

### Page 1 — Management Summary
- **KPI cards**: `DQ Score % Latest Run`, `Active Violations`, `Rules Failed`, `DQ Score Change %`
- **Donut chart**: `DQ Score % by Group` sliced by `rule_group` (Process / Milestone / Invoice)
- **Stacked bar**: Rule count by status (Passed / Failed / Error) per `rule_group`
- **Line chart**: `DQ Score % Latest Run` over `batch_date` (trend)

### Page 2 — Process Rule Detail
- **Slicer**: `batch_date`, `severity`, `rule_id`
- **Table**: `rule_id`, `rule_name`, `severity`, `total_rows`, `failed_rows`,
  `success_pct`, `status`, `details` filtered to `rule_group = "Process"`
- **Bar chart**: `failed_rows` by `rule_id`

### Page 3 — Milestone Rule Detail
- Same layout as Page 2, filtered to `rule_group = "Milestone"`

### Page 4 — Invoice Rule Detail
- Same layout as Page 2, filtered to `rule_group = "Invoice"`

### Page 5 — Violation Drill-down
- **Table**: `violation_detail`, `primary_key_value`, `rule_name`,
  `severity`, `batch_date` filtered to `issue_status = "Active"`
- **Enable drill-through** from Pages 2–4 on `rule_id`

---

## 5. Alert Measures

Connect the following measure to a notification flow (e.g. Power Automate) when alert functionality is added:

```dax
Has Critical Violations Today =
VAR Today = TODAY()
RETURN
IF(
    CALCULATE(
        COUNTROWS( dq_violations ),
        dq_violations[severity]     = "critical",
        dq_violations[issue_status] = "Active",
        dq_violations[batch_date]   = Today
    ) > 0,
    1, 0
)
```

> A value of `1` can trigger a Power Automate alert to the responsible rule owner or team.
