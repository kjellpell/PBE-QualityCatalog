# Power BI — Data Quality Catalog: DAX Measures & Report Guide

This document lists all DAX measures to create in Power BI for the GX Core
data quality framework. The measures operate on the two Delta tables written
by `nb_dq_validate.py`:

| Table | Description |
|---|---|
| `dq_run_results` | One row per rule per validation run (summary) |
| `dq_violations`  | One row per offending record per rule per run (detail) |

---

## 1. Core Quality Score Measures

### 1.1 Overall Data Quality Score (%)

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

> Shows the percentage of rules that passed across all rule groups and runs
> visible in the current filter context.

---

### 1.2 Data Quality Score by Rule Group

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

> Use this measure in a bar / donut chart sliced by `dq_run_results[rule_group]`
> to compare Case vs. Invoice quality side-by-side.

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

### 2.1 Total Violation Rows

```dax
Total Violations =
COUNTROWS( dq_violations )
```

---

### 2.2 Violations by Severity

```dax
Critical Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity] = "critical"
)

High Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity] = "high"
)

Medium Violations =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity] = "medium"
)
```

---

### 2.3 Violation Rate (per 1 000 rows)

```dax
Violation Rate per 1k =
DIVIDE(
    COUNTROWS( dq_violations ),
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
        dq_run_results[status] = "PASSED",
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
DQ Score % Prev Run =
VAR RunDates =
    CALCULATETABLE(
        DISTINCT( dq_run_results[run_timestamp] ),
        ALL( dq_run_results )
    )
VAR LatestRun  = MAXX( RunDates, [run_timestamp] )
VAR PrevRun    = MAXX( FILTER( RunDates, [run_timestamp] < LatestRun ), [run_timestamp] )
RETURN
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status] = "PASSED",
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
DQ Score % Change =
[DQ Score % Latest Run] - [DQ Score % Prev Run]
```

> Display this as a KPI card with a green / red conditional format.

---

## 4. Handler-Level Violation Measures (Case group)

### 4.1 Handlers with Violations

```dax
Handlers with Violations =
CALCULATE(
    DISTINCTCOUNT( dq_violations[saksbehandler_kode] ),
    dq_violations[rule_group] = "Case",
    NOT ISBLANK( dq_violations[saksbehandler_kode] )
)
```

---

### 4.2 Top Violating Handler

```dax
Top Violating Handler =
TOPN(
    1,
    ADDCOLUMNS(
        FILTER(
            DISTINCT( dq_violations[saksbehandler_kode] ),
            NOT ISBLANK( dq_violations[saksbehandler_kode] )
        ),
        "ViolCount",
        CALCULATE( COUNTROWS( dq_violations ) )
    ),
    [ViolCount], DESC
)
```

---

## 5. Recommended Report Pages

### Page 1 — Executive Summary
- **KPI cards**: `DQ Score % Latest Run`, `Total Violations`, `Rules Failed`,
  `DQ Score % Change`
- **Donut chart**: DQ Score % by `rule_group` (Case / Invoice)
- **Stacked bar**: Rule status counts (Passed / Failed / Error) by `rule_group`
- **Line chart**: `DQ Score % Latest Run` over `batch_date` (trend)

### Page 2 — Case Rules Detail
- **Slicer**: `batch_date`, `severity`, `rule_id`
- **Table**: `rule_id`, `rule_name`, `severity`, `total_rows`, `failed_rows`,
  `success_pct`, `status`, `details`  filtered to `rule_group = "Case"`
- **Bar chart**: `failed_rows` by `rule_id`

### Page 3 — Invoice Rules Detail
- Same layout as Page 2 but filtered to `rule_group = "Invoice"`

### Page 4 — Violation Drill-Through
- **Table**: `violation_detail`, `primary_key_value`, `rule_name`,
  `severity`, `saksbehandler_kode`, `batch_date`
- **Enable drill-through** from Pages 2 & 3 on `rule_id`

---

## 6. Alert-Ready Measure (future use)

When alert functionality is added, link the following measure to a
notification flow (e.g. Power Automate):

```dax
Has Critical Violations Today =
VAR Today = TODAY()
RETURN
IF(
    CALCULATE(
        COUNTROWS( dq_violations ),
        dq_violations[severity]  = "critical",
        dq_violations[batch_date] = Today
    ) > 0,
    1, 0
)
```

> A value of `1` can trigger a Power Automate alert to the responsible handler
> or team owner.
