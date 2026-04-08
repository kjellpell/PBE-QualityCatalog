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

---

## IC Measures — Internal Control Monitoring

The following measures operate on the IC tables written by the engine and the
two Fabric notebooks:

| Table | Description |
|---|---|
| `ic_run_results` | One row per IC rule per validation run |
| `ic_exceptions` | One row per IC violation, with 4-state lifecycle |
| `ic_control_register` | Register of all controls (populated manually) |
| `ic_manual_attestations` | Attestation records written by nb_ic_02 |

### IC Control Pass Rate (%)

```dax
IC Control Pass Rate % =
DIVIDE(
    COUNTROWS( FILTER( ic_run_results, ic_run_results[status] = "PASSED" ) ),
    COUNTROWS( ic_run_results )
) * 100
```

### IC Open Exceptions

```dax
IC Open Exceptions =
COUNTROWS( FILTER( ic_exceptions, ic_exceptions[ic_status] = "Open" ) )
```

### IC Exceptions Breaching SLA

```dax
IC Exceptions Breaching SLA =
COUNTROWS(
    FILTER(
        ic_exceptions,
        ic_exceptions[ic_status] = "Open"
            && ic_exceptions[remediation_due_date] < TODAY()
    )
)
```

> An exception breaches SLA when it has been Open past its `remediation_due_date`.
> Exceptions without a due date (remediation_due_days was blank in the rule) are excluded.

### IC Days Open (per exception)

```dax
IC Days Open =
DATEDIFF( ic_exceptions[first_seen_at], TODAY(), DAY )
```

> Add as a calculated column or use in a table visual with exception rows.

### IC Exception Trend (weekly)

Use `ic_exceptions[first_seen_at]` on the axis and `COUNTROWS(ic_exceptions)` as
the value, with a weekly date hierarchy. This shows the volume of new Open
exceptions opened each week.

### Manual Controls Overdue for Attestation

```dax
Manual Controls Overdue =
COUNTROWS(
    FILTER(
        ic_control_register,
        ic_control_register[execution_type] = "Manual"
            && ic_control_register[active] = TRUE
            && CALCULATE(
                MAX( ic_manual_attestations[next_due_date] ),
                RELATEDTABLE( ic_manual_attestations )
               ) < TODAY()
    )
)
```

> Requires a relationship between `ic_control_register[control_id]` and
> `ic_manual_attestations[control_id]`.

### Days Since Last Attestation (per control)

```dax
Days Since Last Attestation =
DATEDIFF(
    CALCULATE(
        MAX( ic_manual_attestations[attested_at] ),
        RELATEDTABLE( ic_manual_attestations )
    ),
    TODAY(),
    DAY
)
```

> Use as a calculated column on `ic_control_register`, or in a table visual
> showing each manual control with its latest attestation age.

---

## Translytical Task Flow Configuration

Fabric Translytical Task Flow replaces Power Automate for both IC notebooks.
The task flow embeds an interactive panel directly in the Power BI report
and calls a Fabric notebook with parameters bound from the selected row.

### Task Flow 1 — IC Exception Transitions (nb_ic_01_manage_exceptions)

**Report page:** IC Exceptions (table visual of `ic_exceptions`)

**Row binding:** `ic_exceptions[primary_key_value]` → notebook parameter `exception_id`

**Form fields:**
| Field | Type | Notes |
|-------|------|-------|
| `new_status` | Dropdown | Values: `Verified`, `Waived` |
| `waiver_reason` | Text | Show conditionally when `new_status = Waived`; min 10 characters |

**Notebook called:** `nb_ic_01_manage_exceptions`

**Identity:** `actioned_by` is derived server-side from the Fabric session — it is not a form field.

**After submission:** Refresh the report to show the updated `ic_status`.

---

### Task Flow 2 — Manual Control Attestation (nb_ic_02_attest_manual_control)

**Report page:** Manual Controls Register (table visual of `ic_control_register` filtered to `execution_type = Manual`)

**Row binding:** `ic_control_register[control_id]` → notebook parameter `control_id`

**Form fields:**
| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `period_covered` | Text | Yes | e.g. `2025-Q2`, `2025-April` |
| `report_link` | Text (URL) | No | Link to evidence document; notebook attempts download |
| `notes` | Text | No | Free text remarks |

**Notebook called:** `nb_ic_02_attest_manual_control`

**Identity:** `attested_by` is derived server-side from the Fabric session — it is not a form field.

**After submission:** Refresh the report to show the updated `next_due_date` and latest `attested_at`.

**Note on report_link:** The notebook will attempt to download the file from the URL and store it in Lakehouse Files under `ic_evidence/{control_id}/{period_covered}/`. This works for SharePoint "Anyone with the link" share links and other direct download URLs. SSO-protected links (standard SharePoint view/edit links) will fail the download, but the attestation is still recorded with `report_link` stored and `evidence_path = NULL`.
