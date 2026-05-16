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

### 2.2 Violation Rate (per 1 000 rows)

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
- **Slicer**: `batch_date`, `rule_id`
- **Table**: `rule_id`, `rule_name`, `total_rows`, `failed_rows`,
  `success_pct`, `status`, `details` filtered to `rule_group = "Process"`
- **Bar chart**: `failed_rows` by `rule_id`

### Page 3 — Milestone Rule Detail
- Same layout as Page 2, filtered to `rule_group = "Milestone"`

### Page 4 — Invoice Rule Detail
- Same layout as Page 2, filtered to `rule_group = "Invoice"`

### Page 5 — Violation Drill-down
- **Table**: `violation_detail`, `primary_key_value`, `rule_name`,
  `batch_date` filtered to `issue_status = "Active"`
- **Enable drill-through** from Pages 2–4 on `rule_id`

### Page 6 — My Violations (handler/manager view)
Source table: `dq_violations_owners` (not `dq_violations` — see Section 7).

- **KPI cards**: `Active Violations`, `Rules With Active Violations`, `Latest Batch Date`
- **Slicers**: `rule_group`, `routing_team`, `batch_date`, `issue_status`, `owner_name`
- **Main table**: `saksnummer`, `rule_name`, `violated_column`, `violation_detail`,
  `indikator`, `batch_date`, `issue_status`, `owner_name`
- **Trend line**: Active violation count by `batch_date`

Handlers see only their own rows (RLS). Managers see all rows and slice by
`owner_name` to inspect a specific handler's work. No bridge tables needed —
context columns carry human-readable identifiers directly.

---

## 5. Relating Violations to Source Tables

> **Handler/manager report (Page 6):** uses `dq_violations_owners`, which already contains context columns (`saksnummer`, `indikator`, etc.) alongside each violation row. No bridge tables are needed for that report.

The bridge table pattern below applies to the management summary report (Pages 1–5), which is built on `dq_run_results` and `dq_violations`. `dq_violations[primary_key_value]` is always stored as **text**. Source tables like `Prosess` use a typed key (integer, GUID, etc.). Power BI cannot create a direct relationship across mismatched types, so use a calculated table as a typed bridge per rule group.

### Pattern — calculated bridge table

```dax
Prosess_Violation_Keys =
SELECTCOLUMNS(
    FILTER(
        dq_violations,
        dq_violations[rule_group]    = "Process"
     && dq_violations[issue_status] = "Active"
    ),
    "prosess_id", INT( dq_violations[primary_key_value] )
)
```

Then in the **Model view** create these two relationships:

| From | To | Cardinality |
|---|---|---|
| `Prosess_Violation_Keys[prosess_id]` | `Prosess[prosess_id]` | Many-to-one |
| `dq_violations[primary_key_value]` | `Prosess_Violation_Keys[prosess_id_text]` | Many-to-one |

Because the bridge only needs to carry the join key back as text, add a second column so the text side can close the loop:

```dax
Prosess_Violation_Keys =
SELECTCOLUMNS(
    FILTER(
        dq_violations,
        dq_violations[rule_group]    = "Process"
     && dq_violations[issue_status] = "Active"
    ),
    "prosess_id",      INT( dq_violations[primary_key_value] ),
    "prosess_id_text", dq_violations[primary_key_value]
)
```

Relationships after adding the text column:

```
dq_violations[primary_key_value]
        ↓  (Many → One)
Prosess_Violation_Keys[prosess_id_text]
        ↓  (Many → One, via prosess_id)
Prosess[prosess_id]
```

### Violation count measure on the source table

Once the bridge is in place this measure resolves in the `Prosess` row context:

```dax
Active Violations for Prosess =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[issue_status] = "Active",
    TREATAS(
        VALUES( Prosess[prosess_id] ),
        Prosess_Violation_Keys[prosess_id]
    )
)
```

> If your source key is already text (e.g. `Saksnummer`), omit the `INT()` cast and use the text value directly — the bridge table reduces to a simple `DISTINCT` of `primary_key_value` filtered to the right rule group.

---

## 7. Handler Report + RLS

### Data source

All measures and visuals on Page 6 use `dq_violations_owners` — the unified
table written by `nb_dq_04_routing.py` after each run.

### Role-Level Security setup

In **Power BI Desktop → Modeling → Manage Roles**, create two roles:

| Role | Table | DAX filter | Who gets this role |
|---|---|---|---|
| `Handler` | `dq_violations_owners` | `[owner_email] = USERPRINCIPALNAME()` | Individual saksbehandlere |
| `Manager` | *(no filter)* | *(leave empty)* | Team leads, business teams, top of hierarchy |

After publishing to Power BI Service, assign users to the correct role under
**Workspace → Dataset settings → Row-level security**.

With RLS active, all DAX measures on the page scope automatically to the
logged-in user's visible rows — `COUNTROWS(dq_violations_owners)` returns the
handler's own count, and the manager's full count. No separate "My violations"
measures are needed.

### DAX measures

```dax
Active Violations =
CALCULATE(
    COUNTROWS( dq_violations_owners ),
    dq_violations_owners[issue_status] = "Active"
)
```

```dax
Rules With Active Violations =
CALCULATE(
    DISTINCTCOUNT( dq_violations_owners[rule_id] ),
    dq_violations_owners[issue_status] = "Active"
)
```

```dax
Latest Batch Date =
CALCULATE( MAX( dq_violations_owners[batch_date] ), ALL( dq_violations_owners ) )
```

---

## 6. Alert Measures

Connect the following measure to a notification flow (e.g. Power Automate) when alert functionality is added:

```dax
Has Active Violations Today =
VAR Today = TODAY()
RETURN
IF(
    CALCULATE(
        COUNTROWS( dq_violations ),
        dq_violations[issue_status] = "Active",
        dq_violations[batch_date]   = Today
    ) > 0,
    1, 0
)
```

> A value of `1` can trigger a Power Automate alert to the responsible rule owner or team.
