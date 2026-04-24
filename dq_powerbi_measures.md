# Power BI — Datakvalitetskatalog: DAX-målinger og rapportguide

Dette dokumentet lister opp alle DAX-målingene som skal opprettes i Power BI
for datakvalitetsrammeverket. Målingene opererer på de to Delta-tabellene
som skrives av `nb_dq_validate.py`:

| Tabell | Beskrivelse |
|---|---|
| `dq_run_results` | Én rad per regel per valideringskjøring (oppsummering) |
| `dq_violations`  | Én rad per feilende post per regel per kjøring (detalj) |

---

## 1. Kjerne: mål for datakvalitet

### 1.1 Total datakvalitetsscore (%)

```dax
DQ-score % =
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status] = "PASSED"
    ),
    COUNTROWS( dq_run_results )
) * 100
```

> Viser prosentandelen av regler som besto på tvers av alle regelgrupper og
> Vises som et KPI-kort med grønn/rød betinget formatering.

---

### 1.2 Datakvalitetsscore etter regelgruppe

```dax
DQ-score % per gruppe =
DIVIDE(
    CALCULATE(
        COUNTROWS( dq_run_results ),
        dq_run_results[status] = "PASSED"
    ),
    COUNTROWS( dq_run_results )
) * 100
```

> Bruk denne målingen i et stolpe-/donutdiagram delt på `dq_run_results[rule_group]`
> for å sammenligne datakvaliteten for Case vs. Invoice side om side.

---

### 1.3 Totalt antall evaluerte regler

```dax
Totale regler =
COUNTROWS( dq_run_results )
```

---

### 1.4 Regler som besto
### 3.3 Endring i kvalitetsscore (vs. forrige kjøring)

```dax
Regler bestått =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "PASSED"
)
```

---

### 1.5 Regler som feilet

```dax
Regler feilet =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "FAILED"
)
```

---

### 1.6 Regler med feil

```dax
Regler i feil =
CALCULATE(
    COUNTROWS( dq_run_results ),
    dq_run_results[status] = "ERROR"
)
```

---

## 2. Feil/avviksmålinger

### 2.1 Totalt antall avviksrader

```dax
Totale avvik =
COUNTROWS( dq_violations )
```

---

### 2.2 Avvik etter alvorlighetsgrad

```dax
Kritiske avvik =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity] = "critical"
)

Alvorlige avvik =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity] = "high"
)

Middels avvik =
CALCULATE(
    COUNTROWS( dq_violations ),
    dq_violations[severity] = "medium"
)
```

---

### 2.3 Avviksrate (per 1 000 rader)

```dax
Avviksrate per 1000 =
DIVIDE(
    COUNTROWS( dq_violations ),
    SUMX( DISTINCT( dq_run_results[run_id] ), CALCULATE( SUM( dq_run_results[total_rows] ) ) )
) * 1000
```

---

## 3. Trend Measures

### 3.1 Quality Score — Latest Run

```dax
DQ-score % siste kjøring =
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
DQ-score % forrige kjøring =
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
Endring i DQ-score % =
[DQ-score % siste kjøring] - [DQ-score % forrige kjøring]
```

> Display this as a KPI card with a green / red conditional format.

---

## 4. Handler-Level Violation Measures (Case group)

### 4.1 Handlers with Violations

```dax
Saksbehandlere med avvik =
CALCULATE(
    DISTINCTCOUNT( dq_violations[saksbehandler_kode] ),
    dq_violations[rule_group] = "Case",
    NOT ISBLANK( dq_violations[saksbehandler_kode] )
)
```

---

### 4.2 Top Violating Handler

```dax
Saksbehandler med flest avvik =
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

## 5. Anbefalte rapportsider

### Side 1 — Oppsummering for ledelsen
- **KPI-kort**: `DQ-score % siste kjøring`, `Totale avvik`, `Regler feilet`,
  `Endring i DQ-score %`
- **Donutdiagram**: `DQ-score % per gruppe` etter `rule_group` (Case / Invoice)
- **Stablet stolpe**: Antall regler per status (Passed / Failed / Error) etter `rule_group`
- **Linjediagram**: `DQ-score % siste kjøring` over `batch_date` (trend)

### Side 2 — Detaljer for Case-regler
- **Slicer**: `batch_date`, `severity`, `rule_id`
- **Tabell**: `rule_id`, `rule_name`, `severity`, `total_rows`, `failed_rows`,
  `success_pct`, `status`, `details` filtrert til `rule_group = "Case"`
- **Stolpediagram**: `failed_rows` etter `rule_id`

### Side 3 — Detaljer for Invoice-regler
- Samme oppsett som Side 2, men filtrert til `rule_group = "Invoice"`

### Side 4 — Avviks-drilldown
- **Tabell**: `violation_detail`, `primary_key_value`, `rule_name`,
  `severity`, `saksbehandler_kode`, `batch_date`
- **Aktiver drill-through** fra Side 2 & 3 på `rule_id`

---

## 6. Mål for varsling (for fremtidig bruk)

Når varslingsfunksjonalitet legges til, koble følgende måling til et
varslingsflyt (f.eks. Power Automate):

```dax
Har kritiske avvik i dag =
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

> En verdi på `1` kan utløse et Power Automate-varsel til ansvarlig saksbehandler
> eller team-eier.

---

## IC-målinger — Intern kontrollovervåkning

Følgende målinger opererer på IC-tabellene skrevet av motoren og de
to Fabric-notebookene:

| Tabell | Beskrivelse |
|---|---|
| `ic_run_results` | Én rad per IC-regel per valideringskjøring |
| `ic_exceptions` | Én rad per IC-avvik, med en 4-state livssyklus |
| `ic_control_register` | Register over alle kontroller (fylt manuelt) |
| `ic_manual_attestations` | Attestasjonslogger skrevet av `nb_ic_02` |

### IC kontrollbeståelsesrate (%)

```dax
IC kontrollbeståelsesrate % =
DIVIDE(
    COUNTROWS( FILTER( ic_run_results, ic_run_results[status] = "PASSED" ) ),
    COUNTROWS( ic_run_results )
) * 100
```

### Åpne IC-avvik

```dax
Åpne IC-avvik =
COUNTROWS( FILTER( ic_exceptions, ic_exceptions[ic_status] = "Open" ) )
```

### IC Exceptions Breaching SLA

```dax
IC-avvik som bryter SLA =
COUNTROWS(
    FILTER(
        ic_exceptions,
        ic_exceptions[ic_status] = "Open"
            && ic_exceptions[remediation_due_date] < TODAY()
    )
)
```

> Et avvik bryter SLA når det har vært "Open" etter `remediation_due_date`.
> Avvik uten forfallsdato (hvor `remediation_due_days` var blank i regelen) er utelatt.

### Antall dager åpen (per avvik)

```dax
IC dager åpne =
DATEDIFF( ic_exceptions[first_seen_at], TODAY(), DAY )
```

> Legg til som en beregnet kolonne eller bruk i en tabellvisning med avviksrader.

### IC Exception Trend (weekly)

Use `ic_exceptions[first_seen_at]` on the axis and `COUNTROWS(ic_exceptions)` as
the value, with a weekly date hierarchy. This shows the volume of new Open
exceptions opened each week.

### Manuelle kontroller som er forfalt for attestasjon

`ic_manual_attestations` lagrer ikke lenger `next_due_date`. Forfallsstatus
beregnes fra `attested_at` kombinert med `attestation_frequency` fra
`ic_control_register`.

```dax
Manuelle kontroller forfalt =
COUNTROWS(
    FILTER(
        ic_control_register,
        ic_control_register[execution_type] = "Manual"
            && ic_control_register[active] = TRUE
            && VAR LastAttestation =
                CALCULATE(
                    MAX( ic_manual_attestations[attested_at] ),
                    RELATEDTABLE( ic_manual_attestations )
                )
               VAR FreqDays =
                SWITCH(
                    ic_control_register[attestation_frequency],
                    "Daily",     1,
                    "Weekly",    7,
                    "Monthly",   30,
                    "Quarterly", 91,
                    30
                )
            RETURN
                ISBLANK( LastAttestation )
                    || LastAttestation + FreqDays < NOW()
    )
)
```

> Krever en relasjon mellom `ic_control_register[control_id]` og
> `ic_manual_attestations[control_id]`. Kontroller uten noen attestasjoner
> inkluderes også (ISBLANK-vakt).

### Dager siden siste attestasjon (per kontroll)

```dax
Dager siden siste attestasjon =
DATEDIFF(
    CALCULATE(
        MAX( ic_manual_attestations[attested_at] ),
        RELATEDTABLE( ic_manual_attestations )
    ),
    TODAY(),
    DAY
)
```

> Brukes som en beregnet kolonne på `ic_control_register`, eller i en tabellvisning
> som viser hver manuelle kontroll med alder på siste attestasjon.

---

## Konfigurasjon for Translytical Task Flow

Fabric Translytical Task Flow erstatter Power Automate for begge IC-notebookene.
Task flow-en innebygger et interaktivt panel direkte i Power BI-rapporten
og kaller en Fabric-notebook med parametere bundet fra den valgte raden.

### Task Flow 1 — IC-avviksovergang (nb_ic_01_manage_exceptions)

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

### Task Flow 2 — Attestasjon av manuelle kontroller (nb_ic_02_attest_manual_control)

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

**After submission:** Refresh the report to show the updated `attested_at` and `conclusion`.

---

## Power Automate — IC Exception Email Notifications

When a new IC exception is inserted (first violation detected for a rule/record pair),
the engine posts to a Power Automate HTTP trigger URL to notify the rule owner by email.

### PA Flow Setup (three steps)

1. **Trigger:** HTTP request (instant cloud flow). Enable **POST** method. Copy the HTTP POST URL.
2. **Action:** Send an email (Office 365 Outlook). Configure:
   - To: `@{triggerBody()?['to']}`
   - Subject: `@{triggerBody()?['subject']}`
   - Body: `@{triggerBody()?['body']}`
3. Save and enable the flow.

### Store the URL in Lakehouse

Paste the HTTP POST URL (from step 1 above) into a plain text file at:

```
/lakehouse/default/Files/Configs/pa_notify_url.txt
```

The engine reads this file at runtime. The URL is not stored in source code.

### How It Works

The engine calls `_notify_new_ic_exceptions()` after writing to `ic_exceptions`. It iterates
over all new violation rows and POSTs one request per row where `owner_email` is set. If the
URL file does not exist, is empty, or the POST fails, the engine logs a warning and continues
— notification failure never blocks a run.

`owner_email` is set in `rule_catalog` (or in the YAML rule file before migration). Rules
without `owner_email` are silently skipped for notification.
