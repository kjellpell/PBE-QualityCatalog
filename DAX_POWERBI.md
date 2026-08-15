# Power BI - DQ Catalog (Core Engine)

This guide contains DAX examples for the core engine outputs only.

## Source Tables

| Table | Description |
|---|---|
| `kjoeringsresultater` | One row per rule per validation run |
| `avvik` | One row per violation key with lifecycle state (`Aktiv` / `Løst`) |

## Core Measures

### DQ Score %

```dax
DQ Score % =
DIVIDE(
    CALCULATE(
        COUNTROWS( kjoeringsresultater ),
        kjoeringsresultater[status] = "Bestått"
    ),
    COUNTROWS( kjoeringsresultater )
) * 100
```

### Total Rules

```dax
Total Rules =
COUNTROWS( kjoeringsresultater )
```

### Rules Passed

```dax
Rules Passed =
CALCULATE(
    COUNTROWS( kjoeringsresultater ),
    kjoeringsresultater[status] = "Bestått"
)
```

### Rules Failed

```dax
Rules Failed =
CALCULATE(
    COUNTROWS( kjoeringsresultater ),
    kjoeringsresultater[status] = "Ikke bestått"
)
```

### Rules In Error

```dax
Rules In Error =
CALCULATE(
    COUNTROWS( kjoeringsresultater ),
    kjoeringsresultater[status] = "Feil"
)
```

### Active Violations

```dax
Active Violations =
CALCULATE(
    COUNTROWS( avvik ),
    avvik[avviksstatus] = "Aktiv"
)
```

### Resolved Violations

```dax
Resolved Violations =
CALCULATE(
    COUNTROWS( avvik ),
    avvik[avviksstatus] = "Løst"
)
```

### Latest Run Timestamp

```dax
Latest Run Timestamp =
MAX( kjoeringsresultater[kjoert_tidspunkt] )
```

## Suggested Report Pages

1. Run Overview:
   KPIs for DQ Score %, Total Rules, Rules Failed, Rules In Error.
2. Rule Group Health:
   Bar/column chart by `regelgruppe` and `status`.
3. Active Violation Backlog:
   Table filtered to `avvik[avviksstatus] = "Aktiv"`.
   `avvikende_kolonne` is always a real column name in `tabellnavn` (or `NULL`
   when a `check:` predicate names no column, e.g. `1 = 0`) — safe to
   group/count by. `avviksomfang` tells you how to read
   `primaernoekkel_verdi`: `"Rad"` means it's the PK of the offending row in
   `tabellnavn`; `"Gruppe"` (used by `event_flow`, `required_event`,
   `aggregate_matches`) means it's a group key, not a
   row PK — don't join it back to `tabellnavn` as if it were one.
   `identifikator_verdi` carries a human-meaningful identifier (saksnummer, for
   every catalog shipped today) alongside the technical `primaernoekkel_verdi` —
   add it to this table so a violation is searchable/filterable by case
   number without a manual lookup. It's `NULL` for a catalog that hasn't set
   `identifier_column`, or when the underlying join found no match.
4. Resolution Trend:
   Time series of `Aktiv` vs `Løst` by `foerst_observert_tidspunkt` / `loest_tidspunkt`.

## Notes

- This core baseline does not use enriched violation output tables.
- This core baseline does not use owner/routing/escalation fields.
