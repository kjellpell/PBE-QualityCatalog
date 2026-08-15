# Norwegian rename glossary

Authoritative old→new mapping for the English→Norwegian rename of the three
output tables this repo owns. `lower_snake_case`, `ø→oe`, `å→aa`, `æ→ae` for all
**identifiers** (table/column names — must stay ASCII-safe for SQL/Delta). Data
**values** (strings stored in columns, e.g. status/enum labels) keep proper Norwegian
orthography with real diacritics (`Bestått`, not `Bestaatt`) since they are just
text, not identifiers — this file marks values explicitly wherever they differ
from identifier rules.

Same conventions as the sibling repo PBE-IncomeForecast's `RENAME_GLOSSARY.md`,
which this rename effort mirrors.

Source schema `saksbehandling` (and every table read from it —
`fakturalinjer`, `faser`, `milepaeler`, `saker`, plus their columns) is already
Norwegian and external — **out of scope, unchanged**. The `datakvalitet` output
schema is already Norwegian — unchanged (renamed in an earlier pass, see commit
`3d5ed32`). Python constant identifiers (`DQ_RESULTS_TABLE`,
`DQ_VIOLATIONS_TABLE`, `DQ_EXECUTION_METRICS_TABLE`, `RUN_ID`, `RUN_TIMESTAMP`,
`BATCH_DATE`, `TARGETS` dict keys) are unchanged — only the string values they
hold and the persisted output-table column names change.

**Rule-type vocabulary stays English.** The `expectation`/`forventning` column's
values (`check`, `unique`, `row_count`, `event_flow`, `required_event`,
`aggregate_matches`) are the literal YAML keys rule authors type in
`notebooks/QC_Rules.py` — code/API-facing, not human-facing labels, so they're
left as-is, matching how Python constant identifiers stay English in the sibling
repo. Translating them would mean renaming the rule-authoring DSL itself
(`QC_Rules.py`, `RULES_GUIDE.md`, the `RULE_TYPES` contract) — out of scope here.

**No schema-generated migration step exists** (by design — see
`notebooks/QC_Setup_Tables.py`'s `_ensure_table`: a pre-existing table with a
different shape is reported, not migrated in place). Any already-deployed
`datakvalitet.dq_run_results` / `dq_violations` / `dq_execution_metrics` tables
must be dropped and recreated via `QC_Setup_Tables` after this rename lands —
see `DEPLOY.md`.

---

## Table names

| Old | New |
|---|---|
| `dq_run_results` | `kjoeringsresultater` |
| `dq_violations` | `avvik` |
| `dq_execution_metrics` | `kjoeringslogg` |

No `dq_` prefix on the Norwegian names (confirmed by user) — the `datakvalitet`
schema already namespaces them.

---

## Column mappings per table

### `kjoeringsresultater` (was `dq_run_results`) — CONFIRMED

| Old | New |
|---|---|
| `run_id` | `kjoert_id` |
| `run_timestamp` | `kjoert_tidspunkt` |
| `batch_date` | `kjoert_dato` |
| `rule_group` | `regelgruppe` |
| `rule_id` | `regel_id` |
| `rule_name` | `regelnavn` |
| `table_name` | `tabellnavn` |
| `expectation` | `forventning` |
| `total_rows` | `totalt_antall_rader` |
| `passed_rows` | `bestaatte_rader` |
| `failed_rows` | `ikke_bestatte_rader` |
| `success_pct` | `suksessprosent` |
| `status` | `status` |
| `details` | `detaljer` |
| `rule_duration_seconds` | `regelvarighet_sekunder` |
| `error_category` | `feilkategori` |

`run_timestamp`/`batch_date` — `kjoert_tidspunkt` is the precise UTC moment the run
started; `kjoert_dato` is the date-only companion (`date.today()` at run start),
used as the coarse reporting-day grain. Both derived from "now" at run start but at
different precision — kept as two distinct names, not collapsed into one.

Values:
- `status`: `PASSED→Bestått`, `FAILED→Ikke bestått`, `ERROR→Feil`. `FAILED`
  (rule ran, found violations) and `ERROR` (rule itself couldn't run — bad
  config, timeout, etc.) are deliberately kept far apart in Norwegian
  (`Ikke bestått` vs. `Feil`) rather than using near-homophones like
  `Feilet`/`Feil`.
- `error_category` (populated only when `status = 'Feil'`):
  `infrastructure→Infrastruktur`, `configuration→Konfigurasjon`,
  `source_data→Kildedata`.
- `expectation`/`forventning`: values stay English — see the note at the top of
  this file.
- `rule_group`: already Norwegian (`Faktura`, `Faser`, `Milepæler`) — unchanged.
- `table_name`: already Norwegian (`fakturalinjer`, `faser`, `milepaeler`) —
  unchanged.

### `avvik` (was `dq_violations`) — CONFIRMED

| Old | New |
|---|---|
| `run_id` | `kjoert_id` |
| `run_timestamp` | `kjoert_tidspunkt` |
| `batch_date` | `kjoert_dato` |
| `rule_group` | `regelgruppe` |
| `rule_id` | `regel_id` |
| `rule_name` | `regelnavn` |
| `table_name` | `tabellnavn` |
| `primary_key_value` | `primaernoekkel_verdi` |
| `identifier_value` | `identifikator_verdi` |
| `violated_column` | `avvikende_kolonne` |
| `actual_value` | `faktisk_verdi` |
| `expected_condition` | `forventet_betingelse` |
| `violation_detail` | `avviksdetaljer` |
| `issue_status` | `avviksstatus` |
| `resolution_timestamp` | `loest_tidspunkt` |
| `first_seen_at` | `foerst_observert_tidspunkt` |
| `violation_scope` | `avviksomfang` |

`identifier_value`/`identifikator_verdi` — human-meaningful identifier for the row
(e.g. `saksnummer`), when the catalog sets `identifier_column`. `NULL` when it
doesn't — enrichment only, never a key.

`resolution_timestamp`/`loest_tidspunkt` stays a STRING column (ISO-8601 text),
not TIMESTAMP — unchanged design, so environments without full Delta/Spark type
coercion can still read it.

Values:
- `issue_status`/`avviksstatus`: `Active→Aktiv`, `Resolved→Løst`.
- `violation_scope`/`avviksomfang`: `row→Rad`, `group→Gruppe`, `table→Tabell`.
- `actual_value`/`faktisk_verdi`: the sentinel literal `"NULL"` (written when the
  underlying value is SQL NULL) stays **as-is, unchanged** — a technical marker
  read by code/dashboards, not human-facing content.
- `avviksdetaljer` (was `violation_detail`) — generated sentences, translated as
  proper Norwegian sentences (capitalized first letter), from
  `_comparison_phrase()` in `QC_Engine.py:615-623`:

  | Old (English) | New (Norwegian) |
  |---|---|
  | is less than the required value | Er mindre enn kravverdien |
  | is less than or equal to the required value | Er mindre enn eller lik kravverdien |
  | is greater than the required value | Er større enn kravverdien |
  | is greater than or equal to the required value | Er større enn eller lik kravverdien |
  | does not match the required value | Samsvarer ikke med kravverdien |
  | matches the required value | Samsvarer med kravverdien |
  | does not satisfy the required value (fallback) | Oppfyller ikke kravet |

### `kjoeringslogg` (was `dq_execution_metrics`) — CONFIRMED

| Old | New |
|---|---|
| `script_name` | `skriptnavn` |
| `status` | `status` |
| `output_target` | `utdestinasjon` |
| `artifact_target` | `maalartifakt` |
| `row_count` | `antall_rader` |
| `started_at_utc` | `starttidspunkt_utc` |
| `finished_at_utc` | `sluttidspunkt_utc` |
| `duration_seconds` | `varighet_sekunder` |
| `is_retryable` | `kan_proeves_igjen` |
| `error_message` | `feilmelding` |

`output_target`/`utdestinasjon` holds the results-table full name;
`artifact_target`/`maalartifakt` holds the violations-table full name.

Values:
- `status`: `Succeeded→Vellykket`, `Failed→Mislykket` — exactly two values,
  matching the code (`run_with_metrics()`, `QC_Engine.py`). This is a **distinct
  enum** from `kjoeringsresultater.status` (run-level vs. rule-level) — do not
  conflate the two, and do not add a third `Feil` value here, since nothing in
  the code writes one.

---

## Doc-drift bugs found during exploration (fix while touching these files)

`DAX_POWERBI.md` had two stale references that never matched the real (even
pre-rename) column names — fix alongside the rename:
- `run_ts` → should be `run_timestamp` → becomes `kjoert_tidspunkt`.
- `resolved_at` → should be `resolution_timestamp` → becomes `loest_tidspunkt`.

---

## Files touched by this rename

- `notebooks/QC_Engine.py` — `RESULT_SCHEMA`, `VIOLATION_SCHEMA`,
  `_EXECUTION_METRIC_SCHEMA`, and every `F.col(...)`/`F.lit(...).alias(...)`/dict
  key referencing the old names. No internal-English/final-write split exists
  here (unlike some PBE-IncomeForecast notebooks) — the schemas are used
  end-to-end, so this is a full sweep of the file.
- `notebooks/QC_Config.py` — table-name string values (`DQ_RESULTS_TABLE` etc.
  constant names unchanged).
- `notebooks/QC_Run_Validation.py`, `notebooks/QC_Preflight.py` — direct column
  references.
- `notebooks/QC_Setup_Tables.py` — comments only (DDL is generated from the
  engine schemas, no manual column list to edit).
- `tests/` — `tests/fixtures.py`, every test module asserting on column/table
  names, and `tests/baseline_equivalence.json` (regenerate with
  `DQ_UPDATE_BASELINE=1 python -m pytest tests/test_equivalence.py`).
- `DAX_POWERBI.md`, `README.md`, `ARCHITECTURE.md`, `DEPLOY.md`,
  `OPERATIONS_QUICK_REF.md`, `RULES_GUIDE.md` — every place these three
  tables/columns are named.
