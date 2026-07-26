# Rules Guide — PBE Quality Catalog

Technical reference for authoring rule catalogs. Written for engineers who know
SQL; if you can write a `WHERE` clause you can write a rule.

Use [README.md](README.md) for runtime and deployment details, and
[ARCHITECTURE.md](ARCHITECTURE.md) for engine internals.

## A rule is a predicate

```yaml
- rule_id: FAS-008
  name: Tidsbruk kan ikke være negativt tall
  check: tidsbruk >= 0
```

`check:` holds a Spark SQL boolean predicate. Every row must satisfy it.

Add `when:` to scope a rule to some of the rows:

```yaml
- rule_id: FAS-006
  name: Åpen fase mangler saksbehandler
  when: seneste_stoppmilepael_dato IS NULL
  check: saksansvarlig_kode IS NOT NULL
```

**One rule asserts one thing.** `check:` takes a single predicate, not a list.
A requirement covering two columns is two rules, so a failing `rule_id` names
exactly one problem.

### NULL handling

`check:` behaves like a SQL `CHECK` constraint: a row violates it only when the
predicate is **FALSE**. If the predicate evaluates to NULL because an operand is
NULL, the row is *not evaluated* — it counts as neither passed nor failed.

```yaml
check: seneste_stoppmilepael_dato >= tidligste_startmilepael_dato
```

Rows where either date is NULL are skipped. To require presence, say so:

```yaml
check: tidligste_startmilepael_dato IS NOT NULL
```

`when:` works the same way: a row is in scope only where the condition is
explicitly true.

## Catalog header

One file per rule group. Everything above `rules:` is declared once and shared
by every rule in the file.

```yaml
rule_group: Faser                 # grouping dimension in Power BI
table: faser
database: saksbehandling
pk_column: stage_recno            # identifies a row in violations
where: fagsystem = 'PB360'        # row filter for the whole file

joins:
- table: saksbehandling.saker
  left_on: to_case_recno
  right_on: case_recno
  how: left                       # default: left
  select:
  - saksnummer
  - saksansvarlig_kode

rules:
- ...
```

This reads as the SQL it compiles to:
`FROM saksbehandling.faser WHERE fagsystem = 'PB360' LEFT JOIN saksbehandling.saker …`

| Key | Required | Meaning |
|---|---|---|
| `rule_group` | yes | Name of the group; stored on every result and violation row |
| `table` | yes | Source table |
| `database` | no | Schema for `table` |
| `pk_column` | yes | Column identifying a row, used as `primary_key_value` |
| `where` | no | SQL predicate narrowing the source for every rule in the file |
| `joins` | no | Pre-joins; `select` lists the columns to bring across |
| `rules` | yes | The rules |

`where:` composes with each rule's `when:` — both must hold for a row to be
evaluated.

### The three predicate words

| Word | Where | Means |
|---|---|---|
| `where:` | catalog header | which rows this whole file looks at |
| `when:` | one rule | which rows that rule applies to |
| `check:` | one rule | what must be true of them |

## Rule types

Most rules are `check:`. The rest cover things a row predicate cannot express —
uniqueness, cross-table lookups, and checks over groups of rows.

<!-- BEGIN RULE TYPES (generated — see tests/test_docs.py) -->

| Rule type | Scope | Required keys |
|---|---|---|
| `check` | row | – |
| `unique` | row | – |
| `exists_in` | row | `column`, `table`, `reference_column` |
| `sql` | row | – |
| `row_count` | table | `threshold` |
| `sequence_ordered` | group | `event_column`, `group_column`, `order_column`, `sequence` |
| `pairs_present` | group | `event_column`, `group_column`, `required_pairs` |
| `gate_complete` | group | `event_column`, `group_column`, `value` |
| `group_aggregate_matches` | group | `group_column`, `aggregate_column`, `reference_column` |

<!-- END RULE TYPES -->

`scope` fixes the unit everything is counted in — see [Counting](#counting).

### unique

```yaml
- rule_id: FAS-003
  name: stage_recno må være unik
  unique:
  - stage_recno
```

### exists_in

```yaml
- rule_id: X-001
  name: Saksbehandler må finnes i kodeverket
  exists_in:
    column: saksansvarlig_kode
    table: kodeverk.saksbehandlere
    reference_column: kode
    active_column: status        # optional; both keys or neither
    active_value: Aktiv
```

Only non-NULL values are checked. With `active_column`/`active_value` the
reference set is narrowed to currently-active rows. Reference tables are read
once per distinct `(table, column, active filter)` and cached across rules.

### pairs_present

```yaml
- rule_id: MIL-004
  name: Begge milepæler i et par må være tilstede
  pairs_present:
    event_column: milestone_title
    group_column: to_stage_recno
    required_pairs:
    - [Merknader oversendt, Mottatt revidert planforslag]
    - [Anmodning om oppdatert plandokumentasjon, Mottatt oppdatert plandokumentasjon]
    mode: both                   # or stop_requires_start
    completion_gate:             # only evaluate groups that reached this event
      event_column: milestone_title
      value: Sendt til politisk behandling
      order_column: milestonedate
```

A pair may name several acceptable stops: `[start, [stop_a, stop_b]]` is
satisfied when either stop is present. `mode: both` flags a group missing
either member; `stop_requires_start` flags only a stop without its start.

`completion_gate` restricts evaluation to groups that have reached a given
event — used to avoid flagging work that is legitimately still in progress.
Also available on `sequence_ordered`.

### group_aggregate_matches

```yaml
- rule_id: FAK-003
  name: Fakturalinjer må summere til totalen
  group_aggregate_matches:
    group_column: fakturanr
    aggregate_column: linje_belop
    reference_column: fakturasum
    aggregate: sum               # sum | count | avg | min | max
    tolerance: 0.01
```

### sequence_ordered / gate_complete

```yaml
- rule_id: X-002
  name: Milepæler må komme i rekkefølge
  sequence_ordered:
    event_column: milestone_title
    group_column: to_stage_recno
    order_column: milestonedate
    sequence: [Mottatt, Under behandling, Vedtak]

- rule_id: X-003
  name: Alle faser må ha godkjenning
  gate_complete:
    event_column: milestone_title
    group_column: to_stage_recno
    value: Godkjent
    order_column: milestonedate  # optional; also require a non-NULL date
```

### row_count / sql

```yaml
- rule_id: X-004
  name: Tabellen må ha rader
  row_count:
    operator: '>='               # default '>='
    threshold: 1000

- rule_id: X-005
  name: Egendefinert sjekk
  sql:
    query: SELECT id FROM saksbehandling.faser WHERE ...
    pk_column: id                # optional; otherwise a row index is used
```

Prefer `check:` over `sql:`. A predicate is validated against the schema before
the run and cannot modify anything; `sql:` runs an arbitrary statement.

## Optional rule keys

| Key | Meaning |
|---|---|
| `rule_id` | Required. Unique within the catalog; stored on every result and violation |
| `name` | Required. Human-readable; stored as `rule_name` |
| `description` | Optional. For whoever reads the YAML — not stored anywhere |
| `when` | Optional. Narrows which rows the rule applies to |
| `pk_column` | Optional. Overrides the catalog `pk_column` for this rule |

## Counting

Each rule type declares a scope, and that fixes the unit for `total_rows`,
`passed_rows` and `failed_rows`:

| Scope | One unit is | Used by |
|---|---|---|
| `row` | one row | `check`, `unique`, `exists_in`, `sql` |
| `group` | one group | `pairs_present`, `sequence_ordered`, `gate_complete`, `group_aggregate_matches` |
| `table` | the whole table | `row_count` |

For a group-scoped rule, a group failing several pairs counts **once**, while the
violation log still lists each failing pair. `passed_rows` is always
`total_rows - failed_rows` in the same unit.

For `check:`, `total_rows` counts rows where the predicate could be evaluated —
so rows skipped for NULL operands are outside both numerator and denominator.

A row whose group key is NULL is not a group: it is excluded from the denominator
and never reported, since a violation keyed on NULL could not be traced back to
anything.

### `when:` on a group-scoped rule

`when:` filters **rows, before they are grouped**. On a row-scoped rule that is all it
can do. On a group-scoped rule it also changes *which events each group still
contains*, which is rarely what you want:

```yaml
# Wrong: hides 'Mottatt revidert planforslag' from every group, so each one now
# looks like it is missing the second half of the pair.
when: milestone_title != 'Mottatt revidert planforslag'
pairs_present: ...
```

To restrict *which groups* are evaluated rather than which rows they contain, use
`completion_gate:` — it keeps whole groups and drops whole groups.

## Violation output

One row in `dq_violations` per failing unit.

| Column | Contents |
|---|---|
| `primary_key_value` | Row key, or group key for group-scoped rules |
| `violation_scope` | `row` or `group` — how to read `primary_key_value` |
| `violated_column` | The column at fault. Always a real column name, or NULL for `sql` |
| `actual_value` | The offending value |
| `expected_condition` | The full predicate or condition that was required |
| `violation_detail` | Human-readable explanation |
| `issue_status` | `Active` while the violation persists, then `Resolved` |
| `first_seen_at` | When first detected; preserved across runs, so age is answerable |
| `resolution_timestamp` | When it stopped appearing |

For a `check:` rule, `violated_column` is the first column referenced by the
predicate — the natural subject (`a` in `a >= b`). `expected_condition` always
carries the whole predicate, so nothing is lost.

Violations are keyed on `(rule_id, primary_key_value, violated_column,
expected_condition)`. A violation that disappears from a run is marked
`Resolved` rather than deleted.

## Preflight

`nb_dq_01_preflight.py` checks catalogs before anything runs:

- every source table exists;
- every `where:`, `when:` and `check:` predicate resolves against the real
  schema — a typo'd column or bad syntax fails here, not at 03:00;
- each rule declares exactly one rule type, with its required keys;
- column-valued keys name real source columns (including joined-in ones);
- `rule_id` values are unique and `pk_column` exists.

Run it after every catalog change.

## Fabric notebook workflow

1. Update the YAML catalogs in `rules/` and deploy them to the Lakehouse
   (`/lakehouse/default/Files/rules/`). Rules are loaded from YAML at runtime;
   there is no Delta-based rule store.
2. Run [nb_dq_01_preflight.py](nb_dq_01_preflight.py).
3. Run one validation cycle with `DRY_RUN = True`.
4. Review the `_tmp` output tables.
5. Switch to `DRY_RUN = False` once the output looks right.

## Why not just write SQL

The predicate is the same either way. What differs is everything around it:

1. **Boilerplate is declared once.** The rules in `faser.yaml` share one join
   and one filter. In SQL each check repeats both.
2. **State, not a result set.** A query answers "what is wrong now". This engine
   tracks each violation as `Active` until it disappears, then marks it
   `Resolved`, preserving `first_seen_at` — so "open for 40 days" is answerable.
   Reproducing that means a MERGE, a key strategy and lifecycle columns per check.
3. **Validation before the run.** Predicates are checked against the real schema
   at deploy time rather than failing in a scheduled job.
