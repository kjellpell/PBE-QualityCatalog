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
uniqueness, and checks over groups of rows. A check needing a second table is a
`check:` over a `joins:` column, not a rule type of its own.

<!-- BEGIN RULE TYPES (generated — see tests/test_docs.py) -->

| Rule type | Scope | Required keys |
|---|---|---|
| `check` | row | – |
| `unique` | row | – |
| `row_count` | table | `minimum`, `maximum` |
| `event_flow` | group | `event_column`, `group_column`, `order_column`, `cycle` |
| `required_event` | group | `event_column`, `group_column`, `value` |
| `aggregate_matches` | group | `group_column`, `aggregate_column`, `reference_column` |

<!-- END RULE TYPES -->

`scope` fixes the unit everything is counted in — see [Counting](#counting).

### unique

```yaml
- rule_id: FAS-003
  name: stage_recno må være unik
  unique:
  - stage_recno
```

### row_count

```yaml
- rule_id: FAS-001
  name: Faser må ha forventet volum
  row_count:
    minimum: 1
    maximum: 500000
```

Use this for bounded table volume checks in silver-layer blocker workflows.
The scoped table after catalog `where:` and rule `when:` must have a row count
between `minimum` and `maximum` (inclusive).

### event_flow

Declared events must occur **in order**, as **whole passes**. Events not named
anywhere are ignored, so unrelated activity in between is fine.

```yaml
- rule_id: MIL-004
  name: Merknader og revidert planforslag må komme parvis og i rekkefølge
  event_flow:
    event_column: milestone_title
    group_column: to_stage_recno
    order_column: milestonedate
    starts_with: Sendt til politisk behandling   # optional, single value, once
    cycle:                                       # required, repeats as whole passes
    - Merknader oversendt
    - Mottatt revidert planforslag
    ends_with: [Vedtak fattet, Sak trukket]      # optional, any one closes
    completion_gate:                             # only evaluate groups that got here
      event_column: milestone_title
      value: Sendt til politisk behandling       # scalar or list; any match
      order_column: milestonedate
```

With `starts_with: start`, `cycle: [A, B]`, `ends_with: end`:

| Events, by date | Verdict | Why |
|---|---|---|
| `start A B end` | valid | one complete pass |
| `start A B A B end` | valid | two complete passes |
| `start B A end` | error | cycle out of order |
| `B start A end` | error | cycle event before the start anchor |
| `start A B A end` | error | the trailing `A` never closes |
| `start A A B B end` | error | passes must alternate, not batch |

The last two are the point: an opened pass that never closes is a real data
problem, and a plain "do both exist?" check cannot see it.

**Anchors are optional.** A bare `cycle:` of two events is a pair check — every
`A` must be closed by a `B`. A group containing none of the listed events is
valid (zero passes). `starts_with` takes a single value and may occur once;
`ends_with` may list several, any one of which closes the flow, and only one may
occur.

`completion_gate` restricts evaluation to groups that have reached a given event,
so work still legitimately in progress is not flagged. Its `value:` accepts a
scalar or a list.

A handler does not always remember to set the gate milestone, so a group can
never fire it and still run its whole cycle to completion. If `ends_with` is
also declared, a group that reaches it is evaluated too, even without the
gate — `ends_with` already means "the event(s) that close this flow", so this
is the same concept read twice, not a second one. The gate stays the earlier,
preferred trigger; `ends_with` is the safety net that catches a case at the
point it actually closed, rather than skipping it forever because the gate
milestone was never set. A `completion_gate` with no `ends_with` declared
keeps the strict behaviour: only gated-in groups are evaluated.

Events on the **same date** are read in declared order, so a group whose
milestones share a timestamp gives the same verdict every run.

### required_event

Every group must contain at least one row carrying the named event.

```yaml
- rule_id: X-003
  name: Alle faser må ha godkjenning
  required_event:
    event_column: milestone_title
    group_column: to_stage_recno
    value: Godkjent              # scalar or list; any one of them satisfies
    order_column: milestonedate  # optional; also require a non-NULL date
```

Groups with no matching row are violations, keyed on `group_column`.

`value:` takes a list wherever several events close the same requirement, so a
group holding **any one** of them passes:

```yaml
    value: [Vedtak fattet, Sak trukket]
```

`order_column:` does more than order here — it tightens the assertion to "the
event exists *and* that row carries a date". Leave it out if a dateless event
still counts as reached.

A row whose `group_column` is NULL is not a group: it is neither counted nor
reported, so the denominator is groups with a real key.

Note how this differs from `event_flow`'s `completion_gate:`, which names the same
kind of thing for the opposite purpose: `required_event:` **asserts** the event is
there and fails the group when it is not, while `completion_gate:` **scopes** which
groups `event_flow` evaluates and silently drops the ones that have not got there.

### aggregate_matches

```yaml
- rule_id: FAK-003
  name: Fakturalinjer må summere til totalen
  aggregate_matches:
    group_column: fakturanr
    aggregate_column: linje_belop
    reference_column: fakturasum
    aggregate: sum               # sum | count | avg | min | max
    tolerance: 0.01
```

### Reaching another table

Every rule type reads only the catalog's own source, so a check that needs a
second table is expressed with `joins:` in the catalog header — the joined
columns are then available to any `check:` predicate:

```yaml
joins:
- table: kodeverk.saksbehandlere
  left_on: saksansvarlig_kode
  right_on: kode
  select: [kode]

rules:
- rule_id: X-006
  name: Saksbehandler må finnes i kodeverket
  when: saksansvarlig_kode IS NOT NULL
  check: kode IS NOT NULL          # NULL means the left join found no match
```

There is deliberately no rule type that executes raw SQL. Every rule is a
predicate or a declarative block, so a rule cannot modify data — see
[ARCHITECTURE.md](ARCHITECTURE.md).

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
| `row` | one row | `check`, `unique` |
| `table` | one table | `row_count` |
| `group` | one group | `event_flow`, `required_event`, `aggregate_matches` |

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
event_flow: ...
```

To restrict *which groups* are evaluated rather than which rows they contain, use
`completion_gate:` — it keeps whole groups and drops whole groups.

`completion_gate:` is accepted by `event_flow` only. On `required_event` and
`aggregate_matches` the only lever is `when:`, which is safe here **when the
predicate is constant within a group** — a phase-level attribute like `indikator`
selects whole groups, while a row-level one like `milestone_title` cuts groups in
half and then judges what is left.

## Violation output

One row in `dq_violations` per failing unit.

| Column | Contents |
|---|---|
| `primary_key_value` | Row key, or group key for group-scoped rules |
| `violation_scope` | `row`, `group` or `table` — how to read `primary_key_value` |
| `violated_column` | The column at fault. A real column name, or NULL if the predicate names none |
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

`scripts/preflight_checks.py` checks catalogs before anything runs:

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
2. Run [scripts/preflight_checks.py](scripts/preflight_checks.py).
3. Run one validation cycle in the target environment.
4. Review the output tables.

