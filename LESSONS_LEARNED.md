# Lessons Learned: Building a Data Quality Rule Engine

**To:** IT / the team building the production validation engine
**From:** The PBE Quality Catalog proof-of-concept
**Status:** Not a spec. Not a codebase to adopt. You will write your own.

We spent roughly twelve weeks (14 May – 4 Aug 2026), 84 commits and 19 merged
pull requests building a working data quality engine, and then spent a large
share of that time *removing* what we had built. This document is the residue:
the decisions worth making deliberately, the mistakes worth skipping, and an
honest note about which of these we actually learned versus which we only
reasoned about.

Take what is useful. Ignore the rest. Build it your own way.

---

## 0. The headline: we built twice as much as we shipped

This is the lesson that cost the most and is the easiest one for you to skip
paying for.

| | At peak | Final |
|---|---|---|
| Core rule logic | 2,021 lines, 15 "expectation" classes | 849 lines, 6 rule types |
| Notebooks / entry points | 5 | 3 |
| Output tables | 6 (incl. notification log, per-catalog violation tables) | 3 |
| Notification stack | Graph API → Power Automate → Teams DM, 2 flow packages | none |
| Routing / ownership layer | 316-line notebook, owner fields, escalation days | none |
| Severity field | in 4 table schemas, all YAML rules, all docs | none |

Every one of those removals was correct. Every one of them was also *work we
paid for twice* — once to build, once to remove, plus the documentation and
tests that had to be rewritten each way.

Final shape, for calibration:

```
engine/     1,970 lines   the actual engine
tests/      1,661 lines   ~= the size of the engine
docs/       1,204 lines
scripts/      729 lines   setup, preflight, entry point
rules/        198 lines   19 real rules across 3 catalogs
config/        68 lines
```

Note the ratio: **19 rules of actual business value** sit on top of ~4,500
lines of machinery. That ratio is normal and fine — but it means the machinery
is the product, and every feature you add to it is permanent. The specific
things we would tell our past selves not to build until someone asks twice:

- **Notifications.** We built email, then Graph API, then Power Automate Teams
  DMs, then deleted all of it. Power BI over the violations table answered the
  same need with none of the delivery-infrastructure surface. Build the table
  first. See whether anyone actually wants a message pushed at them before you
  build a push channel.
- **Ownership and routing inside the engine.** See §10 below — this one is a
  direct contradiction of the advice we nearly gave you.
- **A `severity` field.** Removed with the commit note "severity was
  metadata-only and never drove any engine logic." It was in four table
  schemas, every YAML rule, the Power Automate payloads, and all the docs. It
  never once changed what the engine did. If a field does not change behaviour,
  it belongs in the reporting layer, not the engine's schema.
- **A dry-run mode and temp-table workflow.** Removed. The preflight check
  (§6) covered the real need — "will this break?" — without a second write path
  that had to be kept correct.

**Corollary:** every rule type you add is a permanent tax — docs, tests,
preflight support, a name that will need renaming. Ours went 15 → 9 → 6, and 6
is enough for 19 real rules. Start smaller than feels right.

---

## 1. NULL is not a failure

Decide early how a check treats a predicate that cannot be evaluated because
an input is NULL. The tempting default — "NULL fails" — is wrong: every
optional or not-yet-filled field generates a false violation the moment it is
empty.

The right default: a predicate evaluating to NULL is *unevaluated*, not failed.
It counts in neither the pass total nor the fail total. If you want to require
presence, say so explicitly (`X IS NOT NULL`).

**What made this cheap for us:** we did not implement it. SQL's `CHECK`
constraint already has exactly these semantics, and Spark's `~F.expr(...)`
inherits them. Adopting an existing semantic that your rule authors already
know beats inventing one and documenting it. If your rule authors are
SQL-fluent — ours are — leaning on SQL semantics everywhere is the single
highest-leverage decision in the whole design.

One asymmetry worth deciding consciously: we treat the *scoping* clause
(`when:`) differently from the *check* clause. A row is in scope only where
the scoping condition is explicitly TRUE — an unevaluable scope excludes the
row. Same NULL, opposite direction, and both are right.

## 2. A violation needs a subject, not just a verdict

Never build a check that returns only pass/fail. For every failure capture:
which record, which column, the actual value, the expected condition, and a
plain-language explanation. A check that says only "FAILED" forces someone to
re-derive what went wrong — work you already did at check time.

If you derive "which column is this check about" from the predicate text, be
careful which one you pick. The *first* column referenced is the natural
subject (`a` in `a >= b`).

**The trap we hit:** we first read the column list from Spark's analyzer, which
returns an *unordered* reference set — so `b >= a` reported `a` as the subject.
The fix was to walk the *unresolved* expression tree instead, which preserves
source order and needs no schema. Make the derivation best-effort: if the walk
fails, emit a NULL subject rather than failing the rule. A cosmetic feature
should never be able to break a check.

## 3. Count the right unit

Decide, per check, what "one thing" means before counting anything. A row check
counts rows. A check over a group of related rows (e.g. "did this case follow
the right sequence of steps") counts *groups* — if a group has five things
wrong, that is one failed group, not five.

**Make it a declared property, not a per-check decision.** Each of our rule
types declares `scope: row | group | table`, and one shared driver — not the
individual check implementations — does all counting in that unit. The
invariant this buys you is worth stating in your tests: `passed = total −
failed` can never go negative. Without it, a group failing three separate ways
against a denominator of groups produces a negative pass count, and nobody
notices until a dashboard shows −2.

## 4. Duplicate checks should return the whole colliding set

Do not just flag "duplicate found" — return every row involved in the
collision. Whoever fixes it needs to see all of them side by side, not go
hunting for which other rows share the key.

## 5. Never let a check modify data — make it structural, not a convention

If your check format has any path that executes arbitrary code or SQL — a "run
this query" escape hatch — remove it, even if nothing currently abuses it.

We had one: a `sql:` rule type. Nothing used it. We removed it anyway, and the
commit rationale is the point: `spark.sql()` ran whatever string it was given,
and output-table routing did not protect against it, so a mutating query would
have run against the *source* environment. The danger was never intent. It was
that "this format is read-only" was a property nobody could verify.

Now every rule type is a predicate or a declarative block, and every user-supplied
string goes through an expression builder that *cannot* carry DDL or DML.
Read-only is a property of what is possible to write, not a rule people
remember. If a check needs a second table, give it a declarative join in the
catalog header — never a raw execution path.

## 6. Validate configuration before it runs, not while it runs

Whatever declares a check should be validated against the real schema and its
own contract *before* a scheduled run touches it. Catching a typo at review
time costs two minutes. Catching it at 03:00 costs someone their morning and a
day of missing quality signal.

Four things we learned building this the hard way:

- **Validate predicates with the engine's own analyzer, not a hand-maintained
  list of "which parameters hold column names."** We maintained such a list,
  and it drifted from the engine. Resolving the predicate against the real
  schema catches every typo with no list to maintain.
- **Use a boolean-requiring operation to do it.** We validated with the
  equivalent of `selectExpr`, which accepts *any* expression — so `check:
  saksnummer` (a bare column, not a predicate) passed preflight and then failed
  at run time with `FILTER_NOT_BOOLEAN`. Exactly the 03:00 failure preflight
  exists to prevent. Validate with `filter`, which requires a boolean.
- **Build the validation probe by replaying the real joins on zero-row
  frames.** It costs nothing and carries the joined columns with their *real
  types*. A probe that synthesises joined columns as strings lets a
  type-sensitive predicate like `joined_date >= '2024-01-01'` pass preflight and
  fail in production. Replaying also validates the join configuration itself,
  which nothing else does.
- **Reject unknown keys, at every level.** A header typo is the most expensive
  kind: `wehre:` instead of `where:` silently drops the row filter for every
  rule in the file, and the run looks fine. Unknown-key rejection at both the
  catalog and rule level is three lines and catches a whole class of silent
  wrongness.

**And declare the contract exactly once.** Ours lives in a single registry
structure — each rule type's required keys, which of them name source columns,
what scope it counts in. Preflight *reads that structure* rather than restating
it. We had the two as parallel lists first, and they drifted within weeks.

*Live proof this matters:* our own `faser.yaml` currently contains `FAS-010`
**twice** — a human wrote it, a human reviewed it, and both missed it, twice.
Preflight's duplicate-`rule_id` check catches it in a second. That is the whole
argument for this section in one example.

## 7. Distinguish "the check is broken" from "the data is bad"

These are not the same failure and must never share a status. A check that
cannot run (missing column, missing table, timeout) is a configuration or
infrastructure problem. A check that ran and found something wrong is a data
problem. Collapsing them hides infrastructure failures inside your
quality signal, and your trend line lies to you exactly when your own pipeline
broke.

We use three statuses — `PASSED`, `FAILED`, `ERROR` — and go one step further:
a separate `error_category` column classifies each ERROR as
`infrastructure`, `configuration`, or `source_data`. It costs almost nothing
and means "our engine is unhealthy" and "someone's YAML is wrong" are different
queries, routed to different people.

**Two silent-success traps in the same family, both of which we hit:**

- **An empty source table reports 100% passing.** Every rule validates rows
  that exist; zero rows means zero failures. Guard the *run*, not the data:
  abort before an empty source can be reported as perfect quality.
- **A rule file that fails to load is a silent loss of coverage, and the
  quality score can *rise*** — because the rules that vanished included the
  failing ones. We originally logged a warning and continued. Nobody reads
  stdout at 03:00. Now an unloadable catalog fails the entire run. Any
  degradation that makes your numbers look *better* must be loud.

## 8. Separate "what happened this run" from "what's currently wrong"

Keep two outputs, not one: an append-only log of every check's result on every
run (for trend and reliability reporting), and a *current-state* list of what
is still wrong right now (for someone to fix). Different audiences, different
questions. Answering both from one table means computing "what's open" as an
expensive scan over all history every time someone wants a work queue.

We keep a third — a run-level execution log (start, end, duration, retryable
or not, error message). It is the cheapest table in the system and the first
one you look at when someone asks "did it even run last night?"

## 9. First-seen tracking and resolution state are the foundation — get this exactly right

This deserves its own section, because it is easy to build the current-state
list, feel finished, and not notice you have built something with no memory.
Without this, almost nothing useful is possible later: no age-based triage, no
"open too long" escalation, no resolution trend, no way to answer "is this
getting better or worse."

**The core idea:** every violation carries a `first_seen_at` timestamp, set
once on first detection and never touched again while that violation keeps
recurring. Every later run refreshes the row (latest run ID, latest detail) but
explicitly *preserves* the original `first_seen_at`.

This sounds obvious written down and is still the easiest thing in the whole
system to get wrong — because the naive implementation (recompute violations
fresh, write them out) silently overwrites `first_seen_at` with "now" on every
run. The moment that happens, every violation looks like it started today,
forever. No error, no test failure, nothing to signal it. It just always says
"today."

**The mechanic that gets it right is a three-way diff, run every time:**

1. **Still failing, already open** → refresh run metadata, but copy
   `first_seen_at` forward from the stored row.
2. **Failing for the first time** → insert as new and open, `first_seen_at` =
   now.
3. **Was open, no longer failing** → mark resolved with a resolution timestamp.
   **Mark it, do not delete it.** A deleted row leaves no trace the issue ever
   existed. You lose "how many things got resolved this month," you lose the
   ability to spot a recurring issue that keeps being silently re-inserted as
   new, and you lose any track record of improvement. Keep the row, flip the
   status.

Compare *today's full set* against *yesterday's still-open set*, fresh each
time. Do not mutate rows in place incrementally — that accumulates bugs the
longer the system runs, and a clean deterministic diff is far easier to test.

**Decide what makes two violations "the same issue."** Ours are keyed on
`(rule_id, primary_key_value, violated_column, expected_condition)`. Two traps,
in opposite directions:

- **Too narrow** and a violation whose incidental details shift between runs
  looks brand-new every time, resetting its age to zero — which defeats the
  entire point.
- **Too broad** and genuinely distinct issues collapse into one row and some
  are silently lost. This is why `expected_condition` is in our key: a
  group-scoped rule can emit several distinct violations for one group,
  differing only in which condition failed. Without it in the key, they
  deduplicate down to one and the rest disappear.

**The non-obvious constraint that follows:** every field in the identity key
must be *deterministic at rule level* and must never contain per-row data. Our
`expected_condition` is a fixed string derived from the rule definition, which
is what makes it safe to key on. The instant you interpolate a row's value into
a field that is part of the identity, every run mints a new identity and §9
quietly stops working. Write that constraint down next to the key definition.

Also handle NULLs in the key explicitly. Two NULLs are not equal in a SQL join,
so a NULL-valued key field means a violation never matches its own previous
row — a `first_seen_at` reset with no visible cause. Substitute a sentinel
before joining.

## 10. **Correction: keep ownership and severity *out* of the engine**

An earlier draft of this document recommended that every check declare an owner
and a consequence. We built exactly that — owner fields, routing team, a
316-line routing notebook, escalation days, a severity level in four schemas —
and then deleted all of it. So the honest lesson is the opposite of the
intuitive one:

- **Severity never drove any engine logic.** It was pure metadata sitting in
  the engine's schemas, and every rule author had to fill it in. It changed
  nothing about what ran, what failed, or what was reported.
- **Routing was an org chart encoded in a data pipeline.** Teams reorganise,
  responsibilities move, and every such change became a code change plus a
  deployment.

Both questions are real and worth answering — *who fixes this* and *what
happens when it fails*. They are just not the engine's questions. Emit facts
(rule, record, column, value, expected, first seen, resolved). Let the
reporting layer join those facts to whatever ownership model the organisation
currently has, where it can change without touching the pipeline.

**What we do still endorse:** distinguishing "the fix is a code/pipeline
change" from "the fix is a person correcting a record upstream." Those have
completely different owners and timelines, and mixing them into one
undifferentiated list makes it impossible to route work or measure any team's
trend. Just model that distinction downstream, as an attribute of the rule in a
reporting dimension — not as a required field on every rule in your engine.

## 11. Where completeness checks belong (and don't)

We built a `row_count` rule type, removed it, and restored it in a narrower
form. The removal rationale is the useful part:

> Every other rule type validates rows that exist, so a partial load reports
> 100% passing and raises the quality score while the data actually got worse.
> That gap is real — but it is the ETL's to close: the load layer has the run
> history to judge volume, where a rule can only hold a literal that goes stale
> as data grows.

We restored it scoped to bounded-volume checks for pipeline-blocking workflows,
where a hard literal is genuinely the right tool. The general lesson: **a
quality rule engine is good at "are these rows correct" and bad at "are all the
rows here."** Absence-of-data detection wants run-over-run history, which your
load layer already has and your rule engine does not. Decide deliberately which
system owns completeness, and do not let a hardcoded threshold be your answer
to a question that is really about trend.

## 12. Sequence and ordering checks are the hardest thing you will build

If you need to verify that a *sequence* of events occurred in the right order —
not "did X happen" but "did A then B then C happen, in order, possibly
repeating" — this is disproportionately harder than every other check type.
Ours is the single largest rule type in the engine by a wide margin. Traps,
learned the hard way:

- **Do not reach for full sequence checking unless there is a genuine repeating
  cycle.** If it is just "A must be followed by B, once," a simple pairwise
  check is enough. We originally had two separate rule types (pairwise presence
  and sequence ordering) and merged them into one, because a pair *is* a cycle
  of length two with no anchors — but only reach for the general machine when
  you need the general case.
- **The unclosed-pass check is the real value.** With a cycle of `[A, B]`,
  `start A B A end` is wrong (the trailing A never closed) while `start A B A B
  end` is two complete passes and correct. Expressing that as "the count of
  cycle events must divide evenly by the cycle length" is what catches it.
- **A person will forget to mark something as officially finished, even when it
  obviously is.** If your check relies on an explicit "this is complete" signal
  to know when to evaluate, that signal will sometimes never be set — and the
  case is then excluded from evaluation *forever*, silently dropping coverage
  rather than merely delaying it. Build a fallback: "or it reached its own
  natural end state some other way." We use the sequence's own declared closing
  event, which is the same concept read twice rather than a second concept.
- **When a sequence breaks, report only the *first* break.** One broken
  sequence cascades — everything after it looks anomalous in isolation.
  Reporting all of it drowns the one real signal.
- **Events sharing a timestamp need a deterministic tiebreaker.** Never rely on
  the order rows happen to be read in. Sort by the sequence's own declared
  position when timestamps tie, or you get different verdicts on identical data
  depending on read order — a bug that reproduces roughly never on your machine.
- **Decide up front whether this is worth building well.** A half-correct
  sequence check is worse than no check, because it produces confident-looking
  false violations that burn the credibility of every other check you built. A
  smaller set of simple pairwise checks with an acknowledged coverage gap is
  usually the better trade.

## 13. Budget real time for your platform's limitations

Not a design lesson — a scheduling one. A meaningful share of our effort went
into things that had nothing to do with data quality and everything to do with
the runtime we were on. Yours will be different, but there will be some:

- **`MERGE` was unusable.** Our platform's SQL engine cannot resolve
  schema-qualified metastore names inside a `MERGE`, so the entire violation
  persistence layer had to be written with the DataFrame API instead. This is
  not a small stylistic difference — it is why §9's diff is a full
  read-diff-rewrite of the violations table rather than an upsert, with the
  scaling characteristics that implies. Find out early whether your upsert
  primitive works, because it shapes your persistence design.
- **Read-then-overwrite of the same table is rejected by the query planner**
  unless you break the read's lineage first (a local checkpoint, in our case).
  It fails at analysis time with an error that does not obviously point at the
  cause.
- **The YAML parser turns a bare `on:` key into the boolean `True`.** YAML 1.1
  treats `on`/`off`/`yes`/`no` as booleans. Our join configuration used `on:`
  as a key and it silently disappeared. If your config format has this quirk,
  you will meet it.
- **You cannot cancel a running distributed job from the calling language.** We
  put a timeout on each rule, and the honest behaviour is: the wait is bounded,
  the work is not. The background job runs to completion regardless. Bound your
  wait so one pathological rule cannot hang the run, but do not tell yourself
  you cancelled anything.
- **Test against the real storage format.** Our resolution path does a
  read-then-overwrite that plain Parquet rejects outright, so the test harness
  needs the actual transactional format. A test suite passing against a
  simplified storage layer would have proved nothing about the one code path
  that matters most.

## 14. Test the whole thing end to end, against a known-good baseline

Unit-testing each check type in isolation catches a lot. The thing that
actually saves you is a full run against realistic fixture data, diffed against
a committed, known-good baseline output.

That is what catches the change you did not think to write a targeted test
for — a shared helper you touched that quietly shifted behaviour three check
types away from the one you meant to change. Since our design deliberately puts
all common logic in one shared driver (which is right), a regression there
touches everything, and the baseline diff is the only thing that sees it.

When you intentionally change output shape, regenerate the baseline
deliberately behind an explicit flag and review the diff line by line. Never
let it drift silently.

**Also test your documentation.** Our rule-type reference table in the
authoring guide is checked against the engine's registry by an actual test, so
docs cannot silently fall out of date with the code. It is about fifteen lines
of test and it is the reason a three-month-old guide is still accurate.

## 15. Rule type names are a user interface — expect to rename them

We renamed rule types repeatedly: `gate_complete` → `required_event`,
`group_aggregate_matches` → `aggregate_matches`, and two types merged into
`event_flow`. Every rename touched the engine, preflight, the docs, the tests,
the baseline, and every YAML file using it.

None of the renames were wasted — the names got substantially clearer, and rule
authors read these names far more often than they read anything else you write.
But it is worth knowing up front that (a) you will not get them right the first
time, (b) the cost of a rename scales with the number of rule types, which is
one more argument for keeping that number small, and (c) if the contract is
declared in exactly one place (§6), a rename is a genuinely small change
instead of a scavenger hunt.

---

## What we are *not* telling you, because we did not learn it

In the interest of not passing off reasoning as experience — the following are
real questions we never resolved in code. Treat them as open items to decide
deliberately, not as lessons from us:

- **Blocking gates.** We never built a check that stops a pipeline or a
  release. Our engine observes and reports; it does not gate. If you do build
  gating, the intuition is that a blocking check should run as early as
  possible — before anything downstream has consumed the bad data — and that a
  check which did not run should be treated as "not clear," never as "assume it
  passed." We believe both of those. We did not verify either.
- **Multiple downstream consumers agreeing.** We have one consumer (Power BI
  over the output tables). If a report, a dashboard, and a promotion decision
  all consume your quality signal, they must read the *same* computed signal
  rather than independently re-deriving similar-but-not-identical answers. Two
  systems that can disagree about whether today's data is good is how trust
  breaks, and it breaks in a way that is hard to win back. This is sound
  reasoning; it is not our experience.
- **Scale.** We ran against three source tables and 19 rules. Nothing here is
  validated at hundreds of rules or at large data volumes. In particular, the
  full read-diff-rewrite of the violations table (§9, §13) is the first thing
  we would expect to become a problem, and we never found its ceiling.
- **Rules in YAML files versus rules in a database table.** We built both and
  chose YAML, because it keeps rules diffable and reviewable in version control
  next to the engine that runs them. That was the right call *for a team of
  engineers*. If your rule authors are not comfortable with pull requests, the
  trade-off inverts, and we have no evidence about the other side of it.

---

## In short

The hard part was never writing a single check. Writing a check is a line of
SQL. The hard parts, in the order they cost us:

1. **Not building things.** Half our effort went into features we later
   deleted. Notifications, routing, ownership, severity, dry-run modes — every
   one felt obviously necessary when we built it and obviously unnecessary six
   weeks later.
2. **What happens to a failing check's result over time** — first-seen
   tracking, resolution state, and the identity key that ties a violation to
   itself across runs. Get this right up front; nothing retrofits it.
3. **Making failures loud in the right direction.** Every silent degradation we
   found made the numbers look *better*: an empty source scoring 100%, a
   rule file that failed to load raising the quality score. Those are the ones
   to hunt for.
4. **Validating configuration before the scheduled run, from a contract
   declared exactly once.**

Build those carefully, in whatever language and style you prefer. The checks
themselves can be as simple as you want them to be — ours are one line of SQL
each, and that is the part that worked from the beginning.
