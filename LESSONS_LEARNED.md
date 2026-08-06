# Lessons Learned: Building a Data Quality Rule Engine

**To:** the team building the production validation engine

This is not a specification and not a design to copy. It is the set of things
we would tell ourselves at the start if we could — the decisions that turned
out to matter, the defaults that turned out to be wrong, and the handful of
problems that are much harder than they look from the outside.

You will build your own. These are the parts we think are worth knowing before
you do.

---

## Read this first: what the system is actually for

Everything below only makes sense against the purpose, and the purpose is not
"data quality" in the usual sense of the phrase.

**The case system has no guards.** A caseworker can register almost anything,
in almost any order, and nothing stops them. There is no enforced sequence, no
required combination, no validation at the point of entry. The consequence is
not that fields are empty — it is that **cases go through the process
incorrectly**, and nobody finds out.

**So the point of this system is to tell the caseworker what they got wrong, so
they can go and fix it.** Not to score data, not to produce a dashboard, not to
block a pipeline. It is a feedback loop to the person who made the mistake,
about a mistake the source system allowed them to make.

That reframes what the rules are for. There are two categories, and they are
not equally important:

- **Field-level correctness** — a missing value, a negative number, a date
  before another date. These are the majority of rules by count and they are
  genuinely easy. They are worth having, because they cost almost nothing to
  express. But they are not why you build a system. Each one just means someone
  left a field blank.
- **Process conformance** — did this case actually follow the procedure? Did
  the milestone pairs that must occur together occur together, in order, every
  time they occurred? Is the set of milestones this kind of case is required to
  have actually registered?

**The second category is the entire justification for the project.** It is
where the real errors are, it is what the source system permits and nobody
catches, and it is the only part a caseworker cannot easily see for themselves.
It is also, by a wide margin, the hardest part to build. §1 and §2 are really
one argument about that, split in two only because the second half is long.

One thing we should be straight about: **we found the errors, but we never
built the loop that tells the caseworker about them.** The detection half
works. The delivery half — getting a specific, comprehensible message to the
person who can fix a specific case — was never implemented. Finding out how
many process errors were in there was valuable in itself, and it is why this
document exists. But if you build this, the feedback loop is the deliverable,
not the by-product. Plan for it as a first-class piece of work rather than
something you get to at the end.

---

## 1. Two kinds of rule, and only one of them justifies the project

Our rule format has two tiers. Getting clear about the boundary between them
was one of the more useful things we did, because the two have almost nothing
in common — not in effort, not in value, and not in what they can express.

**Tier one — the easy errors — is a boolean SQL expression**, optionally
scoped by a second one:

```
rule:  "Time spent cannot be negative"
check: tidsbruk >= 0

rule:  "A closed phase must have a closing date"
when:  seneste_stoppmilepael_dato IS NOT NULL
check: fase_lukket_dato IS NOT NULL
```

By count, most rules look like this, and this tier is close to free — both to
build and to author. It works because our rule authors already know SQL, and
the alternative they are implicitly comparing against is hand-written SQL;
anything more abstract than what they'd write by hand is a step backwards.

Be clear-eyed about what this tier finds, though: **a blank field**. Useful,
cheap, worth having — and not a reason to build an engine. If this were all you
needed, a handful of queries would do.

The deeper benefit is that you inherit semantics instead of inventing them.
Every question you would otherwise have to decide, document, and defend — how
comparisons behave, how NULL propagates, what an empty set means — already has
a standard answer your authors have internalised. Every place you deviate from
SQL is a place you now owe someone an explanation.

**Tier two — the errors that matter — is everything a row predicate
structurally cannot express.** Above all, the two process-conformance
questions:

- **Did the required sequence happen?** Milestones that must occur as pairs,
  occurring together and in the right order, however many times the pair
  repeats.
- **Is the required set of milestones registered at all?** Given what kind of
  case this is, the procedure says certain milestones must exist. Do they?

Both are the subject of §2, which follows immediately because it is the other
half of this section rather than a separate topic. Alongside them sit
uniqueness across a set of rows, and an aggregate over a group compared against
a reference value — useful, but not why you are here.

No amount of cleverness collapses any of these into a predicate, and it is
worth understanding exactly why: **a predicate sees one row, and every one of
these questions is about relationships between rows** — between the milestones
of a case, or between a case and the procedure it was supposed to follow. That
is the real boundary in the design, and it is not a matter of taste or
convenience.

**The trap is assuming tier two is a modest extension of tier one.** It is not.
Tier one is a thin layer over the query engine. Tier two *is* the engine —
essentially all of the implementation, all of the hard reasoning, and all of
the bugs. Nothing in this document should be read as suggesting that writing
checks is the easy part. Writing *checks for missing fields* is the easy part.
The checks that justify building this at all are complicated, and they are
complicated all the way down.

Keep the number of tier-two types small — not because they are simple, but
because each one is expensive and permanent: documentation, tests, validation
support, a name you will get wrong the first time. Our instinct at the start
was that we would need many. We ended up with a handful, and each type we added
beyond the core few was eventually removed or merged into something more
general. Fewer, more general, properly built beats a long menu of
half-considered ones.

## 2. Sequence checking is the one rule type you cannot skip

This is the most important section in this document, and the one where we would
most strongly resist any attempt to make it sound simpler than it is.

Every other rule type is, in the end, optional. You could drop any one of them
and still have something worth running. **This one is the reason the project
exists.** Verifying that a *sequence* of events happened correctly — not "did X
happen" but "did A then B happen together, in order, every time that pair
occurred" — is precisely where caseworkers go wrong, because it is precisely
what the case system lets them do freely. Nothing stops someone from
registering the second half of a pair without the first, or in the wrong order,
or opening a pair and never closing it. Nobody notices, and no field is empty.

It is also disproportionately harder than every other kind of check: by a wide
margin the largest, most-revised and most-tested thing we built. That
combination — indispensable *and* hardest — is the single most useful thing we
can tell you about scheduling this work. Budget for it from the start rather
than discovering it, and do not let it be the thing that gets squeezed at the
end, because the easy rules finished early will not substitute for it.

**Start by looking at how much behaviour hides in one small configuration.**
Take a flow declared as a start anchor, a repeating cycle of `[A, B]`, and a
closing anchor — about six lines of configuration:

| Events, in order | Verdict | Why |
|---|---|---|
| `start A B end` | valid | one complete pass |
| `start A B A B end` | valid | two complete passes |
| `start B A end` | error | cycle out of order |
| `B start A end` | error | cycle event before the start anchor |
| `start A B A end` | error | the trailing `A` never closes |
| `start A A B B end` | error | passes must alternate, not batch |

Every one of those verdicts is a decision someone has to make, implement, and
defend. And that is a *single* configuration. The anchors are optional, the
cycle can be any length, several different events may legitimately close the
flow, and the whole rule may be scoped to only evaluate groups that have
reached a given point. Each combination has its own set of valid and invalid
sequences, and a reader who is being clever can keep generating new ones for a
long time. There is no point at which you have obviously enumerated them all —
which is precisely why this needs a real test suite rather than a few examples.

**The last two rows are the entire justification for building this.** An opened
pass that never closes is a real data problem, and a plain "do both events
exist?" check is structurally blind to it — `start A B A end` contains both A
and B, so a presence check passes it happily. Expressing the requirement as
"the number of cycle events must divide evenly by the cycle length" is what
catches the unclosed pass. If you take one thing from this section, take that
the naive version of this check silently misses the exact failure you built it
for.

**Reach for the general mechanism only when there is a genuine repeating
cycle.** If the requirement really is "A must be followed by B, once," that is
a cycle of length two with no anchors — the same machinery, configured
trivially. We started with two separate mechanisms for these and merged them,
which was right. But do not build the general case to serve a requirement that
never repeats.

**A person will forget to mark something as officially finished, even when it
obviously is finished.** If your check relies on an explicit "this is now
complete" signal to know when to evaluate a sequence, that signal will
sometimes never be set — and the case is then excluded from evaluation
*forever*, silently dropping coverage rather than merely delaying it. Build a
fallback: "or it reached its own natural end state some other way." Plan for
this from the start; it is not an edge case, it is normal human behaviour.

**When a sequence breaks, report only the first break.** One broken sequence
cascades — everything positioned after the break also looks anomalous in
isolation. Reporting all of it drowns the one real signal in noise and makes
the violation list look far worse than the data is.

**Events sharing a timestamp need a deterministic tiebreaker.** Never rely on
whatever order the rows happen to be read in. Sort by the sequence's own
declared position when timestamps tie. Without it, identical data produces
different verdicts on different runs — a bug that reproduces roughly never
while you're looking for it.

**Fund it properly or scope it down honestly — but do not ship an
approximation.** A half-correct sequence check is worse than no check at all,
because it produces confident-looking false violations, and those burn the
credibility of every other check you built along with it. Remember who receives
these: a caseworker told three times that they handled a case wrongly, who
checks and finds they did not, will disregard the fourth message — and the
fourth one will be real. The failure mode is not "this check is a bit
unreliable," it is "the people we built this for stop reading it." So either
give this the time and the test coverage it genuinely needs, or deliberately
narrow what you claim it covers and say so out loud. What you must not do is
build eighty percent of it and present the result as if it were complete.

### The other half of process conformance: is the required set registered?

Sequence checking asks whether the milestones that *did* occur were correct.
The companion question is whether the milestones that *should* have occurred
are there at all: given what kind of case this is, the procedure requires a
particular set of milestones — are they registered?

This one is much easier to implement and nearly as valuable, and it is worth
building as a distinct rule type rather than trying to fold it into the
sequence check. Three things we learned:

- **Let the requirement be satisfied by any one of several events.** Different
  procedures close the same obligation with different milestones, and forcing
  one name per rule multiplies your rule count for no gain.
- **Decide whether an event with no date counts as having happened.** We made
  the date requirement optional per rule, which turned out to be the right
  granularity: sometimes the milestone merely existing is the requirement, and
  sometimes "registered but undated" is itself the error.
- **Keep it clearly distinct from scoping.** We have two features that name
  the same kind of thing for opposite purposes — one *asserts* an event is
  present and fails the case when it isn't; the other *scopes* which cases get
  evaluated at all and silently skips those that haven't got there yet. They
  look nearly identical in configuration and mean opposite things. Name them so
  that confusing the two is hard, and document the distinction where people
  will hit it.

## 3. NULL is not a failure

Decide early — deliberately, once — how a check treats a predicate that cannot
be evaluated because one of its inputs is NULL.

The tempting default is "NULL fails the check." It is almost always wrong. It
means every optional field, and every field not yet filled in, silently
generates a false violation the moment it is empty. You will produce a flood of
violations that are not violations, and people stop trusting the whole system
long before they finish working out why.

The right default: a predicate that evaluates to NULL is *unevaluated*, not
failed. It counts in neither the pass total nor the fail total. If you want to
require that a field is present, say so explicitly (`X IS NOT NULL`) rather
than relying on NULL propagation to catch it as a side effect.

This is exactly how a SQL `CHECK` constraint behaves — a row violates it only
when the predicate is definitively FALSE — so if you followed §1 you get it for
free rather than implementing it.

One asymmetry worth deciding consciously: the *scoping* clause and the *check*
clause should treat NULL in opposite directions. A row is in scope only where
the scoping condition is explicitly TRUE — an unevaluable scope excludes the
row. An unevaluable check leaves the row unjudged. Same NULL, opposite
handling, and both are correct.

## 4. A violation needs a subject, not just a verdict

Never build a check that returns only pass/fail. For every failure, capture:
which record, which column, what the actual value was, what was expected, and
a plain-language explanation of the gap.

A check that reports only "FAILED" forces the next person to re-derive what
went wrong. You already did that work at check time. Making someone repeat it
is the difference between a violation list people work from and a violation
list people ignore.

If you derive "which column is this check about" automatically from the
predicate, be careful which one you choose when there are several. The *first*
column referenced is almost always the natural subject — `a` in `a >= b`.
Getting it backwards makes every violation harder to read for no benefit.

Two things we'd flag:

- Deriving the subject is harder than it looks, because the obvious source for
  "which columns does this expression touch" is often an unordered set. You
  need the columns in the order they were *written*, which usually means
  reading the expression as the author wrote it rather than as the engine
  resolved it.
- Make the derivation best-effort. If it fails, emit no subject rather than
  failing the check. A convenience feature must never be able to break a rule.

## 5. Decide what "one thing" is before you count anything

Per check, decide what a single unit is, and count consistently in that unit.

A check over individual rows counts rows. A check over a *group* of related
rows — "did this case go through the right sequence of steps" — counts groups,
not the rows or events inside them. If a group has five things wrong with it,
that is one failed group, not five. Otherwise your pass rate is distorted by
how much internal detail a group happens to contain, and the same quality
problem scores completely differently depending on unrelated data volume.

Make this a declared property of each rule type rather than a decision each
check makes for itself, and have one shared piece of logic do all the counting.
The invariant this buys you is worth writing a test for: **passed = total −
failed must never be able to go negative.** When it can, you have a rule
counting failures in one unit against a denominator in another — and nobody
notices until a dashboard shows a negative number.

## 6. Duplicate checks should return the whole colliding set

Do not just flag "a duplicate exists." Return every row involved in the
collision.

Whoever has to resolve a duplicate needs to see all of them side by side to
decide which is correct. Reporting one and leaving them to find the others
turns a two-minute fix into a hunt.

## 7. Rules must not be able to modify data — make that structural

If your rule format has any path that executes caller-supplied code or SQL — a
"just run this query" escape hatch — remove it, even if nothing currently
misuses it.

We had one. Nothing used it. We removed it anyway, and the reasoning is the
part worth passing on: an escape hatch that runs an arbitrary statement runs it
against the *source* environment, and no amount of care about where output goes
protects against that. The risk was never that someone would write something
malicious. It was that "this format is read-only" was a property nobody could
verify — it was a convention people had to remember.

Now every rule is a predicate or a declarative block, and every author-supplied
string goes through something that builds an expression and structurally
*cannot* carry a data-modifying statement. Read-only became a property of what
is possible to write rather than a rule people follow.

A convention gets violated eventually. A structural limitation does not.

If a check needs data from another table, give it a declarative way to join and
read that table. Never a raw execution path.

## 8. Validate the configuration before the run, not during it

Whatever declares a rule — a file, a form, code — should be validated against
the real schema and against its own contract *before* a scheduled run touches
it. Does the referenced column actually exist? Does each rule declare exactly
one type, with everything that type requires? Are the identifiers unique?

Catching a typo at review time costs two minutes. Catching it during an
unattended overnight run costs someone their morning and a day of missing
quality signal.

Four things we learned making this actually work:

**Validate predicates with the engine's own analyser, not with a hand-written
list of "which settings contain column names."** We maintained such a list. It
drifted from the engine within weeks. Resolving the predicate against the real
schema catches every typo and there is no list to keep in sync.

**Validate with an operation that requires a boolean.** This one bit us
specifically. We validated expressions with an operation that accepts *any*
expression, so a rule whose check was a bare column name — not a predicate at
all — passed validation cleanly and then failed at 03:00 with a type error.
Exactly the failure the whole validation step existed to prevent. Validate with
something that will reject a non-boolean, i.e. the same operation the engine
itself will use.

**Build your validation probe from the real schema, including joins.** Take the
actual source table, take zero rows from it, and replay the configured joins on
it. It costs nothing and gives you every column with its *real type* — a probe
that synthesises joined columns as text will happily accept a type-sensitive
predicate like `joined_date >= '2024-01-01'` and let it fail in production.
Replaying the joins also validates the join configuration itself, which nothing
else does.

**Reject unknown keys, at every level of the configuration.** A misspelled key
in a file header is the most expensive kind of typo, because it does not error
— it silently *drops* whatever that setting did. Misspell the row-filter key
and every rule in the file quietly starts running against unfiltered data, and
the run looks perfectly healthy. Rejecting keys you don't recognise is trivial
to implement and eliminates an entire category of silent wrongness.

**And declare the contract exactly once.** Each rule type's required settings,
which of them name columns, what unit it counts in — that belongs in a single
structure that the pre-run validation *reads*, rather than a parallel list it
restates. We had it as two lists first. They drifted, and the drift was
invisible until a rule that should have been rejected ran and produced
nonsense.

## 9. "The check is broken" and "the data is bad" are different statuses

These are not the same failure and must never share a status.

A check that could not run — a column has gone missing, a table doesn't exist,
something timed out — is a configuration or infrastructure problem. A check
that ran fine and found something genuinely wrong is a data problem. If both
land in one "failed" bucket, real infrastructure failures hide inside what
should be a clean data-quality signal, and your quality trend lies to you at
exactly the moment your own pipeline broke.

We use three statuses: passed, failed, and error. We'd suggest going one small
step further and also recording an *error category* — infrastructure,
configuration, or source data. It costs almost nothing at write time and means
"our engine is unhealthy" and "someone's rule definition is wrong" become
different queries, answerable by different people, without anyone reading
error text by eye.

## 10. Every silent failure we found made the numbers look *better*

This is the pattern we'd most want you to internalise, because it tells you
where to go looking.

Every degradation we discovered that had no visible symptom made the quality
score go *up*:

- **An empty source table reports 100% passing.** Every rule validates rows
  that exist. Zero rows means zero failures means a perfect score. Guard the
  *run*, not the data: abort before an empty source can be reported as flawless
  quality.
- **A rule file that fails to load raises the score** — because the rules that
  vanished included the failing ones. We originally logged a warning and
  carried on. Nobody reads logs at 03:00. Now an unloadable rule file fails the
  entire run, on the grounds that reporting a quality score over fewer rules
  than intended is worse than reporting nothing at all.

The general form: **any failure mode that makes your metric improve will never
be reported by a human**, because nobody escalates good news. Those are the
ones you have to find by reasoning about them in advance. Ask of every
component: what happens to the number if this silently does nothing?

## 11. Separate "what happened this run" from "what is currently wrong"

Keep two outputs, not one.

A run log — every check's result on every run, append-only — answers questions
about trend and reliability. A current-state list — what is still wrong right
now — is what someone works from. Different audiences, different questions,
different shapes.

Trying to serve both from one table means computing "what's currently open" as
an expensive scan over your entire run history every time someone wants a work
queue.

We keep a third: a run-level execution log with start, end, duration, and
whether a failure was retryable. It is the cheapest thing in the system and the
first place you look when someone asks "did it even run last night?"

## 12. First-seen tracking and resolution state are the actual foundation

This deserves its own emphasis, separate from the point above, because it is
easy to build the current-state list, feel finished, and not notice you have
built something with no memory.

Without this done correctly, almost nothing useful is possible later: no
age-based triage, no "this has been open too long" escalation, no resolution
trend, no way to answer "is this getting better or worse." All of it depends
entirely on getting one mechanism right, up front. Nothing here retrofits.

**The core idea:** every violation carries a `first_seen_at` timestamp that is
set once, the first time it is detected, and then never touched again for as
long as that same violation keeps recurring. Every later run *refreshes* the
row — latest run, latest detail — but explicitly *preserves* the original
`first_seen_at`.

This sounds obvious written down, and it is still the single easiest thing in
the entire system to get wrong. The naive implementation — recompute violations
fresh each run, write them out — silently overwrites `first_seen_at` with "now"
on every run. The moment that happens, every violation looks like it started
today, forever. No error. No test failure. Nothing to signal it. It just always
says "today," and you lose the ability to ever answer "how long has this been
broken."

**The mechanic that gets this right is a three-way diff, run every time:**

1. **Still failing, already open** — refresh the row's run metadata, but copy
   `first_seen_at` forward from the stored row rather than setting it to now.
2. **Failing for the first time** — insert as a new open issue, `first_seen_at`
   set to now.
3. **Was open, no longer failing** — mark it resolved, with a resolution
   timestamp. **Mark it, do not delete it.** A deleted row leaves no trace the
   issue ever existed. You lose "how many things got resolved this month," you
   lose the ability to spot a recurring issue that keeps coming back and being
   silently re-inserted as if new, and you lose any evidence of improvement
   over time. Keep the row, flip its status, leave it there as history.

Compare *today's complete set of detected issues* against *yesterday's
still-open set*, computed fresh each time. Do not try to update rows in place
incrementally — that accumulates bugs the longer the system runs, and a clean
deterministic diff is dramatically easier to reason about and to test.

**Then decide, carefully, what makes two violations "the same issue" across
runs** — which combination of fields forms its identity. This decision deserves
real thought rather than a default grabbed without checking it. Two traps, in
opposite directions:

- **Too narrow**, and a violation whose incidental details shift slightly
  between runs looks brand new every time, resetting its age to zero on every
  run — which defeats the entire purpose of tracking age.
- **Too broad**, and genuinely distinct issues collapse into a single row and
  some of them are silently lost. Check this against how your own rule types
  can produce *several* distinct violations from what looks like one rule — a
  group-scoped check can easily emit one violation per failing condition within
  the same group, and if your identity doesn't distinguish them, all but one
  disappear.

**One non-obvious constraint follows from this**, and it is worth writing down
next to your identity definition: every field in the identity must be
deterministic at rule level and must never contain per-record data. The moment
you interpolate a record's own value into a field that forms part of the
identity, every run mints a fresh identity, and all of the above quietly stops
working with no visible symptom.

Also handle NULLs in the identity explicitly. In a normal join, two NULLs are
not equal — so a violation with a NULL in its key never matches its own
previous row, and its age resets every night for reasons nobody will find.
Substitute a placeholder before comparing.

## 13. Keep ownership, severity and delivery *mechanism* out of the engine

To be unambiguous, because the rest of this section could easily be misread:
**getting the message to the caseworker is the whole point of the system.**
Nothing here argues against that. What follows is about where that machinery
should live, and it is the difference between a feedback loop that survives a
reorganisation and one that doesn't.

We deliberately trialled three things inside the engine — a severity level on
every rule, an ownership/routing layer, and push notifications. The trials were
worth running. The verdict was clear, and it went the other way from our
starting assumption:

**Severity never changed anything.** It sat in the output schemas, every rule
author had to fill it in, and it never once influenced what ran, what failed,
or what was reported. It was reporting metadata living in the engine's
contract. A field that does not change behaviour belongs in the reporting
layer, not in every rule definition.

**Routing is an org chart embedded in a data pipeline.** Teams reorganise and
responsibilities move constantly. Every one of those changes became a code
change and a deployment, to express something that was never really about data
quality.

**The delivery channel is a bigger commitment than it looks, and it is a
separate build.** We cycled through several delivery mechanisms wired directly
into the engine, and each one dragged in authentication, message formatting,
delivery logging, failure handling, and a second thing that had to keep running
at 03:00 — none of which is data quality work, all of which competed with §2
for time. Get the detection right and land the results somewhere durable first.
Then build the channel deliberately, as its own piece of work, because that is
what determines whether a caseworker acts on the message.

**What survives all of that** is that the underlying questions are real. *Who
fixes this* and *what happens when it fails* both matter — in particular, the
distinction between "the remedy is a code or pipeline change" and "the remedy
is a person correcting a case in the source system" is fundamental here, since
the second is what this project is for. Those have completely different owners
and timelines, and merging them into one undifferentiated list makes it
impossible to route work or to measure anyone's trend.

They are just not the *engine's* questions. Have the engine emit facts — rule,
record, column, actual value, expected condition, first seen, resolved when —
and make those facts good enough to build a human-readable message from
(see §4, which matters far more than it appears to). Let a layer above join
them to whatever ownership model the organisation has this quarter, where it
can change without touching the pipeline.

## 14. Completeness belongs to the load layer, not the rule engine

Worth deciding explicitly, because the gap is real and easy to miss.

Every rule you write validates rows *that exist*. This means a partial load —
half the data missing — reports 100% passing and *raises* your quality score
while the data actually got worse. (Same family as §10.)

The instinct is to close that gap with a rule: "this table must contain between
X and Y rows." It works, narrowly, and it is the right tool when you need a
hard gate on volume before something downstream runs. But as a general answer
it is weak, because a rule can only hold a fixed number that goes stale as the
data grows, whereas the question is really about trend.

Your load layer already has the run-over-run history to judge whether today's
volume is plausible. Decide deliberately which system owns "is all the data
here," and do not let a hardcoded threshold become your answer by default.

## 15. Budget real time for your platform fighting you

Not a design lesson, a planning one. A meaningful share of our effort went into
problems that had nothing to do with data quality and everything to do with the
runtime underneath. Yours will be different problems, but there will be some,
and they are worth discovering in week one rather than week eight — because
some of them constrain your architecture rather than just costing a day.

The one that reshaped our design: **the natural way to update a table in place
did not work** in our environment. That single limitation is why our violation
state is maintained as a full read-compare-rewrite rather than an in-place
update — which is a completely different thing to reason about and to scale.
Find out early whether your update primitive actually works the way you expect,
because your entire persistence design sits on that answer.

Others in the same family, offered as a flavour of what to expect:

- Reading a table and then overwriting that same table can be rejected outright
  by the query planner unless you explicitly break the dependency between the
  read and the write. It fails with an error that does not obviously point at
  the cause.
- Configuration formats have surprises. Ours silently reinterpreted a
  perfectly reasonable setting name as a boolean, and the setting just quietly
  vanished.
- You generally cannot cancel a running distributed job from the code that
  launched it. We put a time limit on each rule, and the honest description is
  that the *wait* is bounded — the work is not. That is still worth doing, so
  one pathological rule can't hang the whole run, but don't tell yourself you
  cancelled anything.
- Test against your real storage layer. Our state-tracking path does a
  read-then-overwrite that a simpler storage format rejects outright, so
  testing against a simplified stand-in would have proved nothing about the
  single code path that matters most.

## 16. Test end to end against a known-good baseline, not just piece by piece

Testing each check type in isolation catches a lot. The thing that actually
saves you is a full run against realistic fixture data, with the complete
output diffed against a committed, known-good baseline.

That is what catches the change you didn't think to write a targeted test for —
a shared piece of logic you touched that quietly shifted behaviour in three
check types away from the one you meant to change. And note that if you follow
§5 and put all common logic in one shared place (which you should), then a
regression there touches *everything*, and the baseline diff is the only thing
that will see it.

When you intentionally change the output, regenerate the baseline deliberately
behind an explicit switch and read the diff line by line. Never let it drift
silently — the moment a baseline updates itself automatically, it stops being
a baseline.

**Test your documentation too.** Our rule-type reference is checked against the
engine's own definitions by an automated test, so the two cannot silently
diverge. It is a tiny amount of work and it is the reason our authoring guide
is still accurate months later. Rule authors read that document far more often
than they read anything else you produce.

## 17. Rule type names are a user interface, and you will rename them

We renamed rule types repeatedly, and merged two into one. None of it was
wasted — the names got substantially clearer, and rule authors read these names
constantly. But it is worth knowing three things going in:

- You will not get the names right the first time. Plan for renaming rather
  than trying to avoid it.
- The cost of a rename scales with how many rule types you have, which is one
  more argument for §1's advice to keep that number small.
- If the contract lives in exactly one place (§8), a rename is a genuinely
  small change instead of a scavenger hunt across the engine, the validation,
  the docs, the tests, and every rule file.

---

## What we are *not* telling you, because we didn't learn it

To be straight about which of this is experience and which is reasoning — the
following are real questions we never answered in practice. Decide them
deliberately; don't take them from us.

**The feedback loop to the caseworker — the largest gap by far.** We built the
half that finds the errors. We never built the half that tells the person who
made one, in language they can act on, about the specific case they can go and
fix. Everything we know about the hard part of that is guesswork: how to phrase
a process violation so it reads as help rather than accusation, whether people
respond better to a message or to a list they visit, how to avoid the same
unresolved case nagging someone every night for a month, what happens when the
violation is real but the caseworker disagrees. Those are the questions that
decide whether the system changes anyone's behaviour, and we cannot answer any
of them. Treat detection as the prerequisite and this as the actual product —
and consider building a thin version of it early against a handful of rules,
rather than after the engine is finished, because it will tell you things about
the rules that no amount of engine work will.

**Blocking gates.** We never built a check that stops a pipeline or a release.
Our engine observes and reports; it does not gate anything. If you do build
gating, our intuition — and it is only that — is that a blocking check should
run as early as it possibly can, before anything downstream has consumed the
bad data, because a check that can only fire after the data has propagated
isn't preventing bad data, it's unwinding it. And that a check which did not
run should count as "not clear," never as "assume it passed," built into the
gate logic itself rather than left to each caller. We believe both. We verified
neither.

**Several systems consuming the same signal.** We have one consumer. If a
report, a dashboard, and a promotion decision all depend on your quality
signal, they need to read the *same* computed answer rather than independently
re-deriving similar-but-not-identical ones. Two systems that can disagree about
whether today's data is good is how trust breaks, and it breaks in a way that
is very hard to win back — but that is reasoning, not our experience.

**Scale.** We ran against a small number of source tables and a modest number
of rules. Nothing here is validated at hundreds of rules or at large volumes.
The first thing we would expect to become a problem is the full
read-compare-rewrite of violation state described in §12 and §15. We never
found its ceiling, so we cannot tell you where it is.

**Rules in version-controlled files versus rules in a database table.** We
built both and chose files, because it keeps rules diffable and reviewable
alongside the engine that runs them, and because a rule change then goes
through the same review as a code change. That was right *for a team of
engineers*. If your rule authors aren't comfortable with that workflow, the
trade-off may well invert, and we have no real evidence about the other side.

---

## In short

The system exists because the case system has no guards, so caseworkers can and
do take cases through the process incorrectly, and nobody finds out. Everything
worth knowing follows from that.

Do not let anyone tell you this is mostly plumbing around some one-line checks.
The simple rules genuinely are simple — a predicate over a row, working on day
one — but all they find is a blank field. The rules that justify the project
are the ones that ask whether the procedure was actually followed, and those
are hard in their own right, before you have written a single line of the
surrounding machinery. They stayed hard through every revision.

Budget for two separate problems: the process-conformance checks, and
everything below them.

The hard parts, in the order they cost us:

1. **Sequence checking.** The largest, most-revised and most-tested thing we
   built, the one that finds the errors this project exists to find, and the
   only rule type you cannot do without. Assume it will take several attempts,
   and that the naive version silently misses the exact case you built it for.
2. **What happens to a failing check's result over time.** First-seen tracking,
   resolution state, and the identity that ties a violation to itself across
   runs. Get this right up front — nothing retrofits it, and its failure mode
   is completely silent.
3. **Making failures loud in the right direction.** Every silent problem we
   found made the numbers look *better*. Those are the ones to hunt for
   deliberately, because no one will ever report them.
4. **Validating configuration before the scheduled run**, against the real
   schema, from a contract declared in exactly one place.
5. **Being disciplined about what does not belong in the engine.** Ownership,
   severity, delivery mechanism, and completeness all felt like they belonged.
   On inspection, none of them did.

And the part we never reached, which we would now treat as the real deliverable
rather than the last step: **telling the caseworker, in terms they can act on,
what went wrong with a specific case.** Detection without that loop is a
finding, not a fix. We think finding out how much was wrong was worth doing on
its own — but you have the chance to build the half that actually changes
something, and we would start it earlier than we did.
