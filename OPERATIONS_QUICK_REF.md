# Operations Quick Reference

Quick runbook for IT operations and on-call support.

---

## Purpose

Use this checklist to run, verify, and triage the Quality Catalog quickly.

---

## Standard Run Sequence

1. Confirm source data refresh is complete.
2. Run preflight: `QC_Preflight`.
3. Run validation: `QC_Run_Validation`.
4. Check execution evidence in dq_execution_metrics.
5. Check latest summary rows in dq_run_results.
6. Check issue lifecycle behavior in dq_violations.

---

## First-Time Or Schema-Update Setup

1. Create the six notebooks by pasting the cells from `notebooks/`, and attach
   a default lakehouse to the three entry points (see DEPLOY.md).
2. Run `QC_Setup_Tables`.
3. Re-run preflight before scheduling.

---

## Runtime Flags

Set in `QUALITY_CATALOG_RUNTIME`, in the `QC_Config` notebook.

- FAIL_ON_EMPTY_SOURCE:
  fail if a source table is empty.
- MAX_RULE_RETRIES:
  per-rule retry budget for retryable errors.
- RULE_TIMEOUT_SECONDS:
  per-rule timeout; timed-out rules are recorded as ERROR and the run continues.
- RETRYABLE_ERROR_MARKERS:
  strings used to classify retryable failures.

---

## Config Location

All configuration is in the `QC_Config` notebook, as two dicts:
`QUALITY_CATALOG_CONFIG` and `QUALITY_CATALOG_RUNTIME`. Entry-point notebooks
pull it in with `%run QC_Config`, and `configure()` raises a clear error naming
any missing key.

Nothing is read from Lakehouse Files: deployment pipelines do not promote that
section of a lakehouse, which is why config lives in a notebook.

---

## Core Outputs

- dq_run_results:
  one row per rule per run.
- dq_violations:
  current-state issue table with Active and Resolved status.
- dq_execution_metrics:
  one row per runner execution with status and timing.

---

## Fast Health Checks

After each run, confirm:

- A new execution metrics row exists with expected status.
- dq_run_results has rows for the current run_id.
- dq_violations shows expected new/updated issues.
- Rule groups (Faser, Milepæler, Faktura) appear in the run summary.

---

## Failure Triage

### No rule catalogs found

- Verify the `%run QC_Rules` cell ran (run the notebook from the top).
- Verify `QC_Rules` cells populate `RULE_CATALOG_SOURCES`.

### Missing source tables

- Re-run preflight.
- Confirm metastore object names in the catalog headers in `QC_Rules`.
- Confirm upstream load timing.

### Config load failure

- `configure()` names the missing key — add it to `QC_Config`.
- A `NameError` on `QUALITY_CATALOG_CONFIG` means the `%run QC_Config` cell did
  not run.

### Resolution-tracking failure on dq_violations

- The run fails with "Violations not written" — no partial data is committed.
- Re-run `QC_Setup_Tables` to ensure the table and columns exist.
- Confirm Delta support and table availability, then re-run validation.

### Rule execution errors

- Run `QC_Preflight` first — it resolves every `where:`, `when:` and `check:`
  predicate against the real schema and names the offending rule.
- Verify each rule declares exactly one rule type.
- Verify referenced columns exist in source data (including joined-in columns).

---

## Ownership

- IT owns runtime, deployment, scheduling, support, and engine changes.
- Rule authors own the YAML catalogs in `QC_Rules` — rule intent and follow-up
  decisions.
  Authoring assumes SQL fluency; see RULES_GUIDE.md.

Business authoring reference: RULES_GUIDE.md.

---

## Escalation Signals

Escalate to engineering when:

- Repeated retryable failures exceed retry policy.
- Non-retryable failures recur across runs.
- Resolution-tracking failures persist after setup rerun.
- Output schema drift blocks writes.
- Rule execution errors affect multiple domains.
