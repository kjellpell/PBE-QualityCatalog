# Operations Quick Reference

Quick runbook for IT operations and on-call support.

---

## Purpose

Use this checklist to run, verify, and triage the Quality Catalog quickly.

---

## Standard Run Sequence

1. Confirm source data refresh is complete.
2. Run preflight: scripts/preflight_checks.py.
3. Run validation: scripts/run_validation.py (executes engine/runner.py).
4. Check execution evidence in dq_execution_metrics.
5. Check latest summary rows in dq_run_results.
6. Check issue lifecycle behavior in dq_violations.

---

## First-Time Or Schema-Update Setup

1. Run scripts/setup_dq_tables.py.
2. Re-run preflight before scheduling.

---

## Runtime Flags

Set in config/QualityCatalogRuntime.py.

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

Config files must be uploaded to the Lakehouse at:

    /lakehouse/default/Files/configs/QualityCatalogConfig.py
    /lakehouse/default/Files/configs/QualityCatalogRuntime.py

The engine raises a clear error if either file is missing.

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

- Verify RULES_DIR value.
- Verify rules folder is present and contains *.yaml files.

### Missing source tables

- Re-run preflight.
- Confirm metastore object names in YAML headers.
- Confirm upstream load timing.

### Config load failure

- Verify Lakehouse config path: `/lakehouse/default/Files/configs/`.
- Confirm both `QualityCatalogConfig.py` and `QualityCatalogRuntime.py` are uploaded.

### Resolution-tracking failure on dq_violations

- The run fails with "Violations not written" — no partial data is committed.
- Re-run scripts/setup_dq_tables.py to ensure the table and columns exist.
- Confirm Delta support and table availability, then re-run validation.

### Rule execution errors

- Run `scripts/preflight_checks.py` first — it resolves every `where:`, `when:` and
  `check:` predicate against the real schema and names the offending rule.
- Verify each rule declares exactly one rule type.
- Verify referenced columns exist in source data (including joined-in columns).

---

## Ownership

- IT owns runtime, deployment, scheduling, support, and engine changes.
- Rule authors own the YAML catalogs — rule intent and follow-up decisions.
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
