# Operations Quick Reference

Quick runbook for IT operations and on-call support.

---

## Purpose

Use this checklist to run, verify, and triage the Quality Catalog quickly.

---

## Standard Run Sequence

1. Confirm source data refresh is complete.
2. Run preflight: nb_dq_01_preflight.py.
3. Run validation: nb_dq_03_run_validation.py (executes engine/validation_runner.py).
4. Check execution evidence in dq_execution_metrics.
5. Check latest summary rows in dq_run_results.
6. Check issue lifecycle behavior in dq_violations.

---

## First-Time Or Schema-Update Setup

1. Run nb_dq_00_setup.py.
2. Re-run preflight before scheduling.

---

## Runtime Flags

Set in config/QualityCatalogRuntime.py.

- DRY_RUN:
  write to temporary output tables with _tmp suffix.
- FAIL_ON_EMPTY_RULES:
  fail if no YAML catalogs are found.
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

    /lakehouse/default/Files/Configs/QualityCatalogConfig.py
    /lakehouse/default/Files/Configs/QualityCatalogRuntime.py

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
- Check FAIL_ON_EMPTY_RULES behavior.

### Missing source tables

- Re-run preflight.
- Confirm metastore object names in YAML headers.
- Confirm upstream load timing.

### Config load failure

- Verify Lakehouse config path: `/lakehouse/default/Files/Configs/`.
- Confirm both `QualityCatalogConfig.py` and `QualityCatalogRuntime.py` are uploaded.

### Resolution-tracking failure on dq_violations

- The run fails with "Violations not written" — no partial data is committed.
- Re-run nb_dq_00_setup.py to ensure the table and columns exist.
- Confirm Delta support and table availability, then re-run validation.

### Expectation execution errors

- Verify expectation name spelling.
- Verify required parameters are present.
- Verify referenced columns exist in source data.

---

## Ownership

- IT owns runtime, deployment, scheduling, support, and engine changes.
- Business owns YAML rule intent, category/owner, and follow-up decisions.

Business authoring reference: RULES_GUIDE.md.

---

## Escalation Signals

Escalate to engineering when:

- Repeated retryable failures exceed retry policy.
- Non-retryable failures recur across runs.
- Resolution-tracking failures persist after setup rerun.
- Output schema drift blocks writes.
- Rule execution errors affect multiple domains.
