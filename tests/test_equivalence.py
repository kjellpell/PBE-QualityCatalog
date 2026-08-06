"""
End-to-end equivalence gate.

Runs every rule catalog through the real pipeline against the synthetic
fixtures and compares the result/violation output to a committed baseline.

Capture the baseline on the pre-change engine, then re-run after the rewrite.
Only the deliberate diffs recorded in the plan are acceptable; anything else
is a regression. Regenerate deliberately with:

    DQ_UPDATE_BASELINE=1 python -m pytest tests/test_equivalence.py
"""

import json
import os
from pathlib import Path

import pytest

BASELINE = Path(__file__).parent / "baseline_equivalence.json"

# Non-deterministic per run: identity, wall-clock, and timing.
_VOLATILE = {
    "run_id", "run_timestamp", "batch_date", "rule_duration_seconds",
    "resolution_timestamp", "first_seen_at",
}


def _rows(df, sort_keys):
    """Collect a DataFrame as sorted plain dicts with volatile fields dropped."""
    cols = [c for c in df.columns if c not in _VOLATILE]
    out = [
        {c: (str(r[c]) if r[c] is not None else None) for c in cols}
        for r in df.select(*cols).collect()
    ]
    return sorted(out, key=lambda d: tuple(str(d.get(k)) for k in sort_keys))


def _snapshot(spark, vr, rule_sources):
    results, violations = vr.run_quality_catalog(rule_sources)
    assert results > 0, "pipeline produced no result rows"
    schema = vr.CONFIG.DEFAULT_SCHEMA
    return {
        "results": _rows(
            spark.table(f"{schema}.dq_run_results"), ["rule_group", "rule_id"]
        ),
        "violations": _rows(
            spark.table(f"{schema}.dq_violations"),
            ["rule_id", "primary_key_value", "violated_column", "expected_condition"],
        ),
    }


def test_pipeline_matches_baseline(spark, runner, rule_sources):
    snapshot = _snapshot(spark, runner, rule_sources)

    if os.environ.get("DQ_UPDATE_BASELINE") or not BASELINE.exists():
        BASELINE.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n")
        pytest.skip(f"baseline written to {BASELINE.name} ({len(snapshot['results'])} rules, "
                    f"{len(snapshot['violations'])} violations) — re-run to assert against it")

    expected = json.loads(BASELINE.read_text())

    got_ids = [r["rule_id"] for r in snapshot["results"]]
    want_ids = [r["rule_id"] for r in expected["results"]]
    assert got_ids == want_ids, "rule set changed"

    for want, got in zip(expected["results"], snapshot["results"]):
        assert got == want, f"result row changed for {want['rule_id']}"

    assert len(snapshot["violations"]) == len(expected["violations"]), "violation count changed"
    for want, got in zip(expected["violations"], snapshot["violations"]):
        assert got == want, f"violation changed for {want['rule_id']}"
