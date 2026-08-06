"""
QC_Run_Validation's evidence report.

This is the notebook that gets scheduled, and `print_run_evidence` is the part
of it the engine tests never touch — it reads the output tables back rather
than writing them. It also reaches across notebooks for `_qualify`, so it is
exactly the kind of code that breaks on a rename and is not noticed until a
nightly run fails.

Uses its own schema so the assertions do not depend on what other tests have
written to the shared one.
"""

from datetime import datetime, timezone

import pytest

SCHEMA = "dqevidence"

CONFIG = {
    "DEFAULT_SCHEMA": SCHEMA,
    "DQ_RESULTS_TABLE": "dq_run_results",
    "DQ_VIOLATIONS_TABLE": "dq_violations",
    "DQ_EXECUTION_METRICS_TABLE": "dq_execution_metrics",
}


@pytest.fixture
def evidence_tables(spark, setup_tables, engine):
    spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    setup_tables.setup_dq_tables(CONFIG)

    now = datetime.now(timezone.utc)
    spark.createDataFrame(
        [(
            "run-1", now, now.date(), "Faser", "FAS-001", "test rule",
            "faser", "check", 10, 9, 1, 90.0, "FAILED", "1 of 10 rows violate", 0.5, None,
        )],
        schema=engine.RESULT_SCHEMA,
    ).write.mode("append").saveAsTable(f"{SCHEMA}.dq_run_results")

    engine.write_execution_metric(
        spark,
        f"{SCHEMA}.dq_execution_metrics",
        {
            "script_name": "run_validation", "status": "Succeeded",
            "output_target": f"{SCHEMA}.dq_run_results",
            "artifact_target": f"{SCHEMA}.dq_violations",
            "row_count": 1, "started_at_utc": now, "finished_at_utc": now,
            "duration_seconds": 1.0, "is_retryable": False, "error_message": None,
        },
    )
    yield
    spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")


def test_evidence_reports_every_table(run_validation, evidence_tables, capsys):
    run_validation.print_run_evidence(CONFIG)
    out = capsys.readouterr().out

    assert f"{SCHEMA}.dq_run_results" in out
    assert f"{SCHEMA}.dq_violations" in out
    assert f"{SCHEMA}.dq_execution_metrics" in out

    # The rule-group summary only renders when there is a run to summarise, so
    # this is what proves the branch ran rather than being skipped.
    assert "Rule-group summary for latest run_id: run-1" in out
    assert "Faser" in out


def test_evidence_survives_missing_tables(run_validation, spark, capsys):
    """Reporting must not be what fails a run that otherwise succeeded."""
    spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")

    run_validation.print_run_evidence(CONFIG)

    assert "results_table: None" in capsys.readouterr().out


def test_a_malformed_table_name_is_named(run_validation):
    """It reaches Spark SQL through an f-string, so it is checked first."""
    with pytest.raises(ValueError, match="DQ_RESULTS_TABLE"):
        run_validation.print_run_evidence({**CONFIG, "DQ_RESULTS_TABLE": "dq results"})
