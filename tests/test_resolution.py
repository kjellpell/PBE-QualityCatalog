"""
Golden tests for the full violation resolution lifecycle in
engine/resolution.py: new -> Active, still-active preserves first_seen_at,
absent -> Resolved, already-Resolved carried through unchanged.
"""

from datetime import datetime, timezone

from engine.resolution import VIOLATION_SCHEMA, _apply_resolution_tracking

TABLE_NAME = "test_dq_violations"


def _violation_row(
    rule_id="R1",
    pk="P1",
    violated_column="col_a",
    expected_condition="col_a IS NOT NULL",
    issue_status="Active",
    run_id="run-1",
    run_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    resolution_timestamp=None,
    first_seen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    violation_scope="row",
):
    return (
        run_id,
        run_timestamp,
        run_timestamp.date(),
        "TestGroup",
        rule_id,
        "Test rule",
        "test_table",
        pk,
        violated_column,
        "some value",
        expected_condition,
        "violation detail",
        issue_status,
        resolution_timestamp,
        first_seen_at,
        violation_scope,
    )


def _violations_df(spark, rows):
    return spark.createDataFrame(rows, schema=VIOLATION_SCHEMA)


def _create_empty_table(spark):
    empty = spark.createDataFrame([], schema=VIOLATION_SCHEMA)
    empty.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)


def _table_rows(spark):
    return {r.primary_key_value: r for r in spark.table(TABLE_NAME).collect()}


def _spark_roundtrip_timestamp(spark, value: datetime) -> datetime:
    """Mirror Spark's TimestampType conversion for stable timezone-agnostic asserts."""
    return spark.createDataFrame([(value,)], "ts timestamp").collect()[0].ts


def test_resolution_lifecycle(spark):
    _create_empty_table(spark)

    # Run 1: brand-new violation -> inserted as Active.
    run1_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run1_df = _violations_df(spark, [_violation_row(run_timestamp=run1_ts, first_seen_at=run1_ts)])
    _apply_resolution_tracking(run1_df, spark, violations_table=TABLE_NAME, run_timestamp=run1_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].issue_status == "Active"
    expected_first_seen = _spark_roundtrip_timestamp(spark, run1_ts)
    assert rows["P1"].first_seen_at == expected_first_seen
    assert rows["P1"].resolution_timestamp is None

    # Run 2: still-active -> refreshed metadata, but first_seen_at is preserved
    # from run 1 even though this run stamps a later first_seen_at upstream.
    run2_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
    run2_df = _violations_df(spark, [
        _violation_row(run_id="run-2", run_timestamp=run2_ts, first_seen_at=run2_ts)
    ])
    _apply_resolution_tracking(run2_df, spark, violations_table=TABLE_NAME, run_timestamp=run2_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].issue_status == "Active"
    assert rows["P1"].first_seen_at == expected_first_seen  # preserved, not run2_ts
    assert rows["P1"].run_id == "run-2"  # metadata otherwise refreshed

    # Run 3: violation absent -> marked Resolved with a resolution_timestamp.
    run3_ts = datetime(2026, 1, 3, tzinfo=timezone.utc)
    run3_df = _violations_df(spark, [])
    _apply_resolution_tracking(run3_df, spark, violations_table=TABLE_NAME, run_timestamp=run3_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].issue_status == "Resolved"
    assert rows["P1"].resolution_timestamp == run3_ts.isoformat()
    assert rows["P1"].first_seen_at == expected_first_seen  # still preserved

    # Run 4: already-Resolved row is carried through unchanged.
    run4_ts = datetime(2026, 1, 4, tzinfo=timezone.utc)
    run4_df = _violations_df(spark, [])
    _apply_resolution_tracking(run4_df, spark, violations_table=TABLE_NAME, run_timestamp=run4_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].issue_status == "Resolved"
    assert rows["P1"].resolution_timestamp == run3_ts.isoformat()  # unchanged


def test_resolution_distinct_violations_same_pk_and_column(spark):
    """
    Regression test for the A1 data-loss fix: group-style expectations
    (e.g. event_flow) can emit several distinct violations for the same
    (rule_id, primary_key_value, violated_column), differing only in
    expected_condition. Both must survive resolution, not collapse to one.
    """
    _create_empty_table(spark)

    run1_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run1_df = _violations_df(spark, [
        _violation_row(
            pk="GROUP-1", violated_column="milestone",
            expected_condition="Both 'start' and 'stop' must exist",
            run_timestamp=run1_ts, first_seen_at=run1_ts,
        ),
        _violation_row(
            pk="GROUP-1", violated_column="milestone",
            expected_condition="Both 'start' and 'gate' must exist",
            run_timestamp=run1_ts, first_seen_at=run1_ts,
        ),
    ])
    _apply_resolution_tracking(run1_df, spark, violations_table=TABLE_NAME, run_timestamp=run1_ts)

    stored = spark.table(TABLE_NAME).collect()
    assert len(stored) == 2
    assert {r.expected_condition for r in stored} == {
        "Both 'start' and 'stop' must exist",
        "Both 'start' and 'gate' must exist",
    }
