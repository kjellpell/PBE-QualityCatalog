"""
Golden tests for the full violation resolution lifecycle: new -> Aktiv,
still-active preserves foerst_observert_tidspunkt, absent -> Løst, already-Løst
carried through unchanged.
"""

from datetime import datetime, timezone

from tests.notebook_source import engine_namespace

_ENGINE = engine_namespace()

VIOLATION_SCHEMA = _ENGINE.VIOLATION_SCHEMA
_apply_resolution_tracking = _ENGINE._apply_resolution_tracking

TABLE_NAME = "test_avvik"


def _violation_row(
    rule_id="R1",
    pk="P1",
    identifikator_verdi="SAK-001",
    avvikende_kolonne="col_a",
    forventet_betingelse="col_a IS NOT NULL",
    avviksstatus="Aktiv",
    run_id="run-1",
    run_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    loest_tidspunkt=None,
    foerst_observert_tidspunkt=datetime(2026, 1, 1, tzinfo=timezone.utc),
    avviksomfang="Rad",
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
        identifikator_verdi,
        avvikende_kolonne,
        "some value",
        forventet_betingelse,
        "violation detail",
        avviksstatus,
        loest_tidspunkt,
        foerst_observert_tidspunkt,
        avviksomfang,
    )


def _violations_df(spark, rows):
    return spark.createDataFrame(rows, schema=VIOLATION_SCHEMA)


def _create_empty_table(spark):
    empty = spark.createDataFrame([], schema=VIOLATION_SCHEMA)
    empty.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)


def _table_rows(spark):
    return {r.primaernoekkel_verdi: r for r in spark.table(TABLE_NAME).collect()}


def _spark_roundtrip_timestamp(spark, value: datetime) -> datetime:
    """Mirror Spark's TimestampType conversion for stable timezone-agnostic asserts."""
    return spark.createDataFrame([(value,)], "ts timestamp").collect()[0].ts


def test_resolution_lifecycle(spark):
    _create_empty_table(spark)

    # Run 1: brand-new violation -> inserted as Aktiv.
    run1_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run1_df = _violations_df(spark, [_violation_row(run_timestamp=run1_ts, foerst_observert_tidspunkt=run1_ts)])
    _apply_resolution_tracking(run1_df, spark, violations_table=TABLE_NAME, run_timestamp=run1_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Aktiv"
    expected_first_seen = _spark_roundtrip_timestamp(spark, run1_ts)
    assert rows["P1"].foerst_observert_tidspunkt == expected_first_seen
    assert rows["P1"].loest_tidspunkt is None

    # Run 2: still-active -> refreshed metadata, but foerst_observert_tidspunkt
    # is preserved from run 1 even though this run stamps a later value upstream.
    run2_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)
    run2_df = _violations_df(spark, [
        _violation_row(run_id="run-2", run_timestamp=run2_ts, foerst_observert_tidspunkt=run2_ts)
    ])
    _apply_resolution_tracking(run2_df, spark, violations_table=TABLE_NAME, run_timestamp=run2_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Aktiv"
    assert rows["P1"].foerst_observert_tidspunkt == expected_first_seen  # preserved, not run2_ts
    assert rows["P1"].kjoert_id == "run-2"  # metadata otherwise refreshed

    # Run 3: violation absent -> marked Løst with a loest_tidspunkt.
    run3_ts = datetime(2026, 1, 3, tzinfo=timezone.utc)
    run3_df = _violations_df(spark, [])
    _apply_resolution_tracking(run3_df, spark, violations_table=TABLE_NAME, run_timestamp=run3_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Løst"
    assert rows["P1"].loest_tidspunkt == run3_ts.isoformat()
    assert rows["P1"].foerst_observert_tidspunkt == expected_first_seen  # still preserved

    # Run 4: already-Løst row is carried through unchanged.
    run4_ts = datetime(2026, 1, 4, tzinfo=timezone.utc)
    run4_df = _violations_df(spark, [])
    _apply_resolution_tracking(run4_df, spark, violations_table=TABLE_NAME, run_timestamp=run4_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Løst"
    assert rows["P1"].loest_tidspunkt == run3_ts.isoformat()  # unchanged


def test_resolution_distinct_violations_same_pk_and_column(spark):
    """
    Regression test for the A1 data-loss fix: group-style expectations
    (e.g. event_flow) can emit several distinct violations for the same
    (regel_id, primaernoekkel_verdi, avvikende_kolonne), differing only in
    forventet_betingelse. Both must survive resolution, not collapse to one.
    """
    _create_empty_table(spark)

    run1_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run1_df = _violations_df(spark, [
        _violation_row(
            pk="GROUP-1", avvikende_kolonne="milestone",
            forventet_betingelse="Both 'start' and 'stop' must exist",
            run_timestamp=run1_ts, foerst_observert_tidspunkt=run1_ts,
        ),
        _violation_row(
            pk="GROUP-1", avvikende_kolonne="milestone",
            forventet_betingelse="Both 'start' and 'gate' must exist",
            run_timestamp=run1_ts, foerst_observert_tidspunkt=run1_ts,
        ),
    ])
    _apply_resolution_tracking(run1_df, spark, violations_table=TABLE_NAME, run_timestamp=run1_ts)

    stored = spark.table(TABLE_NAME).collect()
    assert len(stored) == 2
    assert {r.forventet_betingelse for r in stored} == {
        "Both 'start' and 'stop' must exist",
        "Both 'start' and 'gate' must exist",
    }
