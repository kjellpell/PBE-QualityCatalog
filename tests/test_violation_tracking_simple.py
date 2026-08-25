"""
QC_ViolationTracking: the same lifecycle contract as tests/test_resolution.py
(new -> Aktiv, still-active preserves foerst_observert_tidspunkt, absent ->
Løst, already-Løst carried through unchanged), checked against the extracted,
QC_Engine-independent notebook. Also covers stamp_violations() and the full
check_event_flow -> stamp_violations -> apply_resolution_tracking pipeline.
"""

from datetime import date, datetime, timezone

from tests.notebook_source import load_notebook

_TRACKING = load_notebook("QC_ViolationTracking")
VIOLATION_SCHEMA = _TRACKING.VIOLATION_SCHEMA
stamp_violations = _TRACKING.stamp_violations
apply_resolution_tracking = _TRACKING.apply_resolution_tracking

check_event_flow = load_notebook("QC_EventFlow").check_event_flow

TABLE_NAME = "test_avvik_simple"


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


def _create_empty_table(spark, table_name=TABLE_NAME):
    empty = spark.createDataFrame([], schema=VIOLATION_SCHEMA)
    empty.write.format("delta").mode("overwrite").saveAsTable(table_name)


def _table_rows(spark, table_name=TABLE_NAME):
    return {r.primaernoekkel_verdi: r for r in spark.table(table_name).collect()}


def _spark_roundtrip_timestamp(spark, value: datetime) -> datetime:
    """Mirror Spark's TimestampType conversion for stable timezone-agnostic asserts."""
    return spark.createDataFrame([(value,)], "ts timestamp").collect()[0].ts


def test_resolution_lifecycle(spark):
    _create_empty_table(spark)

    # Run 1: brand-new violation -> inserted as Aktiv.
    run1_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run1_df = _violations_df(spark, [_violation_row(run_timestamp=run1_ts, foerst_observert_tidspunkt=run1_ts)])
    apply_resolution_tracking(run1_df, spark, violations_table=TABLE_NAME, run_timestamp=run1_ts)

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
    apply_resolution_tracking(run2_df, spark, violations_table=TABLE_NAME, run_timestamp=run2_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Aktiv"
    assert rows["P1"].foerst_observert_tidspunkt == expected_first_seen  # preserved, not run2_ts
    assert rows["P1"].kjoert_id == "run-2"  # metadata otherwise refreshed

    # Run 3: violation absent -> marked Løst with a loest_tidspunkt (closed).
    run3_ts = datetime(2026, 1, 3, tzinfo=timezone.utc)
    run3_df = _violations_df(spark, [])
    apply_resolution_tracking(run3_df, spark, violations_table=TABLE_NAME, run_timestamp=run3_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Løst"
    assert rows["P1"].loest_tidspunkt == run3_ts.isoformat()
    assert rows["P1"].foerst_observert_tidspunkt == expected_first_seen  # still preserved

    # Run 4: already-Løst row is carried through unchanged.
    run4_ts = datetime(2026, 1, 4, tzinfo=timezone.utc)
    run4_df = _violations_df(spark, [])
    apply_resolution_tracking(run4_df, spark, violations_table=TABLE_NAME, run_timestamp=run4_ts)

    rows = _table_rows(spark)
    assert set(rows) == {"P1"}
    assert rows["P1"].avviksstatus == "Løst"
    assert rows["P1"].loest_tidspunkt == run3_ts.isoformat()  # unchanged


def test_resolution_distinct_violations_same_pk_and_column(spark):
    """
    A group-scoped check can emit several distinct violations for the same
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
    apply_resolution_tracking(run1_df, spark, violations_table=TABLE_NAME, run_timestamp=run1_ts)

    stored = spark.table(TABLE_NAME).collect()
    assert len(stored) == 2
    assert {r.forventet_betingelse for r in stored} == {
        "Both 'start' and 'stop' must exist",
        "Both 'start' and 'gate' must exist",
    }


def test_stamp_violations_attaches_run_and_rule_metadata(spark):
    raw = spark.createDataFrame(
        [("P1", "SAK-1", "col_a", "bad", "col_a IS NOT NULL", "detail")],
        "primaernoekkel_verdi string, identifikator_verdi string, "
        "avvikende_kolonne string, faktisk_verdi string, "
        "forventet_betingelse string, avviksdetaljer string",
    )
    ts = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    stamped = stamp_violations(
        raw,
        kjoert_id="run-abc",
        kjoert_tidspunkt=ts,
        regelgruppe="Faser",
        regel_id="FAS-999",
        regelnavn="Test rule",
        tabellnavn="faser",
        avviksomfang="Gruppe",
    )

    # Column names, order, and types must match — nullability is not compared,
    # since F.lit() columns come back non-nullable even where VIOLATION_SCHEMA
    # allows NULL (same as QC_Engine's identical stamping code).
    assert stamped.schema.fieldNames() == VIOLATION_SCHEMA.fieldNames()
    assert [f.dataType for f in stamped.schema] == [f.dataType for f in VIOLATION_SCHEMA]
    row = stamped.collect()[0]
    assert row.kjoert_id == "run-abc"
    assert row.kjoert_dato == ts.date()
    assert row.regel_id == "FAS-999"
    assert row.avviksstatus == "Aktiv"
    assert row.loest_tidspunkt is None
    assert row.avviksomfang == "Gruppe"
    assert row.primaernoekkel_verdi == "P1"


def test_full_pipeline_check_event_flow_to_resolution(spark):
    """check_event_flow -> stamp_violations -> apply_resolution_tracking,
    with no dependency on QC_Engine, end to end across two runs."""
    table = "test_avvik_pipeline"
    _create_empty_table(spark, table)

    flow_cfg = dict(
        event_column="ev", group_column="grp", order_column="d", cycle=["A", "B"],
    )

    def _run(events, run_id, ts):
        rows = [("g1", ev, date(2026, 1, 1 + i)) for i, ev in enumerate(events)]
        df = spark.createDataFrame(rows, "grp string, ev string, d date")
        violations = check_event_flow(df, **flow_cfg)
        stamped = stamp_violations(
            violations,
            kjoert_id=run_id, kjoert_tidspunkt=ts,
            regelgruppe="Milepæler", regel_id="MIL-004", regelnavn="Pairing",
            tabellnavn="milepaeler", avviksomfang="Gruppe",
        )
        apply_resolution_tracking(stamped, spark, violations_table=table, run_timestamp=ts)

    # Run 1: unclosed pass -> a violation, opened.
    ts1 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    _run(["A"], "run-1", ts1)
    rows = _table_rows(spark, table)
    assert set(rows) == {"g1"}
    assert rows["g1"].avviksstatus == "Aktiv"
    assert rows["g1"].loest_tidspunkt is None

    # Run 2: now closed -> violation resolved.
    ts2 = datetime(2026, 1, 2, tzinfo=timezone.utc)
    _run(["A", "B"], "run-2", ts2)
    rows = _table_rows(spark, table)
    assert set(rows) == {"g1"}
    assert rows["g1"].avviksstatus == "Løst"
    assert rows["g1"].loest_tidspunkt == ts2.isoformat()
