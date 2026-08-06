"""
QC_Setup_Tables.

The DDL is generated from the engine's own schemas, so the thing worth pinning
is that the tables it creates are the ones the engine then writes to — same
names, same columns — and that a table whose shape has drifted is reported
rather than quietly patched.
"""

import pytest

SCHEMA = "dqsetup"
OTHER_SCHEMA = "dqsetupalt"


def _config(**over):
    config = {
        "DEFAULT_SCHEMA": SCHEMA,
        "DQ_RESULTS_TABLE": "dq_run_results",
        "DQ_VIOLATIONS_TABLE": "dq_violations",
        "DQ_EXECUTION_METRICS_TABLE": "dq_execution_metrics",
    }
    config.update(over)
    return config


@pytest.fixture
def clean(spark):
    for schema in (SCHEMA, OTHER_SCHEMA):
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    yield
    for schema in (SCHEMA, OTHER_SCHEMA):
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_creates_the_three_tables_and_is_rerunnable(spark, setup_tables, clean):
    created = setup_tables.setup_dq_tables(_config())

    assert created == [
        f"{SCHEMA}.dq_run_results",
        f"{SCHEMA}.dq_violations",
        f"{SCHEMA}.dq_execution_metrics",
    ]
    for table in created:
        assert spark.catalog.tableExists(table)

    # No migration path, but re-running against unchanged schemas must be a no-op.
    assert setup_tables.setup_dq_tables(_config()) == created


def test_columns_match_the_schemas_the_engine_writes(spark, setup_tables, engine, clean):
    setup_tables.setup_dq_tables(_config())

    for table, struct in (
        (f"{SCHEMA}.dq_run_results", engine.RESULT_SCHEMA),
        (f"{SCHEMA}.dq_violations", engine.VIOLATION_SCHEMA),
        (f"{SCHEMA}.dq_execution_metrics", engine._EXECUTION_METRIC_SCHEMA),
    ):
        actual = [(f.name.lower(), f.dataType.simpleString()) for f in spark.table(table).schema.fields]
        expected = [(f.name.lower(), f.dataType.simpleString()) for f in struct.fields]
        assert actual == expected


def test_a_schema_qualified_table_name_is_honoured(spark, setup_tables, clean):
    """Setup must create the table the engine will actually write to.

    The engine resolves targets with `_qualify`, which leaves an already
    qualified name alone. Setup has to agree, or it creates
    `datakvalitet.otherdb.dq_run_results` while the runner writes to
    `otherdb.dq_run_results`.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OTHER_SCHEMA}")

    created = setup_tables.setup_dq_tables(
        _config(DQ_RESULTS_TABLE=f"{OTHER_SCHEMA}.dq_run_results")
    )

    assert created[0] == f"{OTHER_SCHEMA}.dq_run_results"
    assert spark.catalog.tableExists(f"{OTHER_SCHEMA}.dq_run_results")

    # Nothing landed under DEFAULT_SCHEMA for it. Asserted by listing rather
    # than tableExists, because the wrong name is three-part and Spark rejects
    # it outright instead of answering False.
    assert "dq_run_results" not in {t.name for t in spark.catalog.listTables(SCHEMA)}


def test_a_drifted_table_is_reported_not_migrated(spark, setup_tables, clean):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    spark.sql(f"CREATE TABLE {SCHEMA}.dq_run_results (run_id STRING) USING DELTA")

    with pytest.raises(RuntimeError) as exc:
        setup_tables.setup_dq_tables(_config())

    assert "already exists with a different schema" in str(exc.value)
    assert "missing columns" in str(exc.value)
