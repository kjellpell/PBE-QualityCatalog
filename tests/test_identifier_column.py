"""
identifier_column / identifikator_verdi: the optional, catalog-level sibling of
pk_column. Unlike pk_column it is never required, never used as a key, and
exists purely so a violation can be traced to a human-meaningful identifier
(e.g. saksnummer) even when the row's own primary key is a technical id.

The shipped catalogs' own event_flow rules (MIL-004/MIL-005) happen to
evaluate zero groups against the fixture data (a pre-existing, unrelated gap
in that data), so the group-scoped path is exercised directly here instead,
the same way tests/test_event_flow.py exercises event_flow directly.
"""

from tests.notebook_source import engine_namespace

run_rule = engine_namespace().run_rule


def test_row_scoped_identifier_is_resolved(spark):
    df = spark.createDataFrame(
        [(1, 0, "SAK-001"), (2, -5, "SAK-002")],
        "id int, amount int, saksnummer string",
    )
    result, violations = run_rule(
        {"check": "amount >= 0"}, df, spark,
        pk_column="id", identifier_column="saksnummer",
    )
    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert row.primaernoekkel_verdi == "2"
    assert row.identifikator_verdi == "SAK-002"


def test_row_scoped_identifier_defaults_to_null_when_unconfigured(spark):
    df = spark.createDataFrame([(1, -5, "SAK-001")], "id int, amount int, saksnummer string")
    result, violations = run_rule({"check": "amount >= 0"}, df, spark, pk_column="id")
    row = violations.collect()[0]
    assert row.identifikator_verdi is None


def test_row_scoped_identifier_is_null_when_the_value_itself_is_null(spark):
    """Mirrors a left join that found no match: identifier_column resolves, its
    value doesn't."""
    df = spark.createDataFrame(
        [(1, -5, None)], "id int, amount int, saksnummer string",
    )
    result, violations = run_rule(
        {"check": "amount >= 0"}, df, spark,
        pk_column="id", identifier_column="saksnummer",
    )
    row = violations.collect()[0]
    assert row.identifikator_verdi is None


def test_unique_identifier_is_resolved(spark):
    df = spark.createDataFrame(
        [(1, "A", "SAK-001"), (2, "A", "SAK-002")],
        "id int, code string, saksnummer string",
    )
    result, violations = run_rule(
        {"unique": ["code"]}, df, spark,
        pk_column="id", identifier_column="saksnummer",
    )
    assert result["status"] == "Ikke bestått"
    identifiers = {row.identifikator_verdi for row in violations.collect()}
    assert identifiers == {"SAK-001", "SAK-002"}


def test_group_scoped_identifier_is_resolved(spark):
    """required_event: identifikator_verdi comes from a lookup rebuilt against the
    group, not a direct column reference, since the group-scoped frame has
    already dropped every column but the group key by the time violations are
    assembled."""
    df = spark.createDataFrame(
        [
            ("case-1", "Sendt", "SAK-001"),
            ("case-2", "Annet", "SAK-002"),
        ],
        "case_id string, event string, saksnummer string",
    )
    result, violations = run_rule(
        {
            "required_event": {
                "event_column": "event", "group_column": "case_id", "value": "Sendt",
            },
        },
        df, spark,
        identifier_column="saksnummer",
    )
    assert result["status"] == "Ikke bestått"
    row = violations.collect()[0]
    assert row.primaernoekkel_verdi == "case-2"
    assert row.identifikator_verdi == "SAK-002"


def test_group_scoped_identifier_defaults_to_null_when_unconfigured(spark):
    df = spark.createDataFrame(
        [("case-1", "Annet", "SAK-001")], "case_id string, event string, saksnummer string",
    )
    result, violations = run_rule(
        {
            "required_event": {
                "event_column": "event", "group_column": "case_id", "value": "Sendt",
            },
        },
        df, spark,
    )
    row = violations.collect()[0]
    assert row.identifikator_verdi is None


def test_group_scoped_identifier_is_null_when_unmatched(spark):
    """Every row in the failing group has a NULL saksnummer (e.g. the join to
    saker never matched), so the group-level lookup has nothing to find."""
    df = spark.createDataFrame(
        [("case-1", "Annet", None)], "case_id string, event string, saksnummer string",
    )
    result, violations = run_rule(
        {
            "required_event": {
                "event_column": "event", "group_column": "case_id", "value": "Sendt",
            },
        },
        df, spark,
        identifier_column="saksnummer",
    )
    row = violations.collect()[0]
    assert row.identifikator_verdi is None
