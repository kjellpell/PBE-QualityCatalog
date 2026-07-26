"""
Catalog-level `where:` filtering, including the per-rule_group override.

The override is the contract documented in config/QualityCatalogRuntime.py.
Its value shape changed when catalog_filter collapsed to a predicate string,
so it is worth pinning rather than left to the comment alone.
"""

import pytest


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [(1, "PB360"), (2, "PB360"), (3, "ALTINN")], "id int, fagsystem string"
    )


def test_where_filters_the_source(runner, df):
    filtered, description = runner._apply_catalog_where(df, "fagsystem = 'PB360'", "Faser")
    assert filtered.count() == 2
    assert description == "fagsystem = 'PB360'"


def test_absent_where_is_a_passthrough(runner, df):
    filtered, description = runner._apply_catalog_where(df, None, "Faser")
    assert filtered.count() == 3
    assert description is None


def test_non_string_where_is_rejected(runner, df):
    """The retired mapping form fails loudly rather than being ignored."""
    with pytest.raises(ValueError, match="must be a SQL predicate string"):
        runner._apply_catalog_where(df, {"type": "custom", "where_clause": "x"}, "Faser")


def test_override_replaces_the_catalog_where(runner, monkeypatch):
    catalog = {"rule_group": "Faser", "where": "fagsystem = 'PB360'"}
    monkeypatch.setattr(
        runner.RUNTIME, "CATALOG_FILTER_OVERRIDES", {"Faser": "fagsystem = 'ALTINN'"},
        raising=False,
    )
    assert runner._resolve_catalog_where(catalog, runner.RUNTIME) == "fagsystem = 'ALTINN'"


def test_override_of_none_disables_the_catalog_where(runner, monkeypatch):
    catalog = {"rule_group": "Faser", "where": "fagsystem = 'PB360'"}
    monkeypatch.setattr(
        runner.RUNTIME, "CATALOG_FILTER_OVERRIDES", {"Faser": None}, raising=False
    )
    assert runner._resolve_catalog_where(catalog, runner.RUNTIME) is None


def test_override_for_another_group_is_ignored(runner, monkeypatch):
    catalog = {"rule_group": "Faser", "where": "fagsystem = 'PB360'"}
    monkeypatch.setattr(
        runner.RUNTIME, "CATALOG_FILTER_OVERRIDES", {"Faktura": "1 = 0"}, raising=False
    )
    assert runner._resolve_catalog_where(catalog, runner.RUNTIME) == "fagsystem = 'PB360'"


def test_documented_example_shape_is_valid(runner, spark):
    """The example in QualityCatalogRuntime.py must actually resolve."""
    source = spark.createDataFrame(
        [(1, "2026-07-01"), (2, "2020-01-01")], "id int, sist_endret string"
    ).selectExpr("id", "CAST(sist_endret AS DATE) AS sist_endret")

    predicate = "sist_endret >= date_sub(current_date(), 90) OR sist_endret IS NULL"
    filtered, _ = runner._apply_catalog_where(source, predicate, "Faser")
    assert filtered.count() == 1
