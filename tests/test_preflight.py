"""
Preflight tests.

The point of preflight is that a bad rule fails at deploy time rather than at
03:00 in a scheduled job, so these check that real mistakes are actually
caught — especially predicate typos, which the old column-name-list approach
could not see.
"""

import importlib
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(scope="module")
def preflight():
    return importlib.import_module("nb_dq_01_preflight")


@pytest.fixture
def probe(spark):
    from tests import fixtures
    fixtures.create_source_tables(spark)
    return spark.read.table("saksbehandling.faser")


@pytest.fixture
def columns(probe):
    return set(probe.columns)


def _rule(**kwargs):
    base = {"rule_id": "T-001", "name": "test rule"}
    base.update(kwargs)
    return base


def test_valid_predicate_passes(preflight, probe, columns):
    assert preflight.check_rule(_rule(check="tidsbruk >= 0"), probe, columns, "t.yaml") == []


def test_typo_in_predicate_is_caught(preflight, probe, columns):
    """The claim that justifies this format over hand-written SQL."""
    errors = preflight.check_rule(_rule(check="tidsbrukk >= 0"), probe, columns, "t.yaml")
    assert len(errors) == 1
    assert "does not resolve" in errors[0]


def test_syntax_error_in_predicate_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(_rule(check="tidsbruk >>> 0"), probe, columns, "t.yaml")
    assert errors and "does not resolve" in errors[0]


def test_typo_in_when_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(
        _rule(when="nosuchcol IS NULL", check="tidsbruk >= 0"), probe, columns, "t.yaml"
    )
    assert any("'when' does not resolve" in e for e in errors)


def test_missing_rule_type_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(_rule(), probe, columns, "t.yaml")
    assert any("No rule type found" in e for e in errors)


def test_structured_type_missing_key_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(
        _rule(pairs_present={"event_column": "indikator"}), probe, columns, "t.yaml"
    )
    assert any("missing required key" in e for e in errors)


def test_structured_type_bad_column_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(
        _rule(pairs_present={
            "event_column": "nope", "group_column": "stage_recno", "required_pairs": [["a", "b"]],
        }),
        probe, columns, "t.yaml",
    )
    assert any("event_column 'nope' not found" in e for e in errors)


def test_reference_table_is_not_checked_against_source_columns(preflight, probe, columns):
    """
    The old _RULE_COLUMN_KEYS treated reference_table as a source column and
    warned on every reference rule. A reference table is not a source column.
    """
    errors = preflight.check_rule(
        _rule(exists_in={
            "column": "indikator", "table": "some.other_table", "reference_column": "kode",
        }),
        probe, columns, "t.yaml",
    )
    assert errors == []


def test_unknown_key_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(
        _rule(check="tidsbruk >= 0", parameters={"columns": ["x"]}), probe, columns, "t.yaml"
    )
    assert any("Unrecognised key" in e for e in errors)


def test_catalog_requires_pk_column(preflight, probe, columns):
    catalog = {"rules": [_rule(check="tidsbruk >= 0")]}
    errors = preflight.check_catalog(catalog, probe, columns, "t.yaml")
    assert any("Missing 'pk_column'" in e for e in errors)


def test_catalog_where_is_validated(preflight, probe, columns):
    catalog = {
        "pk_column": "stage_recno",
        "where": "nosuchcol = 'X'",
        "rules": [_rule(check="tidsbruk >= 0")],
    }
    errors = preflight.check_catalog(catalog, probe, columns, "t.yaml")
    assert any("'where' does not resolve" in e for e in errors)


def test_duplicate_rule_id_is_caught(preflight, probe, columns):
    catalog = {
        "pk_column": "stage_recno",
        "rules": [_rule(check="tidsbruk >= 0"), _rule(check="bransjetid >= 0")],
    }
    errors = preflight.check_catalog(catalog, probe, columns, "t.yaml")
    assert any("Duplicate rule_id" in e for e in errors)


def test_real_catalogs_pass_preflight(preflight, spark):
    """Every shipped catalog must pass against the fixture schemas."""
    import yaml
    from tests import fixtures

    fixtures.create_source_tables(spark)
    all_errors = []
    for path in sorted((REPO_ROOT / "rules").glob("*.yaml")):
        catalog = yaml.safe_load(path.read_text(encoding="utf-8"))
        full_table = f"{catalog['database']}.{catalog['table']}"
        source_df = spark.read.table(full_table)
        cols = preflight._source_columns(spark, catalog, full_table)

        from pyspark.sql import functions as F
        probe = source_df
        for column in sorted(cols - set(source_df.columns)):
            probe = probe.withColumn(column, F.lit(None).cast("string"))

        all_errors.extend(preflight.check_catalog(catalog, probe, cols, path.name))

    assert all_errors == [], "\n".join(all_errors)
