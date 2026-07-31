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
        _rule(event_flow={"event_column": "indikator"}), probe, columns, "t.yaml"
    )
    assert any("missing required key" in e for e in errors)


def test_structured_type_bad_column_is_caught(preflight, probe, columns):
    errors = preflight.check_rule(
        _rule(event_flow={
            "event_column": "nope", "group_column": "stage_recno",
            "order_column": "decisiondate", "cycle": ["a", "b"],
        }),
        probe, columns, "t.yaml",
    )
    assert any("event_column 'nope' not found" in e for e in errors)


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
        probe, join_errors = preflight.build_probe(spark, catalog, full_table)
        all_errors.extend(join_errors)
        all_errors.extend(
            preflight.check_catalog(catalog, probe, set(probe.columns), path.name)
        )

    assert all_errors == [], "\n".join(all_errors)


# --------------------------------------------------------------------------
# join probe
# --------------------------------------------------------------------------

def _faser_catalog():
    import yaml
    return yaml.safe_load((REPO_ROOT / "rules" / "faser.yaml").read_text(encoding="utf-8"))


def test_probe_carries_joined_columns_with_real_types(preflight, spark):
    """
    Joined columns must keep their real types. Synthesising them as strings
    would let a type-sensitive predicate pass preflight and fail at run time.
    """
    from tests import fixtures
    fixtures.create_source_tables(spark)

    probe, errors = preflight.build_probe(spark, _faser_catalog(), "saksbehandling.faser")
    assert errors == []
    types = dict(probe.dtypes)

    # case_recno is an int in saksbehandling.saker. The previous probe
    # synthesised every joined column as a string, so this is the assertion
    # that actually discriminates between the two approaches.
    assert types["case_recno"] == "int"
    assert types["saksansvarlig_kode"] == "string"
    assert types["tidligste_startmilepael_dato"] == "date"


def test_probe_type_checks_predicates(preflight, spark):
    """Predicates are resolved against real types, so a type misuse is caught."""
    from tests import fixtures
    fixtures.create_source_tables(spark)

    probe, _ = preflight.build_probe(spark, _faser_catalog(), "saksbehandling.faser")
    # size() requires an array or map; saksansvarlig_kode is a string.
    errors = preflight.check_predicate(
        probe, "size(saksansvarlig_kode) > 0", "[t.yaml / T-1] 'check'"
    )
    assert errors and "does not resolve" in errors[0]


def test_probe_reports_a_missing_join_table(preflight, spark):
    from tests import fixtures
    fixtures.create_source_tables(spark)

    catalog = {"joins": [{"table": "saksbehandling.nope", "left_on": "a", "right_on": "b"}]}
    _, errors = preflight.build_probe(spark, catalog, "saksbehandling.faser")
    assert any("table 'saksbehandling.nope' not found" in e for e in errors)


def test_probe_reports_bad_join_keys_and_select(preflight, spark):
    from tests import fixtures
    fixtures.create_source_tables(spark)

    catalog = {"joins": [{
        "table": "saksbehandling.saker", "left_on": "nosuch", "right_on": "case_recno",
    }]}
    _, errors = preflight.build_probe(spark, catalog, "saksbehandling.faser")
    assert any("left_on 'nosuch' not found in source" in e for e in errors)

    catalog = {"joins": [{
        "table": "saksbehandling.saker", "left_on": "to_case_recno",
        "right_on": "case_recno", "select": ["nosuch"],
    }]}
    _, errors = preflight.build_probe(spark, catalog, "saksbehandling.faser")
    assert any("select column(s) not in" in e for e in errors)


def test_probe_reports_a_join_without_keys(preflight, spark):
    from tests import fixtures
    fixtures.create_source_tables(spark)

    catalog = {"joins": [{"table": "saksbehandling.saker"}]}
    _, errors = preflight.build_probe(spark, catalog, "saksbehandling.faser")
    assert any("needs 'on', or both 'left_on' and 'right_on'" in e for e in errors)
