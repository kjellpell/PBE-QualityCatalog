"""
Catalog loading tests.

A rule catalog that cannot be loaded used to be skipped with a warning. That is
the worst possible failure mode for a data-quality system: the run reports
Succeeded over fewer rules, and the quality score can *rise*, because the rules
that vanished included the failing ones. These tests pin the loud behaviour.
"""

import pytest


_GOOD = """
rule_group: Faser
table: faser
database: saksbehandling
pk_column: stage_recno
rules:
- rule_id: T-001
  name: test rule
  check: tidsbruk >= 0
"""


def test_a_valid_catalog_loads(runner):
    catalogs = runner.load_rule_catalogs({"good": _GOOD})

    assert [c["rule_group"] for c in catalogs] == ["Faser"]
    assert len(catalogs[0]["rules"]) == 1


def test_catalogs_load_in_key_order(runner):
    """Run order must not depend on dict insertion order in the QC_Rules cells."""
    catalogs = runner.load_rule_catalogs({
        "zebra": _GOOD.replace("rule_group: Faser", "rule_group: Zebra"),
        "alpha": _GOOD.replace("rule_group: Faser", "rule_group: Alpha"),
    })

    assert [c["rule_group"] for c in catalogs] == ["Alpha", "Zebra"]


@pytest.mark.parametrize(
    "name,content,reason",
    [
        ("broken", "rule_group: [unclosed\n", "could not be parsed"),
        ("scalar", "just a string\n", "did not parse to a mapping"),
        ("empty", "rule_group: Faser\ntable: faser\nrules: []\n", "contains no rules"),
    ],
)
def test_an_unusable_catalog_fails_the_run(runner, name, content, reason):
    with pytest.raises(RuntimeError) as exc:
        runner.load_rule_catalogs({name: content})

    assert name in str(exc.value)
    assert reason in str(exc.value)


def test_one_bad_catalog_fails_even_when_others_load(runner):
    """
    The dangerous case: the run would otherwise succeed on the good catalog and
    quietly drop the other one's rules.
    """
    with pytest.raises(RuntimeError, match="broken"):
        runner.load_rule_catalogs({"good": _GOOD, "broken": "rule_group: [unclosed\n"})


def test_no_catalogs_at_all_fails(runner):
    with pytest.raises(RuntimeError, match="RULE_CATALOG_SOURCES is empty"):
        runner.load_rule_catalogs({})


def test_the_shipped_catalogs_load(runner, rule_sources):
    """The rule set actually shipped must survive the loader unchanged."""
    catalogs = runner.load_rule_catalogs(rule_sources)

    assert [c["rule_group"] for c in catalogs] == ["Faktura", "Faser", "Milepæler"]
