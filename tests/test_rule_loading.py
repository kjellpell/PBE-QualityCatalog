"""
Catalog loading tests.

A rule catalog that cannot be loaded used to be skipped with a warning. That is
the worst possible failure mode for a data-quality system: the run reports
Succeeded over fewer rules, and the quality score can *rise*, because the rules
that vanished included the failing ones. These tests pin the loud behaviour.
"""

import pytest


@pytest.fixture
def rules_dir(runner, tmp_path, monkeypatch):
    """Point _load_all_rules at a throwaway catalog directory."""
    monkeypatch.setattr(runner, "_resolve_rules_dir", lambda: tmp_path)
    return tmp_path


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


def test_a_valid_catalog_loads(runner, rules_dir):
    (rules_dir / "good.yaml").write_text(_GOOD, encoding="utf-8")

    catalogs = runner._load_all_rules()

    assert [c["rule_group"] for c in catalogs] == ["Faser"]
    assert len(catalogs[0]["rules"]) == 1


@pytest.mark.parametrize(
    "name,content,reason",
    [
        ("broken.yaml", "rule_group: [unclosed\n", "could not be parsed"),
        ("scalar.yaml", "just a string\n", "did not parse to a mapping"),
        ("empty.yaml", "rule_group: Faser\ntable: faser\nrules: []\n", "contains no rules"),
    ],
)
def test_an_unusable_catalog_fails_the_run(runner, rules_dir, name, content, reason):
    (rules_dir / name).write_text(content, encoding="utf-8")

    with pytest.raises(RuntimeError) as exc:
        runner._load_all_rules()

    assert name in str(exc.value)
    assert reason in str(exc.value)


def test_one_bad_catalog_fails_even_when_others_load(runner, rules_dir):
    """
    The dangerous case: the run would otherwise succeed on the good catalog and
    quietly drop the other one's rules.
    """
    (rules_dir / "good.yaml").write_text(_GOOD, encoding="utf-8")
    (rules_dir / "broken.yaml").write_text("rule_group: [unclosed\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="broken.yaml"):
        runner._load_all_rules()


def test_an_empty_rules_dir_fails(runner, rules_dir):
    with pytest.raises(RuntimeError, match="No YAML rule files found"):
        runner._load_all_rules()
