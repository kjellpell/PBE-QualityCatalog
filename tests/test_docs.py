"""
Documentation drift guards.

The rule-type reference in RULES_GUIDE.md is derived from RULE_TYPES. This
test regenerates it and compares, so the docs cannot quietly fall behind the
engine. On failure it prints the block to paste in.
"""

from pathlib import Path

from tests.notebook_source import engine_namespace

RULE_TYPES = engine_namespace().RULE_TYPES

DOC = Path(__file__).resolve().parent.parent / "RULES_GUIDE.md"
BEGIN = "<!-- BEGIN RULE TYPES (generated — see tests/test_docs.py) -->"
END = "<!-- END RULE TYPES -->"


def _expected_table() -> str:
    lines = [
        "| Rule type | Scope | Required keys |",
        "|---|---|---|",
    ]
    for rule_type in RULE_TYPES.values():
        required = (
            ", ".join(f"`{key}`" for key in rule_type.required)
            if rule_type.required
            else "–"
        )
        lines.append(f"| `{rule_type.name}` | {rule_type.scope} | {required} |")
    return "\n".join(lines)


def _documented_table() -> str:
    text = DOC.read_text(encoding="utf-8")
    block = text.split(BEGIN)[1].split(END)[0]
    return "\n".join(line for line in block.strip().splitlines() if line.strip())


def test_rule_type_reference_matches_the_engine():
    expected = _expected_table()
    documented = _documented_table()
    assert documented == expected, (
        "RULES_GUIDE.md rule-type table is out of date. Replace the block "
        f"between the markers with:\n\n{expected}\n"
    )


def test_every_rule_type_is_documented():
    text = DOC.read_text(encoding="utf-8")
    for name in RULE_TYPES:
        assert f"`{name}`" in text, f"Rule type '{name}' is not mentioned in RULES_GUIDE.md"


def test_no_retired_vocabulary_survives_in_docs():
    """The old expectation names and scaffolding should be gone from the guide."""
    text = DOC.read_text(encoding="utf-8")
    for retired in (
        "expectation:", "parameters:", "not_null_when", "combination_unique",
        "catalog_filter", "where_clause", "reference_exists", "reference_active",
        "left_column", "filter_column", "CUSTOM_EXPECTATION_REGISTRY",
    ):
        assert retired not in text, f"RULES_GUIDE.md still references '{retired}'"


def test_rules_use_only_supported_keys(rule_sources):
    """Every shipped rule uses a key the engine actually understands."""
    import yaml

    reserved = {"rule_id", "name", "description", "when", "pk_column"}
    for name, text in sorted(rule_sources.items()):
        catalog = yaml.safe_load(text)
        for rule in catalog["rules"]:
            unknown = set(rule) - reserved - set(RULE_TYPES)
            assert not unknown, f"{name} / {rule.get('rule_id')}: unknown keys {unknown}"
