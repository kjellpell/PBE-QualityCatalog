# =============================================================================
# tests/test_yaml_rules.py
#
# Unit tests for engine/yaml_rules.py.
# Tests cover simplified YAML rule parsing, default value application, and
# validation error reporting.
#
# Run with:
#   pytest tests/test_yaml_rules.py -v
# =============================================================================

import pytest
from engine.yaml_rules import (
    load_simplified_yaml,
    parse_simplified_rules,
    _apply_yaml_defaults,
    _format_dry_run_summary,
    _format_validation_table,
    DEFAULT_SORT_COLUMN,
    DEFAULT_GATE_TRIGGER,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_seq_rule(overrides=None):
    """Return a minimal valid sequence_order rule item."""
    cfg = {
        "group":  "order_id",
        "column": "status",
        "values": ["Draft", "Review", "Approve"],
    }
    if overrides:
        cfg.update(overrides)
    return {"sequence_order": cfg}


def _make_pair_rule(overrides=None):
    """Return a minimal valid pair_validation rule item."""
    cfg = {
        "group":  "process_id",
        "column": "step",
        "pairs":  [["Start", "Stop"]],
    }
    if overrides:
        cfg.update(overrides)
    return {"pair_validation": cfg}


def _make_gate_rule(overrides=None):
    """Return a minimal valid gate rule item."""
    cfg = {
        "group":          "process_id",
        "column":         "step",
        "value_to_check": "Approved",
    }
    if overrides:
        cfg.update(overrides)
    return {"gate": cfg}


# ---------------------------------------------------------------------------
# Happy-path: single rules
# ---------------------------------------------------------------------------

class TestParseSimplifiedRulesHappyPath:

    def test_sequence_order_minimal_produces_verbose_rule(self):
        rules = parse_simplified_rules([_make_seq_rule()])
        assert len(rules) == 1
        r = rules[0]
        assert r["expectation"] == "validate_sequence_order"
        p = r["parameters"]
        assert p["value_column"] == "status"
        assert p["group_column"] == "order_id"
        assert p["expected_sequence"] == ["Draft", "Review", "Approve"]

    def test_sequence_order_default_sort_column_applied(self):
        rules = parse_simplified_rules([_make_seq_rule()])
        assert rules[0]["parameters"]["sort_column"] == DEFAULT_SORT_COLUMN

    def test_sequence_order_explicit_sort_column_kept(self):
        rules = parse_simplified_rules([_make_seq_rule({"sort_column": "EventDate"})])
        assert rules[0]["parameters"]["sort_column"] == "EventDate"

    def test_pair_validation_minimal_produces_verbose_rule(self):
        rules = parse_simplified_rules([_make_pair_rule()])
        assert len(rules) == 1
        r = rules[0]
        assert r["expectation"] == "validate_paired_presence"
        p = r["parameters"]
        assert p["value_column"] == "step"
        assert p["group_column"] == "process_id"
        assert p["required_pairs"] == [["Start", "Stop"]]

    def test_pair_validation_default_sort_column_applied(self):
        rules = parse_simplified_rules([_make_pair_rule()])
        assert rules[0]["parameters"]["sort_column"] == DEFAULT_SORT_COLUMN

    def test_pair_validation_explicit_sort_column_kept(self):
        rules = parse_simplified_rules([_make_pair_rule({"sort_column": "Rank"})])
        assert rules[0]["parameters"]["sort_column"] == "Rank"

    def test_gate_minimal_produces_verbose_rule(self):
        rules = parse_simplified_rules([_make_gate_rule()])
        assert len(rules) == 1
        r = rules[0]
        assert r["expectation"] == "validate_gate"
        p = r["parameters"]
        assert p["value_column"] == "step"
        assert p["group_column"] == "process_id"
        assert p["value_to_check"] == "Approved"

    def test_gate_default_trigger_applied(self):
        rules = parse_simplified_rules([_make_gate_rule()])
        assert rules[0]["parameters"]["trigger"] == DEFAULT_GATE_TRIGGER

    def test_gate_explicit_trigger_kept(self):
        rules = parse_simplified_rules([_make_gate_rule({"trigger": "Signed off"})])
        assert rules[0]["parameters"]["trigger"] == "Signed off"

    def test_gate_default_sort_column_applied(self):
        rules = parse_simplified_rules([_make_gate_rule()])
        assert rules[0]["parameters"]["sort_column"] == DEFAULT_SORT_COLUMN

    def test_gate_explicit_sort_column_kept(self):
        rules = parse_simplified_rules([_make_gate_rule({"sort_column": "ClosedDate"})])
        assert rules[0]["parameters"]["sort_column"] == "ClosedDate"

    def test_multiple_rules_of_different_types(self):
        rules = parse_simplified_rules([_make_seq_rule(), _make_pair_rule(), _make_gate_rule()])
        assert len(rules) == 3
        assert rules[0]["expectation"] == "validate_sequence_order"
        assert rules[1]["expectation"] == "validate_paired_presence"
        assert rules[2]["expectation"] == "validate_gate"

    def test_rule_ids_are_unique_per_type(self):
        rules = parse_simplified_rules([_make_seq_rule(), _make_seq_rule({"group": "g2"})])
        ids = [r["rule_id"] for r in rules]
        assert ids[0] != ids[1]

    def test_pair_validation_tuple_pairs_converted_to_lists(self):
        """Pairs can be provided as tuples; they should be normalised to lists."""
        raw = [{"pair_validation": {
            "group":  "gid",
            "column": "col",
            "pairs":  [("A", "B")],
        }}]
        rules = parse_simplified_rules(raw)
        assert rules[0]["parameters"]["required_pairs"] == [["A", "B"]]

    def test_multiple_pairs_supported(self):
        raw = [{"pair_validation": {
            "group":  "gid",
            "column": "col",
            "pairs":  [["A", "B"], ["C", "D"]],
        }}]
        rules = parse_simplified_rules(raw)
        assert rules[0]["parameters"]["required_pairs"] == [["A", "B"], ["C", "D"]]


# ---------------------------------------------------------------------------
# Validation errors — required fields missing
# ---------------------------------------------------------------------------

class TestParseSimplifiedRulesValidation:

    def _expect_errors(self, raw_rules, *expected_snippets):
        """Assert that parse_simplified_rules raises ValueError containing all snippets."""
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw_rules)
        msg = str(exc_info.value)
        for snippet in expected_snippets:
            assert snippet in msg, (
                f"Expected '{snippet}' in error message, but got:\n{msg}"
            )

    # ---- sequence_order ----

    def test_seq_missing_group(self):
        raw = [{"sequence_order": {"column": "col", "values": ["A", "B"]}}]
        self._expect_errors(raw, "[sequence_order]", "'group' field cannot be empty")

    def test_seq_empty_group(self):
        raw = [{"sequence_order": {"group": "", "column": "col", "values": ["A", "B"]}}]
        self._expect_errors(raw, "[sequence_order]", "'group' field cannot be empty")

    def test_seq_missing_column(self):
        raw = [{"sequence_order": {"group": "g", "values": ["A", "B"]}}]
        self._expect_errors(raw, "[sequence_order]", "'column' field cannot be empty")

    def test_seq_missing_values(self):
        raw = [{"sequence_order": {"group": "g", "column": "col"}}]
        self._expect_errors(raw, "[sequence_order]", "'values' field is required")

    def test_seq_empty_values_list(self):
        raw = [{"sequence_order": {"group": "g", "column": "col", "values": []}}]
        self._expect_errors(raw, "[sequence_order]", "'values' list cannot be empty")

    def test_seq_single_value_insufficient(self):
        raw = [{"sequence_order": {"group": "g", "column": "col", "values": ["A"]}}]
        self._expect_errors(raw, "[sequence_order]", "at least 2 items")

    # ---- pair_validation ----

    def test_pair_missing_group(self):
        raw = [{"pair_validation": {"column": "col", "pairs": [["A", "B"]]}}]
        self._expect_errors(raw, "[pair_validation]", "'group' field cannot be empty")

    def test_pair_missing_column(self):
        raw = [{"pair_validation": {"group": "g", "pairs": [["A", "B"]]}}]
        self._expect_errors(raw, "[pair_validation]", "'column' field cannot be empty")

    def test_pair_missing_pairs(self):
        raw = [{"pair_validation": {"group": "g", "column": "col"}}]
        self._expect_errors(raw, "[pair_validation]", "'pairs' field is required")

    def test_pair_empty_pairs_list(self):
        raw = [{"pair_validation": {"group": "g", "column": "col", "pairs": []}}]
        self._expect_errors(raw, "[pair_validation]", "'pairs' list cannot be empty")

    def test_pair_malformed_pair_entry(self):
        raw = [{"pair_validation": {
            "group": "g", "column": "col",
            "pairs": [["A", "B", "C"]],   # 3-element pair — invalid
        }}]
        self._expect_errors(raw, "[pair_validation]", "two-element list")

    def test_pair_single_string_not_a_pair(self):
        raw = [{"pair_validation": {
            "group": "g", "column": "col",
            "pairs": ["A"],              # plain string instead of [start, stop]
        }}]
        self._expect_errors(raw, "[pair_validation]", "two-element list")

    # ---- gate ----

    def test_gate_missing_group(self):
        raw = [{"gate": {"column": "col", "value_to_check": "X"}}]
        self._expect_errors(raw, "[gate]", "'group' field cannot be empty")

    def test_gate_missing_column(self):
        raw = [{"gate": {"group": "g", "value_to_check": "X"}}]
        self._expect_errors(raw, "[gate]", "'column' field cannot be empty")

    def test_gate_missing_value_to_check(self):
        raw = [{"gate": {"group": "g", "column": "col"}}]
        self._expect_errors(raw, "[gate]", "'value_to_check' field is required")

    def test_gate_empty_value_to_check(self):
        raw = [{"gate": {"group": "g", "column": "col", "value_to_check": ""}}]
        self._expect_errors(raw, "[gate]", "'value_to_check' field is required")

    # ---- structural errors ----

    def test_unknown_rule_type(self):
        raw = [{"unknown_type": {"group": "g"}}]
        self._expect_errors(raw, "unknown rule type", "unknown_type")

    def test_non_dict_rule_item(self):
        raw = ["not a dict"]
        self._expect_errors(raw, "expected a mapping")

    def test_rule_with_multiple_keys(self):
        raw = [{"sequence_order": {}, "gate": {}}]
        self._expect_errors(raw, "exactly one key")

    def test_non_dict_rule_body(self):
        raw = [{"sequence_order": "not a dict"}]
        self._expect_errors(raw, "rule body must be a mapping")

    def test_input_not_a_list(self):
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules({"sequence_order": {}})
        assert "must be a list" in str(exc_info.value)

    # ---- multiple errors accumulated ----

    def test_multiple_errors_all_reported(self):
        raw = [
            {"sequence_order": {"column": "col", "values": ["A", "B"]}},  # missing group
            {"pair_validation": {"group": "g", "column": "col", "pairs": []}},  # empty pairs
            {"gate": {"group": "g", "column": "col"}},                     # missing value_to_check
        ]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        msg = str(exc_info.value)
        assert "Total errors: 3" in msg
        assert "[sequence_order]" in msg
        assert "[pair_validation]" in msg
        assert "[gate]" in msg

    def test_error_message_contains_total_count(self):
        raw = [
            {"sequence_order": {"column": "col", "values": []}},  # 2 errors: group + values
        ]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        msg = str(exc_info.value)
        assert "Total errors:" in msg


# ---------------------------------------------------------------------------
# _apply_yaml_defaults
# ---------------------------------------------------------------------------

class TestApplyYamlDefaults:

    def _seq_item(self, cfg):
        return {"sequence_order": cfg}

    def test_no_defaults_returns_rules_unchanged(self):
        rules = [self._seq_item({"group": "g", "column": "c", "values": ["A", "B"]})]
        result = _apply_yaml_defaults(rules, {})
        assert result == rules

    def test_group_column_default_applied_as_group(self):
        rules = [self._seq_item({"column": "c", "values": ["A", "B"]})]
        result = _apply_yaml_defaults(rules, {"group_column": "bu"})
        assert result[0]["sequence_order"]["group"] == "bu"

    def test_sort_column_default_applied(self):
        rules = [self._seq_item({"group": "g", "column": "c", "values": ["A", "B"]})]
        result = _apply_yaml_defaults(rules, {"sort_column": "MyDate"})
        assert result[0]["sequence_order"]["sort_column"] == "MyDate"

    def test_rule_group_overrides_group_column_default(self):
        rules = [self._seq_item({"group": "explicit", "column": "c", "values": ["A", "B"]})]
        result = _apply_yaml_defaults(rules, {"group_column": "default_bu"})
        assert result[0]["sequence_order"]["group"] == "explicit"

    def test_rule_sort_column_overrides_default(self):
        rules = [self._seq_item({"group": "g", "column": "c", "values": ["A", "B"], "sort_column": "EventDate"})]
        result = _apply_yaml_defaults(rules, {"sort_column": "Sluttdato"})
        assert result[0]["sequence_order"]["sort_column"] == "EventDate"

    def test_multiple_defaults_applied_across_rules(self):
        rules = [
            {"sequence_order": {"column": "c", "values": ["A", "B"]}},
            {"pair_validation": {"column": "c", "pairs": [["A", "B"]]}},
        ]
        defaults = {"group_column": "bu", "sort_column": "MyDate"}
        result = _apply_yaml_defaults(rules, defaults)
        assert result[0]["sequence_order"]["group"] == "bu"
        assert result[0]["sequence_order"]["sort_column"] == "MyDate"
        assert result[1]["pair_validation"]["group"] == "bu"
        assert result[1]["pair_validation"]["sort_column"] == "MyDate"

    def test_non_dict_item_passed_through_unchanged(self):
        rules = ["not a dict"]
        result = _apply_yaml_defaults(rules, {"group_column": "bu"})
        assert result == ["not a dict"]

    def test_multi_key_item_passed_through_unchanged(self):
        item = {"sequence_order": {}, "gate": {}}
        result = _apply_yaml_defaults([item], {"group_column": "bu"})
        assert result == [item]

    def test_non_dict_cfg_passed_through_unchanged(self):
        item = {"sequence_order": "not a dict"}
        result = _apply_yaml_defaults([item], {"group_column": "bu"})
        assert result == [item]


# ---------------------------------------------------------------------------
# _format_validation_table
# ---------------------------------------------------------------------------

class TestFormatValidationTable:

    def test_table_contains_headers(self):
        table = _format_validation_table([])
        assert "Rule Type" in table
        assert "Rule #" in table
        assert "Status" in table
        assert "Error" in table

    def test_passed_rule_shows_passed_status(self):
        table = _format_validation_table([("sequence_order", 1, [])])
        assert "PASSED" in table
        assert "No issues detected" in table

    def test_failed_rule_shows_failed_status(self):
        table = _format_validation_table([("gate", 2, ["Missing 'group'"])])
        assert "FAILED" in table
        assert "Missing 'group'" in table

    def test_mixed_rules_in_table(self):
        results = [
            ("sequence_order", 1, []),
            ("pair_validation", 2, ["Empty pairs"]),
            ("gate", 3, []),
        ]
        table = _format_validation_table(results)
        assert "sequence_order" in table
        assert "pair_validation" in table
        assert "gate" in table
        assert "PASSED" in table
        assert "FAILED" in table

    def test_table_has_separator_lines(self):
        table = _format_validation_table([("gate", 1, [])])
        assert "+-" in table
        assert "-+" in table

    def test_multiple_errors_joined_with_semicolon(self):
        table = _format_validation_table([("sequence_order", 1, ["err1", "err2"])])
        assert "err1; err2" in table


# ---------------------------------------------------------------------------
# load_simplified_yaml — new-format dict with defaults
# ---------------------------------------------------------------------------

class TestLoadSimplifiedYaml:

    def _make_doc(self, defaults=None, rules=None):
        doc = {}
        if defaults is not None:
            doc["defaults"] = defaults
        if rules is not None:
            doc["rules"] = rules
        return doc

    # ---- backward compat: plain list ----

    def test_plain_list_accepted_as_legacy_format(self):
        raw = [{"sequence_order": {
            "group": "g", "column": "c", "values": ["A", "B"],
        }}]
        result = load_simplified_yaml(raw)
        assert len(result) == 1
        assert result[0]["expectation"] == "validate_sequence_order"

    # ---- new dict format ----

    def test_doc_with_defaults_and_rules(self):
        doc = self._make_doc(
            defaults={"sort_column": "Sluttdato", "group_column": "bu"},
            rules=[{"sequence_order": {"column": "c", "values": ["A", "B"]}}],
        )
        result = load_simplified_yaml(doc)
        assert len(result) == 1
        p = result[0]["parameters"]
        assert p["group_column"] == "bu"
        assert p["sort_column"] == "Sluttdato"

    def test_defaults_group_column_maps_to_group(self):
        doc = self._make_doc(
            defaults={"group_column": "business_unit"},
            rules=[{"gate": {"column": "step", "value_to_check": "Approved"}}],
        )
        result = load_simplified_yaml(doc)
        assert result[0]["parameters"]["group_column"] == "business_unit"

    def test_rule_overrides_default_sort_column(self):
        doc = self._make_doc(
            defaults={"sort_column": "Sluttdato", "group_column": "bu"},
            rules=[{"sequence_order": {
                "column": "c", "values": ["A", "B"], "sort_column": "EventDate",
            }}],
        )
        result = load_simplified_yaml(doc)
        assert result[0]["parameters"]["sort_column"] == "EventDate"

    def test_rule_overrides_default_group(self):
        doc = self._make_doc(
            defaults={"group_column": "default_bu"},
            rules=[{"sequence_order": {
                "group": "override_group", "column": "c", "values": ["A", "B"],
            }}],
        )
        result = load_simplified_yaml(doc)
        assert result[0]["parameters"]["group_column"] == "override_group"

    def test_no_defaults_section_applies_module_defaults(self):
        doc = self._make_doc(
            rules=[{"sequence_order": {
                "group": "g", "column": "c", "values": ["A", "B"],
            }}],
        )
        result = load_simplified_yaml(doc)
        assert result[0]["parameters"]["sort_column"] == DEFAULT_SORT_COLUMN

    def test_three_mixed_rules_with_defaults(self):
        doc = self._make_doc(
            defaults={"group_column": "bu", "sort_column": "Sluttdato"},
            rules=[
                {"sequence_order": {"column": "status", "values": ["Start", "Middle", "End"]}},
                {"pair_validation": {"column": "step", "pairs": [["Draft", "Finalized"]]}},
                {"gate": {"column": "step", "value_to_check": "Approved"}},
            ],
        )
        result = load_simplified_yaml(doc)
        assert len(result) == 3
        for r in result:
            assert r["parameters"]["group_column"] == "bu"
            assert r["parameters"]["sort_column"] == "Sluttdato"

    # ---- validation errors with new format ----

    def test_validation_error_in_new_format_raises(self):
        doc = self._make_doc(
            defaults={"group_column": "bu"},
            rules=[{"sequence_order": {"column": "c"}}],  # missing values
        )
        with pytest.raises(ValueError) as exc_info:
            load_simplified_yaml(doc)
        assert "values" in str(exc_info.value)

    def test_validation_summary_table_in_error_message(self):
        doc = self._make_doc(
            defaults={"group_column": "bu"},
            rules=[
                {"sequence_order": {"column": "c", "values": ["A", "B"]}},       # valid
                {"gate": {"column": "step"}},                                       # missing value_to_check
            ],
        )
        with pytest.raises(ValueError) as exc_info:
            load_simplified_yaml(doc)
        msg = str(exc_info.value)
        assert "Validation Summary" in msg
        assert "PASSED" in msg
        assert "FAILED" in msg

    # ---- structural errors ----

    def test_non_dict_non_list_raises(self):
        with pytest.raises(ValueError) as exc_info:
            load_simplified_yaml("not a doc")
        assert "mapping" in str(exc_info.value).lower() or "list" in str(exc_info.value).lower()

    def test_missing_rules_key_raises(self):
        with pytest.raises(ValueError) as exc_info:
            load_simplified_yaml({"defaults": {}})
        assert "rules" in str(exc_info.value)

    def test_rules_not_a_list_raises(self):
        with pytest.raises(ValueError) as exc_info:
            load_simplified_yaml({"rules": "not a list"})
        assert "list" in str(exc_info.value)

    def test_defaults_not_a_dict_raises(self):
        with pytest.raises(ValueError) as exc_info:
            load_simplified_yaml({"defaults": "bad", "rules": []})
        assert "defaults" in str(exc_info.value)

    def test_empty_defaults_section_allowed(self):
        doc = self._make_doc(
            defaults={},
            rules=[{"sequence_order": {"group": "g", "column": "c", "values": ["A", "B"]}}],
        )
        result = load_simplified_yaml(doc)
        assert len(result) == 1

    def test_null_defaults_section_allowed(self):
        doc = {"defaults": None, "rules": [
            {"sequence_order": {"group": "g", "column": "c", "values": ["A", "B"]}},
        ]}
        result = load_simplified_yaml(doc)
        assert len(result) == 1

    # ---- validation table printed on success ----

    def test_success_prints_validation_summary(self, capsys):
        doc = self._make_doc(
            defaults={"group_column": "bu"},
            rules=[{"sequence_order": {"column": "c", "values": ["A", "B"]}}],
        )
        load_simplified_yaml(doc)
        captured = capsys.readouterr()
        assert "Validation Summary" in captured.out
        assert "PASSED" in captured.out


# ---------------------------------------------------------------------------
# _format_dry_run_summary
# ---------------------------------------------------------------------------

class TestFormatDryRunSummary:

    def test_header_line_present(self):
        summary = _format_dry_run_summary([])
        assert "Dry Run Validation Summary:" in summary

    def test_passed_rule_bullet(self):
        summary = _format_dry_run_summary([("sequence_order", 1, [])])
        assert "- Rule 1 [sequence_order]: PASSED" in summary

    def test_failed_rule_bullet_with_reason(self):
        summary = _format_dry_run_summary([
            ("pair_validation", 2, ["Missing pair [\"Start\", \"Stop\"]"])
        ])
        assert "FAILED" in summary
        assert "Reason:" in summary
        assert "Missing pair" in summary

    def test_mixed_pass_fail(self):
        results = [
            ("sequence_order", 1, []),
            ("pair_validation", 2, ["Empty pairs list"]),
            ("gate", 3, []),
        ]
        summary = _format_dry_run_summary(results)
        assert "PASSED" in summary
        assert "FAILED" in summary
        assert "Rule 1 [sequence_order]: PASSED" in summary
        assert "Rule 3 [gate]: PASSED" in summary

    def test_multiple_errors_joined_in_reason(self):
        summary = _format_dry_run_summary([
            ("gate", 1, ["Missing 'group'", "Missing 'column'"])
        ])
        assert "Missing 'group'" in summary
        assert "Missing 'column'" in summary


# ---------------------------------------------------------------------------
# Dry-run mode: parse_simplified_rules(dry_run=True)
# ---------------------------------------------------------------------------

class TestDryRunMode:

    def test_dry_run_does_not_raise_on_validation_error(self):
        raw = [{"sequence_order": {"column": "col", "values": ["A", "B"]}}]  # missing group
        # Should NOT raise even though the rule is invalid
        result = parse_simplified_rules(raw, dry_run=True)
        # The invalid rule is excluded from the output
        assert result == []

    def test_dry_run_prints_dry_run_summary(self, capsys):
        raw = [{"sequence_order": {"column": "col", "values": ["A", "B"]}}]
        parse_simplified_rules(raw, dry_run=True)
        captured = capsys.readouterr()
        assert "Dry Run Validation Summary:" in captured.out

    def test_dry_run_prints_failed_rule(self, capsys):
        raw = [{"sequence_order": {"column": "col", "values": ["A", "B"]}}]
        parse_simplified_rules(raw, dry_run=True)
        captured = capsys.readouterr()
        assert "FAILED" in captured.out

    def test_dry_run_returns_only_valid_rules(self):
        raw = [
            _make_seq_rule(),                                          # valid
            {"sequence_order": {"column": "col", "values": ["A"]}},   # invalid: 1 value, missing group
        ]
        result = parse_simplified_rules(raw, dry_run=True)
        assert len(result) == 1
        assert result[0]["expectation"] == "validate_sequence_order"

    def test_dry_run_all_valid_returns_all_rules(self):
        raw = [_make_seq_rule(), _make_pair_rule(), _make_gate_rule()]
        result = parse_simplified_rules(raw, dry_run=True)
        assert len(result) == 3

    def test_dry_run_all_invalid_returns_empty_list(self):
        raw = [
            {"sequence_order": {"column": "col", "values": []}},   # invalid
            {"pair_validation": {"group": "g", "column": "c"}},    # missing pairs
        ]
        result = parse_simplified_rules(raw, dry_run=True)
        assert result == []

    def test_normal_mode_raises_on_error(self):
        raw = [{"sequence_order": {"column": "col", "values": ["A", "B"]}}]
        with pytest.raises(ValueError):
            parse_simplified_rules(raw, dry_run=False)

    def test_dry_run_summary_passes_shown_correctly(self, capsys):
        raw = [_make_seq_rule(), _make_pair_rule()]
        parse_simplified_rules(raw, dry_run=True)
        captured = capsys.readouterr()
        assert "PASSED" in captured.out

    # load_simplified_yaml also forwards dry_run

    def test_load_simplified_yaml_dry_run_does_not_raise(self):
        doc = {
            "defaults": {"group_column": "bu"},
            "rules": [
                {"sequence_order": {"column": "c"}},  # missing values
            ],
        }
        result = load_simplified_yaml(doc, dry_run=True)
        assert result == []

    def test_load_simplified_yaml_dry_run_prints_summary(self, capsys):
        doc = {
            "defaults": {"group_column": "bu"},
            "rules": [
                {"sequence_order": {"column": "c"}},  # missing values
                _make_gate_rule(),                     # valid
            ],
        }
        load_simplified_yaml(doc, dry_run=True)
        captured = capsys.readouterr()
        assert "Dry Run Validation Summary:" in captured.out

    def test_load_simplified_yaml_dry_run_returns_valid_rules_only(self):
        doc = {
            "defaults": {"group_column": "bu"},
            "rules": [
                {"sequence_order": {"column": "c"}},  # missing values — invalid
                _make_gate_rule(),                     # valid
            ],
        }
        result = load_simplified_yaml(doc, dry_run=True)
        assert len(result) == 1
        assert result[0]["expectation"] == "validate_gate"

    def test_load_simplified_yaml_legacy_format_dry_run(self):
        raw = [
            _make_seq_rule(),                                         # valid
            {"pair_validation": {"group": "g", "column": "c"}},      # missing pairs
        ]
        result = load_simplified_yaml(raw, dry_run=True)
        assert len(result) == 1
        assert result[0]["expectation"] == "validate_sequence_order"


# ---------------------------------------------------------------------------
# Real-life use case test coverage — invoice and milestone validators
# ---------------------------------------------------------------------------

class TestRealLifeUseCases:
    """Realistic YAML configurations derived from known business validator needs."""

    # ---- Invoice validation YAML ----

    def test_invoice_yaml_with_defaults(self):
        """An invoice YAML file validates status flow per invoice_id."""
        doc = {
            "defaults": {
                "group_column": "invoice_id",
                "sort_column": "event_date",
            },
            "rules": [
                {
                    "sequence_order": {
                        "column": "status",
                        "values": ["Created", "Approved", "Paid"],
                    }
                },
                {
                    "pair_validation": {
                        "column": "action",
                        "pairs": [["Issued", "Received"]],
                    }
                },
                {
                    "gate": {
                        "column": "status",
                        "value_to_check": "Paid",
                    }
                },
            ],
        }
        result = load_simplified_yaml(doc)
        assert len(result) == 3
        for r in result:
            assert r["parameters"]["group_column"] == "invoice_id"
            assert r["parameters"]["sort_column"] == "event_date"

    def test_invoice_yaml_override_sort_column_per_rule(self):
        """One rule in an invoice YAML uses a different sort column."""
        doc = {
            "defaults": {
                "group_column": "invoice_id",
                "sort_column": "event_date",
            },
            "rules": [
                {
                    "sequence_order": {
                        "column": "status",
                        "values": ["Created", "Approved"],
                        "sort_column": "processing_timestamp",  # override
                    }
                },
                {
                    "gate": {
                        "column": "status",
                        "value_to_check": "Approved",
                    }
                },
            ],
        }
        result = load_simplified_yaml(doc)
        assert result[0]["parameters"]["sort_column"] == "processing_timestamp"
        assert result[1]["parameters"]["sort_column"] == "event_date"

    # ---- Milestone validation YAML ----

    def test_milestone_yaml_with_defaults(self):
        """A milestone YAML validates that each project reaches key milestones."""
        doc = {
            "defaults": {
                "group_column": "project_id",
                "sort_column": "milestone_date",
            },
            "rules": [
                {
                    "sequence_order": {
                        "column": "phase",
                        "values": ["Initiation", "Planning", "Execution", "Closure"],
                    }
                },
                {
                    "pair_validation": {
                        "column": "event",
                        "pairs": [
                            ["Start", "Stop"],
                            ["Open", "Close"],
                        ],
                    }
                },
                {
                    "gate": {
                        "column": "phase",
                        "value_to_check": "Closure",
                    }
                },
            ],
        }
        result = load_simplified_yaml(doc)
        assert len(result) == 3
        # sequence_order
        assert result[0]["parameters"]["expected_sequence"] == [
            "Initiation", "Planning", "Execution", "Closure"
        ]
        # pair_validation — two pairs
        assert result[1]["parameters"]["required_pairs"] == [
            ["Start", "Stop"],
            ["Open", "Close"],
        ]
        # gate
        assert result[2]["parameters"]["value_to_check"] == "Closure"

    def test_milestone_yaml_partial_group_override(self):
        """One milestone rule targets a sub-group (task_id) instead of project_id."""
        doc = {
            "defaults": {
                "group_column": "project_id",
                "sort_column": "milestone_date",
            },
            "rules": [
                {
                    "sequence_order": {
                        "column": "phase",
                        "values": ["Initiation", "Closure"],
                    }
                },
                {
                    "gate": {
                        "group": "task_id",   # per-rule override
                        "column": "status",
                        "value_to_check": "Done",
                    }
                },
            ],
        }
        result = load_simplified_yaml(doc)
        assert result[0]["parameters"]["group_column"] == "project_id"
        assert result[1]["parameters"]["group_column"] == "task_id"

    # ---- Large number of rules ----

    def test_large_rule_set_all_valid(self):
        """Simulate a YAML file with many (50) rules — all should parse correctly."""
        rules = []
        for i in range(50):
            rules.append({
                "sequence_order": {
                    "group": f"group_{i}",
                    "column": f"col_{i}",
                    "values": ["A", "B", "C"],
                }
            })
        result = parse_simplified_rules(rules)
        assert len(result) == 50

    def test_large_rule_set_with_mixed_types(self):
        """50-rule YAML with a mix of all three rule types."""
        rules = []
        for i in range(17):
            rules.append(_make_seq_rule({"group": f"g_seq_{i}"}))
        for i in range(17):
            rules.append(_make_pair_rule({"group": f"g_pair_{i}"}))
        for i in range(16):
            rules.append(_make_gate_rule({"group": f"g_gate_{i}"}))
        result = parse_simplified_rules(rules)
        assert len(result) == 50

    def test_large_rule_set_dry_run_partial_failures(self):
        """50 rules where every 5th is invalid — dry_run returns the 40 valid ones."""
        rules = []
        for i in range(50):
            if i % 5 == 0:
                # Invalid: missing values list (10 rules at indices 0,5,10,...,45)
                rules.append({"sequence_order": {"group": f"g_{i}", "column": "col"}})
            else:
                rules.append(_make_seq_rule({"group": f"g_{i}"}))
        result = parse_simplified_rules(rules, dry_run=True)
        assert len(result) == 40  # 50 total - 10 invalid = 40 valid


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge cases: mismatched group IDs, duplicate entries, whitespace, etc."""

    def test_group_id_whitespace_only_rejected(self):
        raw = [{"sequence_order": {"group": "   ", "column": "c", "values": ["A", "B"]}}]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        assert "'group' field cannot be empty" in str(exc_info.value)

    def test_column_whitespace_only_rejected(self):
        raw = [{"sequence_order": {"group": "g", "column": "  ", "values": ["A", "B"]}}]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        assert "'column' field cannot be empty" in str(exc_info.value)

    def test_value_to_check_whitespace_only_rejected(self):
        raw = [{"gate": {"group": "g", "column": "c", "value_to_check": "   "}}]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        assert "'value_to_check' field is required" in str(exc_info.value)

    def test_values_list_with_duplicate_entries_accepted(self):
        """Duplicate sequence values are a data concern, not a parse error."""
        raw = [{"sequence_order": {
            "group": "g", "column": "c", "values": ["A", "A", "B"],
        }}]
        result = parse_simplified_rules(raw)
        assert result[0]["parameters"]["expected_sequence"] == ["A", "A", "B"]

    def test_pairs_with_identical_start_stop_accepted(self):
        """Pairs where start == stop are unusual but not a parse-level error."""
        raw = [{"pair_validation": {
            "group": "g", "column": "c", "pairs": [["X", "X"]],
        }}]
        result = parse_simplified_rules(raw)
        assert result[0]["parameters"]["required_pairs"] == [["X", "X"]]

    def test_sequence_order_two_item_minimum_accepted(self):
        raw = [{"sequence_order": {"group": "g", "column": "c", "values": ["Start", "End"]}}]
        result = parse_simplified_rules(raw)
        assert result[0]["parameters"]["expected_sequence"] == ["Start", "End"]

    def test_empty_rules_list_produces_empty_output(self):
        result = parse_simplified_rules([])
        assert result == []

    def test_empty_rules_via_load_simplified_yaml(self):
        doc = {"rules": []}
        result = load_simplified_yaml(doc)
        assert result == []

    def test_rule_with_none_group_rejected(self):
        """Explicit None value for 'group' must be rejected with a clear error."""
        raw = [{"sequence_order": {"group": None, "column": "c", "values": ["A", "B"]}}]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        assert "'group' field cannot be empty" in str(exc_info.value)

    def test_rule_with_none_column_rejected(self):
        raw = [{"sequence_order": {"group": "g", "column": None, "values": ["A", "B"]}}]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        assert "'column' field cannot be empty" in str(exc_info.value)

    def test_defaults_group_column_none_not_applied(self):
        """A None group_column default is not applied — rule still needs own group."""
        rules = [{"sequence_order": {"column": "c", "values": ["A", "B"]}}]
        result = _apply_yaml_defaults(rules, {"group_column": None})
        # group becomes None, which should then fail validation
        with pytest.raises(ValueError):
            parse_simplified_rules(result)

    def test_rule_index_in_error_message_is_one_based(self):
        """Error messages should reference rules starting at 1, not 0."""
        raw = [
            _make_seq_rule(),                                          # Rule 1 — valid
            {"pair_validation": {"group": "g", "column": "c"}},       # Rule 2 — invalid
        ]
        with pytest.raises(ValueError) as exc_info:
            parse_simplified_rules(raw)
        assert "Rule 2" in str(exc_info.value)

    def test_multiline_defaults_applied_to_all_rule_types(self):
        """Defaults propagate correctly to each of the three rule types."""
        doc = {
            "defaults": {
                "group_column": "shared_group",
                "sort_column": "SharedDate",
            },
            "rules": [
                _make_seq_rule(),
                _make_pair_rule(),
                _make_gate_rule(),
            ],
        }
        # Remove any existing 'group' so defaults kick in for all three
        for item in doc["rules"]:
            rule_type = next(iter(item))
            item[rule_type].pop("group", None)

        result = load_simplified_yaml(doc)
        assert len(result) == 3
        for r in result:
            assert r["parameters"]["group_column"] == "shared_group"
            assert r["parameters"]["sort_column"] == "SharedDate"

    def test_dry_run_with_entirely_invalid_yaml_returns_empty(self):
        """Dry run on a totally broken rule set silently returns []."""
        raw = [
            {"sequence_order": {}},        # missing group, column, values
            {"pair_validation": {}},       # missing group, column, pairs
            {"gate": {}},                  # missing group, column, value_to_check
        ]
        result = parse_simplified_rules(raw, dry_run=True)
        assert result == []

    def test_numeric_group_value_accepted(self):
        """Group values that are integers (e.g. from YAML unquoted) are accepted."""
        raw = [{"sequence_order": {
            "group": 12345,   # numeric group id, as YAML would parse an unquoted integer
            "column": "status",
            "values": ["A", "B"],
        }}]
        result = parse_simplified_rules(raw)
        assert result[0]["parameters"]["group_column"] == 12345

