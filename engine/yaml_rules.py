# =============================================================================
# engine/yaml_rules.py
#
# Simplified YAML rule parser for sequence ordering, pair validation, and
# gate validation rules.
#
# This module provides a user-friendly shorthand YAML schema that is easier
# to author than the full verbose format, especially for non-technical users.
# It validates rules, applies sensible defaults, and translates to the verbose
# format consumed by CUSTOM_EXPECTATION_REGISTRY.
#
# Supported simplified rule types:
#   sequence_order  - checks that values appear in the expected order
#   pair_validation - checks that both values of each pair exist per group
#   gate            - checks that each group has a required completion value
#
# Usage:
#   from engine.yaml_rules import parse_simplified_rules
#
#   raw = yaml.safe_load(open("my_rules.yaml"))
#   verbose_rules = parse_simplified_rules(raw)
#
# Simplified YAML format (minimal example):
#   - sequence_order:
#       group: "order_id"
#       column: "status"
#       values: ["Draft", "Review", "Approve"]
#   - pair_validation:
#       group: "process_id"
#       column: "step"
#       pairs: [["Start", "Stop"]]
#   - gate:
#       group: "process_id"
#       column: "step"
#       value_to_check: "Approved"
#
# Defaults applied automatically:
#   sort_column  -> "Sluttdato"  (for all three rule types)
#   gate.trigger -> "Approval completed"
# =============================================================================

# Default values applied when the user omits optional fields.
DEFAULT_SORT_COLUMN = "Sluttdato"
DEFAULT_GATE_TRIGGER = "Approval completed"

# Supported simplified rule types and their required fields.
_REQUIRED_FIELDS = {
    "sequence_order":  ["group", "column", "values"],
    "pair_validation": ["group", "column", "pairs"],
    "gate":            ["group", "column", "value_to_check"],
}


def _validate_sequence_order(idx: int, cfg: dict, errors: list) -> None:
    """Validate a sequence_order rule configuration and append errors."""
    label = f"Rule {idx + 1} [sequence_order]"

    group = cfg.get("group", "")
    if not group or not str(group).strip():
        errors.append(f"{label}: 'group' field cannot be empty.")

    column = cfg.get("column", "")
    if not column or not str(column).strip():
        errors.append(f"{label}: 'column' field cannot be empty.")

    values = cfg.get("values")
    if values is None:
        errors.append(f"{label}: 'values' field is required.")
    elif not isinstance(values, list) or len(values) == 0:
        errors.append(f"{label}: 'values' list cannot be empty.")
    elif len(values) < 2:
        errors.append(
            f"{label}: 'values' must contain at least 2 items to define an order."
        )


def _validate_pair_validation(idx: int, cfg: dict, errors: list) -> None:
    """Validate a pair_validation rule configuration and append errors."""
    label = f"Rule {idx + 1} [pair_validation]"

    group = cfg.get("group", "")
    if not group or not str(group).strip():
        errors.append(f"{label}: 'group' field cannot be empty.")

    column = cfg.get("column", "")
    if not column or not str(column).strip():
        errors.append(f"{label}: 'column' field cannot be empty.")

    pairs = cfg.get("pairs")
    if pairs is None:
        errors.append(f"{label}: 'pairs' field is required.")
    elif not isinstance(pairs, list) or len(pairs) == 0:
        errors.append(f"{label}: 'pairs' list cannot be empty.")
    else:
        for pi, pair in enumerate(pairs):
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                errors.append(
                    f"{label}: pair at index {pi} must be a two-element list "
                    f"[start_value, stop_value]."
                )


def _validate_gate(idx: int, cfg: dict, errors: list) -> None:
    """Validate a gate rule configuration and append errors."""
    label = f"Rule {idx + 1} [gate]"

    group = cfg.get("group", "")
    if not group or not str(group).strip():
        errors.append(f"{label}: 'group' field cannot be empty.")

    column = cfg.get("column", "")
    if not column or not str(column).strip():
        errors.append(f"{label}: 'column' field cannot be empty.")

    value_to_check = cfg.get("value_to_check")
    if value_to_check is None or str(value_to_check).strip() == "":
        errors.append(f"{label}: 'value_to_check' field is required and cannot be empty.")


def _translate_sequence_order(idx: int, cfg: dict) -> dict:
    """Translate a simplified sequence_order rule to the verbose format."""
    sort_col = cfg.get("sort_column") or DEFAULT_SORT_COLUMN
    return {
        "rule_id":     f"simplified_seq_{idx + 1}",
        "name":        f"Sequence order: {cfg['group']}",
        "expectation": "validate_sequence_order",
        "parameters":  {
            "value_column":      cfg["column"],
            "group_column":      cfg["group"],
            "sort_column":       sort_col,
            "expected_sequence": list(cfg["values"]),
        },
    }


def _translate_pair_validation(idx: int, cfg: dict) -> dict:
    """Translate a simplified pair_validation rule to the verbose format."""
    sort_col = cfg.get("sort_column") or DEFAULT_SORT_COLUMN
    return {
        "rule_id":     f"simplified_pair_{idx + 1}",
        "name":        f"Pair validation: {cfg['group']}",
        "expectation": "validate_paired_presence",
        "parameters":  {
            "value_column":   cfg["column"],
            "group_column":   cfg["group"],
            "required_pairs": [list(p) for p in cfg["pairs"]],
            "sort_column":    sort_col,
        },
    }


def _translate_gate(idx: int, cfg: dict) -> dict:
    """Translate a simplified gate rule to the verbose format."""
    trigger  = cfg.get("trigger") or DEFAULT_GATE_TRIGGER
    sort_col = cfg.get("sort_column") or DEFAULT_SORT_COLUMN
    return {
        "rule_id":     f"simplified_gate_{idx + 1}",
        "name":        f"Gate: {trigger}",
        "expectation": "validate_gate",
        "parameters":  {
            "value_column":   cfg["column"],
            "group_column":   cfg["group"],
            "value_to_check": cfg["value_to_check"],
            "trigger":        trigger,
            "sort_column":    sort_col,
        },
    }


_VALIDATORS = {
    "sequence_order":  _validate_sequence_order,
    "pair_validation": _validate_pair_validation,
    "gate":            _validate_gate,
}

_TRANSLATORS = {
    "sequence_order":  _translate_sequence_order,
    "pair_validation": _translate_pair_validation,
    "gate":            _translate_gate,
}


def parse_simplified_rules(raw_rules: list) -> list:
    """
    Parse a list of simplified YAML rule dicts, validate them, apply defaults,
    and return the equivalent list of verbose rule dicts ready for use with
    CUSTOM_EXPECTATION_REGISTRY.

    Each element of ``raw_rules`` must be a dict with exactly one key that
    names the rule type (``sequence_order``, ``pair_validation``, or ``gate``).
    The value is a dict of rule configuration fields.

    Validation errors are accumulated across all rules.  If any errors are
    found they are printed in a structured format and a ``ValueError`` is
    raised to stop pipeline execution.

    Parameters
    ----------
    raw_rules : list
        List of simplified rule dicts loaded from YAML.

    Returns
    -------
    list
        Verbose rule dicts compatible with ``CUSTOM_EXPECTATION_REGISTRY``.

    Raises
    ------
    ValueError
        If any rule fails validation.  The error message contains a full
        summary of all validation failures.
    """
    if not isinstance(raw_rules, list):
        raise ValueError(
            "YAML rule input must be a list of rule items. "
            "Check the top-level structure of your rules YAML."
        )

    errors  = []
    results = []

    for idx, item in enumerate(raw_rules):
        if not isinstance(item, dict):
            errors.append(
                f"Rule {idx + 1}: expected a mapping (dict) but got "
                f"{type(item).__name__}."
            )
            continue

        keys = list(item.keys())
        if len(keys) != 1:
            errors.append(
                f"Rule {idx + 1}: each rule entry must have exactly one key "
                f"(rule type), but found {len(keys)} keys: {keys}."
            )
            continue

        rule_type = keys[0]
        cfg = item[rule_type]

        if rule_type not in _VALIDATORS:
            errors.append(
                f"Rule {idx + 1}: unknown rule type '{rule_type}'. "
                f"Supported types: {sorted(_VALIDATORS)}."
            )
            continue

        if not isinstance(cfg, dict):
            errors.append(
                f"Rule {idx + 1} [{rule_type}]: rule body must be a mapping "
                f"(dict) but got {type(cfg).__name__}."
            )
            continue

        _VALIDATORS[rule_type](idx, cfg, errors)
        results.append((rule_type, idx, cfg))

    if errors:
        header = f"YAML Validation Errors ({len(errors)} found):"
        lines  = "\n".join(f"  - {e}" for e in errors)
        footer = f"\nTotal errors: {len(errors)}."
        message = f"\n{header}\n{lines}{footer}"
        print(message)
        raise ValueError(message)

    return [_TRANSLATORS[rt](i, cfg) for rt, i, cfg in results]
