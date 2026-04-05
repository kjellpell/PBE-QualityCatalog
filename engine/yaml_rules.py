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
#   from engine.yaml_rules import load_simplified_yaml
#
#   raw = yaml.safe_load(open("my_rules.yaml"))
#   verbose_rules = load_simplified_yaml(raw)
#
# New YAML format with top-level defaults (recommended):
#   defaults:
#     sort_column: "Sluttdato"
#     group_column: "business_unit"
#   rules:
#     - sequence_order:
#         column: "status"
#         values: ["Draft", "Review", "Approve"]
#     - pair_validation:
#         column: "step"
#         pairs: [["Start", "Stop"]]
#     - gate:
#         column: "step"
#         value_to_check: "Approved"
#
# Legacy list format (still supported for backward compatibility):
#   - sequence_order:
#       group: "order_id"
#       column: "status"
#       values: ["Draft", "Review", "Approve"]
#
# Defaults applied automatically when omitted:
#   sort_column  -> "Sluttdato"  (for all three rule types)
#   gate.trigger -> "Approval completed"
#
# YAML-level defaults mapping:
#   defaults.group_column -> rule.group  (applied when rule omits 'group')
#   defaults.sort_column  -> rule.sort_column
#   Other keys in defaults are merged directly into each rule.
# =============================================================================

# Default values applied when the user omits optional fields.
DEFAULT_SORT_COLUMN = "Sluttdato"
DEFAULT_GATE_TRIGGER = "Approval completed"

# When dry_run=True is passed to parse_simplified_rules / load_simplified_yaml,
# validation errors are summarised and printed but do NOT raise ValueError.
# Only structurally valid (error-free) rules are translated and returned.

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


def _format_dry_run_summary(rule_results: list) -> str:
    """
    Format per-rule validation results as a dry-run bullet summary.

    Parameters
    ----------
    rule_results : list
        List of ``(rule_type, rule_num, errors)`` tuples where ``errors`` is
        a list of error strings for that rule (empty means the rule passed).

    Returns
    -------
    str
        A multi-line summary string with one bullet per rule, ready to be
        printed to pipeline logs.
    """
    lines = ["Dry Run Validation Summary:"]
    for rule_type, rule_num, rule_errors in rule_results:
        if rule_errors:
            reason = "; ".join(rule_errors)
            lines.append(
                f"- Rule {rule_num} [{rule_type}]: FAILED. Reason: {reason}"
            )
        else:
            lines.append(f"- Rule {rule_num} [{rule_type}]: PASSED")
    return "\n".join(lines)


def _format_validation_table(rule_results: list) -> str:
    """
    Format per-rule validation results as an ASCII table for Fabric pipeline
    logs.

    Parameters
    ----------
    rule_results : list
        List of ``(rule_type, rule_num, errors)`` tuples where ``errors`` is
        a list of error strings for that rule (empty means the rule passed).

    Returns
    -------
    str
        A multi-line ASCII table string ready to be printed.
    """
    headers = ["Rule Type", "Rule #", "Status", "Error"]
    rows = []
    for rule_type, rule_num, rule_errors in rule_results:
        status = "FAILED" if rule_errors else "PASSED"
        error_text = "; ".join(rule_errors) if rule_errors else "No issues detected"
        rows.append([str(rule_type), str(rule_num), status, error_text])

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"

    def _fmt_row(cells):
        return "| " + " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(cells)) + " |"

    lines = [sep, _fmt_row(headers), sep]
    for row in rows:
        lines.append(_fmt_row(row))
    lines.append(sep)
    return "\n".join(lines)


def _apply_yaml_defaults(raw_rules: list, defaults: dict) -> list:
    """
    Merge YAML-level defaults into each rule's configuration.

    Defaults are applied with setdefault semantics: a default value is only
    used when the corresponding key is absent from the individual rule config.
    Rule-specific values always take precedence.

    Special key mapping:
      ``group_column`` in defaults is applied as the ``group`` key in each
      rule, matching the simplified rule schema.  All other keys in defaults
      are passed through unchanged.

    Parameters
    ----------
    raw_rules : list
        The raw list of rule dicts from the YAML ``rules`` section.
    defaults : dict
        The defaults mapping from the top-level ``defaults`` section.

    Returns
    -------
    list
        A new list of rule dicts with defaults applied.  Structurally invalid
        items are passed through unchanged so that ``parse_simplified_rules``
        can produce the appropriate error messages.
    """
    if not defaults:
        return raw_rules

    merged = []
    for item in raw_rules:
        if not isinstance(item, dict):
            merged.append(item)
            continue

        keys = list(item.keys())
        if len(keys) != 1:
            merged.append(item)
            continue

        rule_type = keys[0]
        cfg = item[rule_type]

        if not isinstance(cfg, dict):
            merged.append(item)
            continue

        new_cfg = {}
        for key, val in defaults.items():
            if key == "group_column":
                new_cfg["group"] = val
            else:
                new_cfg[key] = val
        new_cfg.update(cfg)

        merged.append({rule_type: new_cfg})

    return merged


def parse_simplified_rules(raw_rules: list, *, dry_run: bool = False) -> list:
    """
    Parse a list of simplified YAML rule dicts, validate them, apply defaults,
    and return the equivalent list of verbose rule dicts ready for use with
    CUSTOM_EXPECTATION_REGISTRY.

    Each element of ``raw_rules`` must be a dict with exactly one key that
    names the rule type (``sequence_order``, ``pair_validation``, or ``gate``).
    The value is a dict of rule configuration fields.

    Validation errors are accumulated across all rules.  In normal mode, if
    any errors are found they are printed as a structured summary table and a
    ``ValueError`` is raised to stop pipeline execution.

    In dry-run mode (``dry_run=True``) validation errors are printed as a
    bullet summary but the function does **not** raise; only the error-free
    rules are translated and returned so the pipeline can continue safely.

    Parameters
    ----------
    raw_rules : list
        List of simplified rule dicts loaded from YAML (defaults already
        merged in by ``load_simplified_yaml`` if applicable).
    dry_run : bool, optional
        When ``True`` the function never raises ``ValueError``; instead it
        prints a dry-run summary and returns only the rules that passed
        validation.  Defaults to ``False``.

    Returns
    -------
    list
        Verbose rule dicts compatible with ``CUSTOM_EXPECTATION_REGISTRY``.
        In dry-run mode this list contains only the rules that passed
        validation.

    Raises
    ------
    ValueError
        If any rule fails validation and ``dry_run`` is ``False``.  The
        error message contains a full summary table of all validation results.
    """
    if not isinstance(raw_rules, list):
        raise ValueError(
            "YAML rule input must be a list of rule items. "
            "Check the top-level structure of your rules YAML."
        )

    all_errors   = []
    rule_results = []  # (rule_type, rule_num, rule_errors) for table display
    valid_rules  = []  # (rule_type, idx, cfg) for translation

    for idx, item in enumerate(raw_rules):
        rule_num = idx + 1

        if not isinstance(item, dict):
            msg = (
                f"Rule {rule_num}: expected a mapping (dict) but got "
                f"{type(item).__name__}."
            )
            all_errors.append(msg)
            rule_results.append(("(unknown)", rule_num, [msg]))
            continue

        keys = list(item.keys())
        if len(keys) != 1:
            msg = (
                f"Rule {rule_num}: each rule entry must have exactly one key "
                f"(rule type), but found {len(keys)} keys: {keys}."
            )
            all_errors.append(msg)
            rule_results.append(("(unknown)", rule_num, [msg]))
            continue

        rule_type = keys[0]
        cfg = item[rule_type]

        if rule_type not in _VALIDATORS:
            msg = (
                f"Rule {rule_num}: unknown rule type '{rule_type}'. "
                f"Supported types: {sorted(_VALIDATORS)}."
            )
            all_errors.append(msg)
            rule_results.append((rule_type, rule_num, [msg]))
            continue

        if not isinstance(cfg, dict):
            msg = (
                f"Rule {rule_num} [{rule_type}]: rule body must be a mapping "
                f"(dict) but got {type(cfg).__name__}."
            )
            all_errors.append(msg)
            rule_results.append((rule_type, rule_num, [msg]))
            continue

        rule_errors: list = []
        _VALIDATORS[rule_type](idx, cfg, rule_errors)
        all_errors.extend(rule_errors)
        rule_results.append((rule_type, rule_num, rule_errors))

        if not rule_errors:
            valid_rules.append((rule_type, idx, cfg))

    if all_errors:
        if dry_run:
            summary = _format_dry_run_summary(rule_results)
            print(summary)
        else:
            table   = _format_validation_table(rule_results)
            total   = len(all_errors)
            message = (
                f"\nValidation Summary:\n{table}"
                f"\n\nTotal errors: {total}."
            )
            print(message)
            raise ValueError(message)
    else:
        if dry_run:
            summary = _format_dry_run_summary(rule_results)
            print(summary)
        else:
            table   = _format_validation_table(rule_results)
            summary = f"\nValidation Summary:\n{table}"
            print(summary)

    return [_TRANSLATORS[rt](i, cfg) for rt, i, cfg in valid_rules]


def load_simplified_yaml(raw_doc, *, dry_run: bool = False) -> list:
    """
    Load a simplified YAML document that may include a top-level ``defaults``
    section and a ``rules`` list, then validate and translate all rules.

    Accepts two formats:

    1. **New format** (recommended) — a mapping with an optional ``defaults``
       section and a required ``rules`` list::

           defaults:
             sort_column: "Sluttdato"
             group_column: "business_unit"
           rules:
             - sequence_order:
                 column: "status"
                 values: ["Draft", "Review", "Approve"]

    2. **Legacy format** — a plain list of rule dicts (no defaults section),
       for backward compatibility with existing YAML files.

    The ``defaults`` mapping is merged into every rule before validation.
    The special key ``group_column`` in defaults is applied as the ``group``
    key in each rule.  Rule-specific values always override defaults.

    Parameters
    ----------
    raw_doc : dict or list
        The Python object produced by ``yaml.safe_load`` on a rules YAML file.
    dry_run : bool, optional
        When ``True``, validation errors are reported via a bullet summary but
        do **not** cause a ``ValueError``; only error-free rules are returned.
        Defaults to ``False``.

    Returns
    -------
    list
        Verbose rule dicts compatible with ``CUSTOM_EXPECTATION_REGISTRY``.
        In dry-run mode only the rules that passed validation are included.

    Raises
    ------
    ValueError
        If the document structure is invalid or any rule fails validation and
        ``dry_run`` is ``False``.
    """
    if isinstance(raw_doc, list):
        return parse_simplified_rules(raw_doc, dry_run=dry_run)

    if not isinstance(raw_doc, dict):
        raise ValueError(
            "YAML document must be a mapping with a 'rules' key, or a plain "
            "list of rule items.  Check the top-level structure of your YAML."
        )

    defaults = raw_doc.get("defaults")
    if defaults is None:
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError(
            "'defaults' section must be a mapping (dict).  "
            "Example:\n  defaults:\n    sort_column: \"Sluttdato\""
        )

    raw_rules = raw_doc.get("rules")
    if raw_rules is None:
        raise ValueError(
            "YAML document is missing the required 'rules' key.  "
            "Provide a list of rules under the 'rules' key."
        )
    if not isinstance(raw_rules, list):
        raise ValueError(
            "'rules' must be a list of rule items.  "
            "Check that 'rules:' is followed by a YAML sequence."
        )

    merged_rules = _apply_yaml_defaults(raw_rules, defaults)
    return parse_simplified_rules(merged_rules, dry_run=dry_run)
