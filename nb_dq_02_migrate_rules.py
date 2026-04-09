# =============================================================================
# nb_dq_02_migrate_rules.py
# One-time migration: reads all YAML rule files and inserts them into the
# rule_catalog Delta table.
#
# Run once after nb_dq_00_setup.py has created the rule_catalog table.
# Idempotent — skips silently if rule_catalog already has rows.
# YAML files are NOT moved or deleted; they remain in rules/ for reference.
# =============================================================================

import yaml
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# CELL 1 — Config and path resolution
# ---------------------------------------------------------------------------
# Use the same path resolution as the engine — avoids relative-path issues
# in Fabric notebooks where the working directory may not be the repo root.
sys.path.insert(0, str(Path(__file__).parent))
from engine.runtime import load_config_module, resolve_rules_dir  # noqa: E402

CONFIG, _ = load_config_module("QualityCatalogConfig")
IC_IDENTIFIER_FIELDS = CONFIG.IC_IDENTIFIER_FIELDS
rules_dir = resolve_rules_dir(CONFIG, Path(__file__).parent)

# ---------------------------------------------------------------------------
# CELL 2 — Guard: skip if already populated
# ---------------------------------------------------------------------------
existing = spark.table("rule_catalog").count()
if existing > 0:
    print(f"rule_catalog already has {existing} rows — skipping migration.")
    mssparkutils.notebook.exit("skipped")

# ---------------------------------------------------------------------------
# CELL 3 — Read YAML files and build rows
# ---------------------------------------------------------------------------
rows = []
now = datetime.now(timezone.utc)

yaml_files = sorted(rules_dir.glob("*.yaml"))
if not yaml_files:
    print(f"Warning: no *.yaml files found in {rules_dir}")
    mssparkutils.notebook.exit("no-rules")

for yaml_path in yaml_files:
    with open(yaml_path, encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)

    rule_group   = doc.get("rule_group", yaml_path.stem)
    source_table = doc.get("table", "")
    database     = doc.get("database", "")
    pk_column    = doc.get("pk_column", "id")
    # Serialise per-catalog join config once; all rules in a file share it
    joins_json = json.dumps(doc["joins"]) if doc.get("joins") else None

    for rule in doc.get("rules", []):
        # Detect rule type per-rule via IC identifier fields (not filename)
        rule_type = "IC" if any(rule.get(f) for f in IC_IDENTIFIER_FIELDS) else "DQ"
        params = rule.get("parameters", {})

        rows.append({
            "rule_id":              rule["rule_id"],
            "rule_type":            rule_type,
            "rule_group":           rule_group,
            "source_table":         source_table,
            "database":             database,
            "pk_column":            pk_column,
            "name":                 rule.get("name", ""),
            "description":          rule.get("description", ""),
            "expectation":          rule.get("expectation", ""),
            "column_name":          rule.get("column"),
            "parameters":           json.dumps(params) if params else None,
            "sql_expression":       rule.get("sql"),
            "joins_json":           joins_json,
            "severity":             rule.get("severity", "medium"),
            "category":             rule.get("category"),
            "owner":                rule.get("owner"),
            "owner_email":          rule.get("owner_email"),
            "control_ref":          rule.get("control_ref"),
            "control_type":         rule.get("control_type"),
            "risk_domain":          rule.get("risk_domain"),
            "remediation_due_days": rule.get("remediation_due_days"),
            "status":               "Active",
            "created_by":           "migration",
            "created_at":           now,
            "updated_at":           now,
        })

    print(f"  Parsed {yaml_path.name}: {len(doc.get('rules', []))} rules ({rule_type if 'rule_type' in dir() else 'DQ/IC'})")

# ---------------------------------------------------------------------------
# CELL 4 — Write to rule_catalog
# ---------------------------------------------------------------------------
spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable("rule_catalog")

print(f"\nMigration complete: {len(rows)} rules inserted into rule_catalog.")
print(f"YAML files remain in {rules_dir} — they are no longer read by the engine.")
mssparkutils.notebook.exit("OK")
