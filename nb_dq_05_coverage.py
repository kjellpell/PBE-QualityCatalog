# =============================================================================
# nb_dq_05_coverage.py
# Rule coverage report: shows which columns in source tables have validation
# rules and which are unmonitored.
#
# Output: qualitycatalog.dq_coverage_report (one row per table/column per run)
#
# Pipeline position: run after nb_dq_03_run_validation.py (standalone — does
# not depend on violation data, only on the YAML catalogs and source schemas).
# =============================================================================

from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
)

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

CONFIG_DIR = Path("/lakehouse/default/Files/Configs")
RULES_DIR = Path("/lakehouse/default/Files/rules")

# -----------------------------------------------------------------------------
# STEP 1: Load config
# -----------------------------------------------------------------------------
_cfg_spec = importlib.util.spec_from_file_location(
    "QualityCatalogConfig", str(CONFIG_DIR / "QualityCatalogConfig.py")
)
_cfg_mod = importlib.util.module_from_spec(_cfg_spec)
_cfg_spec.loader.exec_module(_cfg_mod)
cfg = _cfg_mod

SCHEMA = cfg.DEFAULT_SCHEMA
COVERAGE_TABLE = f"{SCHEMA}.dq_coverage_report"

COVERAGE_SCHEMA = StructType([
    StructField("snapshot_date",  DateType(),   False),
    StructField("table_name",     StringType(), False),
    StructField("column_name",    StringType(), False),
    StructField("has_rule",       BooleanType(), False),
    StructField("rule_ids",       StringType(), True),
])

# -----------------------------------------------------------------------------
# STEP 2: Extract all column references from YAML rule catalogs
# -----------------------------------------------------------------------------
# Map of (database.table, column) → set of rule_ids that reference it
_rule_coverage: dict[tuple[str, str], set[str]] = {}

_PARAMETER_COLUMN_KEYS = [
    "columns", "left_column", "right_column", "group_column",
    "aggregate_column", "reference_column", "event_column",
    "order_column", "when_column", "required_column", "violated_column",
    "pk_column", "filter_column", "ownership_col",
]


def _register(full_table: str, column: str, rule_id: str) -> None:
    if column and isinstance(column, str):
        key = (full_table, column)
        _rule_coverage.setdefault(key, set()).add(rule_id)


for yaml_path in sorted(RULES_DIR.glob("*.yaml")):
    with yaml_path.open(encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    if not isinstance(doc, dict):
        continue

    database = doc.get("database", "")
    table    = doc.get("table", "")
    if not table:
        continue
    full_table = f"{database}.{table}" if database else table

    # Register pk_column from catalog header
    pk_col = doc.get("pk_column", "")
    if pk_col:
        for rule in doc.get("rules", []):
            _register(full_table, pk_col, rule.get("rule_id", ""))

    # Register ownership_col from catalog header
    ownership_col = doc.get("ownership_col", "")
    if ownership_col:
        for rule in doc.get("rules", []):
            _register(full_table, ownership_col, rule.get("rule_id", ""))

    for rule in doc.get("rules", []):
        rule_id = rule.get("rule_id", "")
        params  = rule.get("parameters") or {}

        for key in _PARAMETER_COLUMN_KEYS:
            val = params.get(key)
            if val is None:
                continue
            if isinstance(val, list):
                for col in val:
                    _register(full_table, str(col), rule_id)
            else:
                _register(full_table, str(val), rule_id)

print(f"  Column references extracted from YAML: {len(_rule_coverage)} (table, column) pairs")

# -----------------------------------------------------------------------------
# STEP 3: Read source table schemas from metastore and build coverage rows
# -----------------------------------------------------------------------------
SNAPSHOT_DATE = date.today()
_rows = []

_tables_in_rules = sorted({tbl for (tbl, _col) in _rule_coverage})

for full_table in _tables_in_rules:
    try:
        src_df = spark.table(full_table)
    except Exception as exc:
        print(f"  Warning: could not read schema for '{full_table}': {exc} — skipping.")
        continue

    all_cols = [f.name for f in src_df.schema.fields]
    print(f"  [{full_table}] {len(all_cols)} columns in source schema")

    for col_name in all_cols:
        rule_ids_set = _rule_coverage.get((full_table, col_name), set())
        has_rule     = bool(rule_ids_set)
        rule_ids_str = ", ".join(sorted(rule_ids_set)) if rule_ids_set else None
        _rows.append((SNAPSHOT_DATE, full_table, col_name, has_rule, rule_ids_str))

# -----------------------------------------------------------------------------
# STEP 4: Write dq_coverage_report
# -----------------------------------------------------------------------------
if _rows:
    coverage_df = spark.createDataFrame(_rows, schema=COVERAGE_SCHEMA)
    coverage_df.write.mode("overwrite").format("delta").saveAsTable(COVERAGE_TABLE)
    total_cols   = len(_rows)
    covered_cols = sum(1 for r in _rows if r[3])
    pct          = round(covered_cols * 100 / total_cols, 1) if total_cols else 0.0
    print(f"\n  Coverage: {covered_cols}/{total_cols} columns have at least one rule ({pct}%)")
    print(f"  {COVERAGE_TABLE}: {total_cols} rows written")
else:
    print("  No source tables could be read — coverage report is empty.")

print("\nCoverage report complete.")
