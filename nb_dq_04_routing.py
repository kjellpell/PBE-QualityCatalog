# =============================================================================
# nb_dq_04_routing.py
# Post-validation routing: enriches violations with owner and context data,
# then writes dq_violations_enriched for Power BI reporting.
#
# Pipeline position: run after nb_dq_03_run_validation.py
#
# Prerequisites:
#   - qualitycatalog.dq_run_results and qualitycatalog.dq_violations are populated
#   - rules/*.yaml files contain optional `ownership_col`, `context_columns`,
#     `joins`, and `routing` fields
#   - QualityCatalogConfig.py is deployed to /lakehouse/default/Files/Configs/
#
# Skip conditions:
#   - ownership_col empty OR ansatte config keys empty → owner columns written as NULL
#   - context_columns empty → no context enrichment for that catalog
# =============================================================================

from __future__ import annotations

import importlib.util
import re
from functools import reduce
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CONFIG_DIR = Path("/lakehouse/default/Files/Configs")
RULES_DIR = Path("/lakehouse/default/Files/rules")

spark = SparkSession.builder.getOrCreate()

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
RESULTS_TABLE = f"{SCHEMA}.{cfg.DQ_RESULTS_TABLE}"
VIOLATIONS_TABLE = f"{SCHEMA}.{cfg.DQ_VIOLATIONS_TABLE}"
ENRICHED_TABLE = f"{SCHEMA}.{getattr(cfg, 'DQ_ENRICHED_TABLE', 'dq_violations_enriched')}"

ANSATTE_TABLE = getattr(cfg, "ANSATTE_TABLE", "saksbehandling.ansatte")
ANSATTE_KEY_COL = getattr(cfg, "ANSATTE_KEY_COL", "")
ANSATTE_EMAIL_COL = getattr(cfg, "ANSATTE_EMAIL_COL", "")
ANSATTE_NAME_COL = getattr(cfg, "ANSATTE_NAME_COL", "")

_ansatte_keys = ["ANSATTE_KEY_COL", "ANSATTE_EMAIL_COL", "ANSATTE_NAME_COL"]
_not_set = [k for k in _ansatte_keys if not getattr(cfg, k, "")]
if _not_set:
    print(f"  Info: ansatte config keys not set — owner columns will be NULL: {_not_set}")

# -----------------------------------------------------------------------------
# STEP 2: Build routing index from YAML catalogs
# -----------------------------------------------------------------------------
def _apply_catalog_filter(src_df, catalog_filter, rule_group: str):
    if not catalog_filter:
        return src_df
    filter_type = catalog_filter.get("type")
    if filter_type == "custom":
        where_clause = catalog_filter.get("where_clause", "")
        if not where_clause:
            return src_df
        filtered = src_df.filter(F.expr(where_clause))
        print(f"  [{rule_group}] catalog_filter applied: custom — {where_clause}")
        return filtered
    if filter_type == "date_range":
        date_column   = catalog_filter.get("date_column")
        lookback_days = catalog_filter.get("lookback_days")
        include_nulls = bool(catalog_filter.get("include_nulls", False))
        if not date_column or not lookback_days:
            return src_df
        cutoff    = F.date_sub(F.current_date(), int(lookback_days))
        predicate = F.col(date_column).cast("date") >= cutoff
        if include_nulls:
            predicate = predicate | F.col(date_column).isNull()
        filtered = src_df.filter(predicate)
        print(f"  [{rule_group}] catalog_filter applied: date_range — {date_column} last {lookback_days}d")
        return filtered
    print(f"  [{rule_group}] catalog_filter type '{filter_type}' not recognised — skipped")
    return src_df


# Expectations whose violations use the rule's group_column value (not the
# catalog pk_column) as primary_key_value — see engine/expectations.py.
_GROUP_KEYED_EXPECTATIONS = {
    "pairs_present",
    "gate_complete",
    "sequence_ordered",
    "group_aggregate_matches",
}


def _build_routing_index(rules_dir: Path):
    rules_index = {}  # rule_id → {routing, rule_group}
    catalogs = []     # one entry per YAML file

    for yaml_path in sorted(rules_dir.glob("*.yaml")):
        catalog_name = yaml_path.stem
        with yaml_path.open(encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)

        rule_group      = doc.get("rule_group", catalog_name)
        ownership_col   = doc.get("ownership_col", "")
        database        = doc.get("database", "")
        table           = doc.get("table", "")
        pk_column       = doc.get("pk_column", "")
        context_columns = doc.get("context_columns", [])
        joins_cfg       = doc.get("joins", [])
        catalog_filter  = doc.get("catalog_filter")
        rule_key_columns = {}  # rule_id → source column holding primary_key_value

        for rule in doc.get("rules", []):
            routing = rule.get("routing", "none")
            rules_index[rule["rule_id"]] = {
                "routing":          routing,
                "rule_group":       rule_group,
                "user_message":     rule.get("user_message", "").strip(),
                "rule_description": rule.get("description", "").strip(),
            }
            params = rule.get("parameters") or {}
            if rule.get("expectation") in _GROUP_KEYED_EXPECTATIONS:
                key_column = params.get("group_column") or pk_column
            else:
                key_column = params.get("pk_column") or pk_column
            rule_key_columns[rule["rule_id"]] = key_column

        catalogs.append({
            "catalog_name":    catalog_name,
            "rule_group":      rule_group,
            "database":        database,
            "table":           table,
            "pk_column":       pk_column,
            "ownership_col":   ownership_col,
            "context_columns": context_columns,
            "joins_cfg":       joins_cfg,
            "catalog_filter":  catalog_filter,
            "rule_key_columns": rule_key_columns,
            # Optional: number of days an Active violation may remain open before
            # management follow-up is flagged. NULL means no escalation threshold.
            "escalation_days": doc.get("escalation_days"),
        })

    print(f"  Loaded {len(catalogs)} catalogs, {len(rules_index)} rules from {rules_dir}")
    return rules_index, catalogs


rules_index, catalogs = _build_routing_index(RULES_DIR)

# -----------------------------------------------------------------------------
# STEP 3: Read violations for the latest run
# -----------------------------------------------------------------------------
# run_id is a random uuid4, so MAX(run_id) would pick an arbitrary run rather
# than the most recent one — order by run_timestamp to get the latest run.
_run_row = spark.sql(
    f"SELECT run_id FROM {RESULTS_TABLE} ORDER BY run_timestamp DESC LIMIT 1"
).collect()
if not _run_row or _run_row[0]["run_id"] is None:
    raise RuntimeError(f"No runs found in {RESULTS_TABLE}. Run nb_dq_03 first.")

run_id = _run_row[0]["run_id"]
print(f"  Latest run_id: {run_id}")

_VIOLATION_COLS = [
    "run_id", "run_timestamp", "batch_date",
    "rule_id", "rule_name", "rule_group",
    "primary_key_value", "violated_column", "actual_value",
    "expected_condition", "violation_detail", "issue_status",
    "first_seen_at",
]
violations_df = spark.sql(
    f"SELECT {', '.join(_VIOLATION_COLS)} FROM {VIOLATIONS_TABLE} WHERE run_id = '{run_id}'"
)
print(f"  Violations in this run: {violations_df.count()}")

# Add routing_team and rule_description from YAML
_meta_rows = [
    (rid, m["routing"], m["rule_description"])
    for rid, m in rules_index.items()
]
_meta_df = spark.createDataFrame(_meta_rows, ["rule_id", "routing_team", "rule_description"])
violations_df = violations_df.join(_meta_df, on="rule_id", how="left")

# -----------------------------------------------------------------------------
# STEP 4: Write dq_violations_enriched — unified across all catalogs
#
# One row per violation, enriched with owner_email/owner_name, routing_team,
# and any context_columns declared in the catalog YAML. Columns unique to one
# catalog are NULL for other catalogs' rows (unionByName fills gaps).
# Power BI applies RLS on owner_email for the handler/manager report.
# -----------------------------------------------------------------------------
_ansatte_ready = all([ANSATTE_KEY_COL, ANSATTE_EMAIL_COL, ANSATTE_NAME_COL])


def _template_to_spark_expr(template: str):
    parts = re.split(r'\{(\w+)\}', template)
    spark_parts = []
    for i, part in enumerate(parts):
        if i % 2 == 0:
            if part:
                spark_parts.append(F.lit(part))
        else:
            spark_parts.append(F.coalesce(F.col(part).cast("string"), F.lit("")))
    if not spark_parts:
        return F.lit(template)
    return F.concat(*spark_parts) if len(spark_parts) > 1 else spark_parts[0]


def _build_user_message_expr(rules_idx: dict):
    fallback = F.coalesce(F.col("rule_description"), F.col("rule_name"))
    expr = None
    for rid, meta in rules_idx.items():
        template = meta.get("user_message", "")
        if not template:
            continue
        rendered = _template_to_spark_expr(template)
        cond = F.col("rule_id") == rid
        expr = F.when(cond, rendered) if expr is None else expr.when(cond, rendered)
    return expr.otherwise(fallback) if expr is not None else fallback


def _null_owner_columns(df):
    return (
        df
        .withColumn("owner_email", F.lit(None).cast("string"))
        .withColumn("owner_name", F.lit(None).cast("string"))
    )


def _enrich_catalog(cat: dict):
    rule_group      = cat["rule_group"]
    ownership_col   = cat["ownership_col"]
    database        = cat["database"]
    table           = cat["table"]
    pk_column       = cat["pk_column"]
    context_columns = [c for c in cat.get("context_columns", []) if c]
    escalation_days = cat.get("escalation_days")

    cat_df = violations_df.filter(F.col("rule_group") == rule_group)

    do_owner_join = ownership_col and _ansatte_ready
    need_src = database and table and pk_column and (do_owner_join or context_columns)

    # Row-level expectations write the catalog pk_column value as
    # primary_key_value, but group-keyed expectations (pairs_present etc.)
    # write the rule's group_column value. Group rules by their key column so
    # each violation joins the source on the column its key actually came from.
    key_to_rules: dict[str, list[str]] = {}
    for rid, key_col in (cat.get("rule_key_columns") or {}).items():
        key_to_rules.setdefault(key_col or pk_column, []).append(rid)

    if not need_src or not key_to_rules:
        if ownership_col and not _ansatte_ready:
            print(f"  Info: {cat['catalog_name']} has ownership_col='{ownership_col}' but ansatte keys not configured — owner columns NULL")
        return _null_owner_columns(cat_df).withColumn(
            "escalation_days", F.lit(escalation_days).cast("integer")
        )

    src = spark.table(f"{database}.{table}")

    for jc in cat.get("joins_cfg", []):
        j_table  = jc.get("table")
        j_how    = jc.get("how", "left")
        j_select = jc.get("select")
        j_left   = jc.get("left_on")
        j_right  = jc.get("right_on")
        j_on     = jc.get("on")
        if not j_table:
            continue
        j_df = spark.table(j_table)
        if j_select:
            sel = list(j_select)
            if j_right and j_right not in sel:
                sel.append(j_right)
            j_df = j_df.select(*sel)
        if j_on:
            src = src.join(j_df, on=j_on, how=j_how)
        elif j_left and j_right:
            src = src.join(j_df, src[j_left] == j_df[j_right], how=j_how)

    src = _apply_catalog_filter(src, cat.get("catalog_filter"), rule_group)

    ans = None
    if do_owner_join:
        ans = spark.table(ANSATTE_TABLE).select(
            F.col(ANSATTE_KEY_COL).cast("string").alias("_ansatte_key"),
            F.col(ANSATTE_EMAIL_COL).alias("owner_email"),
            F.col(ANSATTE_NAME_COL).alias("owner_name"),
        )

    parts = []
    for key_col, rule_ids in key_to_rules.items():
        select_exprs = [src[key_col].cast("string").alias("_pk_str")]
        if do_owner_join:
            select_exprs.append(src[ownership_col].cast("string").alias("_owner_ref"))
        for col in context_columns:
            select_exprs.append(src[col])

        src_k = src.select(*select_exprs)

        if do_owner_join:
            lookup = src_k.join(ans, src_k["_owner_ref"] == ans["_ansatte_key"], "left")
        else:
            lookup = _null_owner_columns(src_k)
        # One row per key value. For group key columns this keeps an arbitrary
        # member of the group, so context/owner columns reflect a single row.
        lookup = (
            lookup
            .select(["_pk_str", "owner_email", "owner_name"] + context_columns)
            .dropDuplicates(["_pk_str"])
        )

        parts.append(
            cat_df
            .filter(F.col("rule_id").isin(rule_ids))
            .join(lookup, cat_df["primary_key_value"] == lookup["_pk_str"], "left")
            .drop("_pk_str")
        )

    # Violations whose rule_id is no longer in the YAML catalog (e.g. a rule
    # was removed after the run) keep NULL owner/context instead of vanishing.
    indexed_rule_ids = [rid for rids in key_to_rules.values() for rid in rids]
    parts.append(
        _null_owner_columns(cat_df.filter(~F.col("rule_id").isin(indexed_rule_ids)))
    )

    enriched = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)
    return enriched.withColumn("escalation_days", F.lit(escalation_days).cast("integer"))


_frames = [_enrich_catalog(cat) for cat in catalogs]
owners_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), _frames)
owners_df = owners_df.withColumn("user_message", _build_user_message_expr(rules_index))
owners_df.write.mode("overwrite").format("delta").saveAsTable(ENRICHED_TABLE)
# Count the written table instead of owners_df to avoid recomputing the
# whole enrichment DAG just for this print.
print(f"  {ENRICHED_TABLE}: {spark.table(ENRICHED_TABLE).count()} rows written")

print("\nRouting complete.")
