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

        for rule in doc.get("rules", []):
            routing = rule.get("routing", "none")
            rules_index[rule["rule_id"]] = {"routing": routing, "rule_group": rule_group}

        catalogs.append({
            "catalog_name":   catalog_name,
            "rule_group":     rule_group,
            "database":       database,
            "table":          table,
            "pk_column":      pk_column,
            "ownership_col":  ownership_col,
            "context_columns": context_columns,
            "joins_cfg":      joins_cfg,
        })

    print(f"  Loaded {len(catalogs)} catalogs, {len(rules_index)} rules from {rules_dir}")
    return rules_index, catalogs


rules_index, catalogs = _build_routing_index(RULES_DIR)

# -----------------------------------------------------------------------------
# STEP 3: Read violations for the latest run
# -----------------------------------------------------------------------------
_run_row = spark.sql(f"SELECT MAX(run_id) AS run_id FROM {RESULTS_TABLE}").collect()
if not _run_row or _run_row[0]["run_id"] is None:
    raise RuntimeError(f"No runs found in {RESULTS_TABLE}. Run nb_dq_03 first.")

run_id = _run_row[0]["run_id"]
print(f"  Latest run_id: {run_id}")

_VIOLATION_COLS = [
    "run_id", "run_timestamp", "batch_date",
    "rule_id", "rule_name", "rule_group",
    "primary_key_value", "violated_column", "actual_value",
    "expected_condition", "violation_detail", "issue_status",
]
violations_df = spark.sql(
    f"SELECT {', '.join(_VIOLATION_COLS)} FROM {VIOLATIONS_TABLE} WHERE run_id = '{run_id}'"
)
print(f"  Violations in this run: {violations_df.count()}")

# Add routing_team from YAML so Power BI can slice by responsible team
_routing_rows = [(rid, m["routing"]) for rid, m in rules_index.items()]
_routing_df   = spark.createDataFrame(_routing_rows, ["rule_id", "routing_team"])
violations_df = violations_df.join(_routing_df, on="rule_id", how="left")

# -----------------------------------------------------------------------------
# STEP 4: Write dq_violations_enriched — unified across all catalogs
#
# One row per violation, enriched with owner_email/owner_name, routing_team,
# and any context_columns declared in the catalog YAML. Columns unique to one
# catalog are NULL for other catalogs' rows (unionByName fills gaps).
# Power BI applies RLS on owner_email for the handler/manager report.
# -----------------------------------------------------------------------------
_ansatte_ready = all([ANSATTE_KEY_COL, ANSATTE_EMAIL_COL, ANSATTE_NAME_COL])


def _enrich_catalog(cat: dict):
    rule_group      = cat["rule_group"]
    ownership_col   = cat["ownership_col"]
    database        = cat["database"]
    table           = cat["table"]
    pk_column       = cat["pk_column"]
    context_columns = cat.get("context_columns", [])

    cat_df = violations_df.filter(F.col("rule_group") == rule_group)

    do_owner_join = ownership_col and _ansatte_ready
    need_src = database and table and pk_column and (do_owner_join or context_columns)

    if need_src:
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

        select_exprs = [src[pk_column].cast("string").alias("_pk_str")]
        if do_owner_join:
            select_exprs.append(src[ownership_col].cast("string").alias("_owner_ref"))
        for col in context_columns:
            select_exprs.append(src[col])

        src = src.select(*select_exprs)

        if do_owner_join:
            ans = spark.table(ANSATTE_TABLE).select(
                F.col(ANSATTE_KEY_COL).cast("string").alias("_ansatte_key"),
                F.col(ANSATTE_EMAIL_COL).alias("owner_email"),
                F.col(ANSATTE_NAME_COL).alias("owner_name"),
            )
            lookup = (
                src.join(ans, src["_owner_ref"] == ans["_ansatte_key"], "left")
                .select(["_pk_str", "owner_email", "owner_name"] + context_columns)
                .dropDuplicates(["_pk_str"])
            )
        else:
            lookup = (
                src
                .withColumn("owner_email", F.lit(None).cast("string"))
                .withColumn("owner_name", F.lit(None).cast("string"))
                .select(["_pk_str", "owner_email", "owner_name"] + context_columns)
                .dropDuplicates(["_pk_str"])
            )

        return (
            cat_df
            .join(lookup, cat_df["primary_key_value"] == lookup["_pk_str"], "left")
            .drop("_pk_str")
        )
    else:
        if ownership_col and not _ansatte_ready:
            print(f"  Info: {cat['catalog_name']} has ownership_col='{ownership_col}' but ansatte keys not configured — owner columns NULL")
        return (
            cat_df
            .withColumn("owner_email", F.lit(None).cast("string"))
            .withColumn("owner_name", F.lit(None).cast("string"))
        )


_frames = [_enrich_catalog(cat) for cat in catalogs]
owners_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), _frames)
owners_df.write.mode("overwrite").format("delta").saveAsTable(ENRICHED_TABLE)
print(f"  {ENRICHED_TABLE}: {owners_df.count()} rows written")

print("\nRouting complete.")
