# =============================================================================
# nb_dq_04_routing.py
# Post-validation routing: writes dq_violations_owners (unified, enriched) and
# sends notifications via Power Automate.
#
# Pipeline position: run after nb_dq_03_run_validation.py
#
# Prerequisites:
#   - qualitycatalog.dq_run_results and qualitycatalog.dq_violations are populated
#   - rules/*.yaml files contain optional `ownership_col` and `context_columns` fields
#   - QualityCatalogConfig.py is deployed to /lakehouse/default/Files/Configs/
#
# Skip conditions:
#   - POWER_AUTOMATE_*_URL empty → that notification channel is silently skipped
#   - ownership_col empty OR ansatte config keys empty → owner columns written as NULL
#   - context_columns empty → no context enrichment for that catalog
# =============================================================================

from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

import requests
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

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
NOTIFICATIONS_TABLE = f"{SCHEMA}.{getattr(cfg, 'DQ_NOTIFICATIONS_TABLE', 'dq_notifications')}"
OWNERS_TABLE = f"{SCHEMA}.{getattr(cfg, 'DQ_OWNERS_TABLE', 'dq_violations_owners')}"

ANSATTE_TABLE = getattr(cfg, "ANSATTE_TABLE", "saksbehandling.ansatte")
ANSATTE_KEY_COL = getattr(cfg, "ANSATTE_KEY_COL", "")
ANSATTE_EMAIL_COL = getattr(cfg, "ANSATTE_EMAIL_COL", "")
ANSATTE_NAME_COL = getattr(cfg, "ANSATTE_NAME_COL", "")
PA_ITOPS_URL = getattr(cfg, "POWER_AUTOMATE_ITOPS_URL", "")
PA_INDIVIDUAL_URL = getattr(cfg, "POWER_AUTOMATE_INDIVIDUAL_URL", "")

_optional_keys = [
    "ANSATTE_KEY_COL", "ANSATTE_EMAIL_COL", "ANSATTE_NAME_COL",
    "POWER_AUTOMATE_ITOPS_URL", "POWER_AUTOMATE_INDIVIDUAL_URL",
]
_not_set = [k for k in _optional_keys if not getattr(cfg, k, "")]
if _not_set:
    print(f"  Info: config keys not yet set — owner resolution/notifications skipped for: {_not_set}")

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

        rule_group = doc.get("rule_group", catalog_name)
        ownership_col = doc.get("ownership_col", "")
        database = doc.get("database", "")
        table = doc.get("table", "")
        pk_column = doc.get("pk_column", "")
        context_columns = doc.get("context_columns", [])
        joins_cfg = doc.get("joins", [])
        has_individual = False

        for rule in doc.get("rules", []):
            routing = rule.get("routing", "silent")
            rules_index[rule["rule_id"]] = {"routing": routing, "rule_group": rule_group}
            if routing == "individual":
                has_individual = True

        catalogs.append({
            "catalog_name": catalog_name,
            "rule_group": rule_group,
            "database": database,
            "table": table,
            "pk_column": pk_column,
            "ownership_col": ownership_col,
            "context_columns": context_columns,
            "joins_cfg": joins_cfg,
            "has_individual": has_individual,
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

# -----------------------------------------------------------------------------
# STEP 4: Write dq_violations_owners — unified across all catalogs
#
# One row per violation, enriched with owner_email/owner_name and any
# context_columns declared in the catalog YAML. Columns unique to one catalog
# are NULL for all other catalogs' rows (unionByName fills gaps automatically).
# Power BI applies RLS on owner_email to give each handler their own view.
# -----------------------------------------------------------------------------
_ansatte_ready = all([ANSATTE_KEY_COL, ANSATTE_EMAIL_COL, ANSATTE_NAME_COL])


def _enrich_catalog(cat: dict):
    rule_group    = cat["rule_group"]
    ownership_col = cat["ownership_col"]
    database      = cat["database"]
    table         = cat["table"]
    pk_column     = cat["pk_column"]
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
owners_df.write.mode("overwrite").format("delta").saveAsTable(OWNERS_TABLE)
print(f"  {OWNERS_TABLE}: {owners_df.count()} rows written")

# -----------------------------------------------------------------------------
# STEP 6: IT-ops notifications
# -----------------------------------------------------------------------------
def _send_itops_notification() -> list[dict]:
    logs: list[dict] = []
    now = datetime.now(timezone.utc)

    if not PA_ITOPS_URL:
        print("  Skipping IT-ops notification: POWER_AUTOMATE_ITOPS_URL not configured")
        logs.append(_log(now, "it-ops", None, 0, "skipped", "POWER_AUTOMATE_ITOPS_URL not configured"))
        return logs

    itops_ids = [rid for rid, m in rules_index.items() if m["routing"] == "it-ops"]
    if not itops_ids:
        return logs

    in_list = ", ".join(f"'{r}'" for r in itops_ids)
    rows = spark.sql(f"""
        SELECT rule_id, rule_name, rule_group, failed_rows, status
        FROM {RESULTS_TABLE}
        WHERE run_id = '{run_id}'
          AND rule_id IN ({in_list})
          AND status != 'PASSED'
    """).collect()

    if not rows:
        print("  IT-ops: no failures to notify")
        return logs

    payload = {"run_id": run_id, "failures": [r.asDict() for r in rows]}
    try:
        resp = requests.post(PA_ITOPS_URL, json=payload, timeout=10)
        resp.raise_for_status()
        print(f"  IT-ops notification sent: {len(rows)} failed rules")
        logs.append(_log(now, "it-ops", None, len(rows), "sent", None))
    except Exception as exc:
        print(f"  Warning: IT-ops notification failed — {exc}")
        logs.append(_log(now, "it-ops", None, len(rows), "failed", str(exc)[:500]))

    return logs


# -----------------------------------------------------------------------------
# STEP 7: Individual owner notifications
# -----------------------------------------------------------------------------
def _send_individual_digests() -> list[dict]:
    logs: list[dict] = []
    now = datetime.now(timezone.utc)

    if not PA_INDIVIDUAL_URL:
        print("  Skipping individual notifications: POWER_AUTOMATE_INDIVIDUAL_URL not configured")
        logs.append(_log(now, "individual", None, 0, "skipped", "POWER_AUTOMATE_INDIVIDUAL_URL not configured"))
        return logs

    individual_ids = [rid for rid, m in rules_index.items() if m["routing"] == "individual"]
    if not individual_ids:
        return logs

    in_list = ", ".join(f"'{r}'" for r in individual_ids)
    combined_pd = spark.sql(f"""
        SELECT owner_email, owner_name, rule_id, rule_name,
               primary_key_value, violated_column, violation_detail
        FROM {OWNERS_TABLE}
        WHERE run_id = '{run_id}'
          AND owner_email IS NOT NULL
          AND rule_id IN ({in_list})
    """).toPandas()

    if combined_pd.empty:
        print("  Individual: no violations with resolved owners")
        return logs

    for email, group in combined_pd.groupby("owner_email"):
        send_time = datetime.now(timezone.utc)
        payload = {
            "run_id": run_id,
            "owner_name": group["owner_name"].iloc[0],
            "owner_email": email,
            "violations": group.drop(columns=["owner_email", "owner_name"]).to_dict("records"),
        }
        try:
            resp = requests.post(PA_INDIVIDUAL_URL, json=payload, timeout=10)
            resp.raise_for_status()
            logs.append(_log(send_time, "individual", email, len(group), "sent", None))
        except Exception as exc:
            print(f"  Warning: individual notification for {email} failed — {exc}")
            logs.append(_log(send_time, "individual", email, len(group), "failed", str(exc)[:500]))

    sent = sum(1 for r in logs if r["status"] == "sent")
    print(f"  Individual notifications: {sent} sent, {len(logs) - sent} failed")
    return logs


# -----------------------------------------------------------------------------
# STEP 8: Write notification log to dq_notifications
# -----------------------------------------------------------------------------
def _log(ts: datetime, ntype: str, email, count: int, status: str, error) -> dict:
    return {
        "run_id": run_id,
        "notified_at": ts.replace(tzinfo=None),  # Spark TimestampType is tz-naive
        "notification_type": ntype,
        "recipient_email": email,
        "violation_count": count,
        "status": status,
        "error_message": error,
    }


all_logs = _send_itops_notification() + _send_individual_digests()

if all_logs:
    _log_schema = StructType([
        StructField("run_id",             StringType(),    True),
        StructField("notified_at",        TimestampType(), True),
        StructField("notification_type",  StringType(),    True),
        StructField("recipient_email",    StringType(),    True),
        StructField("violation_count",    IntegerType(),   True),
        StructField("status",             StringType(),    True),
        StructField("error_message",      StringType(),    True),
    ])
    spark.createDataFrame(all_logs, schema=_log_schema).write \
        .mode("append").format("delta").saveAsTable(NOTIFICATIONS_TABLE)
    print(f"  Logged {len(all_logs)} notification event(s) to {NOTIFICATIONS_TABLE}")

print("\nRouting complete.")
