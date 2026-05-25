# =============================================================================
# nb_dq_06_notify.py
# Teams direct-message notifications for active violations via Power Automate.
#
# Two triggers:
#   1. Handler DM  — sent when a handler has brand-new violations this run
#                    (first_seen_at date == today). Not repeated on subsequent runs.
#   2. Manager DM  — sent when any Active violation has been open past its
#                    catalog escalation_days threshold.
#
# Each notification is a JSON POST to a Power Automate HTTP trigger webhook.
# The Power Automate flow receives the payload and sends the Teams DM.
#
# Prerequisites:
#   - Two Power Automate flows with HTTP triggers configured
#   - POWER_AUTOMATE_HANDLER_WEBHOOK and POWER_AUTOMATE_MANAGER_WEBHOOK set
#     in QualityCatalogRuntime.py
#   - nb_dq_04_routing.py has been run (dq_violations_enriched is current)
#
# Pipeline position: run after nb_dq_04_routing.py
# =============================================================================

from __future__ import annotations

import importlib.util
import json
from datetime import date
from pathlib import Path

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CONFIG_DIR = Path("/lakehouse/default/Files/Configs")

# -----------------------------------------------------------------------------
# Load config
# -----------------------------------------------------------------------------
def _load_cfg(name: str):
    spec = importlib.util.spec_from_file_location(name, str(CONFIG_DIR / f"{name}.py"))
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

cfg     = _load_cfg("QualityCatalogConfig")
runtime = _load_cfg("QualityCatalogRuntime")

SCHEMA         = cfg.DEFAULT_SCHEMA
ENRICHED_TABLE = f"{SCHEMA}.{getattr(cfg, 'DQ_ENRICHED_TABLE', 'dq_violations_enriched')}"

REPORT_URL    = getattr(cfg, "REPORT_URL", "")
MANAGER_EMAIL = getattr(cfg, "MANAGER_EMAIL", "")

DRY_RUN_NOTIFY   = getattr(runtime, "DRY_RUN_NOTIFY", True)
HANDLER_WEBHOOK  = getattr(runtime, "POWER_AUTOMATE_HANDLER_WEBHOOK", "")
MANAGER_WEBHOOK  = getattr(runtime, "POWER_AUTOMATE_MANAGER_WEBHOOK", "")

# -----------------------------------------------------------------------------
# Webhook helper
# -----------------------------------------------------------------------------
def _post_webhook(webhook_url: str, payload: dict) -> None:
    if DRY_RUN_NOTIFY:
        print(f"  [DRY RUN] POST to webhook:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return
    resp = requests.post(webhook_url, json=payload, timeout=30)
    resp.raise_for_status()


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
TODAY = date.today()

enriched_df = spark.table(ENRICHED_TABLE).filter(F.col("issue_status") == "Active")

# New violations: first detected today
new_df = enriched_df.filter(
    F.col("first_seen_at").cast("date") == F.lit(str(TODAY)).cast("date")
)

# Escalated violations: open longer than catalog threshold
escalated_df = enriched_df.filter(
    F.col("escalation_days").isNotNull()
    & (F.datediff(F.current_date(), F.col("first_seen_at").cast("date")) > F.col("escalation_days"))
)

new_count       = new_df.count()
escalated_count = escalated_df.count()

print(f"  New violations today : {new_count}")
print(f"  Escalated violations : {escalated_count}")

if new_count == 0 and escalated_count == 0:
    print("  Nothing to notify — exiting.")
else:
    # --- Handler DMs ---
    if new_count > 0 and HANDLER_WEBHOOK:
        handler_rows = new_df.toPandas()
        dm_count = 0
        for email, group in handler_rows.groupby("owner_email"):
            if not email:
                continue
            violations = [
                {
                    "rule_name":  row.get("rule_name", ""),
                    "saksnummer": row.get("saksnummer") or row.get("primary_key_value", ""),
                }
                for _, row in group.iterrows()
            ]
            payload = {
                "recipient_email":    email,
                "owner_name":         group["owner_name"].iloc[0] or email,
                "new_violation_count": len(violations),
                "violations":         violations[:10],
                "report_url":         REPORT_URL,
            }
            _post_webhook(HANDLER_WEBHOOK, payload)
            dm_count += 1
        print(f"  Handler DMs sent: {dm_count}")
    elif new_count > 0 and not HANDLER_WEBHOOK:
        print("  Warning: new violations found but POWER_AUTOMATE_HANDLER_WEBHOOK is not set — skipped.")

    # --- Manager DM ---
    if escalated_count > 0 and MANAGER_WEBHOOK and MANAGER_EMAIL:
        esc_rows = (
            escalated_df
            .withColumn("days_open", F.datediff(F.current_date(), F.col("first_seen_at").cast("date")))
            .select("owner_name", "rule_name", "days_open")
            .orderBy("days_open", ascending=False)
            .toPandas()
        )
        payload = {
            "recipient_email":    MANAGER_EMAIL,
            "escalated_count":    escalated_count,
            "escalated":          esc_rows.head(20).to_dict("records"),
            "report_url":         REPORT_URL,
        }
        _post_webhook(MANAGER_WEBHOOK, payload)
        print(f"  Manager DM sent to {MANAGER_EMAIL}")
    elif escalated_count > 0 and not MANAGER_WEBHOOK:
        print("  Warning: escalated violations found but POWER_AUTOMATE_MANAGER_WEBHOOK is not set — skipped.")

print("\nNotification run complete.")
