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
from pyspark.sql.types import (
    BigIntType, BooleanType, DateType,
    StringType, StructField, StructType, TimestampType,
)

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

NOTIFICATION_LOG_TABLE = f"{SCHEMA}.dq_notification_log"

_LOG_SCHEMA = StructType([
    StructField("run_timestamp",     TimestampType(), False),
    StructField("batch_date",        DateType(),      False),
    StructField("notification_type", StringType(),    False),
    StructField("recipient_email",   StringType(),    True),
    StructField("violation_count",   BigIntType(),    True),
    StructField("status",            StringType(),    False),
    StructField("dry_run",           BooleanType(),   False),
    StructField("error_message",     StringType(),    True),
])

DRY_RUN_NOTIFY   = getattr(runtime, "DRY_RUN_NOTIFY", True)
HANDLER_WEBHOOK  = getattr(runtime, "POWER_AUTOMATE_HANDLER_WEBHOOK", "")
MANAGER_WEBHOOK  = getattr(runtime, "POWER_AUTOMATE_MANAGER_WEBHOOK", "")
TEST_EMAIL       = getattr(runtime, "NOTIFY_TEST_EMAIL", "")

if TEST_EMAIL:
    print(f"  *** TEST MODE: all DMs will be sent to {TEST_EMAIL} ***")

# -----------------------------------------------------------------------------
# Webhook helper
# -----------------------------------------------------------------------------
def _post_webhook(webhook_url: str, payload: dict) -> tuple[str, str | None]:
    """POST payload to webhook. Returns (status, error_message)."""
    if DRY_RUN_NOTIFY:
        print(f"  [DRY RUN] POST to webhook:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return "dry_run", None
    try:
        resp = requests.post(webhook_url, json=payload, timeout=30)
        resp.raise_for_status()
        return "sent", None
    except Exception as exc:
        return "error", str(exc)[:2000]


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
from datetime import datetime, timezone
RUN_TIMESTAMP = datetime.now(timezone.utc)
TODAY         = date.today()
_log_rows: list[tuple] = []

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
            recipient = TEST_EMAIL or email
            payload = {
                "recipient_email":     recipient,
                "owner_name":          group["owner_name"].iloc[0] or email,
                "new_violation_count": len(violations),
                "violations":          violations[:10],
                "report_url":          REPORT_URL,
            }
            status, err = _post_webhook(HANDLER_WEBHOOK, payload)
            _log_rows.append((RUN_TIMESTAMP, TODAY, "handler", recipient, len(violations), status, DRY_RUN_NOTIFY, err))
            dm_count += 1
        print(f"  Handler DMs sent: {dm_count}")
    elif new_count > 0 and not HANDLER_WEBHOOK:
        print("  Warning: new violations found but POWER_AUTOMATE_HANDLER_WEBHOOK is not set — skipped.")

    # --- Manager DM ---
    if escalated_count > 0 and MANAGER_WEBHOOK and (MANAGER_EMAIL or TEST_EMAIL):
        esc_rows = (
            escalated_df
            .withColumn("days_open", F.datediff(F.current_date(), F.col("first_seen_at").cast("date")))
            .select("owner_name", "rule_name", "days_open")
            .orderBy("days_open", ascending=False)
            .toPandas()
        )
        manager_recipient = TEST_EMAIL or MANAGER_EMAIL
        payload = {
            "recipient_email": manager_recipient,
            "escalated_count": escalated_count,
            "escalated":       esc_rows.head(20).to_dict("records"),
            "report_url":      REPORT_URL,
        }
        status, err = _post_webhook(MANAGER_WEBHOOK, payload)
        _log_rows.append((RUN_TIMESTAMP, TODAY, "manager", manager_recipient, escalated_count, status, DRY_RUN_NOTIFY, err))
        print(f"  Manager DM sent to {manager_recipient}")
    elif escalated_count > 0 and not MANAGER_WEBHOOK:
        print("  Warning: escalated violations found but POWER_AUTOMATE_MANAGER_WEBHOOK is not set — skipped.")

if _log_rows:
    log_df = spark.createDataFrame(_log_rows, schema=_LOG_SCHEMA)
    log_df.write.mode("append").saveAsTable(NOTIFICATION_LOG_TABLE)
    print(f"  {NOTIFICATION_LOG_TABLE}: {len(_log_rows)} rows written.")

print("\nNotification run complete.")
