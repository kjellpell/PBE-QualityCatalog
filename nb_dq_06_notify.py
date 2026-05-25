# =============================================================================
# nb_dq_06_notify.py
# Teams direct-message notifications for active violations.
#
# Two triggers:
#   1. Handler DM  — sent when a handler has brand-new violations this run
#                    (first_seen_at date == today). Not repeated on subsequent runs.
#   2. Manager DM  — sent when any Active violation has been open past its
#                    catalog escalation_days threshold.
#
# Prerequisites:
#   - Azure AD app registration with application permissions:
#       Chat.Create, ChatMessage.Send
#   - nb_dq_04_routing.py has been run (dq_violations_enriched is current)
#
# Pipeline position: run after nb_dq_04_routing.py
# =============================================================================

from __future__ import annotations

import importlib.util
import json
from datetime import date, datetime, timezone
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

DRY_RUN_NOTIFY = getattr(runtime, "DRY_RUN_NOTIFY", True)
TENANT_ID      = getattr(runtime, "GRAPH_TENANT_ID", "")
CLIENT_ID      = getattr(runtime, "GRAPH_CLIENT_ID", "")
CLIENT_SECRET  = getattr(runtime, "GRAPH_CLIENT_SECRET", "")

# -----------------------------------------------------------------------------
# Graph API helpers
# -----------------------------------------------------------------------------
_GRAPH_BASE  = "https://graph.microsoft.com/v1.0"
_GRAPH_SCOPE = "https://graph.microsoft.com/.default"


def _get_graph_token() -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope":         _GRAPH_SCOPE,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _get_or_create_dm_chat(token: str, recipient_email: str) -> str:
    """Return the chat_id of the 1:1 chat between the app and the recipient."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "chatType": "oneOnOne",
        "members": [
            {
                "@odata.type":    "#microsoft.graph.aadUserConversationMember",
                "roles":          ["owner"],
                "user@odata.bind": f"{_GRAPH_BASE}/users('{recipient_email}')",
            }
        ],
    }
    resp = requests.post(f"{_GRAPH_BASE}/chats", headers=headers, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()["id"]


def _send_teams_dm(token: str, recipient_email: str, card: dict) -> None:
    if DRY_RUN_NOTIFY:
        print(f"  [DRY RUN] DM to {recipient_email}:")
        print(json.dumps(card, indent=2, ensure_ascii=False))
        return
    chat_id = _get_or_create_dm_chat(token, recipient_email)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {
        "body": {
            "contentType": "html",
            "content": (
                f'<attachment id="1"></attachment>'
            ),
        },
        "attachments": [
            {
                "id":          "1",
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content":     json.dumps(card, ensure_ascii=False),
            }
        ],
    }
    resp = requests.post(
        f"{_GRAPH_BASE}/chats/{chat_id}/messages",
        headers=headers,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()


# -----------------------------------------------------------------------------
# Adaptive card builders
# -----------------------------------------------------------------------------
def _open_report_action() -> dict:
    return {
        "type":  "Action.OpenUrl",
        "title": "Åpne rapport",
        "url":   REPORT_URL or "https://app.powerbi.com",
    }


def _build_handler_card(owner_name: str, violations: list[dict]) -> dict:
    n = len(violations)
    facts = [
        {"title": v.get("rule_name", ""), "value": v.get("saksnummer") or v.get("primary_key_value", "")}
        for v in violations[:5]
    ]
    suffix = f" (viser 5 av {n})" if n > 5 else ""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type":    "AdaptiveCard",
        "version": "1.4",
        "body": [
            {
                "type":   "TextBlock",
                "text":   f"Du har {n} nye regelbrudd i PBE Quality Catalog",
                "weight": "Bolder",
                "size":   "Medium",
                "wrap":   True,
            },
            {
                "type":   "TextBlock",
                "text":   f"Nye avvik registrert i dag{suffix}:",
                "wrap":   True,
                "spacing": "Small",
            },
            {
                "type":  "FactSet",
                "facts": facts,
            },
        ],
        "actions": [_open_report_action()],
    }


def _build_manager_card(escalated: list[dict]) -> dict:
    n = len(escalated)
    rows = [
        {
            "type": "TableRow",
            "cells": [
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": r.get("owner_name", ""), "wrap": True}]},
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": r.get("rule_name", ""), "wrap": True}]},
                {"type": "TableCell", "items": [{"type": "TextBlock", "text": str(r.get("days_open", "")), "wrap": True}]},
            ],
        }
        for r in escalated[:10]
    ]
    suffix = f" (viser 10 av {n})" if n > 10 else ""
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type":    "AdaptiveCard",
        "version": "1.4",
        "body": [
            {
                "type":   "TextBlock",
                "text":   f"⚠ {n} regelbrudd har overskredet eskaleringsgrensen{suffix}",
                "weight": "Bolder",
                "size":   "Medium",
                "color":  "Attention",
                "wrap":   True,
            },
            {
                "type": "Table",
                "columns": [{"width": 2}, {"width": 3}, {"width": 1}],
                "rows": [
                    {
                        "type":  "TableRow",
                        "style": "accent",
                        "cells": [
                            {"type": "TableCell", "items": [{"type": "TextBlock", "text": "Saksbehandler", "weight": "Bolder"}]},
                            {"type": "TableCell", "items": [{"type": "TextBlock", "text": "Regel", "weight": "Bolder"}]},
                            {"type": "TableCell", "items": [{"type": "TextBlock", "text": "Dager", "weight": "Bolder"}]},
                        ],
                    },
                    *rows,
                ],
            },
        ],
        "actions": [_open_report_action()],
    }


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

print(f"  New violations today    : {new_count}")
print(f"  Escalated violations    : {escalated_count}")

if new_count == 0 and escalated_count == 0:
    print("  Nothing to notify — exiting.")
else:
    token = _get_graph_token() if not DRY_RUN_NOTIFY else "dry-run-token"

    # --- Handler DMs ---
    handler_rows = new_df.toPandas()
    grouped = handler_rows.groupby("owner_email")
    dm_count = 0
    for email, group in grouped:
        if not email:
            continue
        owner_name = group["owner_name"].iloc[0] or email
        violations = group.to_dict("records")
        card = _build_handler_card(owner_name, violations)
        _send_teams_dm(token, email, card)
        dm_count += 1
    print(f"  Handler DMs sent: {dm_count}")

    # --- Manager DM ---
    if escalated_count > 0 and MANAGER_EMAIL:
        esc_rows = (
            escalated_df
            .withColumn("days_open", F.datediff(F.current_date(), F.col("first_seen_at").cast("date")))
            .select("owner_name", "rule_name", "days_open")
            .orderBy("days_open", ascending=False)
            .toPandas()
            .to_dict("records")
        )
        card = _build_manager_card(esc_rows)
        _send_teams_dm(token, MANAGER_EMAIL, card)
        print(f"  Manager DM sent to {MANAGER_EMAIL}")
    elif escalated_count > 0 and not MANAGER_EMAIL:
        print("  Warning: escalated violations found but MANAGER_EMAIL is not set — skipped.")

print("\nNotification run complete.")
