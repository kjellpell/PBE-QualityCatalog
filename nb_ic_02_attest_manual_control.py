# =============================================================================
# nb_ic_02_attest_manual_control.py
# Fabric-callable notebook for manual control attestations.
#
# Triggered by a Fabric Translytical Task Flow embedded in the Manual Controls
# page of the Power BI report.  The task flow binds control_id from the
# selected ic_control_register row and asks the user for period_covered,
# report_link (optional URL to evidence), and notes.
#
# File download behaviour:
#   If report_link is a publicly accessible URL (e.g. SharePoint "Anyone with
#   the link" share link), the notebook downloads the file and stores it in
#   Fabric Lakehouse Files at a structured path.  On success, evidence_path
#   is populated.  If the download fails (auth-protected URL, network error),
#   the attestation row is still written with evidence_path = NULL and
#   report_link preserved.
#
# ATTESTED_BY is derived from the Fabric session context — it is never passed
# as a notebook parameter.
# =============================================================================

import os
import uuid
import requests
from urllib.parse import urlparse, unquote
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# CELL 1 — Parameters (bound by Translytical Task Flow)
# ---------------------------------------------------------------------------
control_id     = mssparkutils.notebook.getParam("control_id")
period_covered = mssparkutils.notebook.getParam("period_covered")
report_link    = mssparkutils.notebook.getParam("report_link",  defaultValue="")
notes          = mssparkutils.notebook.getParam("notes",        defaultValue="")

# ---------------------------------------------------------------------------
# CELL 2 — Resolve caller identity from Fabric session context
# ---------------------------------------------------------------------------
try:
    attested_by = mssparkutils.credentials.getToken("storage").split("upn:")[1].split(",")[0]
except Exception:
    attested_by = mssparkutils.runtime.context.get("userName", "unknown")

# ---------------------------------------------------------------------------
# CELL 3 — Validate required parameters
# ---------------------------------------------------------------------------
assert control_id,     "control_id is required"
assert period_covered, "period_covered is required"

# ---------------------------------------------------------------------------
# CELL 4 — Load config and look up control
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, "/lakehouse/default/Files/Configs")
from QualityCatalogConfig import (  # noqa: E402
    IC_CONTROL_REGISTER_TABLE,
    IC_ATTESTATIONS_TABLE,
    IC_EVIDENCE_BASE_PATH,
)

control = (
    spark.table(IC_CONTROL_REGISTER_TABLE)
    .filter(F.col("control_id") == control_id)
    .filter(F.col("active") == True)       # noqa: E712
    .filter(F.col("execution_type") == "Manual")
    .first()
)

assert control, (
    f"No active Manual control found with control_id '{control_id}'. "
    "Check ic_control_register: the control must exist, be active, and have execution_type = 'Manual'."
)

# ---------------------------------------------------------------------------
# CELL 5 — Compute next_due_date from attestation_frequency
# ---------------------------------------------------------------------------
_freq_map = {
    "Daily":     timedelta(days=1),
    "Weekly":    timedelta(weeks=1),
    "Monthly":   timedelta(days=30),
    "Quarterly": timedelta(days=91),
}
now = datetime.now(timezone.utc)
next_due_date = (now + _freq_map.get(control["attestation_frequency"], timedelta(days=30))).date()

# ---------------------------------------------------------------------------
# CELL 6 — Attempt to download and store evidence file
# ---------------------------------------------------------------------------
attestation_id = str(uuid.uuid4())
evidence_path  = None

if report_link.strip():
    try:
        response = requests.get(report_link.strip(), timeout=30, stream=True)
        response.raise_for_status()

        # Derive a filename from the URL path (falls back to "evidence").
        raw_name  = os.path.basename(unquote(urlparse(report_link).path)) or "evidence"
        safe_name = f"{attestation_id[:8]}_{raw_name}"

        dest_dir  = f"{IC_EVIDENCE_BASE_PATH}/{control_id}/{period_covered}"
        dest_path = f"{dest_dir}/{safe_name}"

        # /lakehouse/default/Files/ is a locally mounted ABFS path in Fabric Spark.
        # Standard os.makedirs() and open() work here without mssparkutils.fs.put().
        os.makedirs(dest_dir, exist_ok=True)

        with open(dest_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=65_536):
                fh.write(chunk)

        # Verify the file landed.
        assert os.path.exists(dest_path) and os.path.getsize(dest_path) > 0, (
            f"File write verification failed: {dest_path}"
        )

        evidence_path = dest_path
        print(f"  Evidence file stored: {evidence_path}")

    except Exception as exc:
        # Non-fatal — proceed with attestation, log warning.
        # Common causes: auth-protected SharePoint URL (SSO required),
        # network error, or invalid URL.
        print(
            f"  Warning: could not download evidence file from report_link ({exc}). "
            f"report_link is stored; evidence_path will be NULL. "
            f"For authenticated SharePoint URLs, use an 'Anyone with the link' share link."
        )

# ---------------------------------------------------------------------------
# CELL 7 — Write attestation row to ic_manual_attestations
# ---------------------------------------------------------------------------
row = [{
    "attestation_id":  attestation_id,
    "control_id":      control_id,
    "attested_by":     attested_by,
    "attested_at":     now,
    "period_covered":  period_covered,
    "report_link":     report_link.strip() or None,
    "evidence_path":   evidence_path,
    "notes":           notes.strip() or None,
    "next_due_date":   next_due_date,
}]

spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(IC_ATTESTATIONS_TABLE)

print(
    f"OK: attestation '{attestation_id}' recorded for control '{control_id}' "
    f"(period: {period_covered}) by '{attested_by}'"
)
mssparkutils.notebook.exit("OK")
