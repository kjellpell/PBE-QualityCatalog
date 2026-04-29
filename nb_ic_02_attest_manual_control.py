# =============================================================================
# nb_ic_02_attest_manual_control.py
# Fabric-callable notebook for manual control attestations.
#
# Triggered by a Fabric Translytical Task Flow on the Manual Controls page
# of the Power BI report. The task flow binds control_id from the selected
# ic_control_register row and presents a form for the review fields.
#
# Identity is derived from the Fabric runtime session context — it is never
# passed as a notebook parameter.
# =============================================================================

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# CELL 1 — Parameters (bound by Translytical Task Flow)
# ---------------------------------------------------------------------------
control_id       = mssparkutils.notebook.getParam("control_id")
period_covered   = mssparkutils.notebook.getParam("period_covered")
observations     = mssparkutils.notebook.getParam("observations",     defaultValue="")
items_checked    = mssparkutils.notebook.getParam("items_checked",    defaultValue="")
exceptions_found = mssparkutils.notebook.getParam("exceptions_found", defaultValue="")
conclusion       = mssparkutils.notebook.getParam("conclusion",       defaultValue="")
follow_up_action = mssparkutils.notebook.getParam("follow_up_action", defaultValue="")
notes            = mssparkutils.notebook.getParam("notes",            defaultValue="")

# ---------------------------------------------------------------------------
# CELL 2 — Identity from Fabric session context
# ---------------------------------------------------------------------------
attested_by = (mssparkutils.runtime.context.get("userEmailAddress") or mssparkutils.runtime.context.get("userName") or "").strip().lower()
assert attested_by, "Could not resolve caller identity from Fabric session"

# ---------------------------------------------------------------------------
# CELL 3 — Validate parameters
# ---------------------------------------------------------------------------
assert control_id,     "control_id is required"
assert period_covered, "period_covered is required"
assert conclusion in ("Working", "Needs Attention", "Not Working"), \
    "conclusion must be one of: Working, Needs Attention, Not Working"
if conclusion != "Working":
    assert follow_up_action and len(follow_up_action.strip()) >= 10, \
        "follow_up_action (min 10 chars) is required when conclusion is not Working"

# Notebook params are always strings — coerce integers safely
try:
    items_checked_int    = int(items_checked)    if items_checked    else None
    exceptions_found_int = int(exceptions_found) if exceptions_found else None
except ValueError:
    items_checked_int, exceptions_found_int = None, None

# ---------------------------------------------------------------------------
# CELL 4 — Load config and look up control
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, "/lakehouse/default/Files/Configs")
from QualityCatalogConfig import (  # noqa: E402
    IC_ATTESTATION_AUTHORIZED_EMAILS,
    IC_CONTROL_REGISTER_TABLE,
    IC_ATTESTATIONS_TABLE,
)

authorized_emails = {e.strip().lower() for e in str(IC_ATTESTATION_AUTHORIZED_EMAILS).split(";") if e.strip()}
if not authorized_emails:
    raise RuntimeError(
        "[AUTH] QualityCatalogConfig.IC_ATTESTATION_AUTHORIZED_EMAILS must contain at least one semicolon-separated e-mail."
    )
if attested_by not in authorized_emails:
    raise PermissionError(
        "User '{0}' is not authorized for manual attestations. Update QualityCatalogConfig.IC_ATTESTATION_AUTHORIZED_EMAILS to grant access.".format(attested_by)
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
# CELL 5 — Write attestation row
# ---------------------------------------------------------------------------
now = datetime.now(timezone.utc)

row = [{
    "attestation_id":   str(uuid.uuid4()),
    "control_id":       control_id,
    "attested_by":      attested_by,
    "attested_at":      now,
    "period_covered":   period_covered,
    "observations":     observations.strip() or None,
    "items_checked":    items_checked_int,
    "exceptions_found": exceptions_found_int,
    "conclusion":       conclusion,
    "follow_up_action": follow_up_action.strip() or None,
    "notes":            notes.strip() or None,
}]

spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(IC_ATTESTATIONS_TABLE)

print(
    f"OK: attestation recorded for '{control_id}' "
    f"(period: {period_covered}) by '{attested_by}' — {conclusion}"
)
mssparkutils.notebook.exit("OK")
