# =============================================================================
# nb_ic_01_manage_exceptions.py
# Fabric-callable notebook for IC exception status transitions.
#
# Triggered by a Fabric Translytical Task Flow embedded in the IC Exceptions
# page of the Power BI report.  The task flow binds exception_id from the
# selected ic_exceptions row and asks the user for new_status and (when
# Waived) a waiver_reason.
#
# Supported transitions:
#   Open        → Verified   (also accepts Remediated → Verified)
#   Open        → Waived
#
# States set by humans only — the engine never calls this notebook.
# Terminal states (Verified, Waived) are protected: the MERGE in
# _apply_ic_resolution_tracking() only touches Open rows, so a re-appearing
# violation creates a new Open row rather than overwriting a closed one.
#
# ACTIONED_BY is derived from the Fabric session context — it is never
# passed as a notebook parameter.
# =============================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# CELL 1 — Parameters (bound by Translytical Task Flow)
# ---------------------------------------------------------------------------
exception_id  = mssparkutils.notebook.getParam("exception_id")
new_status    = mssparkutils.notebook.getParam("new_status")
waiver_reason = mssparkutils.notebook.getParam("waiver_reason", defaultValue="")

# ---------------------------------------------------------------------------
# CELL 2 — Resolve caller identity from Fabric session context
# ---------------------------------------------------------------------------
try:
    # Extract UPN from the storage token claims — works in most Fabric tenants.
    actioned_by = mssparkutils.credentials.getToken("storage").split("upn:")[1].split(",")[0]
except Exception:
    actioned_by = mssparkutils.runtime.context.get("userName", "unknown")

# ---------------------------------------------------------------------------
# CELL 3 — Validate parameters
# ---------------------------------------------------------------------------
assert exception_id, "exception_id is required"
assert new_status in ("Verified", "Waived"), (
    f"new_status must be 'Verified' or 'Waived', got: '{new_status}'"
)
if new_status == "Waived":
    assert waiver_reason and len(waiver_reason.strip()) >= 10, (
        "waiver_reason is required and must be at least 10 characters when new_status is 'Waived'"
    )

# ---------------------------------------------------------------------------
# CELL 4 — Load config and open Delta table
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, "/lakehouse/default/Files/Configs")
from QualityCatalogConfig import IC_EXCEPTIONS_TABLE  # noqa: E402

now = datetime.now(timezone.utc)

dt = DeltaTable.forName(spark, IC_EXCEPTIONS_TABLE)

# ---------------------------------------------------------------------------
# CELL 5 — Apply transition
# ---------------------------------------------------------------------------
if new_status == "Verified":
    # Verified accepts from Open or Remediated.
    dt.update(
        condition=(
            (F.col("primary_key_value") == exception_id)
            & (F.col("ic_status").isin("Open", "Remediated"))
        ),
        set={
            "ic_status":   F.lit("Verified"),
            "verified_by": F.lit(actioned_by),
            "verified_at": F.lit(now).cast("timestamp"),
        },
    )

elif new_status == "Waived":
    # Waived accepts from Open only.
    dt.update(
        condition=(
            (F.col("primary_key_value") == exception_id)
            & (F.col("ic_status") == F.lit("Open"))
        ),
        set={
            "ic_status":    F.lit("Waived"),
            "waived_by":    F.lit(actioned_by),
            "waived_at":    F.lit(now).cast("timestamp"),
            "waiver_reason": F.lit(waiver_reason.strip()),
        },
    )

# If no rows matched the condition (e.g. already terminal, or invalid
# exception_id) the update silently affects zero rows — this is intentional.
# The caller (Translytical Task Flow) refreshes the report after completion.

print(f"OK: exception '{exception_id}' transitioned to '{new_status}' by '{actioned_by}'")
mssparkutils.notebook.exit("OK")
