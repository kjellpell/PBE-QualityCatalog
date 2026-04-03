# =============================================================================
# dq_expectations/__init__.py
# Registers all custom GX expectations so that the validation notebook can
# import them with a single  "from dq_expectations import *".
# =============================================================================

from dq_expectations.case_expectations import (
    CaseMilestoneOrderExpectation,
    CaseMilestonePairsExpectation,
    CaseNoOpenMilestonePairsExpectation,
)
from dq_expectations.invoice_expectations import (
    InvoiceRefundValidationExpectation,
    InvoiceTotalConsistencyExpectation,
)
from dq_expectations.milestone_expectations import (
    MilestoneNoDuplicatesExpectation,
    MilestoneSequenceExpectation,
    MilestonePairsCompleteExpectation,
    MilestoneNoOrphanExpectation,
)

# Map YAML expectation names to validator classes.
# Add new entries here when creating new custom validators.
CUSTOM_EXPECTATION_REGISTRY = {
    # Case / Process validators (Saksbehandling.Prosesser)
    "expect_milestone_order":              CaseMilestoneOrderExpectation,
    "expect_milestone_pairs":              CaseMilestonePairsExpectation,
    "expect_no_open_milestone_pairs":      CaseNoOpenMilestonePairsExpectation,
    # Invoice validators (Saksbehandling.Fakturalinjer)
    "expect_refund_validation":            InvoiceRefundValidationExpectation,
    "expect_invoice_total_consistency":    InvoiceTotalConsistencyExpectation,
    # Milestone validators (Saksbehandling.Milepel)
    "expect_no_duplicate_milestones":      MilestoneNoDuplicatesExpectation,
    "expect_milestone_sequence":           MilestoneSequenceExpectation,
    "expect_milestone_pairs_complete":     MilestonePairsCompleteExpectation,
    "expect_no_orphan_milestones":         MilestoneNoOrphanExpectation,
}

__all__ = [
    "CaseMilestoneOrderExpectation",
    "CaseMilestonePairsExpectation",
    "CaseNoOpenMilestonePairsExpectation",
    "InvoiceRefundValidationExpectation",
    "InvoiceTotalConsistencyExpectation",
    "MilestoneNoDuplicatesExpectation",
    "MilestoneSequenceExpectation",
    "MilestonePairsCompleteExpectation",
    "MilestoneNoOrphanExpectation",
    "CUSTOM_EXPECTATION_REGISTRY",
]
