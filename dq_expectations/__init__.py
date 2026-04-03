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

# Map YAML expectation names to validator classes
CUSTOM_EXPECTATION_REGISTRY = {
    "expect_milestone_order":        CaseMilestoneOrderExpectation,
    "expect_milestone_pairs":        CaseMilestonePairsExpectation,
    "expect_no_open_milestone_pairs": CaseNoOpenMilestonePairsExpectation,
    "expect_refund_validation":       InvoiceRefundValidationExpectation,
    "expect_invoice_total_consistency": InvoiceTotalConsistencyExpectation,
}

__all__ = [
    "CaseMilestoneOrderExpectation",
    "CaseMilestonePairsExpectation",
    "CaseNoOpenMilestonePairsExpectation",
    "InvoiceRefundValidationExpectation",
    "InvoiceTotalConsistencyExpectation",
    "CUSTOM_EXPECTATION_REGISTRY",
]
