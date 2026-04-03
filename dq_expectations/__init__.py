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
from dq_expectations.common_expectations import (
    ColumnComparisonExpectation,
    SqlValidationExpectation,
    ColumnSumExpectation,
    RowCountExpectation,
    UniqueColumnCombinationExpectation,
    ForeignKeyExpectation,
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
    # Generic cross-table validators (any table)
    "validate_column_comparison":          ColumnComparisonExpectation,
    "sql_validation":                      SqlValidationExpectation,
    # sql is a shorthand alias for sql_validation (top-level sql: key)
    "sql":                                 SqlValidationExpectation,
    # Aggregate-level validators
    "expect_column_sum_to_equal":          ColumnSumExpectation,
    "expect_row_count_to_be_between":      RowCountExpectation,
    "expect_unique_combination_of_columns": UniqueColumnCombinationExpectation,
    # Cross-table / referential integrity validators
    "validate_foreign_key":                ForeignKeyExpectation,
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
    "ColumnComparisonExpectation",
    "SqlValidationExpectation",
    "ColumnSumExpectation",
    "RowCountExpectation",
    "UniqueColumnCombinationExpectation",
    "ForeignKeyExpectation",
    "CUSTOM_EXPECTATION_REGISTRY",
]
