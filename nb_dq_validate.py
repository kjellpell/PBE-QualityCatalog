# =============================================================================
# NB_DQ_VALIDATE.py
# YAML-driven data quality validation using Great Expectations (GX Core).
#
# Flow:
#   1. Install GX Core and load YAML rule catalogs.
#   2. Load source tables from Saksbehandling lakehouse:
#        Saksbehandling.Prosesser   → Process (case) records
#        Saksbehandling.Milepel     → Milestone records per process
#        Saksbehandling.Fakturalinjer → Invoice lines per process
#   3. For each rule group (Process, Milestone, Invoice):
#        a. Dispatch standard GX expectations via the GX Core validator.
#        b. Dispatch custom expectations to the dedicated validator classes.
#        c. Collect per-rule results (counts, success %, status).
#        d. Collect per-row violation details (including prosess_id).
#   4. Write summary rows to dq_run_results (Delta table).
#   5. Write violation rows to dq_violations (Delta table).
#   6. Print a human-readable run summary.
#
# All validation rules (columns, thresholds, sequences, pairs) are defined in
# YAML files under dq_rules/.  No rule logic is hardcoded in this script.
#
# Schedule: nightly (after source tables are refreshed).
# Prerequisites: nb_dq_00_setup.py must have been run at least once.
# =============================================================================


# CELL 1 — Install Great Expectations
# Run this cell only once per cluster / Fabric session restart.
# Comment it out after the first run to save startup time.
# -----------------------------------------------------------------------------
# %pip install great-expectations==1.3.10


# CELL 2 — Imports and run metadata
# -----------------------------------------------------------------------------
import yaml
import uuid
from datetime import datetime, date
from pathlib import Path

import great_expectations as gx
from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
)
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, DateType,
)

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")

RUN_ID        = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.utcnow()
BATCH_DATE    = date.today()

# Absolute path to the YAML rule folder (adjust if notebooks run elsewhere)
RULES_DIR = Path(__file__).parent / "dq_rules"

print(f"Run ID    : {RUN_ID}")
print(f"Timestamp : {RUN_TIMESTAMP.isoformat()}")
print(f"Batch date: {BATCH_DATE}")


# CELL 3 — Import custom GX expectation validators
# These classes are defined in the dq_expectations package.
# -----------------------------------------------------------------------------
import sys, os

# Add repo root to path so the package can be imported inside a Fabric notebook
_repo_root = str(Path(__file__).parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from dq_expectations import CUSTOM_EXPECTATION_REGISTRY   # noqa: E402

print("Custom expectation registry loaded:", list(CUSTOM_EXPECTATION_REGISTRY))


# CELL 4 — GX Core context and helper functions
# -----------------------------------------------------------------------------
# GX native expectation names supported via the GX validator.
# Any name not listed here is dispatched to the custom registry.
GX_NATIVE_EXPECTATIONS = {
    "expect_column_values_to_not_be_null",
    "expect_column_values_to_be_greater_than",
    "expect_column_values_to_be_less_than",
    "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set",
    "expect_column_to_exist",
    "expect_table_row_count_to_be_between",
}

# GX operates on a Pandas sample for standard expectations.
# Adjust SAMPLE_SIZE for the trade-off between accuracy and runtime.
SAMPLE_SIZE = 50_000


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _get_gx_context() -> gx.DataContext:
    """Return an ephemeral (in-memory) GX data context."""
    return gx.get_context()


def _run_gx_expectation(
    context: gx.DataContext,
    pdf,
    rule: dict,
    suite_name: str,
) -> dict:
    """
    Run a single GX built-in expectation against a Pandas DataFrame sample.
    Returns a result_dict compatible with the standard contract.
    """
    try:
        # Re-create the datasource and asset each time (ephemeral context)
        ds_name    = f"ds_{suite_name}_{rule['rule_id']}"
        asset_name = rule["rule_id"]

        datasource = context.data_sources.add_pandas(ds_name)
        asset      = datasource.add_dataframe_asset(name=asset_name)
        batch_def  = asset.add_batch_definition_whole_dataframe(
            name=f"{asset_name}_batch"
        )
        batch = batch_def.get_batch(
            batch_parameters={"dataframe": pdf}
        )

        suite = context.suites.add(
            gx.ExpectationSuite(name=f"{suite_name}_{rule['rule_id']}")
        )

        # Build expectation config — column at top level OR inside parameters
        kwargs = {}
        if "column" in rule:
            kwargs["column"] = rule["column"]
        if "parameters" in rule:
            kwargs.update(rule["parameters"])

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type=rule["expectation"],
                kwargs=kwargs,
            )
        )

        validation_def = context.validation_definitions.add(
            gx.ValidationDefinition(
                name=f"vd_{suite_name}_{rule['rule_id']}",
                data=batch_def,
                suite=suite,
            )
        )
        results = validation_def.run()

        # Extract counts from GX result
        er = results.results[0] if results.results else None
        if er is None:
            raise ValueError("No result returned from GX validator.")

        total     = int(er.result.get("element_count", len(pdf)))
        failed    = int(er.result.get("unexpected_count", 0))
        passed    = total - failed
        success   = er.success

        return {
            "total_rows":  total,
            "passed_rows": passed,
            "failed_rows": failed,
            "success_pct": round(passed / total * 100, 2) if total else 100.0,
            "status":      "PASSED" if success else "FAILED",
            "details": (
                f"GX: {failed} unexpected value(s) for "
                f"'{rule.get('column', '')}'."
                if not success
                else (
                    f"GX: All {total} sampled rows passed "
                    f"'{rule['expectation']}'."
                )
            ),
        }

    except Exception as exc:
        return {
            "total_rows":  0,
            "passed_rows": 0,
            "failed_rows": 0,
            "success_pct": 0.0,
            "status":      "ERROR",
            "details":     f"GX execution error: {exc}",
        }


def _violations_for_gx_rule(
    full_df,
    rule: dict,
    pk_col: str,
    prosess_id_col: str | None = None,
    saksbehandler_col: str | None = None,
) -> "DataFrame | None":
    """
    For GX null-check expectations, compute violations against the FULL
    Spark DataFrame so the violation table is complete (not limited to sample).
    Returns None for other GX expectations (not implemented at row level).

    The column to check is read from rule["column"] — fully dynamic.
    """
    if rule["expectation"] != "expect_column_values_to_not_be_null":
        return None

    # Column is specified dynamically in YAML
    col = rule.get("column")
    if not col or col not in full_df.columns:
        return None

    base_cols = [
        F.col(pk_col).cast("string").alias("primary_key_value"),
        F.lit(col).alias("violated_column"),
        F.lit(None).cast("string").alias("actual_value"),
        F.lit(f"{col} must not be null").alias("expected_condition"),
        F.lit(f"{col} is null").alias("violation_detail"),
    ]
    if saksbehandler_col and saksbehandler_col in full_df.columns:
        base_cols.append(
            F.col(saksbehandler_col).cast("string").alias("saksbehandler_kode")
        )
    return full_df.filter(F.col(col).isNull()).select(*base_cols)


# CELL 5 — Schema for result accumulation
# -----------------------------------------------------------------------------
RESULT_SCHEMA = StructType([
    StructField("run_id",         StringType(),    False),
    StructField("run_timestamp",  TimestampType(), False),
    StructField("batch_date",     DateType(),      False),
    StructField("rule_group",     StringType(),    False),
    StructField("rule_id",        StringType(),    False),
    StructField("rule_name",      StringType(),    False),
    StructField("table_name",     StringType(),    False),
    StructField("expectation",    StringType(),    False),
    StructField("severity",       StringType(),    False),
    StructField("owner",          StringType(),    False),
    StructField("total_rows",     LongType(),      True),
    StructField("passed_rows",    LongType(),      True),
    StructField("failed_rows",    LongType(),      True),
    StructField("success_pct",    DoubleType(),    True),
    StructField("status",         StringType(),    False),
    StructField("details",        StringType(),    True),
    # Cross-column comparison metadata (populated for validate_column_comparison)
    StructField("column_a",       StringType(),    True),
    StructField("column_b",       StringType(),    True),
    StructField("operator",       StringType(),    True),
    # SQL fallback metadata (populated for sql_validation)
    StructField("sql_query",      StringType(),    True),
])

VIOLATION_SCHEMA = StructType([
    StructField("run_id",             StringType(),    False),
    StructField("run_timestamp",      TimestampType(), False),
    StructField("batch_date",         DateType(),      False),
    StructField("rule_group",         StringType(),    False),
    StructField("rule_id",            StringType(),    False),
    StructField("rule_name",          StringType(),    False),
    StructField("table_name",         StringType(),    False),
    StructField("severity",           StringType(),    False),
    StructField("owner",              StringType(),    False),
    StructField("prosess_id",         StringType(),    True),
    StructField("primary_key_value",  StringType(),    True),
    StructField("violated_column",    StringType(),    True),
    StructField("actual_value",       StringType(),    True),
    StructField("expected_condition", StringType(),    True),
    StructField("violation_detail",   StringType(),    True),
    StructField("saksbehandler_kode", StringType(),    True),
])


def _empty_results():
    return spark.createDataFrame([], RESULT_SCHEMA)


def _empty_violations():
    return spark.createDataFrame([], VIOLATION_SCHEMA)


# CELL 6 — Main validation engine
# -----------------------------------------------------------------------------
def run_validation(
    rule_catalog: dict,
    source_df,
    pk_col: str,
    prosess_id_col: str | None = None,
    saksbehandler_col: str | None = None,
) -> tuple:
    """
    Validate source_df against all rules in rule_catalog.

    Parameters
    ----------
    rule_catalog     : dict loaded from a YAML rule file
    source_df        : full Spark DataFrame to validate
    pk_col           : primary key column of source_df (for violation rows)
    prosess_id_col   : column in source_df that holds the prosess_id link
                       (used to populate the prosess_id field in violations).
                       If None, prosess_id is set to primary_key_value when
                       pk_col == prosess_id_col, else NULL.
    saksbehandler_col: optional handler code column (Process table only)

    Returns
    -------
    results_df    Spark DataFrame matching RESULT_SCHEMA
    violations_df Spark DataFrame matching VIOLATION_SCHEMA
    """
    rule_group  = rule_catalog["rule_group"]
    table_name  = rule_catalog["table"]
    rules       = rule_catalog.get("rules", [])

    all_results    = []
    all_violations = _empty_violations()

    gx_context = _get_gx_context()

    # Pandas sample for GX native expectations.
    pdf_sample = source_df.limit(SAMPLE_SIZE).toPandas()

    for rule in rules:
        rule_id   = rule["rule_id"]
        rule_name = rule["name"]
        exp_name  = rule["expectation"]
        severity  = rule.get("severity", "medium")
        owner     = rule.get("owner", "")

        # Extract metadata for cross-column and SQL rules
        params    = rule.get("parameters", {})
        column_a  = params.get("column_A") if exp_name == "validate_column_comparison" else None
        column_b  = params.get("column_B") if exp_name == "validate_column_comparison" else None
        operator  = params.get("operator")  if exp_name == "validate_column_comparison" else None
        sql_query = params.get("sql")       if exp_name == "sql_validation"             else None

        print(f"  → [{rule_id}] {rule_name} ({exp_name}) ... ", end="")

        try:
            if exp_name in GX_NATIVE_EXPECTATIONS:
                # ── GX built-in expectation ──────────────────────────────────
                result = _run_gx_expectation(
                    gx_context, pdf_sample, rule, suite_name=rule_group
                )
                # For null checks: also get row-level violations from full df
                viols_spark = _violations_for_gx_rule(
                    source_df, rule, pk_col, prosess_id_col, saksbehandler_col
                )

            elif exp_name in CUSTOM_EXPECTATION_REGISTRY:
                # ── Custom PySpark expectation ───────────────────────────────
                validator    = CUSTOM_EXPECTATION_REGISTRY[exp_name]()
                result, viols_spark = validator.validate(
                    source_df, rule, spark
                )

            else:
                result = {
                    "total_rows":  0,
                    "passed_rows": 0,
                    "failed_rows": 0,
                    "success_pct": 0.0,
                    "status":      "ERROR",
                    "details":     f"Unknown expectation: '{exp_name}'",
                }
                viols_spark = None

        except Exception as exc:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"Unexpected error: {exc}",
            }
            viols_spark = None

        print(result["status"])

        all_results.append((
            RUN_ID,
            RUN_TIMESTAMP,
            BATCH_DATE,
            rule_group,
            rule_id,
            rule_name,
            table_name,
            exp_name,
            severity,
            owner,
            result["total_rows"],
            result["passed_rows"],
            result["failed_rows"],
            result["success_pct"],
            result["status"],
            result["details"],
            column_a,
            column_b,
            operator,
            sql_query,
        ))

        # Attach run metadata and prosess_id to violation rows
        if viols_spark is not None and viols_spark.count() > 0:
            # Ensure optional columns exist
            if "saksbehandler_kode" not in viols_spark.columns:
                viols_spark = viols_spark.withColumn(
                    "saksbehandler_kode",
                    F.lit(None).cast("string"),
                )

            # Derive prosess_id for this violation batch:
            #   - If prosess_id_col == pk_col (or pk_col is the process key):
            #     prosess_id = primary_key_value
            #   - If a separate prosess_id column is available in source_df:
            #     join it in from the source table
            #   - Otherwise: NULL
            if prosess_id_col and prosess_id_col == pk_col:
                # Primary key IS the prosess_id (Process table)
                prosess_id_expr = F.col("primary_key_value")
            elif (
                prosess_id_col
                and prosess_id_col in source_df.columns
                and prosess_id_col != pk_col
            ):
                # primary_key_value links back to source; join for prosess_id
                lookup = source_df.select(
                    F.col(pk_col).cast("string").alias("_pk"),
                    F.col(prosess_id_col).cast("string").alias("_prosess_id"),
                ).dropDuplicates(["_pk"])
                viols_spark = viols_spark.join(
                    lookup,
                    viols_spark["primary_key_value"] == lookup["_pk"],
                    how="left",
                ).withColumn(
                    "_resolved_prosess_id",
                    F.col("_prosess_id"),
                )
                prosess_id_expr = F.col("_resolved_prosess_id")
            else:
                prosess_id_expr = F.lit(None).cast("string")

            viols_spark = viols_spark.select(
                F.lit(RUN_ID).alias("run_id"),
                F.lit(RUN_TIMESTAMP).alias("run_timestamp"),
                F.lit(str(BATCH_DATE)).cast("date").alias("batch_date"),
                F.lit(rule_group).alias("rule_group"),
                F.lit(rule_id).alias("rule_id"),
                F.lit(rule_name).alias("rule_name"),
                F.lit(table_name).alias("table_name"),
                F.lit(severity).alias("severity"),
                F.lit(owner).alias("owner"),
                prosess_id_expr.cast("string").alias("prosess_id"),
                F.col("primary_key_value"),
                F.col("violated_column"),
                F.col("actual_value"),
                F.col("expected_condition"),
                F.col("violation_detail"),
                F.col("saksbehandler_kode"),
            )
            all_violations = all_violations.unionByName(viols_spark)

    results_df = spark.createDataFrame(all_results, schema=RESULT_SCHEMA)
    return results_df, all_violations


# CELL 7 — Load source tables
# -----------------------------------------------------------------------------
print("Loading source tables…")

# Process data — Saksbehandling.Prosesser
# Attach case status from Saksbehandling.Saker so milestone-pair rules
# that reference Status can work correctly.
prosesser_df = spark.read.table("Saksbehandling.Prosesser")
saker_df = (
    spark.read.table("Saksbehandling.Saker")
    .select("Saksnummer", "Status")
)
prosesser_df = prosesser_df.join(saker_df, on="Saksnummer", how="left")
print(f"  Prosesser rows       : {prosesser_df.count():,}")

# Milestone data — Saksbehandling.Milepel
milepel_df = spark.read.table("Saksbehandling.Milepel")
print(f"  Milepel rows         : {milepel_df.count():,}")

# Invoice data — Saksbehandling.Fakturalinjer
fakturalinjer_df = spark.read.table("Saksbehandling.Fakturalinjer")
print(f"  Fakturalinjer rows   : {fakturalinjer_df.count():,}")


# CELL 8 — Run Process validations
# -----------------------------------------------------------------------------
print("\n=== PROCESS VALIDATIONS (Saksbehandling.Prosesser) ===")
process_catalog = _load_yaml(RULES_DIR / "process_rules.yaml")
process_results, process_violations = run_validation(
    rule_catalog=process_catalog,
    source_df=prosesser_df,
    pk_col="Saksnummer",
    prosess_id_col="Saksnummer",   # the process IS the case; PK = prosess_id
    saksbehandler_col="Saksbehandler_kode",
)


# CELL 9 — Run Milestone validations
# -----------------------------------------------------------------------------
print("\n=== MILESTONE VALIDATIONS (Saksbehandling.Milepel) ===")
milestone_catalog = _load_yaml(RULES_DIR / "milestone_rules.yaml")
milestone_results, milestone_violations = run_validation(
    rule_catalog=milestone_catalog,
    source_df=milepel_df,
    pk_col="prosess_id",
    prosess_id_col="prosess_id",   # PK is prosess_id for the milestone table
    saksbehandler_col=None,
)


# CELL 10 — Run Invoice validations
# -----------------------------------------------------------------------------
print("\n=== INVOICE VALIDATIONS (Saksbehandling.Fakturalinjer) ===")
invoice_catalog = _load_yaml(RULES_DIR / "invoice_rules.yaml")
invoice_results, invoice_violations = run_validation(
    rule_catalog=invoice_catalog,
    source_df=fakturalinjer_df,
    pk_col="Fakturanr",
    prosess_id_col="prosess_id",   # FK to Prosesser (NULL if column absent)
    saksbehandler_col=None,
)


# CELL 11 — Combine and write results to Delta tables
# -----------------------------------------------------------------------------
print("\nWriting results to Delta tables…")

all_results = (
    process_results
    .unionByName(milestone_results)
    .unionByName(invoice_results)
)
all_violations = (
    process_violations
    .unionByName(milestone_violations)
    .unionByName(invoice_violations)
)

all_results.write.mode("append").saveAsTable("dq_run_results")
print(f"  dq_run_results    : {all_results.count()} rows appended.")

all_violations.write.mode("append").saveAsTable("dq_violations")
print(f"  dq_violations     : {all_violations.count()} rows appended.")


# CELL 12 — Run summary
# -----------------------------------------------------------------------------
print("\n=== DATA QUALITY RUN SUMMARY ===")
spark.sql(f"""
    SELECT
        rule_group,
        COUNT(*)                                                    AS total_rules,
        SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END)         AS passed,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)         AS failed,
        SUM(CASE WHEN status = 'ERROR'  THEN 1 ELSE 0 END)         AS errors,
        ROUND(
            SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END)
            * 100.0 / COUNT(*), 1
        )                                                           AS quality_score_pct
    FROM dq_run_results
    WHERE run_id = '{RUN_ID}'
    GROUP BY rule_group
    ORDER BY rule_group
""").show(truncate=False)

print("\n=== FAILED / ERROR RULES ===")
spark.sql(f"""
    SELECT
        rule_group,
        rule_id,
        rule_name,
        severity,
        failed_rows,
        ROUND(success_pct, 1) AS success_pct,
        details
    FROM dq_run_results
    WHERE run_id = '{RUN_ID}'
    AND   status IN ('FAILED', 'ERROR')
    ORDER BY
        CASE severity
            WHEN 'critical' THEN 1
            WHEN 'high'     THEN 2
            WHEN 'medium'   THEN 3
            ELSE 4
        END,
        rule_group,
        rule_id
""").show(50, truncate=False)

print(f"\nRun complete.  Run ID: {RUN_ID}")
