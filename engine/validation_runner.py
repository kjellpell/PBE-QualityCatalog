# =============================================================================
# engine/validation_runner.py
# YAML-driven data quality validation using Great Expectations (GX Core).
#
# Flow:
#   1. Install GX Core and load all YAML rule catalogs from the rules/ folder.
#   2. For each rule file, load the source table from the Spark metastore
#      using the table and database fields in the YAML header, then apply
#      any configured pre-joins.
#   3. For each rule in the catalog:
#        a. Dispatch standard GX expectations via the GX Core validator.
#        b. Dispatch custom expectations to the CUSTOM_EXPECTATION_REGISTRY.
#        c. Collect per-rule results (counts, success %, status).
#        d. Collect per-row violation details (including prosess_id).
#   4. Write summary rows to dq_run_results (Delta table).
#   5. Write violation rows to dq_violations (Delta table).
#   6. Print a human-readable run summary.
#
# All validation rules are defined in YAML files under rules/.
# The engine automatically discovers and processes every *.yaml file in that
# folder — no code changes are needed when adding new rule files.
#
# Phase 1 structure:
#   engine/expectations.py   – all custom expectation classes (consolidated)
#   engine/validation_runner.py – this file
#   rules/                   – domain-specific YAML rule files
#   outputs/validation_results/ – Delta table result logs
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
import time
import yaml
import uuid
from datetime import datetime, date, timezone
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


# CELL 3 — Import custom expectation registry and resolution helpers
# All expectation classes are consolidated in engine/expectations.py.
# Resolution-tracking helpers live in engine/resolution.py.
# -----------------------------------------------------------------------------
import sys, os

_repo_root = str(Path(__file__).parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from engine.expectations import CUSTOM_EXPECTATION_REGISTRY   # noqa: E402
from engine.resolution import (                                # noqa: E402
    VIOLATION_SCHEMA,
    _find_stale_violations,
    _apply_resolution_tracking,
)
from engine.runtime import (                                   # noqa: E402
    classify_retryable_error,
    load_config_module,
    require_config_keys,
    resolve_rules_dir,
    resolve_targets,
    write_execution_metric,
)

print("Custom expectation registry loaded:", list(CUSTOM_EXPECTATION_REGISTRY))

CONFIG, CONFIG_PATH = load_config_module("QualityCatalogConfig")
RUNTIME, RUNTIME_PATH = load_config_module("QualityCatalogRuntime")
require_config_keys(
    CONFIG,
    [
        "DEFAULT_SCHEMA",
        "RULES_DIR",
        "GX_SAMPLE_SIZE",
        "DQ_RESULTS_TABLE",
        "DQ_VIOLATIONS_TABLE",
        "DQ_EXECUTION_METRICS_TABLE",
    ],
    "QualityCatalogConfig",
)
require_config_keys(
    RUNTIME,
    [
        "DRY_RUN",
        "FAIL_ON_EMPTY_RULES",
        "FAIL_ON_EMPTY_SOURCE",
        "MAX_RETRIES",
        "RETRYABLE_ERROR_MARKERS",
    ],
    "QualityCatalogRuntime",
)
TARGETS = resolve_targets(CONFIG, RUNTIME)

RUN_ID = str(uuid.uuid4())
RUN_TIMESTAMP = datetime.utcnow()
BATCH_DATE = date.today()
STARTED_AT = datetime.now(timezone.utc)
RULES_DIR = resolve_rules_dir(CONFIG, Path(__file__).parent.parent)

print(f"Run ID    : {RUN_ID}")
print(f"Timestamp : {RUN_TIMESTAMP.isoformat()}")
print(f"Batch date: {BATCH_DATE}")
print(f"Rules dir : {RULES_DIR}")
print(f"Config    : {CONFIG_PATH}")
print(f"Runtime   : {RUNTIME_PATH}")
print(f"Dry run   : {RUNTIME.DRY_RUN}")
print(f"Results   : {TARGETS['results_table']}")
print(f"Violations: {TARGETS['violations_table']}")
print(f"Metrics   : {TARGETS['execution_metrics_table']}")


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
SAMPLE_SIZE = CONFIG.GX_SAMPLE_SIZE


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _load_all_rules(rules_dir: Path) -> list[dict]:
    """
    Discover and load all *.yaml files in rules_dir.
    Returns a list of rule catalog dicts, one per file.
    Files are processed in alphabetical order for deterministic runs.
    """
    catalogs = []
    yaml_files = sorted(rules_dir.glob("*.yaml"))
    if not yaml_files:
        print(f"  Warning: no *.yaml files found in {rules_dir}")
    for yaml_path in yaml_files:
        catalog = _load_yaml(yaml_path)
        catalogs.append(catalog)
        print(f"  Loaded rule file: {yaml_path.name} "
              f"({len(catalog.get('rules', []))} rules)")
    return catalogs


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
        ds_name    = f"ds_{suite_name}_{rule['rule_id']}"
        asset_name = rule["rule_id"]

        datasource = context.data_sources.add_pandas(ds_name)
        asset      = datasource.add_dataframe_asset(name=asset_name)
        batch_def  = asset.add_batch_definition_whole_dataframe(
            name=f"{asset_name}_batch"
        )
        batch = batch_def.get_batch(batch_parameters={"dataframe": pdf})

        suite = context.suites.add(
            gx.ExpectationSuite(name=f"{suite_name}_{rule['rule_id']}")
        )

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

        er = results.results[0] if results.results else None
        if er is None:
            raise ValueError("No result returned from GX validator.")

        total   = int(er.result.get("element_count", len(pdf)))
        failed  = int(er.result.get("unexpected_count", 0))
        passed  = total - failed
        success = er.success

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
        rule_id  = rule.get("rule_id", "?")
        exp_name = rule.get("expectation", "?")
        return {
            "total_rows":  0,
            "passed_rows": 0,
            "failed_rows": 0,
            "success_pct": 0.0,
            "status":      "ERROR",
            "details":     f"[{rule_id}/{exp_name}] GX execution error: {exc}",
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
    """
    if rule["expectation"] != "expect_column_values_to_not_be_null":
        return None

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
    StructField("run_id",               StringType(),    False),
    StructField("run_timestamp",        TimestampType(), False),
    StructField("batch_date",           DateType(),      False),
    StructField("rule_group",           StringType(),    False),
    StructField("rule_id",              StringType(),    False),
    StructField("rule_name",            StringType(),    False),
    StructField("table_name",           StringType(),    False),
    StructField("expectation",          StringType(),    False),
    StructField("severity",             StringType(),    False),
    StructField("owner",                StringType(),    False),
    StructField("total_rows",           LongType(),      True),
    StructField("passed_rows",          LongType(),      True),
    StructField("failed_rows",          LongType(),      True),
    StructField("success_pct",          DoubleType(),    True),
    StructField("status",               StringType(),    False),
    StructField("details",              StringType(),    True),
    StructField("column_a",             StringType(),    True),
    StructField("column_b",             StringType(),    True),
    StructField("operator",             StringType(),    True),
    StructField("sql_query",            StringType(),    True),
    StructField("rule_category",        StringType(),    True),
    StructField("reference_table",      StringType(),    True),
    StructField("reference_column",     StringType(),    True),
    StructField("rule_duration_seconds", DoubleType(),   True),
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
    saksbehandler_col: optional handler code column (Process table only)

    Returns
    -------
    results_df    Spark DataFrame matching RESULT_SCHEMA
    violations_df Spark DataFrame matching VIOLATION_SCHEMA
    """
    rule_group = rule_catalog["rule_group"]
    table_name = rule_catalog["table"]
    rules      = rule_catalog.get("rules", [])

    all_results    = []
    all_violations = _empty_violations()

    gx_context = _get_gx_context()
    pdf_sample = source_df.limit(SAMPLE_SIZE).toPandas()

    for rule in rules:
        rule_id   = rule["rule_id"]
        rule_name = rule["name"]
        exp_name  = rule["expectation"]
        severity  = rule.get("severity", "medium")
        owner     = rule.get("owner", "")

        params    = rule.get("parameters", {})
        column_a  = params.get("column_A") if exp_name == "validate_column_comparison" else None
        column_b  = params.get("column_B") if exp_name == "validate_column_comparison" else None
        operator  = params.get("operator")  if exp_name == "validate_column_comparison" else None
        sql_query = (
            params.get("sql") or rule.get("sql")
            if exp_name in ("sql_validation", "sql")
            else None
        )

        rule_category    = rule.get("category") or rule.get("rule_category")
        ref_block        = params.get("reference", {})
        reference_table  = ref_block.get("table")  if exp_name == "validate_foreign_key" else None
        reference_column = ref_block.get("column") if exp_name == "validate_foreign_key" else None

        print(f"  → [{rule_id}] {rule_name} ({exp_name}) ... ", end="")
        _rule_start = time.perf_counter()

        try:
            if exp_name in GX_NATIVE_EXPECTATIONS:
                result = _run_gx_expectation(
                    gx_context, pdf_sample, rule, suite_name=rule_group
                )
                viols_spark = _violations_for_gx_rule(
                    source_df, rule, pk_col, prosess_id_col, saksbehandler_col
                )

            elif exp_name in CUSTOM_EXPECTATION_REGISTRY:
                validator   = CUSTOM_EXPECTATION_REGISTRY[exp_name]()
                result, viols_spark = validator.validate(source_df, rule, spark)

            else:
                result = {
                    "total_rows":  0,
                    "passed_rows": 0,
                    "failed_rows": 0,
                    "success_pct": 0.0,
                    "status":      "ERROR",
                    "details":     f"[{table_name}/{rule_id}] Unknown expectation: '{exp_name}'",
                }
                viols_spark = None

        except Exception as exc:
            result = {
                "total_rows":  0,
                "passed_rows": 0,
                "failed_rows": 0,
                "success_pct": 0.0,
                "status":      "ERROR",
                "details":     f"[{table_name}/{rule_id}/{exp_name}] Unexpected error: {exc}",
            }
            viols_spark = None

        _rule_elapsed = time.perf_counter() - _rule_start
        print(f"{result['status']} ({_rule_elapsed:.2f}s)")

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
            rule_category,
            reference_table,
            reference_column,
            round(_rule_elapsed, 3),
        ))

        if viols_spark is not None and viols_spark.count() > 0:
            if "saksbehandler_kode" not in viols_spark.columns:
                viols_spark = viols_spark.withColumn(
                    "saksbehandler_kode",
                    F.lit(None).cast("string"),
                )

            if prosess_id_col and prosess_id_col == pk_col:
                prosess_id_expr = F.col("primary_key_value")
            elif (
                prosess_id_col
                and prosess_id_col in source_df.columns
                and prosess_id_col != pk_col
            ):
                lookup = source_df.select(
                    F.col(pk_col).cast("string").alias("_pk"),
                    F.col(prosess_id_col).cast("string").alias("_prosess_id"),
                ).dropDuplicates(["_pk"])
                viols_spark = viols_spark.join(
                    lookup,
                    viols_spark["primary_key_value"] == lookup["_pk"],
                    how="left",
                ).withColumn("_resolved_prosess_id", F.col("_prosess_id"))
                prosess_id_expr = F.col("_resolved_prosess_id")
                # Warn if 100% of join keys missed — usually a type mismatch
                # between primary_key_value (string) and pk_col in source_df.
                null_count = viols_spark.filter(
                    F.col("_resolved_prosess_id").isNull()
                ).count()
                if null_count > 0 and null_count == viols_spark.count():
                    print(
                        f"  Warning [{rule_id}]: prosess_id join on "
                        f"'{prosess_id_col}' produced no matches — all "
                        f"{null_count} violation row(s) will have null "
                        f"prosess_id. Check key type alignment."
                    )
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
                F.lit(rule_category).alias("failure_type"),
                prosess_id_expr.cast("string").alias("prosess_id"),
                F.col("primary_key_value"),
                F.col("violated_column"),
                F.col("actual_value"),
                F.col("expected_condition"),
                F.col("violation_detail"),
                F.col("saksbehandler_kode"),
                F.lit("Active").alias("issue_status"),
                F.lit(None).cast("string").alias("resolution_timestamp"),
            )
            all_violations = all_violations.unionByName(viols_spark)

    results_df = spark.createDataFrame(all_results, schema=RESULT_SCHEMA)
    return results_df, all_violations


def main() -> tuple[int, int]:
    print("\nLoading rule catalogs from rules/ folder…")
    all_catalogs = _load_all_rules(RULES_DIR)
    if not all_catalogs and RUNTIME.FAIL_ON_EMPTY_RULES:
        raise RuntimeError(f"No rule catalogs found in {RULES_DIR}")

    all_results_combined = _empty_results()
    all_violations_combined = _empty_violations()

    for catalog in all_catalogs:
        rule_group = catalog["rule_group"]
        table_name = catalog["table"]
        database = catalog.get("database", "")
        pk_col = catalog.get("pk_column", "id")
        prosess_col = catalog.get("prosess_id_column")
        handler_col = catalog.get("saksbehandler_column")
        joins_cfg = catalog.get("joins", [])

        full_table = f"{database}.{table_name}" if database else table_name
        print(f"\n=== {rule_group.upper()} VALIDATIONS ({full_table}) ===")

        source_df = spark.read.table(full_table)
        source_count = source_df.count()
        if source_count == 0 and RUNTIME.FAIL_ON_EMPTY_SOURCE:
            raise RuntimeError(f"Source table is empty: {full_table}")
        print(f"  Source rows: {source_count:,}")

        for join_cfg in joins_cfg:
            join_table = join_cfg["table"]
            join_on = join_cfg["on"]
            join_how = join_cfg.get("how", "left")
            join_select = join_cfg.get("select")

            join_df = spark.read.table(join_table)
            if join_select:
                join_df = join_df.select(*join_select)
            source_df = source_df.join(join_df, on=join_on, how=join_how)
            print(f"  Joined with {join_table} on {join_on} ({join_how})")

        results_df, violations_df = run_validation(
            rule_catalog=catalog,
            source_df=source_df,
            pk_col=pk_col,
            prosess_id_col=prosess_col,
            saksbehandler_col=handler_col,
        )

        all_results_combined = all_results_combined.unionByName(results_df)
        all_violations_combined = all_violations_combined.unionByName(violations_df)

    # Print the top-5 slowest rules to help identify bottlenecks.
    if all_results_combined.count() > 0:
        print("\n--- Top-5 slowest rules (by rule_duration_seconds) ---")
        all_results_combined.orderBy(
            "rule_duration_seconds", ascending=False
        ).select("rule_id", "rule_name", "status", "rule_duration_seconds").show(
            5, truncate=False
        )

    print("\nWriting results to Delta tables…")

    all_results_combined.write.mode("append").saveAsTable(TARGETS["results_table"])
    results_count = all_results_combined.count()
    print(f"  {TARGETS['results_table']} : {results_count} rows appended.")

    _apply_resolution_tracking(
        all_violations_combined,
        spark_session=spark,
        violations_table=TARGETS["violations_table"],
        run_timestamp=RUN_TIMESTAMP,
    )
    violations_count = all_violations_combined.count()
    print(f"  {TARGETS['violations_table']} : {violations_count} violations processed.")

    print("\n=== DATA QUALITY RUN SUMMARY ===")
    spark.sql(
        f"""
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
        FROM {TARGETS['results_table']}
        WHERE run_id = '{RUN_ID}'
        GROUP BY rule_group
        ORDER BY rule_group
        """
    ).show(truncate=False)

    return results_count, violations_count


try:
    results_count, violations_count = main()
    FINISHED_AT = datetime.now(timezone.utc)
    write_execution_metric(
        spark,
        TARGETS["execution_metrics_table"],
        {
            "script_name": "validation_runner",
            "status": "Succeeded",
            "dry_run": RUNTIME.DRY_RUN,
            "output_target": TARGETS["results_table"],
            "artifact_target": TARGETS["violations_table"],
            "row_count": int(results_count),
            "started_at_utc": STARTED_AT,
            "finished_at_utc": FINISHED_AT,
            "duration_seconds": float((FINISHED_AT - STARTED_AT).total_seconds()),
            "is_retryable": False,
            "error_message": None,
        },
    )
except Exception as exc:
    FINISHED_AT = datetime.now(timezone.utc)
    write_execution_metric(
        spark,
        TARGETS["execution_metrics_table"],
        {
            "script_name": "validation_runner",
            "status": "Failed",
            "dry_run": RUNTIME.DRY_RUN,
            "output_target": TARGETS["results_table"],
            "artifact_target": TARGETS["violations_table"],
            "row_count": 0,
            "started_at_utc": STARTED_AT,
            "finished_at_utc": FINISHED_AT,
            "duration_seconds": float((FINISHED_AT - STARTED_AT).total_seconds()),
            "is_retryable": classify_retryable_error(str(exc), RUNTIME),
            "error_message": str(exc)[:4000],
        },
    )
    raise
