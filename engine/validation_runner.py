# =============================================================================
# engine/validation_runner.py
# Data quality validation engine for the PBE Quality Catalog.
#
# Flow:
#   1. Load active rules from the rule_catalog Delta table.
#   2. For each rule group, load the source table from the Spark metastore
#      and apply any configured pre-joins.
#   3. For each rule:
#        a. Dispatch to the matching validator in CUSTOM_EXPECTATION_REGISTRY.
#        b. Collect per-rule results (counts, success %, status).
#        c. Collect per-row violation details.
#   4. Write summary rows to dq_run_results (Delta table).
#   5. Write violation rows to dq_violations via MERGE-based resolution tracking.
#   6. Write IC exceptions to ic_exceptions (for IC-flagged rules).
#   7. Write execution metrics to dq_execution_metrics.
#
# Schedule: nightly (after source tables are refreshed).
# Prerequisites: nb_dq_00_setup.py must have been run at least once.
#                nb_dq_02_migrate_rules.py must have populated rule_catalog.
# =============================================================================


# CELL 2 — Imports and run metadata
# -----------------------------------------------------------------------------
import time
import uuid
from datetime import datetime, date, timezone
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, DateType, IntegerType,
)

spark = SparkSession.builder.getOrCreate()
spark.sql("SET spark.sql.ansi.enabled = false")


# CELL 3 — Import custom expectation registry and resolution helpers
# All expectation classes are consolidated in engine/expectations.py.
# Resolution-tracking helpers live in engine/resolution.py.
# -----------------------------------------------------------------------------
import sys
from functools import reduce

_repo_root = str(Path(__file__).parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from engine.expectations import CUSTOM_EXPECTATION_REGISTRY   # noqa: E402
from engine.resolution import (                                # noqa: E402
    VIOLATION_SCHEMA,
    IC_EXCEPTION_SCHEMA,
    _apply_resolution_tracking,
    _apply_ic_resolution_tracking,
    # _notify_new_ic_exceptions,  # DISABLED — PA flow not yet configured
)
from engine.runtime import (                                   # noqa: E402
    classify_retryable_error,
    load_config_module,
    require_config_keys,
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
RUN_TIMESTAMP = datetime.now(timezone.utc)
BATCH_DATE = date.today()
STARTED_AT = datetime.now(timezone.utc)

print(f"Run ID    : {RUN_ID}")
print(f"Timestamp : {RUN_TIMESTAMP.isoformat()}")
print(f"Batch date: {BATCH_DATE}")
print(f"Config    : {CONFIG_PATH}")
print(f"Runtime   : {RUNTIME_PATH}")
print(f"Dry run   : {RUNTIME.DRY_RUN}")
print(f"Results   : {TARGETS['results_table']}")
print(f"Violations: {TARGETS['violations_table']}")
print(f"Metrics   : {TARGETS['execution_metrics_table']}")


# CELL 4 — Helper functions
# -----------------------------------------------------------------------------
def _load_all_rules() -> list[dict]:
    """Load active rules from the rule_catalog Delta table."""
    catalog_df = (
        spark.table("rule_catalog")
        .filter(F.col("status") == "Active")
        .toPandas()
    )
    if len(catalog_df) == 0:
        raise RuntimeError(
            "rule_catalog is empty or has no Active rules. "
            "Run nb_dq_02_migrate_rules.py to populate it."
        )
    print(f"  Loaded {len(catalog_df)} active rules from rule_catalog.")
    return _build_catalogs_from_df(catalog_df)


def _build_catalogs_from_df(df) -> list[dict]:
    """
    Convert a rule_catalog Pandas DataFrame into the list-of-catalog-dicts
    format the validation engine expects. Groups rows by
    (rule_group, source_table, database, pk_column, joins_json).
    """
    import json as _json

    catalogs = []
    group_cols = ["rule_group", "source_table", "database", "pk_column", "joins_json"]
    for key, group_df in df.groupby(group_cols, dropna=False):
        rule_group, source_table, database, pk_column, joins_json = key
        rules = []
        for _, row in group_df.iterrows():
            rule = {
                "rule_id":     row["rule_id"],
                "name":        row["name"] or "",
                "expectation": row["expectation"] or "",
                "severity":    row["severity"] or "medium",
                "category":    row.get("category"),
                "owner":       row.get("owner"),
                "owner_email": row.get("owner_email"),
            }
            if row.get("column_name"):
                rule["column"] = row["column_name"]
            if row.get("parameters"):
                try:
                    rule["parameters"] = _json.loads(row["parameters"])
                except Exception as exc:
                    print(f"  Warning: invalid JSON in parameters for rule {row['rule_id']}: {exc}")
            if row.get("sql_expression"):
                rule["sql"] = row["sql_expression"]
            for ic_field in ("control_ref", "control_type", "risk_domain",
                             "remediation_due_days"):
                if row.get(ic_field) is not None:
                    rule[ic_field] = row[ic_field]
            rules.append(rule)

        joins = []
        if joins_json and str(joins_json) not in ("None", "nan", ""):
            try:
                joins = _json.loads(joins_json)
            except Exception as exc:
                print(f"  Warning: invalid JSON in joins_json for rule group {rule_group}: {exc}")

        catalogs.append({
            "rule_group": rule_group or "",
            "table":      source_table or "",
            "database":   database or "",
            "pk_column":  pk_column or "id",
            "joins":      joins,
            "rules":      rules,
        })
        print(f"  Rule group: {rule_group} ({len(rules)} rules)")
    return catalogs


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
    StructField("owner_email",          StringType(),    True),
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
    # IC passthrough fields — populated for IC rules, None for DQ-only rules
    StructField("control_ref",           StringType(),   True),
    StructField("control_type",          StringType(),   True),
    StructField("risk_domain",           StringType(),   True),
    StructField("remediation_due_days",  IntegerType(),  True),
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
) -> tuple:
    """
    Validate source_df against all rules in rule_catalog.

    Parameters
    ----------
    rule_catalog : dict loaded from the rule_catalog Delta table (one rule group)
    source_df    : full Spark DataFrame to validate
    pk_col       : primary key column of source_df (stored as primary_key_value
                   in each violation row)

    Returns
    -------
    results_df    Spark DataFrame matching RESULT_SCHEMA
    violations_df Spark DataFrame matching VIOLATION_SCHEMA
    """
    rule_group = rule_catalog["rule_group"]
    table_name = rule_catalog["table"]
    rules      = rule_catalog.get("rules", [])

    all_results    = []
    violation_dfs  = []

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
            if exp_name in CUSTOM_EXPECTATION_REGISTRY:
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
            rule.get("owner_email"),
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
            # IC passthrough (None for DQ-only rules)
            rule.get("control_ref"),
            rule.get("control_type"),
            rule.get("risk_domain"),
            rule.get("remediation_due_days"),
        ))

        if viols_spark is not None:
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
                F.col("primary_key_value"),
                F.col("violated_column"),
                F.col("actual_value"),
                F.col("expected_condition"),
                F.col("violation_detail"),
                F.lit("Active").alias("issue_status"),
                F.lit(None).cast("string").alias("resolution_timestamp"),
            )
            violation_dfs.append(viols_spark)

    results_df = spark.createDataFrame(all_results, schema=RESULT_SCHEMA)
    all_violations = (
        reduce(lambda a, b: a.unionByName(b), violation_dfs)
        if violation_dfs
        else _empty_violations()
    )
    return results_df, all_violations


def _empty_ic_exceptions():
    return spark.createDataFrame([], IC_EXCEPTION_SCHEMA)


def main() -> tuple[int, int]:
    print("\nLoading rule catalogs from rule_catalog Delta table…")
    all_catalogs = _load_all_rules()
    if not all_catalogs and RUNTIME.FAIL_ON_EMPTY_RULES:
        raise RuntimeError("No rule catalogs loaded from rule_catalog.")

    # Pre-compute IC rule metadata so we can enrich violations after the main loop.
    # A rule is IC if it carries at least one of the IC_IDENTIFIER_FIELDS.
    _ic_id_fields = getattr(CONFIG, "IC_IDENTIFIER_FIELDS", ["control_ref", "control_type", "risk_domain"])
    ic_rule_meta: dict[str, dict] = {}
    for _cat in all_catalogs:
        for _rule in _cat.get("rules", []):
            if any(_rule.get(f) for f in _ic_id_fields):
                ic_rule_meta[_rule["rule_id"]] = {
                    "control_ref":          _rule.get("control_ref"),
                    "control_type":         _rule.get("control_type"),
                    "risk_domain":          _rule.get("risk_domain"),
                    "remediation_due_days": _rule.get("remediation_due_days"),
                    "owner_email":          _rule.get("owner_email"),
                }
    ic_rule_ids = set(ic_rule_meta.keys())
    if ic_rule_ids:
        print(f"  IC rules detected: {sorted(ic_rule_ids)}")

    result_dfs    = []
    violation_dfs = []

    for catalog in all_catalogs:
        rule_group = catalog["rule_group"]
        table_name = catalog["table"]
        database = catalog.get("database", "")
        pk_col = catalog.get("pk_column", "id")
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
        )

        result_dfs.append(results_df)
        violation_dfs.append(violations_df)

    all_results_combined = (
        reduce(lambda a, b: a.unionByName(b), result_dfs)
        if result_dfs
        else _empty_results()
    )
    all_violations_combined = (
        reduce(lambda a, b: a.unionByName(b), violation_dfs)
        if violation_dfs
        else _empty_violations()
    )

    all_results_combined.cache()
    all_violations_combined.cache()
    results_count    = all_results_combined.count()
    violations_count = all_violations_combined.count()

    # Print the top-5 slowest rules to help identify bottlenecks.
    if results_count > 0:
        print("\n--- Top-5 slowest rules (by rule_duration_seconds) ---")
        all_results_combined.orderBy(
            "rule_duration_seconds", ascending=False
        ).select("rule_id", "rule_name", "status", "rule_duration_seconds").show(
            5, truncate=False
        )

    print("\nWriting results to Delta tables…")

    all_results_combined.write.mode("append").saveAsTable(TARGETS["results_table"])
    print(f"  {TARGETS['results_table']} : {results_count} rows appended.")

    _apply_resolution_tracking(
        all_violations_combined,
        spark_session=spark,
        violations_table=TARGETS["violations_table"],
        run_timestamp=RUN_TIMESTAMP,
    )
    print(f"  {TARGETS['violations_table']} : {violations_count} violations processed.")

    # ------------------------------------------------------------------
    # IC dual-write: results → ic_run_results, violations → ic_exceptions
    # ------------------------------------------------------------------
    if ic_rule_ids:
        # Build a metadata DataFrame to join IC-specific fields onto violations.
        _ic_meta_schema = StructType([
            StructField("rule_id",               StringType(),  False),
            StructField("control_ref",           StringType(),  True),
            StructField("control_type",          StringType(),  True),
            StructField("risk_domain",           StringType(),  True),
            StructField("remediation_due_days",  IntegerType(), True),
            StructField("owner_email",           StringType(),  True),
        ])
        _ic_meta_rows = [
            (rid, m["control_ref"], m["control_type"], m["risk_domain"],
             m["remediation_due_days"], m["owner_email"])
            for rid, m in ic_rule_meta.items()
        ]
        ic_meta_df = spark.createDataFrame(_ic_meta_rows, schema=_ic_meta_schema)

        # Filter violations to IC rules only.
        ic_base_violations = all_violations_combined.filter(
            F.col("rule_id").isin(ic_rule_ids)
        )
        ic_violations_count = ic_base_violations.count()

        if ic_violations_count > 0:
            # Enrich with IC metadata and lifecycle columns, reordering to match
            # IC_EXCEPTION_SCHEMA so that MERGE INSERT * works correctly.
            ic_violations = (
                ic_base_violations
                .join(ic_meta_df, on="rule_id", how="left")
                .withColumn("ic_status",   F.lit("Open"))
                .withColumn("first_seen_at", F.lit(RUN_TIMESTAMP).cast("timestamp"))
                .withColumn(
                    "remediation_due_date",
                    F.expr(
                        "CASE WHEN remediation_due_days IS NOT NULL "
                        "THEN date_add(to_date(first_seen_at), "
                        "     cast(remediation_due_days AS INT)) "
                        "ELSE NULL END"
                    ).cast("date"),
                )
                # Human-set workflow fields — engine always writes NULL
                .withColumn("remediated_by",  F.lit(None).cast("string"))
                .withColumn("remediated_at",  F.lit(None).cast("timestamp"))
                .withColumn("verified_by",    F.lit(None).cast("string"))
                .withColumn("verified_at",    F.lit(None).cast("timestamp"))
                .withColumn("waived_by",      F.lit(None).cast("string"))
                .withColumn("waived_at",      F.lit(None).cast("timestamp"))
                .withColumn("waiver_reason",  F.lit(None).cast("string"))
                # Drop DQ-only columns not present in IC_EXCEPTION_SCHEMA
                .drop("issue_status", "resolution_timestamp")
                # Select in IC_EXCEPTION_SCHEMA column order for clean INSERT *
                .select([f.name for f in IC_EXCEPTION_SCHEMA.fields])
            )

            _apply_ic_resolution_tracking(
                ic_violations,
                spark_session=spark,
                ic_exceptions_table=TARGETS["ic_exceptions_table"],
                run_timestamp=RUN_TIMESTAMP,
            )
            print(f"  {TARGETS['ic_exceptions_table']} : {ic_violations_count} IC violations processed.")

            # DISABLED — uncomment once the Power Automate flow is configured
            # and the URL is stored in IC_NOTIFY_URL_PATH:
            # _notify_new_ic_exceptions(
            #     ic_violations,
            #     notify_url_path=getattr(CONFIG, "IC_NOTIFY_URL_PATH",
            #                             "/lakehouse/default/Files/Configs/pa_notify_url.txt"),
            # )
        else:
            print(f"  {TARGETS['ic_exceptions_table']} : 0 IC violations this run.")

        # Write IC run summaries (subset of dq_run_results rows for IC rules).
        ic_results = all_results_combined.filter(F.col("rule_id").isin(ic_rule_ids))
        ic_results_count = ic_results.count()
        ic_results.write.mode("append").saveAsTable(TARGETS["ic_results_table"])
        print(f"  {TARGETS['ic_results_table']} : {ic_results_count} IC summary rows appended.")

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

    all_results_combined.unpersist()
    all_violations_combined.unpersist()
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
