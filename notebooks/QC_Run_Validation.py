# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run QC_Config

# CELL ********************

%run QC_Rules

# CELL ********************

%run QC_Engine

# CELL ********************

# =============================================================================
# QC_Run_Validation
#
# Runs the Quality Catalog and writes `kjoeringsresultater`, `avvik` and
# `kjoeringslogg`. Schedule this nightly, after the source tables have
# refreshed.
# =============================================================================

from pyspark.sql import SparkSession


def _table_exists(spark, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def print_run_evidence(config_mapping: dict) -> None:
    """Show the latest execution metrics, rule-group summary, and open violations."""
    spark = SparkSession.builder.getOrCreate()

    config = build_settings(config_mapping, CONFIG_REQUIRED_KEYS, "QUALITY_CATALOG_CONFIG")

    # resolve_targets, not a second set of _qualify calls: the report has to
    # name the tables the run actually wrote to, and it validates the names
    # before they reach the f-strings below.
    targets = resolve_targets(config)
    results_table = targets["results_table"]
    violations_table = targets["violations_table"]

    # Each _table_exists is a Spark job, so probe once and reuse the answer.
    has_results = _table_exists(spark, results_table)
    has_violations = _table_exists(spark, violations_table)

    # write_execution_metric falls back to an unqualified table name when the
    # namespace is not resolvable, so look for the metrics table in both places.
    metrics_table = next(
        (
            candidate
            for candidate in (
                targets["execution_metrics_table"],
                config.DQ_EXECUTION_METRICS_TABLE,
            )
            if _table_exists(spark, candidate)
        ),
        None,
    )

    print("Detected tables:")
    print("  results_table:", results_table if has_results else None)
    print("  violations_table:", violations_table if has_violations else None)
    print("  metrics_table:", metrics_table)

    if metrics_table:
        print("\nLatest execution metrics rows:")
        spark.sql(
            f"""
            SELECT
                skriptnavn,
                status,
                antall_rader,
                starttidspunkt_utc,
                sluttidspunkt_utc,
                varighet_sekunder,
                kan_proeves_igjen,
                feilmelding
            FROM {metrics_table}
            ORDER BY sluttidspunkt_utc DESC
            LIMIT 10
            """
        ).show(truncate=False)

    if has_results:
        latest = spark.sql(
            f"SELECT kjoert_id FROM {results_table} ORDER BY kjoert_tidspunkt DESC LIMIT 1"
        ).collect()
        if latest:
            run_id = latest[0]["kjoert_id"]
            print(f"\nRule-group summary for latest kjoert_id: {run_id}")
            # Pass run_id via a temp view rather than string interpolation to
            # prevent second-order SQL injection from crafted values in the table.
            spark.createDataFrame([(run_id,)], ["_run_id"]).createOrReplaceTempView("_ev_run_id")
            spark.sql(
                f"""
                SELECT
                    regelgruppe,
                    COUNT(*) AS total_rules,
                    SUM(CASE WHEN status = 'Bestått' THEN 1 ELSE 0 END) AS passed,
                    SUM(CASE WHEN status = 'Ikke bestått' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 'Feil' THEN 1 ELSE 0 END) AS errors
                FROM {results_table}
                WHERE kjoert_id = (SELECT _run_id FROM _ev_run_id)
                GROUP BY regelgruppe
                ORDER BY regelgruppe
                """
            ).show(truncate=False)
            try:
                spark.catalog.dropTempView("_ev_run_id")
            except Exception:
                pass  # best-effort cleanup; non-fatal

    if has_violations:
        print("\nCurrent violations by avviksstatus:")
        spark.sql(
            f"""
            SELECT avviksstatus, COUNT(*) AS cnt
            FROM {violations_table}
            GROUP BY avviksstatus
            ORDER BY cnt DESC
            """
        ).show(truncate=False)

# CELL ********************

# ENTRYPOINT — the cell that runs this notebook.
configure(QUALITY_CATALOG_CONFIG, QUALITY_CATALOG_RUNTIME)

results_count, violations_count = run_with_metrics(RULE_CATALOG_SOURCES, "run_validation")
print(f"\nResult rows: {results_count}   Violations processed: {violations_count}")

print_run_evidence(QUALITY_CATALOG_CONFIG)
