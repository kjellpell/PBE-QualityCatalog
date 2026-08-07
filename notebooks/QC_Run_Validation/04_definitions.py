# =============================================================================
# QC_Run_Validation
#
# Runs the Quality Catalog and writes `dq_run_results`, `dq_violations` and
# `dq_execution_metrics`. Schedule this nightly, after the source tables have
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
                script_name,
                status,
                row_count,
                started_at_utc,
                finished_at_utc,
                duration_seconds,
                is_retryable,
                error_message
            FROM {metrics_table}
            ORDER BY finished_at_utc DESC
            LIMIT 10
            """
        ).show(truncate=False)

    if has_results:
        latest = spark.sql(
            f"SELECT run_id FROM {results_table} ORDER BY run_timestamp DESC LIMIT 1"
        ).collect()
        if latest:
            run_id = latest[0]["run_id"]
            print(f"\nRule-group summary for latest run_id: {run_id}")
            # Pass run_id via a temp view rather than string interpolation to
            # prevent second-order SQL injection from crafted values in the table.
            spark.createDataFrame([(run_id,)], ["_run_id"]).createOrReplaceTempView("_ev_run_id")
            spark.sql(
                f"""
                SELECT
                    rule_group,
                    COUNT(*) AS total_rules,
                    SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
                    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors
                FROM {results_table}
                WHERE run_id = (SELECT _run_id FROM _ev_run_id)
                GROUP BY rule_group
                ORDER BY rule_group
                """
            ).show(truncate=False)
            try:
                spark.catalog.dropTempView("_ev_run_id")
            except Exception:
                pass  # best-effort cleanup; non-fatal

    if has_violations:
        print("\nCurrent violations by issue_status:")
        spark.sql(
            f"""
            SELECT issue_status, COUNT(*) AS cnt
            FROM {violations_table}
            GROUP BY issue_status
            ORDER BY cnt DESC
            """
        ).show(truncate=False)
