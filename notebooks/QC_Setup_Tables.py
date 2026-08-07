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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run QC_Engine

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# QC_Setup_Tables
#
# Creates the three Delta output tables from the engine's own schemas. Run
# once per workspace before the first validation run, and again after an
# engine schema change.
# =============================================================================

from pyspark.sql import SparkSession


# Nothing here runs at load time; setup_dq_tables() opens the session and
# threads it through, rather than binding the `spark` that QC_Engine owns.
#
# Each output table is described once, by the schema the engine writes.
#   dq_run_results       one row per rule per validation run (Power BI scorecard)
#   dq_violations        one row per violation with lifecycle state
#                        (Active / Resolved); violation_scope says whether
#                        primary_key_value is a row key or a group key
#   dq_execution_metrics one row per runner execution, for operator visibility
# -----------------------------------------------------------------------------
_TABLE_SCHEMAS = {
    "DQ_RESULTS_TABLE": RESULT_SCHEMA,
    "DQ_VIOLATIONS_TABLE": VIOLATION_SCHEMA,
    "DQ_EXECUTION_METRICS_TABLE": _EXECUTION_METRIC_SCHEMA,
}


def _column_ddl(struct) -> str:
    width = max(len(f.name) for f in struct.fields)
    return ",\n".join(
        f"    {f.name.ljust(width)} {f.dataType.simpleString().upper()}"
        for f in struct.fields
    )


def _ensure_table(spark, table: str, struct) -> None:
    """Create the table if absent; verify it matches the schema if it already exists.

    There is deliberately no migration path. A table left over from an earlier
    deployment is reported so it can be dropped, rather than quietly patched into
    something that only resembles the current schema.
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} (\n{_column_ddl(struct)}\n) USING DELTA"
    )

    expected = [(f.name.lower(), f.dataType.simpleString()) for f in struct.fields]
    actual = [(f.name.lower(), f.dataType.simpleString()) for f in spark.table(table).schema.fields]
    if actual == expected:
        return

    expected_types, actual_types = dict(expected), dict(actual)
    missing = [n for n in expected_types if n not in actual_types]
    extra = [n for n in actual_types if n not in expected_types]
    changed = [
        f"{n} (expected {t}, found {actual_types[n]})"
        for n, t in expected
        if n in actual_types and actual_types[n] != t
    ]
    detail = "\n".join(
        f"  {label}: {', '.join(items)}"
        for label, items in (
            ("missing columns", missing),
            ("unexpected columns", extra),
            ("wrong type", changed),
        )
        if items
    ) or "  column order differs from the engine schema"

    raise RuntimeError(
        f"Table {table} already exists with a different schema.\n{detail}\n"
        "Drop the table and re-run this notebook — no in-place migration is applied."
    )


def setup_dq_tables(config_mapping: dict) -> list[str]:
    """Create the schema and the three output tables. Returns their full names."""
    spark = SparkSession.builder.getOrCreate()
    spark.sql("SET spark.sql.ansi.enabled = false")

    config = build_settings(
        config_mapping,
        ["DEFAULT_SCHEMA", *_TABLE_SCHEMAS],
        "QUALITY_CATALOG_CONFIG",
    )

    schema = config.DEFAULT_SCHEMA
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Spark ready. Target schema: {schema}")

    created = []
    for config_key, struct in _TABLE_SCHEMAS.items():
        # _qualify, not an unconditional prefix: a config that already names a
        # schema must create the table the engine will actually write to.
        table = _qualify(getattr(config, config_key), schema)
        _ensure_table(spark, table, struct)
        created.append(table)
        print(f"  {table} ready ({len(struct.fields)} columns).")

    # Performance tip: after the first significant data load, apply Z-order
    # clustering on the columns most used in WHERE / JOIN predicates:
    #
    #   spark.sql("OPTIMIZE dq_violations ZORDER BY (rule_id, primary_key_value)")
    #
    # Re-run periodically (e.g. weekly) or after large backfills.

    print("\n=== DQ SETUP COMPLETE ===")
    print(f"Tables: {', '.join(created)}")
    return created

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ENTRYPOINT — the cell that runs this notebook.
setup_dq_tables(QUALITY_CATALOG_CONFIG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
