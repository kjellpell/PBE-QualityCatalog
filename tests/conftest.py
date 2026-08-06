import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.notebook_source import engine_namespace, load_notebook  # noqa: E402

WAREHOUSE_DIR = REPO_ROOT / "spark-warehouse"


@pytest.fixture(scope="session")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("pbe-quality-catalog-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Fabric's Spark defaults to Delta; the local session does not. Without
        # this, saveAsTable() on an existing Delta table fails with a format
        # mismatch that never happens in the environment the engine runs in.
        .config("spark.sql.sources.default", "delta")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session.sql("SET spark.sql.ansi.enabled = false")

    yield session

    session.stop()
    shutil.rmtree(WAREHOUSE_DIR, ignore_errors=True)
    shutil.rmtree(REPO_ROOT / "metastore_db", ignore_errors=True)
    derby_log = REPO_ROOT / "derby.log"
    if derby_log.exists():
        derby_log.unlink()


TEST_SCHEMA = "dqtest"

TEST_CONFIG = {
    "DEFAULT_SCHEMA": TEST_SCHEMA,
    "DQ_RESULTS_TABLE": "dq_run_results",
    "DQ_VIOLATIONS_TABLE": "dq_violations",
    "DQ_EXECUTION_METRICS_TABLE": "dq_execution_metrics",
}

# MAX_RULE_RETRIES = 0 keeps the retry backoff (2s, 4s) out of the test run.
TEST_RUNTIME = {
    "FAIL_ON_EMPTY_SOURCE": True,
    "RETRYABLE_ERROR_MARKERS": ["timeout", "temporar", "connection", "unavailable", "throttle"],
    "MAX_RULE_RETRIES": 0,
    "RULE_TIMEOUT_SECONDS": 300,
    "CATALOG_FILTER_OVERRIDES": {},
}


@pytest.fixture(scope="session")
def rule_sources():
    """The rule catalogs, exactly as the engine receives them in Fabric."""
    return load_notebook("QC_Rules").RULE_CATALOG_SOURCES


@pytest.fixture(scope="session")
def engine(spark):
    """QC_Engine, configured against a throwaway schema.

    Named `engine` because that is what the notebook is; `runner` is kept as an
    alias so the test modules that predate the notebooks still read naturally.
    """
    from tests import fixtures

    fixtures.create_source_tables(spark)

    nb = engine_namespace()
    nb.configure(TEST_CONFIG, TEST_RUNTIME)

    fixtures.create_output_tables(
        spark,
        TEST_SCHEMA,
        nb.RESULT_SCHEMA,
        nb.VIOLATION_SCHEMA,
        nb._EXECUTION_METRIC_SCHEMA,
    )

    return nb


@pytest.fixture(scope="session")
def runner(engine):
    return engine


@pytest.fixture(scope="session")
def preflight(engine):
    """QC_Preflight composed on top of QC_Engine, the way `%run` composes them."""
    return load_notebook("QC_Preflight", into=engine)


@pytest.fixture(scope="session")
def setup_tables(engine):
    """QC_Setup_Tables composed on top of QC_Engine, the way `%run` composes them."""
    return load_notebook("QC_Setup_Tables", into=engine)
