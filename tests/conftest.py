import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

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

_TEST_CONFIG = f'''
DEFAULT_SCHEMA = "{TEST_SCHEMA}"
RULES_DIR = "rules"
DQ_RESULTS_TABLE = "dq_run_results"
DQ_VIOLATIONS_TABLE = "dq_violations"
DQ_EXECUTION_METRICS_TABLE = "dq_execution_metrics"
'''

# DRY_RUN routes writes to the _tmp tables. MAX_RULE_RETRIES = 0 keeps the
# retry backoff (2s, 4s) out of the test run.
_TEST_RUNTIME = '''
DRY_RUN = True
FAIL_ON_EMPTY_RULES = True
FAIL_ON_EMPTY_SOURCE = True
RETRYABLE_ERROR_MARKERS = ["timeout", "temporar", "connection", "unavailable", "throttle"]
MAX_RULE_RETRIES = 0
RULE_TIMEOUT_SECONDS = 300
CATALOG_FILTER_OVERRIDES = {}
'''


@pytest.fixture(scope="session")
def runner(spark, tmp_path_factory):
    """
    Import engine.validation_runner against a throwaway config.

    load_config_module() only searches the Lakehouse config directories, and
    QualityCatalogConfig.py in this repo is explicitly a non-loaded template.
    Rather than change that contract, point the candidate list at a temp dir
    for the duration of the test session.
    """
    from tests import fixtures

    config_dir = tmp_path_factory.mktemp("configs")
    (config_dir / "QualityCatalogConfig.py").write_text(_TEST_CONFIG)
    (config_dir / "QualityCatalogRuntime.py").write_text(_TEST_RUNTIME)

    import engine.runtime as _runtime
    _original = _runtime.LAKEHOUSE_CONFIG_DIR_CANDIDATES
    _runtime.LAKEHOUSE_CONFIG_DIR_CANDIDATES = (config_dir,)

    fixtures.create_source_tables(spark)

    # Import after the config redirect: the module bootstraps config at import
    # time. Output tables are only touched at write time, so they can follow.
    import engine.validation_runner as vr

    fixtures.create_output_tables(
        spark,
        TEST_SCHEMA,
        vr.RESULT_SCHEMA,
        vr.VIOLATION_SCHEMA,
        _runtime._EXECUTION_METRIC_SCHEMA,
    )

    yield vr

    _runtime.LAKEHOUSE_CONFIG_DIR_CANDIDATES = _original
