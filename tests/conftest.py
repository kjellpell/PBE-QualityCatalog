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
