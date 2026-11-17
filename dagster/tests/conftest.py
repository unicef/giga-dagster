import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local Spark session for tests that exercise pure DataFrame logic.

    Deliberately minimal: no Delta, no Hive, no ADLS. Only modules that build
    Spark expressions from configuration are tested here.
    """
    session = (
        SparkSession.builder.master("local[1]")
        .appName("giga-dagster-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
