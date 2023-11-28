from dagster_ge.factory import ge_data_context
from dagster_pyspark import PySparkResource

from dagster import Definitions, load_assets_from_package_module
from src import assets
from src._utils.adls import ADLSFileClient
from src._utils.sentry import setup_sentry
from src.jobs import (
    school_master__get_gold_delta_tables_job,
    school_master__run_automated_data_checks_job,
    school_master__run_failed_manual_checks_job,
    school_master__run_successful_manual_checks_job,
)
from src.resources.io_manager import StagingADLSIOManager
from src.sensors import (
    school_master__failed_manual_checks_sensor,
    school_master__get_gold_delta_tables_sensor,
    school_master__raw_file_uploads_sensor,
    school_master__successful_manual_checks_sensor,
)
from src.settings import settings

setup_sentry()

pyspark = PySparkResource(
    spark_config={
        "spark.master": f"spark://{settings.SPARK_MASTER_HOST}",
        "spark.driver.extraJavaOptions": str.join(
            " ",
            [
                "-Divy.cache.dir=/tmp",
                "-Divy.home=/tmp",
                "-Dio.netty.tryReflectionSetAccessible=true",
            ],
        ),
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.warehouse.dir": "/opt/spark/warehouse",
        "spark.sql.catalogImplementation": "hive",
        "spark.driver.cores": "1",
        "spark.driver.memory": "512m",
        "spark.executor.cores": "1",
        "spark.executor.memory": "512m",
        "spark.executor.instances": "1",
        "spark.authenticate": "true",
        "spark.authenticate.secret": settings.SPARK_RPC_AUTHENTICATION_SECRET,
        "spark.authenticate.enableSaslEncryption": "true",
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        "spark.databricks.delta.properties.defaults.appendOnly": "false",
        f"fs.azure.sas.{settings.AZURE_BLOB_CONTAINER_NAME}.{settings.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net": (
            settings.AZURE_SAS_TOKEN
        ),
    }
)

defs = Definitions(
    assets=[
        *load_assets_from_package_module(
            package_module=assets, group_name="school_master_data"
        ),
    ],
    resources={
        "ge_data_context": ge_data_context.configured(
            {"ge_root_dir": "src/resources/great_expectations"}
        ),
        "adls_io_manager": StagingADLSIOManager(pyspark=pyspark),
        "adls_file_client": ADLSFileClient(),
        "pyspark": pyspark,
    },
    jobs=[
        school_master__run_automated_data_checks_job,
        school_master__run_successful_manual_checks_job,
        school_master__run_failed_manual_checks_job,
        school_master__get_gold_delta_tables_job,
    ],
    sensors=[
        school_master__raw_file_uploads_sensor,
        school_master__successful_manual_checks_sensor,
        school_master__failed_manual_checks_sensor,
        school_master__get_gold_delta_tables_sensor,
    ],
)
