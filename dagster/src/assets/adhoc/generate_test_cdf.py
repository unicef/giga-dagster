from delta import DeltaTable
from pyspark.sql.functions import expr
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.sentry import capture_op_exceptions
from src.utils.spark import PySparkResource, transform_school_types

from dagster import OpExecutionContext, asset

source_path = f"{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables/school-master/BLZ_school_geolocation_coverage_master"
clone_path = (
    f"{settings.AZURE_BLOB_CONNECTION_URI}/gold/delta-tables/school-master/ZCDF"
)


@asset
@capture_op_exceptions
def clone_table(
    _: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
) -> None:
    df = adls_file_client.download_delta_table_as_spark_dataframe(
        spark=spark.spark_session,
        table_path=source_path,
    )
    df = transform_school_types(df)
    adls_file_client.upload_spark_dataframe_as_delta_table(
        data=df,
        table_path=clone_path,
        dataset_type="master",
        spark=spark.spark_session,
    )


@asset(deps=["clone_table"])
@capture_op_exceptions
def update_values_u1(_: OpExecutionContext, spark: PySparkResource) -> None:
    dt = DeltaTable.forPath(spark.spark_session, clone_path)
    dt.update(
        condition=expr("cellular_coverage_availability == 'No'"),
        set={"cellular_coverage_availability": expr("'Yes'")},
    )
    dt.delete(condition=expr("cellular_coverage_type == 'no coverage'"))


@asset(deps=["update_values_u1"])
@capture_op_exceptions
def update_values_u2(_: OpExecutionContext, spark: PySparkResource) -> None:
    dt = DeltaTable.forPath(spark.spark_session, clone_path)
    dt.update(
        condition=expr("connectivity == 'Yes'"),
        set={"cellular_coverage_availability": expr("'No'")},
    )
    dt.delete(condition=expr("cellular_coverage_type == '2G'"))
