# from dagster_pyspark import PySparkResource
# from pyspark import sql
# from pyspark.sql import (
#     SparkSession,
# )
# from src.constants import DataTier
# from src.resources import ResourceKey
# from src.utils.metadata import get_output_metadata, get_table_preview
# from src.utils.op_config import FileConfig
# from src.utils.schema import (
#     construct_full_table_name,
#     construct_schema_name_for_tier,
#     get_schema_columns,
# )
# from src.utils.spark import compute_row_hash, transform_types
#
# from dagster import OpExecutionContext, Output, asset
#
#
# def get_df(
#     s: SparkSession,
#     schema_name: str,
#     country_code: str,
#     tier: DataTier,
# ) -> Output[sql.DataFrame]:
#     schema_columns = get_schema_columns(s, schema_name)
#     tier_schema_name = construct_schema_name_for_tier(
#         schema_name=schema_name, tier=tier
#     )
#     table_name = construct_full_table_name(
#         schema_name=tier_schema_name, table_name=country_code
#     )
#     df = (
#         s.read.format("delta")
#         .option("readChangeFeed", "true")
#         .option("startingVersion", 0)
#         .table(table_name)
#     )
#     df = df.select(*[c.name for c in schema_columns])
#
#     return df
#
#
# def join_gold(
#     config: FileConfig,
#     s: SparkSession,
# ) -> Output[sql.DataFrame]:
#     adhoc__publish_master_to_gold = get_df(
#         s,
#         schema_name="school_master",
#         country_code=config.country_code,
#         tier=DataTier.GOLD,
#     )
#     adhoc__publish_reference_to_gold = get_df(
#         s,
#         schema_name="school_reference",
#         country_code=config.country_code,
#         tier=DataTier.GOLD,
#     )
#     adhoc__publish_master_to_gold = adhoc__publish_master_to_gold.drop("signature")
#     df_one_gold = adhoc__publish_master_to_gold.join(
#         adhoc__publish_reference_to_gold, on="school_id_giga", how="left"
#     )
#     columns_with_null = [
#         "cellular_coverage_availability",
#         "cellular_coverage_type",
#         "school_id_govt_type",
#         "education_level_govt",
#     ]
#     df_one_gold = df_one_gold.fillna("Unknown", columns_with_null)
#     return df_one_gold
#
#
# @asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
# def adhoc__generate_silver_geolocation(
#     context: OpExecutionContext,
#     config: FileConfig,
#     spark: PySparkResource,
# ) -> Output[sql.DataFrame]:
#     s: SparkSession = spark.spark_session
#
#     df_one_gold = join_gold(config, s)
#
#     schema_name = "school_geolocation"
#     schema_columns = get_schema_columns(s, schema_name)
#
#     df_silver = df_one_gold.select([c.name for c in schema_columns])
#     df_silver = df_silver.drop("signature")
#     df_silver = transform_types(df_silver, schema_name, context)
#     df_silver = compute_row_hash(df_silver)
#
#     return Output(
#         df_silver,
#         metadata={
#             **get_output_metadata(config),
#             "preview": get_table_preview(df_silver),
#         },
#     )
#
#
# @asset(io_manager_key=ResourceKey.ADLS_DELTA_IO_MANAGER.value)
# def adhoc__generate_silver_coverage(
#     context: OpExecutionContext,
#     config: FileConfig,
#     spark: PySparkResource,
# ) -> Output[sql.DataFrame]:
#     s: SparkSession = spark.spark_session
#
#     df_one_gold = join_gold(config, s)
#
#     schema_name = "school_coverage"
#     schema_columns = get_schema_columns(s, schema_name)
#
#     df_silver = df_one_gold.select([c.name for c in schema_columns])
#     df_silver = df_silver.drop("signature")
#     df_silver = transform_types(df_silver, schema_name, context)
#     df_silver = compute_row_hash(df_silver)
#
#     return Output(
#         df_silver,
#         metadata={
#             **get_output_metadata(config),
#             "preview": get_table_preview(df_silver),
#         },
#     )
#
#
# if __name__ == "__main__":
#     from src.utils.spark import get_spark_session
#
#     s = get_spark_session()
#
#     def test_join_gold(
#         # config: FileConfig,
#         s: SparkSession = s,
#     ) -> Output[sql.DataFrame]:
#         adhoc__publish_master_to_gold = get_df(
#             s,
#             schema_name="school_master",
#             country_code="BEN",  # config.country_code,
#             tier=DataTier.GOLD,
#         )
#         adhoc__publish_reference_to_gold = get_df(
#             s,
#             schema_name="school_reference",
#             country_code="BEN",  # config.country_code,
#             tier=DataTier.GOLD,
#         )
#         adhoc__publish_master_to_gold = adhoc__publish_master_to_gold.drop("signature")
#         df_one_gold = adhoc__publish_master_to_gold.join(
#             adhoc__publish_reference_to_gold, on="school_id_giga", how="left"
#         )
#         columns_with_null = [
#             "cellular_coverage_availability",
#             "cellular_coverage_type",
#             "school_id_govt_type",
#             "education_level_govt",
#         ]
#         df_one_gold = df_one_gold.fillna("Unknown", columns_with_null)
#         return df_one_gold
#
#     gold = test_join_gold()
