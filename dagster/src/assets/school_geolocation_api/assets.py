import numpy as np
import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from requests import get
from requests.auth import HTTPBasicAuth
from src.internal.school_geolocation_api_queries import get_mng_api_last_update_date
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.delta import check_table_exists, create_delta_table, create_schema
from src.utils.geolocation_apis.data_updates import (
    delete_schools_from_master,
    upload_data_and_create_db_entry,
)
from src.utils.schema import construct_full_table_name, get_schema_columns

from dagster import OpExecutionContext, Output, asset


@asset
def mng_school_geolocation_api_raw(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
):
    s: SparkSession = spark.spark_session

    table_name = "mng_api_schools"
    table_schema_name = "emis_api_data"

    table_exists = check_table_exists(s, table_schema_name, table_name, None)

    if table_exists:
        last_update_date = get_mng_api_last_update_date()
        context.log.info(f"The last update date is {last_update_date}")

    else:
        last_update_date = None
        context.log.info(
            "There is no last update date because the table does not exist"
        )

    def pull_data_from_api(last_update_date=None):
        full_schools_list = []
        more_data = True
        page = 1

        if last_update_date:
            base_url = (
                f"{settings.MONGOLIA_API_URL}?size=100&UPDATED_AFTER={last_update_date}"
            )
        else:
            base_url = f"{settings.MONGOLIA_API_URL}?size=100"

        auth = HTTPBasicAuth(settings.MONGOLIA_API_USER, settings.MONGOLIA_API_PASSWORD)

        while more_data:
            context.log.info(f"Page Number: {page}")

            url = base_url + f"&page={page}"
            response = get(url, auth=auth)
            result_schools = response.json()["RESULT"]

            if result_schools:
                full_schools_list.extend(result_schools)
                page += 1
            else:
                more_data = False

        return full_schools_list

    full_schools_list = pull_data_from_api(last_update_date=last_update_date)
    schools_pdf = pd.DataFrame(full_schools_list)
    # For testing remove after
    schools_pdf = schools_pdf.sort_values(by=["updated_at"]).head(100)
    context.log.info(f"Number of schools pulled from API: {schools_pdf.shape[0]}")

    schools_pdf["education_level_govt"] = schools_pdf["education_level_govt"].fillna(
        "Unknown"
    )
    schools_pdf["latitude"] = schools_pdf["latitude"].replace("None", np.nan)
    schools_pdf["longitude"] = schools_pdf["longitude"].replace("None", np.nan)
    schools_pdf.rename(
        columns={"latitude": "longitude", "longitude": "latitude"}, inplace=True
    )

    schools_pdf["ingestion_id"] = schools_pdf["school_id"].astype(str) + schools_pdf[
        "updated_at"
    ].str.replace("-", "")

    schools_pdf["operation"] = np.where(
        schools_pdf["created_at"] > last_update_date, "Create", "Update"
    )
    schools_pdf["operation"] = np.where(
        schools_pdf["deleted_at"].notna(), "Delete", schools_pdf["operation"]
    )

    schools_pdf["ingestion_timestamp"] = pd.Timestamp.now()

    table_schema_columns = get_schema_columns(s, "mongolia_emis_api")

    schools_pdf = schools_pdf[[col.name for col in table_schema_columns]]

    schools_sdf = s.createDataFrame(
        schools_pdf, schema=StructType(table_schema_columns)
    )

    # create the schema and delta table if the table does not exist
    if not table_exists:
        context.log.info(
            f"Creating the {table_schema_name}.{table_name} table to store the raw data"
        )

        create_schema(s, table_schema_name)
        create_delta_table(
            s,
            table_schema_name,
            table_name,
            table_schema_columns,
            context,
            if_not_exists=True,
        )

    # add the data pulled to the delta table
    context.log.info("Upsert the latest mongolia api school data updates")
    s.catalog.refreshTable(construct_full_table_name(table_schema_name, table_name))
    current_table = DeltaTable.forName(
        s, construct_full_table_name(table_schema_name, table_name)
    )

    (
        current_table.alias("current")
        .merge(
            schools_sdf.alias("updates"),
            "current.ingestion_id = updates.ingestion_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    context.log.info("Upsert operation completed")

    # if the table already exists, we can push the API updates. if it doesn't exist, the data should have been
    # updated via giga sync for the first data pull so we don't run the automated updates here
    if table_exists:
        context.log.info("Push create and update schools to ADLS and create DB entries")

        # schools to create
        schools_to_create = schools_pdf[schools_pdf["operation"] == "Create"].drop(
            columns=["operation"]
        )
        upload_data_and_create_db_entry(
            schools_to_create,
            mode="Create",
            country_code="MNG",
            adls_file_client=adls_file_client,
            context=context,
        )

        # schools to update
        schools_to_update = schools_pdf[schools_pdf["operation"] == "Update"].drop(
            columns=["operation"]
        )
        upload_data_and_create_db_entry(
            schools_to_update,
            mode="Update",
            country_code="MNG",
            adls_file_client=adls_file_client,
            context=context,
        )

        # schools to delete
        schools_to_delete = schools_pdf[schools_pdf["deleted_at"].notna()]
        ids_to_delete = schools_to_delete["school_id"].astype(str).tolist()
        delete_schools_from_master(
            ids_to_delete=ids_to_delete,
            country_code="MNG",
            adls_file_client=adls_file_client,
            context=context,
        )

    return Output(None)
