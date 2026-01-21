import numpy as np
import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from models.file_upload import FileUpload
from pyspark.sql import SparkSession
from requests import get
from requests.auth import HTTPBasicAuth
from src.internal.school_geolocation_api_queries import get_mng_api_last_update_date
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.delta import check_table_exists, create_delta_table, create_schema
from src.utils.schema import construct_full_table_name

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
    context.log.info(f"Number of schools pulled from API: {schools_pdf.shape[0]}")

    schools_pdf["update_id"] = schools_pdf["school_id"].astype(str) + schools_pdf[
        "updated_at"
    ].str.replace("-", "")
    schools_pdf["operation"] = np.where(
        schools_pdf["created_at"] > last_update_date, "Create", "Update"
    )

    schools_sdf = s.createDataFrame(schools_pdf)
    # create the schema and table if the table does not exist
    if not table_exists:
        context.log.info(
            f"Creating the {table_schema_name}.{table_name} table to store the raw data"
        )
        table_columns = schools_sdf.schema.fields
        for col in table_columns:
            col.nullable = True

        create_schema(s, table_schema_name)
        create_delta_table(
            s,
            table_schema_name,
            table_name,
            table_columns,
            context,
            if_not_exists=True,
        )

    context.log.info("Upsert the latest mongolia api school data updates")
    s.catalog.refreshTable(construct_full_table_name(table_schema_name, table_name))
    current_table = DeltaTable.forName(
        s, construct_full_table_name(table_schema_name, table_name)
    )

    (
        current_table.alias("current")
        .merge(
            schools_sdf.alias("updates"),
            "current.update_id = updates.update_id",
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
            adls_file_client=adls_file_client,
            context=context,
        )

    return Output(None)


def upload_data_and_create_db_entry(data, mode, adls_file_client, context):
    # create db entry in the file_uploads table
    context.log.info("Creating DB entry for API data upload")
    column_mapping = {"school_id": "school_id_govt"}
    file_upload = FileUpload(
        uploader_id="automated",
        uploader_email="apiautomated@gigasync.org",
        country="MNG",
        dataset="geolocation",
        source="",
        original_filename="mongolia_api_upload.csv",
        column_to_schema_mapping=column_mapping,
        column_license={},
    )

    with get_db_context() as db:
        db.add(file_upload)
        db.commit()

    # upload to ADLS
    context.log.info("Uploading API data to ADLS")
    adls_file_path = file_upload.upload_path
    context.log.info(f"Uploading to: {adls_file_path}")
    metadata = {
        "country": "Mongolia",
        "data_owner": "Mongolia government",
        "data_quality_issues": "None",
        "description": "mongolia_api",
        "emis_system": "None",
        "focal_point_contact": "",
        "focal_point_name": "Mongolia government",
        "frequency_of_school_data_collection": "",
        "modality_of_data_collection": "",
        "mode": mode,
        "next_school_data_collection": "",
        "school_contacts": "",
        "school_ids_type": "",
        "uploader_email": "apiautomated@gigasync.org",
        "year_of_data_collection": "",
    }
    adls_file_client.upload_raw(
        context,
        data=data.to_csv(index=False).encode(),
        filepath=adls_file_path,
        metadata=metadata,
    )
