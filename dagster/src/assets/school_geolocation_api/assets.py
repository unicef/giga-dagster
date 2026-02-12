from datetime import datetime

import numpy as np
import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from models.deletion_requests import DeletionRequest
from models.file_upload import FileUpload
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from requests import get
from requests.auth import HTTPBasicAuth
from src.constants import constants
from src.internal.school_geolocation_api_queries import get_mng_api_last_update_date
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context
from src.utils.delta import check_table_exists, create_delta_table, create_schema
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

        # schools to delete
        schools_to_delete = schools_pdf[schools_pdf["deleted_at"].notna()]
        delete_schools_from_master(
            data=schools_to_delete,
            country_code="MNG",
            adls_file_client=adls_file_client,
        )

    return Output(None)


def upload_data_and_create_db_entry(
    data: pd.DataFrame,
    mode: str,
    adls_file_client: ADLSFileClient,
    context: OpExecutionContext,
):
    """
    Uploads data to Azure Data Lake Storage (ADLS) and creates an entry in the database
    for tracking the file upload process.

    Parameters:
        data (pandas.DataFrame): The dataset to upload, provided as a Pandas DataFrame.
        mode (str): The mode of operation for the upload, Create or Update
        adls_file_client (ADLSFileClient): An instance of ADLSFileClient used to upload files
        context (Dagster context): The dagster run context for logging.

    Raises:
        Any exceptions raised during either the database entry creation or the ADLS upload
        process will propagate to the caller.

    Returns:
        None
    """
    if data.empty:
        context.log.info(f"There are no schools to {mode.lower()}")
        return
    else:
        context.log.info(f"Uploading {data.shape[0]} schools to be {mode.lower()}d")

    # create db entry in the file_uploads table
    context.log.info("Creating DB entry for API data upload")
    column_mapping = {"school_id": "school_id_govt"}
    file_upload = FileUpload(
        uploader_id="305f7203-c97e-46bb-b2da-352379fa1c4e",
        uploader_email="apiautomated@gigasync.org",
        country="MNG",
        dataset="geolocation",
        source="api",
        original_filename=f"MNG_school_data_{mode.lower()}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
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
        "description": "api",
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


def delete_schools_from_master(
    data: list[str],
    country_code: str,
    adls_file_client: ADLSFileClient,
    context: OpExecutionContext,
):
    """
    Deletes schools from the master list by creating a deletion request in the Ingestion Portal Database and uploading
    a list of IDs to delete to Azure Data Lake Storage (ADLS).

    Parameters:
    ids_to_delete (list[str]): List of school IDs to be deleted.
    country_code (str): The country code
    adls_file_client (ADLSFileClient): An instance of the file client used to upload the deletion file.
    context (OpExecutionContext): The dagster run context for logging.

    Returns:
    None
    """
    if data.empty:
        context.log.info(f"There are no schools to delete for {country_code}")
        return
    else:
        ids_to_delete = data["school_id_giga"].tolist()
        context.log.info(
            f"{data.shape[0]} schools will be deleted from school master for {country_code}"
        )

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"{country_code}_{timestamp}.json"

    delete_filepath = (
        f"{constants.staging_folder}/delete-row-ids/{country_code}/{filename}"
    )
    context.log.info(
        f"Uploading file with list of schools to delete to {delete_filepath}"
    )
    adls_file_client.upload_json(data=ids_to_delete, filepath=delete_filepath)

    context.log.info(f"Create DB entry for deletion request for {country_code}")
    deletion_request = DeletionRequest(
        requested_by_email="apiautomated@gigasync.org",
        requested_by_id="305f7203-c97e-46bb-b2da-352379fa1c4e",
        country=country_code,
    )

    with get_db_context() as db:
        db.add(deletion_request)
        db.commit()

    context.log.info(f"Deletion request created for {country_code}")
