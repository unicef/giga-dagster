from datetime import datetime

import pandas as pd
from models.deletion_requests import DeletionRequest
from models.file_upload import FileUpload

from dagster import OpExecutionContext
from src.constants import constants
from src.internal.school_geolocation_api_queries import get_schools_by_govt_id
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context


def upload_data_and_create_db_entry(
    data: pd.DataFrame,
    mode: str,
    country_code: str,
    adls_file_client: ADLSFileClient,
    context: OpExecutionContext,
):
    """
    Uploads data to Azure Data Lake Storage (ADLS) and creates an entry in the database
    for tracking the file upload process.

    Parameters:
        data (pandas.DataFrame): The dataset to upload, provided as a Pandas DataFrame.
        mode (str): The mode of operation for the upload, "Create" or "Update"
        country_code (str): The country code for the schools being uploaded.
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
        country=country_code,
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
    ids_to_delete: list[str],
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
    if not ids_to_delete:
        context.log.info(f"There are no schools to delete for {country_code}")
        return
    else:
        deletion_schools_master = get_schools_by_govt_id(
            country_code.lower(), ids_to_delete
        )
        giga_ids_to_delete = deletion_schools_master["school_id_giga"].tolist()
        if not giga_ids_to_delete:
            context.log.info(f"There are no schools to delete for {country_code}")
            return
        context.log.info(
            f"{len(giga_ids_to_delete)} schools will be deleted from school master for {country_code}"
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
