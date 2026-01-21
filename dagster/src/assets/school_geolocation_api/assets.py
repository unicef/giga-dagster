from dagster import OpExecutionContext, Output, asset
from requests import get
from requests.auth import HTTPBasicAuth
import pandas as pd

from models.file_upload import FileUpload
from src.constants import constants
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.db.primary import get_db_context


@asset
def mng_school_geolocation_api_raw(
    context: OpExecutionContext, adls_file_client: ADLSFileClient
):
    auth = HTTPBasicAuth(settings.MONGOLIA_API_USER, settings.MONGOLIA_API_PASSWORD)

    def pull_data_from_api(last_update_date="2019-10-26T00:00:00Z"):
        full_schools_list = []
        more_data = True
        page = 1

        if last_update_date:
            base_url = (
                f"{settings.MONGOLIA_API_URL}?size=100&UPDATED_AFTER={last_update_date}"
            )
        else:
            base_url = f"{settings.MONGOLIA_API_URL}?size=100"

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

    full_schools_list = pull_data_from_api()
    schools_df = pd.DataFrame(full_schools_list)

    context.log.info(f"Number of schools pulled from API: {schools_df.shape[0]}")

    # create db entry
    context.log.info("Creating DB entry for API data upload")
    column_mapping = {"school_id": "school_id_govt"}
    file_upload = FileUpload(
        uploader_id="automated",
        uploader_email="apiautomated@gigasync.org",
        country="MNG",
        dataset="geolocation",
        source="api",
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
        "mode": "Create",
        "next_school_data_collection": "",
        "school_contacts": "",
        "school_ids_type": "",
        "uploader_email": "apiautomated@gigasync.org",
        "year_of_data_collection": "",
    }
    adls_file_client.upload_raw(
        context,
        data=schools_df.to_csv(index=False).encode(),
        filepath=adls_file_path,
        metadata=metadata,
    )

    return Output(None)
