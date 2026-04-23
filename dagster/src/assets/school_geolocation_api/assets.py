import numpy as np
import pandas as pd
from dagster_pyspark import PySparkResource
from delta import DeltaTable
from pyspark.sql import (
    SparkSession,
    functions as f,
)
from pyspark.sql.types import StructType
from requests import get
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, HTTPError, Timeout
from src.settings import settings
from src.utils.adls import ADLSFileClient
from src.utils.delta import check_table_exists, create_delta_table, create_schema
from src.utils.geolocation_apis.data_updates import (
    delete_schools_from_master,
    upload_data_and_create_db_entry,
)
from src.utils.schema import construct_full_table_name, get_schema_columns
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from dagster import OpExecutionContext, Output, asset


def _is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, ConnectionError | Timeout):
        return True
    if isinstance(exc, HTTPError) and exc.response is not None:
        return exc.response.status_code >= 500
    return False


@retry(
    retry=retry_if_exception(_is_transient_error),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _fetch_page(url: str, auth: HTTPBasicAuth) -> list[dict]:
    response = get(url, auth=auth, timeout=30)
    response.raise_for_status()
    return response.json()["RESULT"]


def _pull_data_from_api(
    context: OpExecutionContext,
    last_update_date=None,
) -> list[dict]:
    full_schools_list = []
    page = 1

    if last_update_date:
        base_url = (
            f"{settings.MONGOLIA_API_URL}?size=100&UPDATED_AFTER={last_update_date}"
        )
    else:
        base_url = f"{settings.MONGOLIA_API_URL}?size=100"

    auth = HTTPBasicAuth(settings.MONGOLIA_API_USER, settings.MONGOLIA_API_PASSWORD)

    while True:
        context.log.info(f"Page Number: {page}")
        url = base_url + f"&page={page}"
        result_schools = _fetch_page(url, auth)

        if result_schools:
            full_schools_list.extend(result_schools)
            page += 1
        else:
            break

    return full_schools_list


def _mark_pushed_to_pipeline(
    s: SparkSession,
    full_table_name: str,
    ingestion_ids: list[str],
):
    """Mark records as successfully pushed to the pipeline."""
    if not ingestion_ids:
        return
    DeltaTable.forName(s, full_table_name).update(
        condition=f.col("ingestion_id").isin(ingestion_ids),
        set={"is_pushed_to_pipeline": f.lit(True)},
    )


def _push_to_portal(
    s: SparkSession,
    pdf: pd.DataFrame,
    full_table_name: str,
    adls_file_client: ADLSFileClient,
    context: OpExecutionContext,
):
    """
    Push a batch of records to the portal (create/update/delete) and mark them as pushed.
    pdf must contain the 'operation' column.
    """
    # Only upload columns that belong to the school_geolocation schema.
    # school_id is kept because it maps to school_id_govt in the column_to_schema_mapping.
    school_geolocation_columns = {
        field.name for field in get_schema_columns(s, "school_geolocation")
    }
    upload_cols = [
        column
        for column in pdf.columns
        if column in school_geolocation_columns or column == "school_id"
    ]

    # schools to create
    schools_to_create = pdf[pdf["operation"] == "Create"]
    upload_data_and_create_db_entry(
        schools_to_create[upload_cols],
        mode="Create",
        country_code="MNG",
        adls_file_client=adls_file_client,
        context=context,
    )
    _mark_pushed_to_pipeline(
        s, full_table_name, schools_to_create["ingestion_id"].tolist()
    )

    # schools to update
    schools_to_update = pdf[pdf["operation"] == "Update"]
    upload_data_and_create_db_entry(
        schools_to_update[upload_cols],
        mode="Update",
        country_code="MNG",
        adls_file_client=adls_file_client,
        context=context,
    )
    _mark_pushed_to_pipeline(
        s, full_table_name, schools_to_update["ingestion_id"].tolist()
    )

    # schools to delete
    schools_to_delete = pdf[pdf["deleted_at"].notna() & (pdf["deleted_at"] != "None")]
    ids_to_delete = schools_to_delete["school_id"].astype(str).tolist()
    delete_schools_from_master(
        ids_to_delete=ids_to_delete,
        country_code="MNG",
        adls_file_client=adls_file_client,
        context=context,
        spark=s,
    )
    _mark_pushed_to_pipeline(
        s, full_table_name, schools_to_delete["ingestion_id"].tolist()
    )


@asset
def school_geolocation_emis_api_mng(
    context: OpExecutionContext,
    adls_file_client: ADLSFileClient,
    spark: PySparkResource,
):
    s: SparkSession = spark.spark_session

    table_name = "mng"
    table_schema_name = "school_geolocation_emis_api"
    full_table_name = construct_full_table_name(table_schema_name, table_name)

    table_exists = check_table_exists(s, table_schema_name, table_name, None)

    if table_exists:
        last_update_date = s.sql(
            "SELECT MAX(updated_at) AS last_update_date FROM school_geolocation_emis_api.mng"
        ).collect()[0]["last_update_date"]
        context.log.info(f"The last update date is {last_update_date}")
    else:
        last_update_date = None
        context.log.info(
            "There is no last update date because the table does not exist"
        )

    # Re-push any records that were upserted but never successfully pushed to the pipeline
    if table_exists and last_update_date is not None:
        unpushed_sdf = s.sql(
            f"SELECT * FROM {full_table_name} WHERE is_pushed_to_pipeline = false"  # nosec B608
        )
        if not unpushed_sdf.isEmpty():
            unpushed_pdf = unpushed_sdf.drop("is_pushed_to_pipeline").toPandas()
            context.log.info(
                f"Re-pushing {len(unpushed_pdf)} records not yet pushed to the pipeline"
            )
            _push_to_portal(s, unpushed_pdf, full_table_name, adls_file_client, context)

    full_schools_list = _pull_data_from_api(context, last_update_date=last_update_date)

    if not full_schools_list:
        context.log.info("No new records from API since last update.")
        return Output(None)

    schools_pdf = pd.DataFrame(full_schools_list)
    context.log.info(f"Number of schools pulled from API: {schools_pdf.shape[0]}")

    schools_pdf["education_level_govt"] = schools_pdf["education_level_govt"].fillna(
        "Unknown"
    )
    schools_pdf["latitude"] = schools_pdf["latitude"].replace("None", np.nan)
    schools_pdf["longitude"] = schools_pdf["longitude"].replace("None", np.nan)

    schools_pdf["ingestion_id"] = schools_pdf["school_id"].astype(str) + pd.to_datetime(
        schools_pdf["updated_at"]
    ).dt.strftime("%Y%m%d%H%M%S")

    schools_pdf["ingestion_timestamp"] = pd.Timestamp.now()

    # operation is only meaningful for incremental runs where we know last_update_date
    if table_exists and last_update_date is not None:
        schools_pdf["operation"] = np.where(
            pd.to_datetime(schools_pdf["created_at"], utc=True)
            > pd.to_datetime(last_update_date, utc=True),
            "Create",
            "Update",
        )
        schools_pdf["operation"] = np.where(
            schools_pdf["deleted_at"].notna() & (schools_pdf["deleted_at"] != "None"),
            "Delete",
            schools_pdf["operation"],
        )
    else:
        schools_pdf["operation"] = "Create"

    table_schema_columns = get_schema_columns(s, "mongolia_emis_api")

    # All incoming records start as not pushed to pipeline.
    # whenMatchedUpdateAll() resets is_pushed_to_pipeline=False for updated records
    # (correct — they need re-pushing with latest data).
    schools_pdf["is_pushed_to_pipeline"] = False

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
    s.catalog.refreshTable(full_table_name)
    current_table = DeltaTable.forName(s, full_table_name)

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

    if table_exists and last_update_date is not None:
        # Incremental run: push create/update/delete to portal
        context.log.info("Push create and update schools to ADLS and create DB entries")
        _push_to_portal(s, schools_pdf, full_table_name, adls_file_client, context)
    else:
        # First run (initial load): mark all records as handled so the next incremental
        # run does not try to re-push the entire backfill as "previously un-ingested".
        context.log.info(
            "First run — marking all records as pushed to pipeline (initial load, no portal push)"
        )
        _mark_pushed_to_pipeline(
            s, full_table_name, schools_pdf["ingestion_id"].tolist()
        )

    context.add_output_metadata(
        {
            "rows_pulled": schools_pdf.shape[0],
            "last_update_date": str(last_update_date),
            "incremental": table_exists and last_update_date is not None,
        }
    )

    return Output(None)
