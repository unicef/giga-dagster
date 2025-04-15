from dagster import AssetSelection, define_asset_job
from src.assets.school_connectivity import GROUP_NAME as SCHOOL_CONNECTIVITY_GROUP_NAME
from src.assets.school_list import GROUP_NAME as SCHOOL_LIST_GROUP_NAME
from src.settings import settings

qos_school_list__automated_data_checks_job = define_asset_job(
    name="qos_school_list__automated_data_checks_job",
    selection=AssetSelection.groups(SCHOOL_LIST_GROUP_NAME),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


qos_school_connectivity__automated_data_checks_job = define_asset_job(
    name="qos_connectivity__automated_data_checks_job",
    selection=AssetSelection.groups(SCHOOL_CONNECTIVITY_GROUP_NAME),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

qos_availability_create_silver_job = define_asset_job(
    name="qos_availability_create_silver_job",
    selection=AssetSelection.keys(
        "qos_availability_raw",
        "qos_availability_bronze",
        "qos_availability_silver",
        # "qos_availability_error",
    ),
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
