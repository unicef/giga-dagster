from dagster import AssetSelection, define_asset_job
from src.assets.school_list import GROUP_NAME as SCHOOL_LIST_GROUP_NAME

qos_school_list__automated_data_checks_job = define_asset_job(
    name="qos_school_list__automated_data_checks_job",
    selection=AssetSelection.groups(SCHOOL_LIST_GROUP_NAME),
)


qos_school_connectivity__automated_data_checks_job = define_asset_job(
    name="qos_connectivity__automated_data_checks_job",
    selection=[
        "qos_school_connectivity_raw",
        "qos_school_connectivity_bronze",
        "qos_school_connectivity_dq_results",
        "qos_school_connectivity_dq_summary_statistics",
        "qos_school_connectivity_dq_passed_rows",
        "qos_school_connectivity_dq_failed_rows",
        "qos_school_connectivity_silver",
        "qos_school_connectivity_gold",
    ],
)
