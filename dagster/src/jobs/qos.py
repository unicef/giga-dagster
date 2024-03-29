from dagster import AssetSelection, define_asset_job
from src.assets.school_list import GROUP_NAME as SCHOOL_LIST_GROUP_NAME

qos_school_list__automated_data_checks_job = define_asset_job(
    name="qos_school_list__automated_data_checks_job",
    selection=AssetSelection.groups(SCHOOL_LIST_GROUP_NAME),
)


qos_school_list__successful_manual_checks_job = define_asset_job(
    name="qos_school_list__successful_manual_checks_job",
    selection="manual_review_passed_rows*",
)
