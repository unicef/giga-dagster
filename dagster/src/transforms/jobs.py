from dagster import define_asset_job, job
from dagster.src.transforms.assets.delta_poc_op import write_delta_lake_poc


@job
def delta_poc_job():
    write_delta_lake_poc()


school_master__run_automated_data_checks_job = define_asset_job(
    name="school_master__run_automated_data_checks",
    selection=[
        "raw_file",
        "bronze",
        "expectation_suite_asset",
        "dq_failed_rows",
        "dq_passed_rows",
    ],
)


school_master__run_manual_checks_and_transforms_job = define_asset_job(
    name="school_master__run_manual_checks_and_transforms",
    selection=[
        "manual_review_passed_rows",
        "manual_review_failed_rows",
        "silver",
        "gold",
    ],
)
