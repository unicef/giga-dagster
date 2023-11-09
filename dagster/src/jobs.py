from dagster import define_asset_job

school_master__run_automated_data_checks_job = define_asset_job(
    name="school_master__run_automated_data_checks",
    selection=[
        "raw",
        "bronze",
        # "expectation_suite_asset",
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
