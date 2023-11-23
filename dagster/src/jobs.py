from dagster import define_asset_job

school_master__run_automated_data_checks_job = define_asset_job(
    name="school_master__run_automated_data_checks",
    selection=[
        "raw",
        "bronze",
        "data_quality_results",
        # "ge_data_docs",
        "dq_passed_rows",
        # "dq_failed_rows",
    ],
)


school_master__run_successful_manual_checks_job = define_asset_job(
    name="school_master__run_successful_manual_checks_job",
    selection=[
        "manual_review_passed_rows",
        "silver",
        "gold",
    ],
)


school_master__run_failed_manual_checks_job = define_asset_job(
    name="school_master__run_failed_manual_checks_job",
    selection=[
        "manual_review_failed_rows",
    ],
)

school_master__convert_gold_to_delta_job = define_asset_job(
    name="school_master__convert_gold_to_delta_job",
    selection=[
        "gold",
    ],
)
