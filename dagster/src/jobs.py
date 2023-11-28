from dagster import define_asset_job

school_master__run_automated_data_checks_job = define_asset_job(
    name="school_master__run_automated_data_checks",
    selection=[
        "raw",
        "bronze",
        "data_quality_results",
        "dq_passed_rows",
        "dq_failed_rows",
    ],
)


school_master__run_successful_manual_checks_job = define_asset_job(
    name="school_master__run_successful_manual_checks_job",
    selection=[
        # "manual_review_passed_rows",
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

school_master__get_gold_delta_tables_job = define_asset_job(
    name="school_master__get_gold_delta_tables_job",
    selection=[
        "fake_gold",
    ],
)
