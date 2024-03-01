from dagster import define_asset_job

school_master_geolocation__automated_data_checks_job = define_asset_job(
    name="school_master_geolocation__automated_data_checks_job",
    selection=[
        "geolocation_raw",
        "geolocation_bronze",
        "geolocation_dq_results",
        "geolocation_dq_summary_statistics",
        "geolocation_dq_passed_rows",
        "geolocation_dq_failed_rows",
        # "geolocation_staging",
    ],
)

school_master_coverage__automated_data_checks_job = define_asset_job(
    name="school_master_coverage__automated_data_checks_job",
    selection=[
        "coverage_raw",
        "coverage_dq_results",
        "coverage_dq_summary_statistics",
        # "coverage_dq_checks",
        "coverage_dq_passed_rows",
        "coverage_dq_failed_rows",
        "coverage_bronze",
        # "coverage_staging",
    ],
)


school_master_geolocation__successful_manual_checks_job = define_asset_job(
    name="school_master_geolocation__successful_manual_checks_job",
    selection=[
        "manual_review_passed_rows",
        "silver",
        "master",
        "reference",
    ],
)


school_master_geolocation__failed_manual_checks_job = define_asset_job(
    name="school_master_geolocation__failed_manual_checks_job",
    selection=[
        "manual_review_failed_rows",
    ],
)


school_master_coverage__successful_manual_checks_job = define_asset_job(
    name="school_master_coverage__successful_manual_checks_job",
    selection=["manual_review_passed_rows", "silver", "master", "reference"],
)


school_master_coverage__failed_manual_checks_job = define_asset_job(
    name="school_master_coverage__failed_manual_checks_job",
    selection=[
        "manual_review_failed_rows",
    ],
)
