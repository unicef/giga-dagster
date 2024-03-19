from dagster import define_asset_job
from src.hooks.school_master import geolocation_dq_checks_location_db_update_hook

school_master_geolocation__automated_data_checks_job = define_asset_job(
    name="school_master_geolocation__automated_data_checks_job",
    # TODO: Include staging
    selection=[
        "*geolocation_dq_failed_rows",
        "*geolocation_dq_passed_rows",
        "*geolocation_data_quality_results_summary",
    ],
    hooks={geolocation_dq_checks_location_db_update_hook},
)

school_master_coverage__automated_data_checks_job = define_asset_job(
    name="school_master_coverage__automated_data_checks_job",
    # TODO: Include staging
    selection="*coverage_bronze",
)


school_master_geolocation__successful_manual_checks_job = define_asset_job(
    name="school_master_geolocation__successful_manual_checks_job",
    selection="manual_review_passed_rows*",
)


school_master_geolocation__failed_manual_checks_job = define_asset_job(
    name="school_master_geolocation__failed_manual_checks_job",
    selection="manual_review_failed_rows",
)


school_master_coverage__successful_manual_checks_job = define_asset_job(
    name="school_master_coverage__successful_manual_checks_job",
    selection="manual_review_passed_rows*",
)


school_master_coverage__failed_manual_checks_job = define_asset_job(
    name="school_master_coverage__failed_manual_checks_job",
    selection="manual_review_failed_rows",
)
