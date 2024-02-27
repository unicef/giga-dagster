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
        "coverage_dq_checks",
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
        "gold",
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
    selection=[
        "manual_review_passed_rows",
        "silver",
        "gold",
    ],
)


school_master_coverage__failed_manual_checks_job = define_asset_job(
    name="school_master_coverage__failed_manual_checks_job",
    selection=[
        "manual_review_failed_rows",
    ],
)

school_master__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_master__convert_gold_csv_to_deltatable_job",
    selection=[
        "master_csv_to_gold",
    ],
)

school_reference__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_reference__convert_gold_csv_to_deltatable_job",
    selection=[
        "reference_csv_to_gold",
    ],
)

qos__convert_csv_to_deltatable_job = define_asset_job(
    name="qos__convert_csv_to_deltatable_job",
    selection=[
        "qos_csv_to_gold",
    ],
)

datahub__ingest_azure_ad_users_groups_job = define_asset_job(
    name="datahub__ingest_azure_ad_users_job",
    selection=[
        "azure_ad_users_groups",
    ],
)

datahub__create_domains_job = define_asset_job(
    name="datahub__create_domains_job",
    selection=[
        "datahub_domains",
    ],
)

datahub__create_tags_job = define_asset_job(
    name="datahub__create_tags_job",
    selection=[
        "datahub_tags",
    ],
)

datahub__update_policies_job = define_asset_job(
    name="datahub__update_policies_job",
    selection=[
        "datahub_policies",
    ],
)

datahub__ingest_coverage_notebooks_from_github_job = define_asset_job(
    name="datahub__ingest_coverage_notebooks_from_github_job",
    selection=[
        "github_coverage_workflow_notebooks",
    ],
)
