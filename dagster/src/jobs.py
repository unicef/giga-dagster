from dagster import define_asset_job

school_master__automated_data_checks_job = define_asset_job(
    name="school_master__automated_data_checks",
    selection=[
        "raw",
        "bronze",
        "data_quality_results",
        "dq_passed_rows",
        "dq_failed_rows",
    ],
)


school_master__successful_manual_checks_job = define_asset_job(
    name="school_master__successful_manual_checks_job",
    selection=[
        "manual_review_passed_rows",
        "silver",
        "gold",
    ],
)


school_master__failed_manual_checks_job = define_asset_job(
    name="school_master__failed_manual_checks_job",
    selection=[
        "manual_review_failed_rows",
    ],
)

school_master__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_master__convert_gold_csv_to_deltatable_job",
    selection=[
        "gold_delta_table_from_csv",
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
