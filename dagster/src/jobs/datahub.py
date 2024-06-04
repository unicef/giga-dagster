from dagster import define_asset_job

datahub__test_connection_job = define_asset_job(
    name="datahub__test_connection_job",
    selection=["datahub_test_connection"],
)

datahub__materialize_prerequisites_job = define_asset_job(
    name="datahub__materialize_prerequisites_job",
    selection=[
        "azure_ad_users_groups",
        "datahub_domains",
        "datahub_tags",
        "datahub_platform_metadata",
        "datahub_policies",
    ],
)

datahub__update_access_job = define_asset_job(
    name="datahub__update_access_job",
    selection=[
        "azure_ad_users_groups",
        "datahub_policies",
    ],
)

datahub__ingest_azure_ad_users_groups_job = define_asset_job(
    name="datahub__ingest_azure_ad_users_job",
    selection=["azure_ad_users_groups"],
)

datahub__create_domains_job = define_asset_job(
    name="datahub__create_domains_job",
    selection=["datahub_domains"],
)

datahub__create_tags_job = define_asset_job(
    name="datahub__create_tags_job",
    selection=["datahub_tags"],
)

datahub__update_policies_job = define_asset_job(
    name="datahub__update_policies_job",
    selection=["datahub_policies"],
)

datahub__ingest_coverage_notebooks_from_github_job = define_asset_job(
    name="datahub__ingest_coverage_notebooks_from_github_job",
    selection=["github_coverage_workflow_notebooks"],
)

datahub__add_platform_metadata_job = define_asset_job(
    name="datahub__add_platform_metadata_job",
    selection=["datahub_platform_metadata"],
)

datahub__soft_delete_qos_job = define_asset_job(
    name="datahub__soft_delete_qos_job",
    selection=[
        "list_qos_datasets_to_delete",
        "delete_references_to_qos_dry_run",
        "soft_delete_qos_datasets",
    ],
)

datahub__hard_delete_qos_job = define_asset_job(
    name="datahub__hard_delete_qos_job",
    selection=[
        "list_qos_datasets_to_delete",
        "delete_references_to_qos",
        "hard_delete_qos_datasets",
    ],
)
