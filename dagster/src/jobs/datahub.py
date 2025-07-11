from dagster import define_asset_job
from src.settings import settings

datahub__test_connection_job = define_asset_job(
    name="datahub__test_connection_job",
    selection="datahub__test_connection",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__materialize_prerequisites_job = define_asset_job(
    name="datahub__materialize_prerequisites_job",
    selection=[
        "datahub__get_azure_ad_users_groups",
        "datahub__create_domains",
        "datahub__create_tags",
        "datahub__create_platform_metadata",
        "datahub__update_policies",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__update_access_job = define_asset_job(
    name="datahub__update_access_job",
    selection=[
        "datahub__get_azure_ad_users_groups",
        "datahub__update_policies",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__ingest_datahub__get_azure_ad_users_groups_job = define_asset_job(
    name="datahub__ingest_azure_ad_users_job",
    selection=["datahub__get_azure_ad_users_groups"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__create_domains_job = define_asset_job(
    name="datahub__create_domains_job",
    selection=["datahub__create_domains"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__create_tags_job = define_asset_job(
    name="datahub__create_tags_job",
    selection=["datahub__create_tags"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__update_policies_job = define_asset_job(
    name="datahub__update_policies_job",
    selection=["datahub__update_policies"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__ingest_coverage_notebooks_from_github_job = define_asset_job(
    name="datahub__ingest_coverage_notebooks_from_github_job",
    selection=["datahub__ingest_github_coverage_workflow_notebooks"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__add_platform_metadata_job = define_asset_job(
    name="datahub__add_platform_metadata_job",
    selection=["datahub__create_platform_metadata"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__soft_delete_qos_job = define_asset_job(
    name="datahub__soft_delete_qos_job",
    selection=[
        "datahub__list_qos_datasets_to_delete",
        "datahub__delete_references_to_qos_dry_run",
        "datahub__soft_delete_qos_datasets",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__hard_delete_qos_job = define_asset_job(
    name="datahub__hard_delete_qos_job",
    selection=[
        "datahub__list_qos_datasets_to_delete",
        "datahub__delete_references_to_qos",
        "datahub__hard_delete_qos_datasets",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__add_business_glossary_job = define_asset_job(
    name="datahub__add_business_glossary_job",
    selection=["datahub__add_business_glossary"],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


datahub__purge_assertions_job = define_asset_job(
    name="datahub__purge_assertions_job",
    description="Hard deletes all assertions and references to them.",
    selection=[
        "datahub__purge_assertions",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

datahub__purge_entities_job = define_asset_job(
    name="datahub__purge_entities_job",
    description="Deletes entities filtered by platform. Soft deletes by default.",
    selection=[
        "datahub__list_entities_to_delete",
        "datahub__delete_entities",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
