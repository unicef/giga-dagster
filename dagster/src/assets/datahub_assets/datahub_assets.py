from http import HTTPStatus

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.filters import RemovedStatusFilter
from loguru import logger
from pydantic import Field
from src.settings import settings
from src.utils.datahub.add_glossary import add_business_glossary
from src.utils.datahub.add_platform_metadata import add_platform_metadata
from src.utils.datahub.batch_processing import (
    create_parallel_batches,
    delete_assertion_batch,
    process_batches_in_parallel,
)
from src.utils.datahub.create_domains import create_domains
from src.utils.datahub.create_tags import create_tags
from src.utils.datahub.datahub_ingest_nb_metadata import NotebookIngestionAction
from src.utils.datahub.graphql import datahub_graph_client
from src.utils.datahub.ingest_azure_ad import (
    ingest_azure_ad_to_datahub_pipeline,
)
from src.utils.datahub.list_datasets import list_datasets_by_filter
from src.utils.datahub.update_policies import update_policies
from src.utils.github_api_calls import list_ipynb_from_github_repo
from src.utils.sentry import capture_op_exceptions

from dagster import Config, MetadataValue, OpExecutionContext, Output, asset


@asset
@capture_op_exceptions
def datahub__test_connection(context: OpExecutionContext) -> Output[None]:
    context.log.info(f"Using {settings.DATAHUB_METADATA_SERVER_URL=}")
    emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
        retry_max_times=5,
        retry_status_codes=[
            HTTPStatus.FORBIDDEN,
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ],
    )
    config = emitter.get_server_config()
    return Output(None, metadata={"result": MetadataValue.json(config)})


@asset
@capture_op_exceptions
def datahub__create_domains(context: OpExecutionContext) -> None:
    context.log.info("CREATING DOMAINS IN DATAHUB")
    domains = create_domains()
    context.log.info(f"Domains created: {domains}")


@asset
@capture_op_exceptions
def datahub__create_tags(context: OpExecutionContext) -> None:
    context.log.info("CREATING TAGS IN DATAHUB")
    create_tags(context)


@asset
@capture_op_exceptions
def datahub__get_azure_ad_users_groups(context: OpExecutionContext) -> None:
    context.log.info("INGESTING AZURE AD USERS AND GROUPS TO DATAHUB")
    ingest_azure_ad_to_datahub_pipeline()


@asset(deps=[datahub__get_azure_ad_users_groups])
@capture_op_exceptions
def datahub__update_policies(context: OpExecutionContext) -> None:
    context.log.info("UPDATING POLICIES IN DATAHUB")
    update_policies(context)


@asset
@capture_op_exceptions
def datahub__ingest_github_coverage_workflow_notebooks(
    context: OpExecutionContext,
) -> None:
    context.log.info("INGESTING COVERAGE WORKFLOW NOTEBOOKS TO DATAHUB")

    owner = "unicef"
    repo = "coverage_workflow"
    path = "Notebooks/"
    notebook_metadata_list = list_ipynb_from_github_repo(owner, repo, path, context)

    for notebook_metadata in notebook_metadata_list:
        run_notebook_ingestion = NotebookIngestionAction(
            notebook_metadata=notebook_metadata,
        )
        run_notebook_ingestion()


@asset
@capture_op_exceptions
def datahub__create_platform_metadata(context: OpExecutionContext) -> None:
    context.log.info("ADDING PLATFORM METADATA IN DATAHUB...")
    context.log.info("Delta Lake")
    add_platform_metadata(
        platform="deltaLake",
        display_name="Delta Lake",
        logo_url="https://delta.io/static/3bd8fea55ff57287371f4714232cd4ef/ac8f8/delta-lake-logo.webp",
        filepath_delimiter="/",
    )
    context.log.info("PLATFORM METADATA ADDED TO DATAHUB")


@asset
@capture_op_exceptions
def datahub__list_qos_datasets_to_delete(
    context: OpExecutionContext,
) -> Output[list[str]]:
    context.log.info("LISTING QOS DATASETS FROM DATAHUB...")

    qos_list = list_datasets_by_filter("qos")
    context.log.info("COMPLETE QOS DATASETS LIST:")
    context.log.info(qos_list)

    qos_not_dq_delta_list = [
        urn for urn in qos_list if not ("deltaLake" not in urn or "dq-results" in urn)
    ]
    context.log.info("QOS DELTA TABLES:")
    context.log.info(qos_not_dq_delta_list)

    qos_list_minus_not_dq_delta = [
        urn for urn in qos_list if ("deltaLake" not in urn or "dq-results" in urn)
    ]
    context.log.info("QOS DATASETS MINUS DELTA TABLES:")
    context.log.info(qos_list_minus_not_dq_delta)

    return Output(
        qos_list_minus_not_dq_delta,
        metadata={
            "qos_datasets_count": len(qos_list),
            "qos_not_dq_delta_count": len(qos_not_dq_delta_list),
            "qos_list_minus_not_dq_delta_count": len(qos_list_minus_not_dq_delta),
        },
    )


@asset
@capture_op_exceptions
def datahub__delete_references_to_qos_dry_run(
    context: OpExecutionContext,
    datahub__list_qos_datasets_to_delete: list[str],
) -> Output[None]:
    context.log.info("DRY RUN OF DELETING QOS DATASETS FROM DATAHUB...")
    count = len(datahub__list_qos_datasets_to_delete)
    i = 0
    for qos_dataset in datahub__list_qos_datasets_to_delete:
        references_info = datahub_graph_client.delete_references_to_urn(
            qos_dataset, dry_run=True
        )
        i += 1
        context.log.info(f"{i} of {count}: {qos_dataset}")
        context.log.info(f"Count of references: {references_info[0]}")
        context.log.info(f"List of related aspects: {references_info[1]}")

    context.log.info(f"DRY RUN COMPLETE FOR {count} QOS DATASETS.")
    return Output(None, metadata={"qos_datasets_count": count})


@asset
@capture_op_exceptions
def datahub__delete_references_to_qos(
    context: OpExecutionContext,
    datahub__list_qos_datasets_to_delete: list[str],
) -> Output[None]:
    context.log.warning("DELETING REFERENCES TO QOS DATASETS FROM DATAHUB...")
    count = len(datahub__list_qos_datasets_to_delete)
    i = 0
    for qos_dataset in datahub__list_qos_datasets_to_delete:
        references_info = datahub_graph_client.delete_references_to_urn(
            qos_dataset, dry_run=False
        )
        i += 1
        context.log.info(f"{i} of {count}: {qos_dataset}")
        context.log.info(f"Count of references: {references_info[0]}")
        context.log.info(f"List of related aspects: {references_info[1]}")

    context.log.info("DELETE QOS REFERENCES COMPLETED.")
    return Output(None, metadata={"count": count})


@asset(deps=[datahub__delete_references_to_qos_dry_run])
@capture_op_exceptions
def datahub__soft_delete_qos_datasets(
    context: OpExecutionContext,
    datahub__list_qos_datasets_to_delete: list[str],
) -> Output[None]:
    context.log.warning("SOFT DELETING QOS DATASETS FROM DATAHUB...")
    count = len(datahub__list_qos_datasets_to_delete)
    i = 0
    for qos_dataset in datahub__list_qos_datasets_to_delete:
        datahub_graph_client.soft_delete_entity(qos_dataset)
        i += 1
        context.log.info(f"SOFT DELETED {i} of {count}: {qos_dataset}")

    context.log.info(f"SOFT DELETED {count} QOS DATASETS FROM DATAHUB.")
    return Output(None, metadata={"count": count})


@asset(deps=[datahub__delete_references_to_qos])
@capture_op_exceptions
def datahub__hard_delete_qos_datasets(
    context: OpExecutionContext,
    datahub__list_qos_datasets_to_delete: list[str],
) -> Output[None]:
    context.log.warning("HARD DELETING QOS DATASETS FROM DATAHUB...")
    count = len(datahub__list_qos_datasets_to_delete)
    i = 0
    for qos_dataset in datahub__list_qos_datasets_to_delete:
        datahub_graph_client.hard_delete_entity(qos_dataset)
        i += 1
        context.log.info(f"HARD DELETED {i} of {count}: {qos_dataset}")

    context.log.info(f"HARD DELETED {count} QOS DATASETS FROM DATAHUB.")
    return Output(None, metadata={"count": count})


@asset
@capture_op_exceptions
def datahub__add_business_glossary(context: OpExecutionContext) -> None:
    context.log.info("ADDING BUSINESS GLOSSARY TO DATAHUB...")
    add_business_glossary()


class DeleteAssertionsConfig(Config):
    hard: bool = Field(False, description="Whether to hard delete assertions.")


class ListAssertionsConfig(Config):
    status: RemovedStatusFilter = Field(
        RemovedStatusFilter.ALL,
        description="Filter for the status of entities during search",
    )

    batch_size: int = Field(100, description="Assertions to delete at a time")
    max_iterations: int = Field(10, description="Number of batches to delete")


@asset
@capture_op_exceptions
def datahub__purge_assertions(
    context: OpExecutionContext, config: ListAssertionsConfig
) -> Output[list[str]]:
    total_deleted = 0
    total_references_deleted = 0

    for iteration in range(config.max_iterations):
        # Get all assertion URNs for this iteration
        all_assertion_urns = datahub_graph_client.list_all_entity_urns(
            entity_type="assertion", start=0, count=config.batch_size * 5
        )

        if not all_assertion_urns:
            context.log.info(f"No more assertions found after {iteration} iterations")
            break

        # Create parallel batches
        batches = create_parallel_batches(all_assertion_urns, num_parallel=5)

        logger.info(
            f"Iteration {iteration + 1}: Processing {len(all_assertion_urns)} assertions in {len(batches)} parallel batches"
        )

        # Process batches in parallel
        batch_deleted_total, batch_refs_deleted_total = process_batches_in_parallel(
            context, batches, delete_assertion_batch, max_workers=len(batches)
        )

        total_deleted += batch_deleted_total
        total_references_deleted += batch_refs_deleted_total

    context.log.info(
        f"Purge complete: deleted {total_deleted} assertions and {total_references_deleted} references"
    )

    return Output(
        [],
        metadata={
            "total_assertions_deleted": total_deleted,
            "total_references_deleted": total_references_deleted,
            "iterations_completed": iteration + 1 if "iteration" in locals() else 0,
        },
    )


class ListPlatformEntitiesConfig(Config):
    status: RemovedStatusFilter = Field(
        RemovedStatusFilter.NOT_SOFT_DELETED,
        description="Filter for the status of entities during search",
    )
    platform: str = Field(
        "",
        description="Platform to filter entities by",
    )
    urns_to_keep: list[str] = Field(
        [],
        description="List of specific URNs to filter entities by. If empty, all entities for the platform will be considered for deletion.",
    )


@asset
@capture_op_exceptions
def datahub__list_entities_to_delete(
    context: OpExecutionContext, config: ListPlatformEntitiesConfig
) -> Output[list[str]]:
    if config.platform == "":
        raise ValueError("Platform must be specified in the configuration.")

    assertion_urns = list(
        datahub_graph_client.get_urns_by_filter(
            status=config.status,
            platform=config.platform,
        )
    )

    valid_urns = []
    for item in assertion_urns:
        dataset_urn = item.split(",")[1]

        if not any(valid_urn in dataset_urn for valid_urn in config.urns_to_keep):
            logger.info(f"Found valid URN for deletion: {item}")
            valid_urns.append(item)

    context.log.info(
        f"Found {len(valid_urns)} valid URNs for platform {config.platform}."
    )

    return Output(
        valid_urns,
        metadata={
            "count": len(valid_urns),
            "preview": assertion_urns[:10],
        },
    )


@asset
@capture_op_exceptions
def datahub__delete_entities(
    context: OpExecutionContext,
    config: DeleteAssertionsConfig,
    datahub__list_entities_to_delete: list[str],
):
    num_of_entities = len(datahub__list_entities_to_delete)

    if config.hard:
        logger.warning("Hard deleting entities...")

        for idx, entity in enumerate(datahub__list_entities_to_delete):
            logger.info(
                f"Hard deleting entity #{idx + 1} out of {num_of_entities}: {entity}"
            )
            datahub_graph_client.hard_delete_entity(urn=entity)

    else:
        logger.warning("Soft deleting entities...")
        for idx, entity in enumerate(datahub__list_entities_to_delete):
            logger.info(
                f"Soft deleting entity #{idx + 1} out of {num_of_entities}: {entity}"
            )
            datahub_graph_client.soft_delete_entity(urn=entity)

    return Output(
        None,
        metadata={
            "count": len(datahub__list_entities_to_delete),
            "preview": datahub__list_entities_to_delete[:10],
        },
    )
