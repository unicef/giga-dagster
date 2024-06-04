import json

from datahub.emitter.rest_emitter import DatahubRestEmitter
from src.settings import settings
from src.utils.datahub.add_platform_metadata import add_platform_metadata
from src.utils.datahub.create_domains import create_domains
from src.utils.datahub.create_tags import create_tags
from src.utils.datahub.datahub_ingest_nb_metadata import NotebookIngestionAction
from src.utils.datahub.graphql import datahub_graph_client as emitter
from src.utils.datahub.ingest_azure_ad import (
    ingest_azure_ad_to_datahub_pipeline,
)
from src.utils.datahub.list_datasets import list_datasets_by_filter
from src.utils.datahub.update_policies import update_policies
from src.utils.github_api_calls import list_ipynb_from_github_repo

from dagster import OpExecutionContext, Output, asset


@asset
def datahub_test_connection(context: OpExecutionContext):
    context.log.info(f"Using {settings.DATAHUB_METADATA_SERVER_URL=}")
    emitter = DatahubRestEmitter(
        gms_server=settings.DATAHUB_METADATA_SERVER_URL,
        token=settings.DATAHUB_ACCESS_TOKEN,
        retry_max_times=5,
        retry_status_codes=[
            403,
            429,
            500,
            502,
            503,
            504,
        ],
    )
    context.log.info(json.dumps(emitter.test_connection(), indent=2))


@asset
def datahub_domains(context: OpExecutionContext):
    context.log.info("CREATING DOMAINS IN DATAHUB")
    domains = create_domains()
    context.log.info(f"Domains created: {domains}")
    yield Output(None)


@asset
def datahub_tags(context: OpExecutionContext):
    context.log.info("CREATING TAGS IN DATAHUB")
    create_tags(context)
    yield Output(None)


@asset
def azure_ad_users_groups(context: OpExecutionContext):
    context.log.info("INGESTING AZURE AD USERS AND GROUPS TO DATAHUB")
    ingest_azure_ad_to_datahub_pipeline()
    yield Output(None)


@asset(deps=[azure_ad_users_groups])
def datahub_policies(context: OpExecutionContext):
    context.log.info("UPDATING POLICIES IN DATAHUB")
    update_policies(context)
    yield Output(None)


@asset
def github_coverage_workflow_notebooks(context: OpExecutionContext):
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
    yield Output(None)


@asset
def datahub_platform_metadata(context: OpExecutionContext):
    context.log.info("ADDING PLATFORM METADATA IN DATAHUB...")
    context.log.info("Delta Lake")
    add_platform_metadata(
        platform="deltaLake",
        display_name="Delta Lake",
        logo_url="https://delta.io/static/3bd8fea55ff57287371f4714232cd4ef/ac8f8/delta-lake-logo.webp",
        filepath_delimiter="/",
    )
    context.log.info("PLATFORM METADATA ADDED TO DATAHUB")
    yield Output(None)


@asset
def list_qos_datasets_to_delete(context: OpExecutionContext):
    context.log.info("LISTING QOS DATASETS FROM DATAHUB...")
    qos_list = list_datasets_by_filter("qos")
    context.log.info(qos_list)
    yield Output(qos_list, metadata={"qos_datasets_count": len(qos_list)})


@asset
def soft_delete_qos_datasets(
    context: OpExecutionContext,
    list_qos_datasets_to_delete: list[str],
):
    context.log.warn("SOFT DELETING QOS DATASETS FROM DATAHUB...")
    count = len(list_qos_datasets_to_delete)
    i = 0
    for qos_dataset in list_qos_datasets_to_delete:
        emitter.soft_delete_entity(qos_dataset)
        i += 1
        context.log.info(f"SOFT DELETED {i} of {count}: {qos_dataset}")

    context.log.info(f"SOFT DELETED {count} QOS DATASETS FROM DATAHUB.")
    yield Output(None, metadata={"qos_datasets_soft_deleted_count": count})


@asset
def hard_delete_qos_datasets(
    context: OpExecutionContext,
    list_qos_datasets_to_delete: list[str],
):
    context.log.warn("HARD DELETING QOS DATASETS FROM DATAHUB...")
    count = len(list_qos_datasets_to_delete)
    i = 0
    for qos_dataset in list_qos_datasets_to_delete:
        emitter.hard_delete_entity(qos_dataset)
        i += 1
        context.log.info(f"HARD DELETED {i} of {count}: {qos_dataset}")

    context.log.info(f"HARD DELETED {count} QOS DATASETS FROM DATAHUB.")
    yield Output(None, metadata={"qos_datasets_hard_deleted_count": count})
