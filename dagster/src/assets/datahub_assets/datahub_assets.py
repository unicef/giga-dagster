import json

from datahub.emitter.rest_emitter import DatahubRestEmitter
from src.settings import settings
from src.utils.datahub.add_platform_metadata import add_platform_metadata
from src.utils.datahub.create_domains import create_domains
from src.utils.datahub.create_tags import create_tags
from src.utils.datahub.datahub_ingest_nb_metadata import NotebookIngestionAction
from src.utils.datahub.ingest_azure_ad import (
    ingest_azure_ad_to_datahub_pipeline,
)
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
