from src.utils.datahub.create_domains import create_domains
from src.utils.datahub.create_tags import create_tags
from src.utils.datahub.datahub_ingest_nb_metadata import NotebookIngestionAction
from src.utils.datahub.ingest_azure_ad import (
    ingest_azure_ad_to_datahub_pipeline,
)
from src.utils.datahub.datahub_ingest_nb_metadata import NotebookIngestionAction
from src.utils.datahub.update_policies import update_policies
from src.utils.github_api_calls import list_ipynb_from_github_repo

from dagster import OpExecutionContext, Output, asset


@asset
def datahub_domains(context: OpExecutionContext):
    context.log.info("CREATING DOMAINS IN DATAHUB")
    domains = create_domains()
    context.log.info(f"Domains created: {domains}")
    yield Output(None)


@asset
def datahub_tags(context: OpExecutionContext):
    context.log.info("CREATING TAGS IN DATAHUB")
    create_tags()
    yield Output(None)


@asset
def azure_ad_users_groups(context: OpExecutionContext):
    context.log.info("INGESTING AZURE AD USERS AND GROUPS TO DATAHUB")
    ingest_azure_ad_to_datahub_pipeline()
    yield Output(None)


@asset(deps=[azure_ad_users_groups])
def datahub_policies(context: OpExecutionContext):
    context.log.info("UPDATING POLICIES IN DATAHUB")
    update_policies()
    yield Output(None)


@asset
def coverage_workflow_notebooks(context: OpExecutionContext):
    context.log.info("INGESTING COVERAGE WORKFLOW NOTEBOOKS TO DATAHUB")

    owner = "unicef"
    repo = "coverage_workflow"
    path = "Notebooks/"
    notebook_metadata_list = list_ipynb_from_github_repo(owner, repo, path)

    for notebook_metadata in notebook_metadata_list:
        run_notebook_ingestion = NotebookIngestionAction(
            notebook_metadata=notebook_metadata
        )
        run_notebook_ingestion()
    yield Output(None)
