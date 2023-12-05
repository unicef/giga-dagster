from dagster import OpExecutionContext, Output, asset
from src.utils.datahub.datahub_create_domains import create_domains
from src.utils.datahub.datahub_ingest_azure_ad import (
    ingest_azure_ad_to_datahub_pipeline,
)


@asset
def datahub_domains(context: OpExecutionContext):
    context.log.info("CREATING DOMAINS IN DATAHUB")
    domains = create_domains()
    context.log.info(f"Domains created: {domains}")
    yield Output(None)


@asset
def azure_ad_users_groups(context: OpExecutionContext):
    context.log.info("INGESTING AZURE AD USERS AND GROUPS TO DATAHUB")
    ingest_azure_ad_to_datahub_pipeline()
    yield Output(None)
