from datahub.ingestion.run.pipeline import Pipeline
from src.settings import settings


def ingest_azure_ad_to_datahub_pipeline():
    # The pipeline configuration is similar to the recipe YAML files provided to the CLI tool.
    pipeline = Pipeline.create(
        {
            "source": {
                "type": "azure-ad",
                "config": {
                    "client_id": f"{settings.AUTH_OIDC_CLIENT_ID}",
                    "tenant_id": f"{settings.AUTH_OIDC_TENANT_ID}",
                    "client_secret": f"{settings.AUTH_OIDC_CLIENT_SECRET}",
                    "redirect": f"{settings.AUTH_OIDC_REDIRECT_URL}",
                    "authority": f"https://login.microsoftonline.com/{settings.AUTH_OIDC_TENANT_ID}",
                    "token_url": f"https://login.microsoftonline.com/{settings.AUTH_OIDC_TENANT_ID}/oauth2/token",
                    "graph_url": "https://graph.microsoft.com/v1.0",
                    "ingest_users": True,
                    "ingest_groups": True,
                },
            },
            "sink": {
                "type": "datahub-rest",
                "config": {
                    "server": f"{settings.DATAHUB_METADATA_SERVER_URL}",
                    "token": f"{settings.DATAHUB_ACCESS_TOKEN}",
                },
            },
        }
    )

    # Run the pipeline and report the results.
    pipeline.run()
    pipeline.pretty_print_summary()
