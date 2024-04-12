from msgraph import GraphServiceClient

from azure.identity import ClientSecretCredential
from src.settings import settings

graph_credentials = ClientSecretCredential(
    tenant_id=settings.DATAHUB_OIDC_TENANT_ID,
    client_id=settings.DATAHUB_OIDC_CLIENT_ID,
    client_secret=settings.DATAHUB_OIDC_CLIENT_SECRET,
)

graph_scopes = ["https://graph.microsoft.com/.default"]

graph_endpoint = "https://graph.microsoft.com/v1.0"

graph_client = GraphServiceClient(credentials=graph_credentials, scopes=graph_scopes)
