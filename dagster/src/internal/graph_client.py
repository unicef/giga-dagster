from msgraph import GraphServiceClient

from azure.identity import ClientSecretCredential
from src.settings import settings

graph_credentials = ClientSecretCredential(
    tenant_id=settings.AAD_B2C_TENANT_ID,
    client_id=settings.AAD_B2C_CLIENT_ID,
    client_secret=settings.AAD_B2C_CLIENT_SECRET,
)

graph_scopes = ["https://graph.microsoft.com/.default"]

graph_endpoint = "https://graph.microsoft.com/v1.0"

graph_client = GraphServiceClient(credentials=graph_credentials, scopes=graph_scopes)
