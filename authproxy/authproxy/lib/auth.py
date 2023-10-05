import identity.web

from authproxy.settings import settings

AUTHORITY_URL = f"https://{settings.AZURE_TENANT_NAME}.b2clogin.com/{settings.AZURE_TENANT_NAME}.onmicrosoft.com/{settings.AZURE_AUTH_POLICY_NAME}"

AZURE_AD_SCOPES = [
    f"https://{settings.AZURE_TENANT_NAME}.onmicrosoft.com/{settings.AZURE_CLIENT_ID}/User.Impersonate"
]


def get_auth(session):
    return identity.web.Auth(
        session=session,
        authority=AUTHORITY_URL,
        client_id=settings.AZURE_CLIENT_ID,
        client_credential=settings.AZURE_CLIENT_SECRET,
    )
