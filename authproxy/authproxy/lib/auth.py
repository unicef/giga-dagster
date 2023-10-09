import identity.web

from authproxy.settings import settings


def get_auth(session: dict):
    return identity.web.Auth(
        session=session,
        authority=settings.AUTHORITY_URL,
        client_id=settings.AZURE_CLIENT_ID,
        client_credential=settings.AZURE_CLIENT_SECRET,
    )
