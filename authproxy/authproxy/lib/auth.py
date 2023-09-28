import msal

from authproxy.settings import settings

AUTHORITY_URL = f"https://{settings.AZURE_TENANT_NAME}.b2clogin.com/{settings.AZURE_TENANT_NAME}.onmicrosoft.com/{settings.AZURE_AUTH_POLICY_NAME}"

AZURE_AD_SCOPES = [
    f"https://{settings.AZURE_TENANT_NAME}.onmicrosoft.com/{settings.AZURE_CLIENT_ID}/User.Impersonate"
]


# MSAL Reference:
# https://learn.microsoft.com/en-us/azure/active-directory-b2c/enable-authentication-python-web-app?tabs=windows


def build_msal_app(cache=None):
    return msal.ConfidentialClientApplication(
        settings.AZURE_CLIENT_ID,
        client_credential=settings.AZURE_CLIENT_SECRET,
        authority=AUTHORITY_URL,
        token_cache=cache,
    )


def build_auth_code_flow():
    return build_msal_app().initiate_auth_code_flow(
        scopes=AZURE_AD_SCOPES,
        redirect_uri=settings.AZURE_REDIRECT_URI,
    )


def load_cache(session):
    cache = msal.SerializableTokenCache()
    if session.get("token_cache"):
        cache.deserialize(session["token_cache"])
    return cache


def save_cache(session, cache: msal.SerializableTokenCache):
    if cache.has_state_changed:
        session["token_cache"] = cache.serialize()


def get_token_from_cache(session):
    cache = load_cache(session)
    cca = build_msal_app(cache)
    accounts = cca.get_accounts()
    if accounts:
        result = cca.acquire_token_silent(scopes=AZURE_AD_SCOPES, account=accounts[0])
        save_cache(session, cache)
        return result
