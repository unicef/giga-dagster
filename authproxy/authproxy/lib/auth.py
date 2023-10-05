import time

import msal
from loguru import logger


def _is_valid(
    id_token_claims: dict, skew: int | float = None, seconds: int | float = None
) -> bool:
    skew = 210 if skew is None else skew
    now = time.time()
    iat = id_token_claims["iat"]
    logger.debug(f"{now=}, {iat=}, {skew=}")
    return now < skew + (
        id_token_claims["exp"] if seconds is None else id_token_claims["iat"] + seconds
    )


class AzureIdentity(object):
    _TOKEN_CACHE = "_token_cache"
    _AUTH_FLOW = "_auth_flow"
    _USER = "_logged_in_user"

    def __init__(
        self,
        *,
        session,
        authority,
        client_id,
        client_credential=None,
    ):
        self._session = session
        self._authority = authority
        self._client_id = client_id
        self._client_credential = client_credential
        self._http_cache = {}

    def _load_cache(self):
        cache = msal.SerializableTokenCache()
        if self._session.get(self._TOKEN_CACHE):
            cache.deserialize(self._session[self._TOKEN_CACHE])
        return cache

    def _save_cache(self, cache):
        if cache.has_state_changed:
            self._session[self._TOKEN_CACHE] = cache.serialize()

    def _build_msal_app(self, client_credential: str = None, cache=None):
        return msal.PublicClientApplication(
            self._client_id,
            client_credential=client_credential,
            authority=self._authority,
            token_cache=cache,
            http_cache=self._http_cache,
        )

    def _get_user(self):
        id_token_claims = self._session.get(self._USER)
        return (
            id_token_claims
            if id_token_claims is not None and _is_valid(id_token_claims)
            else None
        )

    def log_in(
        self,
        redirect_uri: str,
        scopes: list[str] = None,
        state: str = None,
        prompt: str = None,
    ) -> dict:
        _scopes = scopes or []
        app = self._build_msal_app()
        flow = app.initiate_auth_code_flow(
            _scopes, redirect_uri=redirect_uri, state=state, prompt=prompt
        )
        self._session[self._AUTH_FLOW] = flow
        return {
            "auth_uri": self._session[self._AUTH_FLOW]["auth_uri"],
        }

    def complete_log_in(self, auth_response: dict = None) -> dict:
        auth_flow = self._session.get(self._AUTH_FLOW, {})
        if not auth_flow:
            raise ValueError(
                "The web page with complete_log_in() MUST be visited after the web page with log_in()"
            )
        cache = self._load_cache()
        try:
            result = self._build_msal_app(
                client_credential=self._client_credential,
                cache=cache,
            ).acquire_token_by_auth_code_flow(auth_flow, auth_response)
        except ValueError as e:
            return {"error": "invalid_grant", "error_description": str(e)}

        if "error" in result:
            return result
        self._session[self._USER] = result["id_token_claims"]
        self._save_cache(cache)
        self._session.pop(self._AUTH_FLOW, None)
        return self._get_user()

    def get_user(self) -> dict | None:
        return self._get_user()

    def get_token_for_user(self, scopes: list[str]) -> dict:
        user = self._get_user()
        if not user:
            return {
                "error": "interaction_required",
                "error_description": "Log in required",
            }
        cache = self._load_cache()
        app = self._build_msal_app(
            client_credential=self._client_credential, cache=cache
        )
        accounts = app.get_accounts(username=user.get("preferred_username"))
        if accounts:
            result = app.acquire_token_silent_with_error(scopes, account=accounts[0])
            self._save_cache(cache)
            if result:
                return result
        return {"error": "interaction_required", "error_description": "Cache missed"}

    def log_out(self, homepage: str) -> str:
        self._session.pop(self._USER, None)  # Must
        self._session.pop(self._TOKEN_CACHE, None)  # Optional
        return (
            f"{self._authority}/oauth2/v2.0/logout?post_logout_redirect_uri={homepage}"
        )
