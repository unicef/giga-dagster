from fastapi import APIRouter, Request, Response, status
from fastapi.responses import RedirectResponse

from authproxy.lib.auth import (
    AUTHORITY_URL,
    build_auth_code_flow,
    build_msal_app,
    load_cache,
    save_cache,
)
from authproxy.lib.templates import templates

router = APIRouter(prefix="/auth")


@router.get("/login", include_in_schema=False)
async def login(request: Request, response: Response):
    flow = build_auth_code_flow()
    request.session["flow"] = flow
    return RedirectResponse(flow["auth_uri"], status_code=status.HTTP_303_SEE_OTHER)


@router.get("/callback", include_in_schema=False)
async def callback(request: Request):
    try:
        cache = load_cache(request.session)
        result = build_msal_app(cache).acquire_token_by_auth_code_flow(
            request.session.get("flow", {}), dict(request.query_params)
        )
        if "error" in result.keys():
            return templates.TemplateResponse("error.html", {"request": request})
        request.session["user"] = result.get("id_token_claims")
        save_cache(request.session, cache)
    except ValueError:
        pass
    return RedirectResponse("/", status_code=status.HTTP_302_FOUND)


@router.get("/logout", include_in_schema=False)
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(
        f"{AUTHORITY_URL}/oauth2/v2.0/logout?post_logout_redirect_uri={request.url_for('reverse_proxy')}",
        status_code=status.HTTP_303_SEE_OTHER,
    )
