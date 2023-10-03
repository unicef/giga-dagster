from fastapi import APIRouter, Request, status
from fastapi.responses import RedirectResponse

from authproxy.lib.auth import AZURE_AD_SCOPES, get_auth
from authproxy.lib.templates import templates
from authproxy.settings import settings

router = APIRouter(prefix="/auth")


@router.get("/login", include_in_schema=False, response_class=RedirectResponse)
async def login(request: Request):
    auth = get_auth(request.session).log_in(
        scopes=AZURE_AD_SCOPES,
        redirect_uri=settings.AZURE_REDIRECT_URI,
    )
    return auth["auth_uri"]


@router.get(
    "/callback",
    include_in_schema=False,
    response_class=RedirectResponse,
    status_code=status.HTTP_302_FOUND,
)
async def callback(request: Request):
    auth = get_auth(request.session).complete_log_in(dict(request.query_params))
    if "error" in auth.keys():
        return templates.TemplateResponse(
            "login.html.j2", {"request": request, "error": True}
        )
    request.session.pop("_token_cache")
    return "/"


@router.get("/logout", include_in_schema=False, response_class=RedirectResponse)
async def logout(request: Request):
    url = get_auth(request.session).log_out(settings.AZURE_LOGOUT_REDIRECT_URI)
    request.session.clear()
    return url
