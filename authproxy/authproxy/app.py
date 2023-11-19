from datetime import timedelta

import httpx
import sentry_sdk
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from loguru import logger
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware

from authproxy.lib.auth import get_auth
from authproxy.lib.templates import templates
from authproxy.settings import settings

if settings.IN_PRODUCTION and settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        traces_sample_rate=1.0,
        profiles_sample_rate=1.0,
        environment=settings.PYTHON_ENV,
    )

app = FastAPI(title="Giga Dagster", docs_url=None, redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    SessionMiddleware,
    secret_key=settings.SECRET_KEY,
    session_cookie="session",
    max_age=int(timedelta(days=7).total_seconds()),
    path="/",
    same_site="strict",
    https_only=settings.IN_PRODUCTION,
)

app.mount(
    "/static",
    StaticFiles(directory=settings.BASE_DIR / "authproxy" / "static"),
    name="static",
)

upstream_rw = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_URL)
upstream_ro = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_READONLY_URL)


@app.get("/health")
async def health(response: Response):
    res = dict(proxy="ok", dagster="unhealthy", dagster_readonly="unhealthy")

    try:
        up_rw_res = await upstream_rw.get("/server_info")
        if up_rw_res.is_error:
            raise ConnectionError(up_rw_res.text)
    except Exception as e:
        logger.error(e)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        res["dagster"] = "ok"
        response.status_code = status.HTTP_200_OK

    try:
        up_ro_res = await upstream_ro.get("/server_info")
        if up_ro_res.is_error:
            raise ConnectionError(up_ro_res.text)
    except Exception as e:
        logger.error(e)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        res["dagster_readonly"] = "ok"
        response.status_code = status.HTTP_200_OK

    return res


@app.get("/favicon.ico", response_class=FileResponse)
async def favicon():
    return settings.BASE_DIR / "authproxy" / "static" / "dagster.svg"


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request):
    auth = get_auth(request.session)
    return templates.TemplateResponse(
        "login.html.j2",
        context={
            "request": request,
            **auth.log_in(
                scopes=settings.AZURE_AD_SCOPES,
                redirect_uri=settings.AZURE_REDIRECT_URI,
            ),
        },
    )


@app.get("/auth/callback", response_class=RedirectResponse)
async def callback(request: Request):
    auth = get_auth(request.session)
    try:
        res = auth.complete_log_in(dict(request.query_params))
        if "error" in res.keys():
            raise ValueError(res)
    except ValueError as e:
        logger.error(e)
        return "/error"
    request.session.pop("_token_cache")
    return "/"


@app.get("/logout", response_class=RedirectResponse)
async def logout(request: Request):
    auth = get_auth(request.session)
    return auth.log_out(settings.AZURE_LOGOUT_REDIRECT_URI)


@app.get("/error")
async def auth_error(request: Request):
    auth = get_auth(request.session)
    return templates.TemplateResponse(
        "error.html.j2",
        context={
            "request": request,
            **auth.log_in(
                scopes=settings.AZURE_AD_SCOPES,
                redirect_uri=settings.AZURE_REDIRECT_URI,
            ),
        },
    )


@app.get("/unauthorized")
async def unauthorized(request: Request):
    auth = get_auth(request.session)
    return templates.TemplateResponse(
        "unauthorized.html.j2",
        context={
            "request": request,
            "logout_url": auth.log_out(settings.AZURE_LOGOUT_REDIRECT_URI),
        },
    )


@app.get("/{path:path}", name="proxy")
@app.post("/{path:path}")
@app.put("/{path:path}")
@app.patch("/{path:path}")
@app.delete("/{path:path}")
@app.head("/{path:path}")
@app.options("/{path:path}")
async def proxy(path: str, request: Request):
    auth = get_auth(request.session)
    if not (user := auth.get_user()):
        if path.startswith("favicon") or path.startswith("static"):
            return Response("", status_code=status.HTTP_401_UNAUTHORIZED)
        return RedirectResponse("/login")

    email: str = user.get("preferred_username")
    if (email_domain := email.split("@")[-1]) not in settings.EMAIL_DOMAIN_ALLOWLIST:
        return RedirectResponse("/unauthorized")

    is_tm_user = email_domain == "thinkingmachin.es"
    upstream = upstream_rw if is_tm_user else upstream_ro

    url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
    up_req = upstream.build_request(
        request.method,
        url,
        headers=request.headers.raw,
        content=request.stream(),
        cookies=request.cookies,
    )
    up_res = await upstream.send(up_req, stream=True)
    return StreamingResponse(
        up_res.aiter_raw(),
        status_code=up_res.status_code,
        headers=up_res.headers,
        background=BackgroundTask(up_res.aclose),
    )
