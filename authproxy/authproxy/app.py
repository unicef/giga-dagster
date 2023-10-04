import httpx
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from starlette.background import BackgroundTask

from authproxy.lib.auth import AZURE_AD_SCOPES, get_auth
from authproxy.lib.session import get_request_session, set_response_session
from authproxy.lib.templates import templates
from authproxy.settings import settings

app = FastAPI(title="Dagster", docs_url=None, redoc_url=None)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.ALLOWED_HOSTS,
    www_redirect=False,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = httpx.AsyncClient(base_url="http://dagster-webserver:3002/")

app.mount(
    "/static",
    StaticFiles(directory=settings.BASE_DIR / "authproxy" / "static"),
    name="static",
)

upstream_rw = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_URL)
upstream_ro = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_URL)


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

    try:
        up_ro_res = await upstream_ro.get("/server_info")
        if up_ro_res.is_error:
            raise ConnectionError(up_ro_res.text)
    except Exception as e:
        logger.error(e)
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        res["dagster_readonly"] = "ok"

    return res


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request):
    session = get_request_session(request)
    auth = get_auth(session).log_in(
        scopes=AZURE_AD_SCOPES,
        redirect_uri=settings.AZURE_REDIRECT_URI,
    )
    res = templates.TemplateResponse("login.html.j2", dict(request=request, **auth))
    set_response_session(res, session)
    return res


@app.get("/auth/callback")
async def callback(request: Request):
    session = get_request_session(request)
    auth = get_auth(session).complete_log_in(dict(request.query_params))
    if "error" in auth.keys():
        logger.error(auth)
        return RedirectResponse(request.url_for("auth_error"))
    session.pop("_token_cache")
    res = RedirectResponse("/")
    set_response_session(res, session)
    return res


@app.get("/error", response_class=HTMLResponse)
async def auth_error(request: Request):
    session = get_request_session(request)
    auth = get_auth(session).log_in(
        scopes=AZURE_AD_SCOPES,
        redirect_uri=settings.AZURE_REDIRECT_URI,
    )
    res = templates.TemplateResponse("error.html.j2", dict(request=request, **auth))
    set_response_session(res, session)
    return res


@app.get("/unauthorized", response_class=HTMLResponse)
async def unauthorized(request: Request):
    session = get_request_session(request)
    url = get_auth(session).log_out(settings.AZURE_LOGOUT_REDIRECT_URI)
    res = templates.TemplateResponse(
        "error.html.j2", dict(request=request, logout_url=url)
    )
    set_response_session(res, session)
    return res


@app.get("/logout")
async def logout(request: Request):
    session = get_request_session(request)
    url = get_auth(session).log_out(settings.AZURE_LOGOUT_REDIRECT_URI)
    res = RedirectResponse(url)
    res.delete_cookie(
        "session",
        **settings.SESSION_COOKIE_DELETE_PARAMS,
    )
    return res


@app.head("/{path:path}")
@app.options("/{path:path}")
@app.get("/{path:path}")
@app.post("/{path:path}")
@app.put("/{path:path}")
@app.patch("/{path:path}")
@app.delete("/{path:path}")
async def reverse_proxy(request: Request):
    session = get_request_session(request)
    if not (user := get_auth(session).get_user()):
        return RedirectResponse("/login")

    email: str = user.get("emails", [""])[0]
    if not (
        (is_tm_email := email.endswith("@thinkingmachin.es"))
        or email.endswith("@unicef.org")
    ):
        res = RedirectResponse(request.url_for("unauthorized"))
        res.delete_cookie(
            "session",
            **settings.SESSION_COOKIE_DELETE_PARAMS,
        )
        return res

    upstream = upstream_rw if is_tm_email else upstream_ro

    url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
    up_req = upstream.build_request(
        request.method,
        url,
        headers=request.headers.raw,
        content=request.stream(),
        cookies=request.cookies,
    )
    up_res = await upstream.send(up_req, stream=True)
    res = StreamingResponse(
        up_res.aiter_raw(),
        status_code=up_res.status_code,
        headers=up_res.headers,
        background=BackgroundTask(up_res.aclose),
    )
    set_response_session(res, session)
    return res
