import httpx
from fastapi import FastAPI, Request, status
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

app = FastAPI(
    title="Dagster",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)
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

upstream = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_URL)


@app.get("/health")
async def health():
    up_res = await upstream.get("/server_info")
    if up_res.status_code == 200:
        return dict(proxy="ok", dagster="ok")
    return dict(proxy="ok", dagster="unhealthy")


@app.get("/login", include_in_schema=False, response_class=HTMLResponse)
async def login(request: Request):
    session = get_request_session(request)
    auth = get_auth(session).log_in(
        scopes=AZURE_AD_SCOPES,
        redirect_uri=settings.AZURE_REDIRECT_URI,
    )
    res = templates.TemplateResponse("login.html.j2", dict(request=request, **auth))
    set_response_session(res, session)
    return res


@app.get("/error", include_in_schema=False, response_class=HTMLResponse)
async def auth_error(request: Request):
    session = get_request_session(request)
    auth = get_auth(session).log_in(
        scopes=AZURE_AD_SCOPES,
        redirect_uri=settings.AZURE_REDIRECT_URI,
    )
    res = templates.TemplateResponse("error.html.j2", dict(request=request, **auth))
    set_response_session(res, session)
    return res


@app.get("/unauthorized", include_in_schema=False, response_class=HTMLResponse)
async def unauthorized(request: Request):
    session = get_request_session(request)
    url = get_auth(session).log_out(settings.AZURE_LOGOUT_REDIRECT_URI)
    res = templates.TemplateResponse(
        "error.html.j2", dict(request=request, logout_url=url)
    )
    set_response_session(res, session)
    return res


@app.get("/auth/callback", include_in_schema=False)
async def callback(request: Request):
    session = get_request_session(request)
    auth = get_auth(session).complete_log_in(dict(request.query_params))
    if "error" in auth.keys():
        logger.error(auth)
        return RedirectResponse(request.url_for("auth_error"))
    session.pop("_token_cache")
    res = RedirectResponse(
        request.url_for("reverse_proxy"),
        status_code=status.HTTP_302_FOUND,
    )
    set_response_session(res, session)
    return res


@app.get("/logout", include_in_schema=False)
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
@app.get("/{path:path}", name="reverse_proxy")
@app.post("/{path:path}")
@app.put("/{path:path}")
@app.patch("/{path:path}")
@app.delete("/{path:path}")
async def reverse_proxy(request: Request):
    session = get_request_session(request)
    if not (user := get_auth(session).get_user()):
        return RedirectResponse(request.url_for("login"))

    email: str = user.get("emails", [""])[0]
    if not (email.endswith("@thinkingmachin.es") or email.endswith("@unicef.org")):
        res = RedirectResponse(request.url_for("unauthorized"))
        res.delete_cookie(
            "session",
            **settings.SESSION_COOKIE_DELETE_PARAMS,
        )
        return res

    url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
    upstream_req = upstream.build_request(
        request.method,
        url,
        headers=request.headers.raw,
        content=request.stream(),
        cookies=request.cookies,
    )
    upstream_res = await upstream.send(upstream_req, stream=True)
    res = StreamingResponse(
        upstream_res.aiter_raw(),
        status_code=upstream_res.status_code,
        headers=upstream_res.headers,
        background=BackgroundTask(upstream_res.aclose),
    )
    set_response_session(res, session)
    return res
