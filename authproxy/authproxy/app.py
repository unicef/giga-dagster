from datetime import timedelta

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware

from authproxy.settings import settings

app = FastAPI(
    title="Ingestion Portal",
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
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.SECRET_KEY,
    session_cookie="session",
    max_age=int(timedelta(days=7).total_seconds()),
    same_site="lax",
)

client = httpx.AsyncClient(base_url="http://dagster-webserver:3002/")

app.mount(
    "/static",
    StaticFiles(directory=settings.BASE_DIR / "authproxy" / "static"),
    name="static",
)

templates = Jinja2Templates(directory=settings.BASE_DIR / "authproxy" / "templates")


@app.get("/health")
async def health():
    return PlainTextResponse("ok")


@app.get("/login", response_class=HTMLResponse)
async def login(request: Request):
    return templates.TemplateResponse(
        "login.html.j2",
        {"request": request},
    )


async def reverse_proxy(request: Request):
    url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
    upstream_req = client.build_request(
        request.method,
        url,
        headers=request.headers.raw,
        content=request.stream(),
    )
    upstream_res = await client.send(upstream_req, stream=True)
    return StreamingResponse(
        upstream_res.aiter_raw(),
        status_code=upstream_res.status_code,
        headers=upstream_res.headers,
        background=BackgroundTask(upstream_res.aclose),
    )


app.add_route(
    "/{path:path}",
    reverse_proxy,
    ["GET", "POST"],
)
