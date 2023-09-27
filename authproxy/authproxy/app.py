from datetime import timedelta

import httpx
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask
from starlette.middleware.sessions import SessionMiddleware

from authproxy.routers import auth
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
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.SECRET_KEY,
    max_age=int(timedelta(days=7).total_seconds()),
    same_site="lax",
    path="/",
)

client = httpx.AsyncClient(base_url="http://dagster-webserver:3002/")

app.include_router(auth.router)

app.mount(
    "/static",
    StaticFiles(directory=settings.BASE_DIR / "authproxy" / "static"),
    name="static",
)


@app.get("/health")
async def health() -> str:
    return "ok"


@app.head("/{path:path}")
@app.options("/{path:path}")
@app.get("/{path:path}")
@app.post("/{path:path}")
async def reverse_proxy(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/auth/login", status_code=status.HTTP_303_SEE_OTHER)

    url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
    upstream_req = client.build_request(
        request.method,
        url,
        headers=request.headers.raw,
        content=request.stream(),
        cookies=request.cookies,
    )
    upstream_res = await client.send(upstream_req, stream=True)
    return StreamingResponse(
        upstream_res.aiter_raw(),
        status_code=upstream_res.status_code,
        headers=upstream_res.headers,
        background=BackgroundTask(upstream_res.aclose),
    )
