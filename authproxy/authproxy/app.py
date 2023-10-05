from http import HTTPStatus

import httpx
import identity.web
from loguru import logger
from quart import (
    Quart,
    Response,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from authproxy.settings import settings

app = Quart(
    __name__,
    static_url_path="/static",
    static_folder=settings.BASE_DIR / "authproxy" / "static",
    template_folder=settings.BASE_DIR / "authproxy" / "templates",
)
app.config.update(
    dict(
        SECRET_KEY=settings.SECRET_KEY,
        TEMPLATES_AUTO_RELOAD=True,
    )
)

auth = identity.web.Auth(
    session=session,
    authority=settings.AUTHORITY_URL,
    client_id=settings.AZURE_CLIENT_ID,
)

upstream_rw = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_URL)
upstream_ro = httpx.AsyncClient(base_url=settings.DAGSTER_WEBSERVER_READONLY_URL)


@app.get("/health")
async def health():
    res = dict(proxy="ok", dagster="unhealthy", dagster_readonly="unhealthy")

    try:
        up_rw_res = await upstream_rw.get("/server_info")
        if up_rw_res.is_error:
            raise ConnectionError(up_rw_res.text)
    except Exception as e:
        logger.error(e)
    else:
        res["dagster"] = "ok"

    try:
        up_ro_res = await upstream_ro.get("/server_info")
        if up_ro_res.is_error:
            raise ConnectionError(up_ro_res.text)
    except Exception as e:
        logger.error(e)
        status_code = HTTPStatus.SERVICE_UNAVAILABLE
    else:
        res["dagster_readonly"] = "ok"
        status_code = HTTPStatus.OK

    return make_response(res, status_code)


@app.get("/login")
async def login():
    return await render_template(
        "login.html.j2",
        **auth.log_in(
            scopes=settings.AZURE_AD_SCOPES,
            redirect_uri=url_for("callback", _external=True),
        ),
    )


@app.get("/auth/callback")
async def callback():
    try:
        res = auth.complete_log_in(request.args)
        if "error" in res.keys():
            raise ValueError(res)
    except ValueError as e:
        logger.error(e)
        return redirect(url_for("auth_error"))
    return redirect(url_for("proxy"))


@app.get("/logout")
async def logout():
    return redirect(auth.log_out(url_for("proxy", _external=True)))


@app.get("/error")
async def auth_error():
    return await render_template(
        "error.html.j2",
        **auth.log_in(
            scopes=settings.AZURE_AD_SCOPES,
            redirect_uri=url_for("callback", _external=True),
        ),
    )


@app.get("/unauthorized")
async def unauthorized():
    return await render_template(
        "unauthorized.html.j2",
        logout_url=auth.log_out(url_for("proxy", _external=True)),
    )


@app.route("/<path:path>")
@app.route("/", defaults={"path": ""})
async def proxy(path: str):
    if not (user := auth.get_user()):
        return redirect(url_for("login"))

    email: str = user.get("emails", [""])[0]
    if not (
        (is_tm_email := email.endswith("@thinkingmachin.es"))
        or email.endswith("@unicef.org")
    ):
        return redirect(url_for("unauthorized"))

    upstream = upstream_rw if is_tm_email else upstream_ro

    url = httpx.URL(path=path, query=request.query_string)
    up_req = upstream.build_request(
        request.method,
        url,
        headers=[(k, v) for k, v in request.headers.items()],
        content=await request.get_data(),
        cookies=request.cookies,
    )
    up_res = await upstream.send(up_req)
    res = Response(
        up_res.content,
        status=up_res.status_code,
        headers=dict(up_res.headers),
    )
    app.add_background_task(up_res.aclose)
    return res
